from ccdexplorer_fundamentals.tooter import Tooter, TooterType, TooterChannel
from ccdexplorer_fundamentals.GRPCClient import GRPCClient
from ccdexplorer_fundamentals.mongodb import (
    MongoDB,
    Collections,
    MongoTypePaydayAPYIntermediate,
    MongoImpactedAddress,
    AccountStatementEntryType,
    MongoTypeAccountReward,
)
from pymongo.collection import Collection
from pymongo import ReplaceOne
from ccdexplorer_fundamentals.GRPCClient.CCD_Types import (
    CCD_AccountInfo,
    CCD_PoolInfo,
    CCD_BlockHash,
    CCD_BakerId,
    CCD_AccountAddress,
    CCD_DelegatorRewardPeriodInfo,
    CCD_BlockSpecialEvent_PaydayAccountReward,
    CCD_BlockSpecialEvent_PaydayPoolReward,
)
import datetime as dt
import dateutil.parser
import math
import sys
import time
from typing import Dict
from rich.progress import track
from rich.console import Console

console = Console()
from env import *


def calc_apy_for_period(daily_apy: list) -> float:
    daily_ln = [math.log(1 + x) for x in daily_apy]
    avg_ln = sum(daily_ln) / len(daily_ln)
    expp = math.exp(avg_ln)
    apy = expp - 1
    return apy


class Payday:
    """
    Class Payday is the class that calculates and stores all payday related information.
    It should be called with the date_string (ex. "2022-12-30") and blockHeight of the
    block that contains the rewards.

    Process:
    1. Create PaydayInformation entry and store in collection_paydays
    2. From PaydayInformation, property `bakerAccountIds` (or `bakersWithDelegators`), get list of
    all bakers that have participated in this payday. Call `process_payday_performance_for_baker`,
    which stores an entry for every baker in collection_paydays_performance.
    3. Loop through all RewardEvents and call `process_payday_rewards_for_account_or_baker`,
    which stores an entry for every reward in collection_paydays_rewards.
    4. Call `fill_apy_intermediate_for_accounts_for_date` to calculate daily APY figures
    for all accounts that need calculation (this includes all delegators and all baker accounts).
    5. Call `fill_apy_intermediate_for_bakers_for_date` to calculate daily APY figures
    for all bakers that have participated in this payday).
    6. From intermediate results, calculate the averages through ...
    """

    def __init__(
        self,
        payday_date_string: str,
        payday_block_hash: CCD_BlockHash,
        grpcclient: GRPCClient,
        mongodb: MongoDB,
        tooter: Tooter,
        TESTNET: bool = False,
    ):
        self.mongodb = mongodb
        self.TESTNET = TESTNET
        self.payday_block_hash = payday_block_hash
        self.db: Dict[Collections, Collection] = (
            self.mongodb.mainnet if not self.TESTNET else self.mongodb.testnet
        )
        self.grpcclient = grpcclient
        self.tooter = tooter

        # current payday information
        self.payday_block_info = self.grpcclient.get_block_info(self.payday_block_hash)
        self.payday_date_string = payday_date_string

        self.special_events_with_rewards = self.grpcclient.get_block_special_events(
            self.payday_block_info.hash
        )

        # current payday information first block
        self.previous_payday = self.get_previous_payday_information_entry(
            self.payday_date_string
        )
        self.height_first = (
            self.previous_payday["height_for_last_block"] + 1
            if self.previous_payday
            else 3_232_445
        )
        _hash: CCD_BlockHash = self.grpcclient.get_blocks_at_height(self.height_first)[
            0
        ]
        self.payday_block_info_first_block = self.grpcclient.get_block_info(_hash)

        # current payday information last block
        self.height_for_pool_status = self.payday_block_info.height - 1
        _hash: CCD_BlockHash = self.grpcclient.get_blocks_at_height(
            self.height_for_pool_status
        )[0]
        self.payday_block_info_last_block = self.grpcclient.get_block_info(_hash)

        # duration is measured from the slot_time of the last block from
        # previous Reward period until slot_time from the last block in this
        # Reward period
        _height_start_duration = (
            self.previous_payday["height_for_last_block"]
            if self.previous_payday
            else 3_232_444
        )
        _hash_start_duration = self.grpcclient.get_blocks_at_height(
            _height_start_duration
        )[
            0
        ]  # type:ignore
        block_start_duration = self.grpcclient.get_block_info(_hash_start_duration)
        self.payday_duration = (
            self.payday_block_info_last_block.slot_time - block_start_duration.slot_time
        ).total_seconds()

        self.seconds_per_year = 3_153_6000

        console.log(self.payday_date_string)
        try:
            self.tooter.send(
                channel=TooterChannel.NOTIFIER,
                message=f"(Payday: {self.payday_date_string}) \nStart.",
                notifier_type=TooterType.INFO,
            )
        except:
            console.log("Step 0, can't toot.")
        start_time = dt.datetime.now()
        # step 1
        self.create_and_save_payday_information_entry()
        self.get_accounts_and_bakers_for_APY_calc()
        # # step 2
        self.process_payday_performance_for_bakers()
        # # step 3
        self.process_payday_rewards_for_account_or_baker()
        # # step 4
        self.fill_apy_intermediate_for_accounts_for_date()
        # # step 5
        self.fill_apy_intermediate_for_bakers_for_date()
        # # step 6
        self.calc_moving_averages()
        # done
        console.log(
            f"{self.payday_date_string} | {(dt.datetime.now() - start_time).total_seconds():,.0f} sec"
        )

    def get_accounts_and_bakers_for_APY_calc(self):
        """
        This method determines for which accounts and baker_ids we need
        to calculate APY.
        """

        list_of_lists_of_delegators = [
            x for x in self.bakers_with_delegation_information.values()
        ]
        flat_list_of_delegators = [
            item for sublist in list_of_lists_of_delegators for item in sublist
        ]
        self.list_of_delegators = [x.account for x in flat_list_of_delegators]
        # dict of all delegators accounts...
        self.account_with_stake_by_account_id = {
            x.account: x.stake for x in flat_list_of_delegators
        }

        # add the account_ids of bakers...
        for account_id, pool_info in self.pool_info_by_account_id.items():
            self.account_with_stake_by_account_id[account_id] = (
                pool_info.current_payday_info.baker_equity_capital
            )

        baker_account_ids = self.baker_account_ids_by_baker_id.values()

        self.accounts_that_need_APY = list(
            set(self.list_of_delegators) | set(baker_account_ids)
        )
        self.bakers_that_need_APY = list(self.bakers_with_delegation_information.keys())

    def get_previous_payday_information_entry(self, payday_date_string: str):
        payday_date = dateutil.parser.parse(payday_date_string)
        previous_payday_date = payday_date - dt.timedelta(days=1)
        previous_payday_date_string = f"{previous_payday_date:%Y-%m-%d}"

        data = self.db[Collections.paydays].find({})
        result = list(data)
        all_paydays = {x["date"]: x for x in result}

        if previous_payday_date_string in all_paydays.keys():
            return all_paydays[previous_payday_date_string]
        else:
            # self.tooter.send(channel=TooterChannel.NOTIFIER, message=f'(Payday: {payday_date_string}): Cannot find this date in collection_paydays', notifier_type=TooterType.INFO)
            return None

    def get_expected_blocks_per_day(self, lp):
        slots_in_day = 14400 * 24
        return slots_in_day * (1.0 - (1 - 1 / 40) ** (lp))

    def reverse_search_from_dictionary(self, dictionary, keyword):
        for key, values in dictionary.items():
            if keyword in values:
                return key

    def retrieve_state_information_for_current_payday(self):
        """
        State information for the current payday from the last block in the payday.
        """

        # bakers for this payday
        last_hash = self.payday_block_info_last_block.hash
        first_hash = self.payday_block_info_first_block.hash

        self.bakers_in_block = self.grpcclient.get_election_info(
            last_hash
        ).baker_election_info

        # needed for current payday information to show pools at /staking
        self.bakers_in_block_current_payday = self.grpcclient.get_election_info(
            self.payday_block_hash
        ).baker_election_info

        self.baker_account_ids_by_baker_id: Dict[str, CCD_AccountAddress] = {}
        self.baker_account_ids_by_account_id: Dict[str, CCD_BakerId] = {}
        self.bakers_with_delegation_information: Dict[
            str, list[CCD_DelegatorRewardPeriodInfo]
        ] = {}
        self.bakers_with_delegation_information_current_payday: Dict[
            str, list[CCD_DelegatorRewardPeriodInfo]
        ] = {}
        self.pool_info_by_baker_id: Dict[str, CCD_PoolInfo] = {}
        self.pool_info_by_baker_id_current_payday: Dict[str, CCD_PoolInfo] = {}
        self.pool_info_by_account_id: Dict[str, CCD_PoolInfo] = {}

        self.account_info_by_baker_id: Dict[str, CCD_AccountInfo] = {}
        self.account_info_by_account_id: Dict[str, CCD_AccountInfo] = {}

        self.pool_status_dict: Dict[str, list] = {}
        self.pool_status_dict_current_payday: Dict[str, list] = {}
        for election_info_baker in track(self.bakers_in_block):
            baker_id = election_info_baker.baker
            account_info = self.grpcclient.get_account_info(
                last_hash, account_index=baker_id
            )
            self.account_info_by_baker_id[str(baker_id)] = account_info
            self.account_info_by_account_id[account_info.address] = account_info

            # future me: this needs to be collected from the last_hash,
            # as we are using this to collect the actually baked blocks
            # in a payday (in baker-tally).
            pool_info_for_baker = self.grpcclient.get_pool_info_for_pool(
                baker_id, last_hash
            )

            self.pool_info_by_baker_id[str(baker_id)] = pool_info_for_baker
            self.pool_info_by_account_id[account_info.address] = pool_info_for_baker

            # lookup mappings from acount_id <---> baker_id
            self.baker_account_ids_by_baker_id[str(baker_id)] = (
                pool_info_for_baker.address
            )
            self.baker_account_ids_by_account_id[pool_info_for_baker.address] = baker_id

            # contains delegators with info
            self.bakers_with_delegation_information[str(baker_id)] = (
                self.grpcclient.get_delegators_for_pool_in_reward_period(
                    baker_id, self.payday_block_info_last_block.hash
                )
            )

            # add dictionary with payday pool status for each baker/pool
            current_baker_pool_status = pool_info_for_baker.pool_info.open_status

            if current_baker_pool_status in self.pool_status_dict.keys():
                self.pool_status_dict[current_baker_pool_status].append(baker_id)
            else:
                self.pool_status_dict[current_baker_pool_status] = [baker_id]

        # needed for current payday information to show pools at /staking
        for election_info_baker in track(self.bakers_in_block_current_payday):
            baker_id = election_info_baker.baker

            # future me: this needs to be collected from the payday_block_hash,
            # as we are using this to display the current payday information
            pool_info_for_baker_current_payday = self.grpcclient.get_pool_info_for_pool(
                baker_id, self.payday_block_hash
            )

            self.pool_info_by_baker_id_current_payday[str(baker_id)] = (
                pool_info_for_baker_current_payday
            )

            # contains delegators with info
            self.bakers_with_delegation_information_current_payday[str(baker_id)] = (
                self.grpcclient.get_delegators_for_pool_in_reward_period(
                    baker_id, self.payday_block_hash
                )
            )

            # add dictionary with payday pool status for each baker/pool
            current_baker_pool_status = (
                pool_info_for_baker_current_payday.pool_info.open_status
            )

            if current_baker_pool_status in self.pool_status_dict_current_payday.keys():
                self.pool_status_dict_current_payday[current_baker_pool_status].append(
                    baker_id
                )
            else:
                self.pool_status_dict_current_payday[current_baker_pool_status] = [
                    baker_id
                ]

        # add passive delegators
        self.bakers_with_delegation_information["passive_delegation"] = (
            self.grpcclient.get_delegators_for_passive_delegation_in_reward_period(
                last_hash
            )
        )

        self.passive_delegation_info = self.grpcclient.get_passive_delegation_info(
            last_hash
        )

        # for saving to payday collection
        self.bakers_with_delegation_information_mongo = {}
        for k, v in self.bakers_with_delegation_information.items():
            save_list = []
            for info in v:
                save_list.append(info.model_dump(exclude_none=True))

            self.bakers_with_delegation_information_mongo[k] = save_list

        self.bakers_with_delegation_information_mongo_current_payday = {}
        for k, v in self.bakers_with_delegation_information_current_payday.items():
            save_list = []
            for info in v:
                save_list.append(info.model_dump(exclude_none=True))

            self.bakers_with_delegation_information_mongo_current_payday[k] = save_list

    # step 1
    def create_and_save_payday_information_entry(self):
        """
        If the payday is triggered on block 3_232_445, it's the start of the very first payday.
        In that case, we need to retrieve stake information and store in the collection.
        For any future payday, we can (hopefully) read the info back, as we store it during
        processing of the previous payday. If we can't read it, we still have to retrieve it.
        """
        console.log("Step 1: create_and_save_payday_information_entry")

        self.retrieve_state_information_for_current_payday()
        payday_information_entry = {
            "_id": self.payday_block_info.hash,
            "date": self.payday_date_string,
            "height_for_first_block": self.payday_block_info_first_block.height,
            "height_for_last_block": self.payday_block_info_last_block.height,
            "hash_for_first_block": self.payday_block_info_first_block.hash,
            "hash_for_last_block": self.payday_block_info_last_block.hash,
            "payday_duration_in_seconds": self.payday_duration,
            "payday_block_slot_time": self.payday_block_info.slot_time,
            "bakers_with_delegation_information": self.bakers_with_delegation_information_mongo,
            "baker_account_ids": self.baker_account_ids_by_baker_id,
            "pool_status_for_bakers": self.pool_status_dict,
        }
        self.payday_information = payday_information_entry
        try:
            query = {"_id": self.payday_block_info.hash}
            self.db[Collections.paydays].replace_one(
                query, payday_information_entry, upsert=True
            )
            try:
                self.tooter.send(
                    channel=TooterChannel.NOTIFIER,
                    message=f"(Payday: {self.payday_date_string}) \nStep 1: create_and_save_payday_information_entry...done.",
                    notifier_type=TooterType.INFO,
                )
            except:
                console.log("Step 1, can't toot.")

        except Exception as e:
            console.log(e)

    # step 2
    def process_payday_performance_for_bakers(self):
        console.log("Step 2: process_payday_performance_for_bakers")
        estimated_blocks_per_day = (
            self.payday_block_info_last_block.height
            - self.payday_block_info_first_block.height
            + 1
        )
        queue = []
        for baker_id in track(self.bakers_with_delegation_information.keys()):
            _id = f"{self.payday_date_string}-{baker_id}"
            d = {}
            if baker_id == "passive_delegation":
                d["pool_status"] = self.passive_delegation_info.model_dump(
                    exclude_none=True
                )
            else:
                d["pool_status"] = self.pool_info_by_baker_id[str(baker_id)].model_dump(
                    exclude_none=True
                )
                if self.pool_info_by_baker_id[str(baker_id)].current_payday_info:
                    d["expectation"] = (
                        self.pool_info_by_baker_id[
                            str(baker_id)
                        ].current_payday_info.lottery_power
                        * estimated_blocks_per_day
                    )

                else:
                    d["expectation"] = 0

            pool_owner = baker_id
            d["_id"] = _id
            d["date"] = self.payday_date_string
            d["payday_block_slot_time"] = self.payday_block_info.slot_time
            d["baker_id"] = pool_owner

            queue.append(ReplaceOne({"_id": _id}, d, upsert=True))

        _ = self.db[Collections.paydays_performance].bulk_write(queue)

        # for current payday...
        queue = []
        for baker_id in track(
            self.bakers_with_delegation_information_current_payday.keys()
        ):
            _id = f"{self.payday_date_string}-{baker_id}"
            d = {}
            d["pool_status"] = self.pool_info_by_baker_id_current_payday[
                str(baker_id)
            ].model_dump(exclude_none=True)
            if self.pool_info_by_baker_id_current_payday[
                str(baker_id)
            ].current_payday_info:
                d["expectation"] = (
                    self.pool_info_by_baker_id_current_payday[
                        str(baker_id)
                    ].current_payday_info.lottery_power
                    * estimated_blocks_per_day
                )

            else:
                d["expectation"] = 0

            pool_owner = baker_id
            d["_id"] = _id
            d["date"] = self.payday_date_string
            d["payday_block_slot_time"] = self.payday_block_info.slot_time
            d["baker_id"] = pool_owner

            queue.append(ReplaceOne({"_id": _id}, d, upsert=True))

        _ = self.db[Collections.paydays_current_payday].delete_many({})
        _ = self.db[Collections.paydays_current_payday].bulk_write(queue)

        try:
            self.tooter.send(
                channel=TooterChannel.NOTIFIER,
                message=f"(Payday: {self.payday_date_string}) \nStep 2: process_payday_performance_for_bakers...done.\nProcessed {len(self.baker_account_ids_by_baker_id.keys())} bakers.",
                notifier_type=TooterType.INFO,
            )
        except:
            console.log("Step 2, can't toot.")

    def file_a_balance_movement(
        self,
        block_height: int,
        impacted_addresses_in_tx: dict[str, MongoImpactedAddress],
        impacted_address: str,
        balance_movement_to_add: AccountStatementEntryType,
    ):
        if impacted_addresses_in_tx.get(impacted_address):
            impacted_address_as_class: MongoImpactedAddress = impacted_addresses_in_tx[
                impacted_address
            ]
            bm = impacted_address_as_class.balance_movement
            field_set = list(balance_movement_to_add.model_fields_set)[0]
            if field_set == "transfer_in":
                if not bm.transfer_in:
                    bm.transfer_in = []
                bm.transfer_in.extend(balance_movement_to_add.transfer_in)
            elif field_set == "transfer_out":
                if not bm.transfer_out:
                    bm.transfer_out = []
                bm.transfer_out.extend(balance_movement_to_add.transfer_out)
            elif field_set == "amount_encrypted":
                bm.amount_encrypted = balance_movement_to_add.amount_encrypted
            elif field_set == "amount_decrypted":
                bm.amount_decrypted = balance_movement_to_add.amount_decrypted
            elif field_set == "baker_reward":
                bm.baker_reward = balance_movement_to_add.baker_reward
            elif field_set == "finalization_reward":
                bm.finalization_reward = balance_movement_to_add.finalization_reward
            elif field_set == "foundation_reward":
                bm.foundation_reward = balance_movement_to_add.foundation_reward
            elif field_set == "transaction_fee_reward":
                bm.transaction_fee_reward = (
                    balance_movement_to_add.transaction_fee_reward
                )

            impacted_address_as_class.balance_movement = bm
        else:
            impacted_address_as_class = MongoImpactedAddress(
                **{
                    "_id": f"{block_height}-{impacted_address[:29]}",
                    "impacted_address": impacted_address,
                    "impacted_address_canonical": impacted_address[:29],
                    "effect_type": "Account Reward",
                    "balance_movement": balance_movement_to_add,
                    "block_height": block_height,
                    "date": self.payday_date_string,
                }
            )
            impacted_addresses_in_tx[impacted_address] = impacted_address_as_class

    def add_reward_to_impacted_accounts(
        self, account_rewards: Dict[str, CCD_BlockSpecialEvent_PaydayAccountReward]
    ):
        impacted_addresses_queue = []
        for ar in track(account_rewards.values()):
            impacted_addresses_in_tx: dict = {}
            balance_movement = AccountStatementEntryType(
                transaction_fee_reward=ar.transaction_fees,
                baker_reward=ar.baker_reward,
                finalization_reward=ar.finalization_reward,
            )
            self.file_a_balance_movement(
                self.payday_block_info_last_block.height + 1,
                impacted_addresses_in_tx,
                ar.account,
                balance_movement,
            )

            # now this tx is done, so add impacted_addresses to queue
            for ia in impacted_addresses_in_tx.values():
                ia: MongoImpactedAddress
                repl_dict = ia.model_dump(exclude_none=True)
                if "id" in repl_dict:
                    del repl_dict["id"]

                impacted_addresses_queue.append(
                    ReplaceOne(
                        {"_id": ia.id},
                        repl_dict,
                        upsert=True,
                    )
                )
        _ = self.db[Collections.impacted_addresses].bulk_write(impacted_addresses_queue)
        self.tooter.send(
            channel=TooterChannel.NOTIFIER,
            message=f"(Payday: {self.payday_date_string}) \nStep 3.5: add_reward_to_impacted_accounts...done.",
            notifier_type=TooterType.INFO,
        )

    # # step 3
    def process_payday_rewards_for_account_or_baker(self):
        console.log("Step 3: process_payday_rewards_for_account_or_baker")
        """
        This method runs through all rewards for the payday and stores an entry for each in collection_paydays_rewards.
        """
        queue = []
        self.account_rewards: Dict[str, CCD_BlockSpecialEvent_PaydayAccountReward] = {}
        self.pool_rewards: Dict[str, CCD_BlockSpecialEvent_PaydayPoolReward] = {}

        for e in track(self.special_events_with_rewards):  #
            if (e.payday_pool_reward) or e.payday_account_reward:
                d = {}
                if e.payday_account_reward:
                    self.account_rewards[e.payday_account_reward.account] = (
                        e.payday_account_reward
                    )
                    _tag = "payday_account_reward"
                    d["account_id"] = e.payday_account_reward.account
                    d["reward"] = e.payday_account_reward.model_dump()
                    receiver = e.payday_account_reward.account
                    if e.payday_account_reward.account in self.list_of_delegators:
                        d["account_is_delegator"] = True
                        d["delegation_target"] = self.reverse_search_from_dictionary(
                            self.bakers_with_delegation_information,
                            e.payday_account_reward.account,
                        )
                        d["staked_amount"] = self.account_with_stake_by_account_id[
                            e.payday_account_reward.account
                        ]

                    if (
                        e.payday_account_reward.account
                        in self.baker_account_ids_by_baker_id.values()
                    ):
                        # request poolstatus to get a stable stakedAmount for an account from the baker itself.
                        d["staked_amount"] = self.pool_info_by_account_id[
                            e.payday_account_reward.account
                        ].current_payday_info.baker_equity_capital
                        d["account_is_baker"] = True
                        d["baker_id"] = self.baker_account_ids_by_account_id[
                            e.payday_account_reward.account
                        ]

                elif e.payday_pool_reward:

                    d["pool_owner"] = (
                        e.payday_pool_reward.pool_owner
                        if e.payday_pool_reward.pool_owner
                        else "passive_delegation"
                    )
                    receiver = (
                        self.baker_account_ids_by_baker_id[
                            str(e.payday_pool_reward.pool_owner)
                        ]
                        if e.payday_pool_reward.pool_owner
                        else "passive_delegation"
                    )
                    self.pool_rewards[str(d["pool_owner"])] = e.payday_pool_reward
                    _tag = "payday_pool_reward"
                    d["pool_status"] = (
                        self.pool_info_by_baker_id[
                            str(e.payday_pool_reward.pool_owner)
                        ].model_dump(exclude_none=True)
                        if e.payday_pool_reward.pool_owner
                        else self.passive_delegation_info.model_dump(exclude_none=True)
                    )
                    d["reward"] = e.payday_pool_reward.model_dump(exclude_none=True)

                # receiver = "passive_delegation" if not receiver else receiver #type: ignore
                d["_id"] = f"{self.payday_date_string}-{_tag}-{receiver}"  # type: ignore
                d["date"] = self.payday_date_string
                d["slot_time"] = self.payday_block_info.slot_time

                queue.append(ReplaceOne({"_id": d["_id"]}, d, upsert=True))

        # BULK_WRITE
        _ = self.db[Collections.paydays_rewards].bulk_write(queue)
        try:
            self.tooter.send(
                channel=TooterChannel.NOTIFIER,
                message=f"(Payday: {self.payday_date_string}) \nStep 3: process_payday_rewards_for_account_or_baker...done.\nProcessed {len(self.baker_account_ids_by_baker_id.keys())} bakers.",
                notifier_type=TooterType.INFO,
            )
        except:
            console.log("Step 3, can't toot.")

        self.add_reward_to_impacted_accounts(self.account_rewards)

    # # step 4
    def fill_apy_intermediate_for_accounts_for_date(self):
        console.log("Step 4: fill_apy_intermediate_for_accounts_for_date")
        """
        We only get into this method if it's a account that is either a baker or a delegator.
        
        This method fills the paydays_apy_intermediate collection, for a given payday. 
        This contains the daily apy (reward/relevant_stake). There are documents for every account,
        with a property daily_apy, which is a dictionary, keyed by date, valued is daily apy.
        """

        queue = []
        for account_id in track(self.accounts_that_need_APY):
            _id = account_id
            result = self.db[Collections.paydays_apy_intermediate].find_one(
                {"_id": _id}
            )
            if result:
                current_daily_apy_dict_for_account = result["daily_apy_dict"]
            else:
                current_daily_apy_dict_for_account = {}
                result = {}

            if account_id in self.account_rewards.keys():
                reward_for_account = self.account_rewards[str(account_id)]
                sum_reward = (
                    reward_for_account.baker_reward
                    + reward_for_account.finalization_reward
                    + reward_for_account.transaction_fees
                )

                staked_amount_for_account = self.account_with_stake_by_account_id[
                    str(account_id)
                ]

                daily_apy = (
                    math.pow(
                        1 + (sum_reward / staked_amount_for_account),
                        self.seconds_per_year / self.payday_duration,
                    )
                    - 1
                )
            else:
                daily_apy = 0
                reward_for_account = {}
                sum_reward = 0

            # add daily_apy to the dict for this account
            current_daily_apy_dict_for_account[self.payday_date_string] = {
                "apy": daily_apy,
                "reward": sum_reward / 1_000_000,
            }

            apy_to_insert = result
            apy_to_insert.update(
                {
                    "_id": _id,
                    "calculation_type": "daily apy (intermediate value)",
                    "daily_apy_dict": current_daily_apy_dict_for_account,
                }
            )

            queue.append(ReplaceOne({"_id": _id}, apy_to_insert, upsert=True))

        # BULK_WRITE
        _ = self.db[Collections.paydays_apy_intermediate].bulk_write(queue)

        try:
            self.tooter.send(
                channel=TooterChannel.NOTIFIER,
                message=f"(Payday: {self.payday_date_string}) \nStep 4: fill_apy_intermediate_for_accounts_for_date...done.\nProcessed {len(self.accounts_that_need_APY)} accounts.",
                notifier_type=TooterType.INFO,
            )
        except:
            console.log("Step 4, can't toot.")

    # # step 5
    def fill_apy_intermediate_for_bakers_for_date(self):
        console.log("Step 5: fill_apy_intermediate_for_bakers_for_date")
        """
        We only get into this method if it's a baker.
        
        This method fills the paydays_apy_intermediate collection, for a given payday. 
        This contains the daily apy (reward/relevant_stake). There are documents for every account,
        with a property daily_apy, which is a dictionary, keyed by date, valued is daily apy.
        For bakers
        """
        queue = []
        for baker_id in track(self.bakers_that_need_APY):
            _id = baker_id

            result = self.db[Collections.paydays_apy_intermediate].find_one(
                {"_id": _id}
            )
            if result:
                current_daily_apy_dict_for_baker = result["daily_apy_dict"]
            else:
                current_daily_apy_dict_for_baker = {}
                result = {}

            daily_total = None
            daily_baker = None
            daily_delegator = None
            daily_passive = None

            if baker_id in self.pool_rewards.keys():
                reward_for_baker = self.pool_rewards[baker_id]
                total_reward = (
                    reward_for_baker.baker_reward
                    + reward_for_baker.finalization_reward
                    + reward_for_baker.transaction_fees
                )

                if baker_id == "passive_delegation":
                    pool_info_for_baker = self.passive_delegation_info
                    if pool_info_for_baker.current_payday_delegated_capital > 0:
                        daily_apy = (
                            math.pow(
                                1
                                + (
                                    total_reward
                                    / pool_info_for_baker.current_payday_delegated_capital
                                ),
                                self.seconds_per_year / self.payday_duration,
                            )
                            - 1
                        )
                    else:
                        daily_apy = 0
                    sum_rewards = total_reward / 1_000_000
                    daily_passive = {"apy": daily_apy, "reward": sum_rewards}

                else:
                    pool_info_for_baker = self.pool_info_by_baker_id[baker_id]
                    delegation_info_for_baker = self.bakers_with_delegation_information[
                        baker_id
                    ]

                    delegator_ratio = (
                        pool_info_for_baker.current_payday_info.delegated_capital
                        / pool_info_for_baker.current_payday_info.effective_stake
                    )

                    delegators_baking_reward = (
                        1 - pool_info_for_baker.pool_info.commission_rates.baking
                    ) * (delegator_ratio * reward_for_baker.baker_reward)

                    delegators_transaction_reward = (
                        1 - pool_info_for_baker.pool_info.commission_rates.transaction
                    ) * (delegator_ratio * reward_for_baker.transaction_fees)

                    delegators_finalization_reward = (
                        1 - pool_info_for_baker.pool_info.commission_rates.finalization
                    ) * (delegator_ratio * reward_for_baker.finalization_reward)

                    delegator_reward = (
                        delegators_baking_reward
                        + delegators_transaction_reward
                        + delegators_finalization_reward
                    )

                    baker_reward = total_reward - delegator_reward

                    if pool_info_for_baker.current_payday_info.effective_stake > 0:
                        daily_apy = (
                            math.pow(
                                1
                                + (
                                    total_reward
                                    / pool_info_for_baker.current_payday_info.effective_stake
                                ),
                                self.seconds_per_year / self.payday_duration,
                            )
                            - 1
                        )
                    else:
                        daily_apy = 0
                    sum_rewards = total_reward / 1_000_000
                    daily_total = {"apy": daily_apy, "reward": sum_rewards}

                    if pool_info_for_baker.current_payday_info.baker_equity_capital > 0:
                        daily_apy = (
                            math.pow(
                                1
                                + (
                                    baker_reward
                                    / pool_info_for_baker.current_payday_info.baker_equity_capital
                                ),
                                self.seconds_per_year / self.payday_duration,
                            )
                            - 1
                        )
                    else:
                        daily_apy = 0
                    sum_rewards = baker_reward / 1_000_000
                    daily_baker = {"apy": daily_apy, "reward": sum_rewards}

                    if len(delegation_info_for_baker) > 0:
                        if (
                            pool_info_for_baker.current_payday_info.delegated_capital
                            > 0
                        ):
                            daily_apy = (
                                math.pow(
                                    1
                                    + (
                                        delegator_reward
                                        / pool_info_for_baker.current_payday_info.delegated_capital
                                    ),
                                    self.seconds_per_year / self.payday_duration,
                                )
                                - 1
                            )
                        else:
                            daily_apy = 0
                        sum_rewards = delegator_reward / 1_000_000
                        daily_delegator = {"apy": daily_apy, "reward": sum_rewards}

            else:
                daily_apy = 0
                reward_for_baker = {}
                sum_rewards = 0

            # add daily_apy to the dict for this account
            current_daily_apy_dict_for_baker[self.payday_date_string] = {}
            if daily_baker:
                current_daily_apy_dict_for_baker[self.payday_date_string].update(
                    {"baker": daily_baker}
                )
            else:
                current_daily_apy_dict_for_baker[self.payday_date_string].update(
                    {"baker": {"apy": 0, "reward": 0}}
                )
            if daily_total:
                current_daily_apy_dict_for_baker[self.payday_date_string].update(
                    {"total": daily_total}
                )
            else:
                current_daily_apy_dict_for_baker[self.payday_date_string].update(
                    {"total": {"apy": 0, "reward": 0}}
                )

            if daily_delegator:
                current_daily_apy_dict_for_baker[self.payday_date_string].update(
                    {"delegator": daily_delegator}
                )
            else:
                current_daily_apy_dict_for_baker[self.payday_date_string].update(
                    {"delegator": {"apy": 0, "reward": 0}}
                )

            if baker_id == "passive_delegation":
                if daily_passive:
                    current_daily_apy_dict_for_baker[self.payday_date_string].update(
                        {"passive": daily_passive}
                    )
                else:
                    current_daily_apy_dict_for_baker[self.payday_date_string].update(
                        {"passive": {"apy": 0, "reward": 0}}
                    )

            apy_to_insert = result
            apy_to_insert.update(
                {
                    "_id": _id,
                    "calculation_type": "daily apy (intermediate value)",
                    "daily_apy_dict": current_daily_apy_dict_for_baker,
                }
            )

            queue.append(ReplaceOne({"_id": _id}, apy_to_insert, upsert=True))

        # BULK_WRITE
        _ = self.db[Collections.paydays_apy_intermediate].bulk_write(queue)

        try:
            self.tooter.send(
                channel=TooterChannel.NOTIFIER,
                message=f"(Payday: {self.payday_date_string}) \nStep 5: fill_apy_intermediate_for_bakers_for_date...done.\nProcessed {len(self.baker_account_ids_by_baker_id.keys())} bakers.",
                notifier_type=TooterType.INFO,
            )
        except:
            console.log("Step 5, can't toot.")

    # step 6
    def calc_moving_averages(self):
        print("getting paydays", end=" ")
        paydays_days = [
            x["date"]
            for x in self.db[Collections.paydays].find(
                filter={},
                projection={
                    "_id": 0,
                    "date": 1,
                },
            )
        ]
        print(len(paydays_days))
        print("Getting accounts", end=" ")

        periods = [30, 90, 180]

        index_in_list = paydays_days.index(self.payday_date_string)

        apy_periods = {}
        queue = []
        for x in (xy for xy in self.db[Collections.paydays_apy_intermediate].find()):
            account = MongoTypePaydayAPYIntermediate(**x)
            apy_to_insert = account.__dict__
            made_modifications = False

            for period in periods:
                account_period_apy_dict = (
                    account.__dict__[f"d{period}_apy_dict"]
                    if account.__dict__[f"d{period}_apy_dict"] is not None
                    else {}
                )
                # if the index of the current payday is less than the period we want to calculate
                # we can't continue with this period (ie, if 70 days have passed, we can't calculate 90d avg).
                if index_in_list < period:
                    pass
                else:
                    term_dates = paydays_days[
                        (index_in_list - period + 1) : (index_in_list + 1)
                    ]
                    if account.id == "passive_delegation":
                        this_account_apy_objects_for_term = [
                            v["passive"]["apy"]
                            for k, v in account.daily_apy_dict.items()
                            if k in term_dates
                        ]
                        this_account_reward_objects_for_term = [
                            v["passive"]["reward"]
                            for k, v in account.daily_apy_dict.items()
                            if k in term_dates
                        ]
                    elif account.id.isnumeric():
                        this_account_apy_objects_for_term = [
                            v["delegator"]["apy"]
                            for k, v in account.daily_apy_dict.items()
                            if k in term_dates
                        ]
                        this_account_reward_objects_for_term = [
                            v["delegator"]["reward"]
                            for k, v in account.daily_apy_dict.items()
                            if k in term_dates
                        ]
                    else:
                        this_account_apy_objects_for_term = [
                            v["apy"]
                            for k, v in account.daily_apy_dict.items()
                            if k in term_dates
                        ]
                        this_account_reward_objects_for_term = [
                            v["reward"]
                            for k, v in account.daily_apy_dict.items()
                            if k in term_dates
                        ]

                    if len(this_account_apy_objects_for_term) > 0.90 * period:
                        apy_periods[period] = {
                            "apy": calc_apy_for_period(
                                this_account_apy_objects_for_term
                            ),
                            "sum_of_rewards": sum(this_account_reward_objects_for_term),
                            "count_of_days": len(this_account_apy_objects_for_term),
                        }
                        account_period_apy_dict[self.payday_date_string] = apy_periods[
                            period
                        ]
                        apy_to_insert.update(
                            {f"d{period}_apy_dict": account_period_apy_dict}
                        )
                        account.__dict__[f"d{period}_apy_dict"] = (
                            account_period_apy_dict
                        )
                        made_modifications = True
                    else:
                        pass
            if made_modifications:
                queue.append(
                    ReplaceOne({"_id": account.id}, apy_to_insert, upsert=True)
                )
        # BULK_WRITE
        if len(queue) > 0:
            print(f"{len(queue)=}", end="||")
            _ = self.db[Collections.paydays_apy_intermediate].bulk_write(queue)

        try:
            self.tooter.send(
                channel=TooterChannel.NOTIFIER,
                message=f"(Payday: {self.payday_date_string}) \nStep 6: Calculate moving averages. done.",
                notifier_type=TooterType.INFO,
            )
        except:
            console.log("Step 6, can't toot.")


grpcclient = GRPCClient()
tooter = Tooter()
mongodb = MongoDB(tooter)

db: Dict[Collections, Collection] = mongodb.mainnet


if __name__ == "__main__":
    while True:
        result = db[Collections.paydays].find_one(
            {}, sort=list({"height_for_last_block": -1}.items())
        )
        if result:
            last_processed_payday_date = result["date"]
        else:
            last_processed_payday_date = None

        result = db[Collections.helpers].find_one({"_id": "last_known_payday"})
        if result:
            last_known_payday_date = result["date"]
            last_known_payday_hash = result["hash"]
        else:
            last_known_payday_date = None
            last_known_payday_hash = None

        if last_known_payday_date != last_processed_payday_date:
            if not (last_known_payday_date is None) and not (
                last_known_payday_hash is None
            ):
                console.log(
                    f"Starting Payday calculations for {last_known_payday_date}..."
                )
                Payday(
                    last_known_payday_date,
                    last_known_payday_hash,
                    grpcclient,
                    mongodb,
                    tooter,
                )
                console.log("Sleeping after execution...")
        else:
            pass

        payday_timeframe_start = dt.time(7, 55, 0)
        payday_timeframe_end = dt.time(9, 10, 0)

        if (dt.time() > payday_timeframe_start) and (dt.time() < payday_timeframe_end):
            time.sleep(1)
        else:
            time.sleep(5)
