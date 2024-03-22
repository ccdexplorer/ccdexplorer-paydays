import os
from dotenv import load_dotenv

load_dotenv()

REPO_DIR = os.environ.get("REPO_DIR")
BRANCH = os.environ.get("BRANCH", "dev")
ENVIRONMENT = os.environ.get("ENVIRONMENT", "prod")
MONGODB_PASSWORD = os.environ.get("MONGODB_PASSWORD")
NOTIFIER_API_TOKEN = os.environ.get("NOTIFIER_API_TOKEN")
API_TOKEN = os.environ.get("API_TOKEN")
FASTMAIL_TOKEN = os.environ.get("FASTMAIL_TOKEN")
FALLBACK_URI = os.environ.get("FALLBACK_URI")
MONGO_URI = os.environ.get("MONGO_URI")
ON_SERVER = os.environ.get("ON_SERVER", False)
COIN_API_KEY = os.environ.get("COIN_API_KEY")
DEBUG = os.environ.get("DEBUG", False)
RUN_ON_NET = os.environ.get("NET")
