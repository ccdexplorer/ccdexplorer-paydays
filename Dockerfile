# FROM tiangolo/uvicorn-gunicorn-fastapi:python3.9
FROM python:3.11-slim-bullseye

WORKDIR /home/code
RUN cd /home/code

# Install Python dependencies.
COPY ./requirements.txt .
RUN pip install --user --no-cache-dir -r requirements.txt
# Copy application files.
COPY . .
#
CMD ["python3", "/home/code/main.py"]