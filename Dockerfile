FROM python:3.11

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get -y install vim

# Copy Python script and requirements.txt
COPY ./config.py .
COPY ./main.py .
COPY ./requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

CMD ["python3", "main.py"]