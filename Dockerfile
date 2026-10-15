# Multi-stage Dockerfile for Python RabbitMQ Service
FROM python:3.12-alpine AS runner

WORKDIR /app

# Install dependencies
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY . .

ENV PYTHONUNBUFFERED=1

CMD ["python", "src/demo.py"]
