# Use a slim Python image to keep the footprint small
FROM python:3.11-slim

# Set the working directory inside the container
WORKDIR /app

# Copy the script and config into the container
COPY archiver.py .
COPY config.json .

# Run the script when the container starts
ENTRYPOINT ["python", "archiver.py", "--config", "config.json"]


