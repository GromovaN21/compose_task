FROM python:3.11-slim

WORKDIR /app
COPY . .    
RUN apt update
RUN apt install -y curl
RUN pip install -r requirements.txt

CMD [ "python", "app.py" ]
