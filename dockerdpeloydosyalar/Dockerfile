# app/Dockerfile

FROM python:3.9-slim

EXPOSE 8501
WORKDIR /app

RUN apt-get update && apt-get install -y \
    build-essential \
    software-properties-common \
    git \
    && rm -rf /var/lib/apt/lists/*



RUN mkdir -p /app/.streamlit
ADD secrets.toml /app/.streamlit/secrets.toml
ADD main.py /app/main.py


# copy over and install packages
COPY requirements.txt ./requirements.txt
RUN pip3 install -r requirements.txt


# run app
ENTRYPOINT ["streamlit", "run", "main.py", "--server.port=8501", "--server.address=0.0.0.0"]