FROM 566034038752.dkr.ecr.us-west-2.amazonaws.com/python:latest
LABEL maintainer "sykang@amazon.com" 
LABEL "enviroment"="dev"
RUN apt-get update
ADD . /www
WORKDIR /www
RUN pip3 install -r requirements.txt
CMD python3 app.py
