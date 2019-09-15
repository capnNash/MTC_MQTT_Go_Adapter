FROM ubuntu:latest

RUN apt-get update \
    && apt-get -y upgrade \
    && apt-get install wget -y \
    && apt-get install git -y \
    && apt-get install dos2unix \
    && wget https://dl.google.com/go/go1.12.7.linux-amd64.tar.gz \
    && tar -xvf go1.12.7.linux-amd64.tar.gz \
    && mv go /usr/local 

ENV GOROOT=/usr/local/go 
ENV GOPATH=$HOME/Projects/Proj1 
ENV PATH=$GOPATH/bin:$GOROOT/bin:$PATH
ENV GOBIN=$GOPATH/bin

RUN go get github.com/clearblade/Go-SDK \
    && go get github.com/clearblade/mqtt_parsing \
    && go get github.com/clearblade/paho.mqtt.golang

WORKDIR /home

#For compiling -> GOARCH=arm GOARM=5 GOOS=linux go build -o mqttBridgeAdapter
