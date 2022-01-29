FROM golang as build

WORKDIR /root

COPY main.go go.mod go.sum /root/

RUN go build -o hse-cloud-logbroker

FROM ubuntu:18.04

WORKDIR /root
COPY --from=build /root/hse-cloud-logbroker /root/hse-cloud-logbroker
CMD /root/hse-cloud-logbroker
