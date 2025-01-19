FROM golang:1.23.4

WORKDIR /app
COPY ./server /app

RUN go build -o node main.go node.go

CMD ["/app/node"]
