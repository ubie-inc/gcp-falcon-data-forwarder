FROM golang:1.17 as go
FROM gcr.io/distroless/base-debian10 as run

FROM go as build

WORKDIR /app
COPY . .

RUN go build -mod=readonly -v -o /app/forwarder

FROM run
COPY --from=build /app/forwarder /forwarder

CMD ["/forwarder"]
