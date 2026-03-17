FROM golang:1-alpine AS builder
RUN apk add --no-cache gcc musl-dev
COPY go.mod /src/go.mod
COPY app.go /src/app.go
WORKDIR /src
RUN go build -o main .

FROM alpine:latest
RUN apk add --no-cache ca-certificates
WORKDIR /src
COPY --from=builder /src/main .
CMD ["/src/main"]
