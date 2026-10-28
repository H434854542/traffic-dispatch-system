FROM golang:1.24-alpine AS builder
WORKDIR /app
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -mod=vendor -o schedule-service main.go

FROM alpine:3.18
WORKDIR /app
COPY --from=builder /app/schedule-service .
EXPOSE 8080
ENTRYPOINT ["./schedule-service"]