FROM golang:1.26 AS builder
WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /agentops-memory .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder /agentops-memory /agentops-memory
VOLUME /data
EXPOSE 7437
ENTRYPOINT ["/agentops-memory"]
