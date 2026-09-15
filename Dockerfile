FROM rust:1.97-alpine AS builder

RUN apk add --no-cache musl-dev

WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY src ./src

RUN cargo build --release

FROM alpine:3.22
RUN apk add --no-cache ca-certificates

COPY --from=builder /app/target/release/imageworker /usr/bin/imageworker

ENTRYPOINT ["/usr/bin/imageworker"]