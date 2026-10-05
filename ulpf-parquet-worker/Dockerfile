# Build Stage
FROM rust:bookworm AS builder
WORKDIR /usr/src/ulpf-parquet-worker
RUN apt-get update && apt-get install -y cmake build-essential
COPY Cargo.toml Cargo.lock* ./
COPY src ./src
RUN cargo build --release

# Runtime Stage
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/src/ulpf-parquet-worker/target/release/ulpf-parquet-worker /usr/local/bin/
CMD ["ulpf-parquet-worker"]
