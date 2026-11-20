# Shopping Sessions: a single static Rust binary in an empty image.
# Build stage: std only, no crates, so the build needs no network beyond the base image.
FROM rust:1.85-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml ./
COPY src ./src
# AVX2-level code (every Cascade Lake or newer Xeon has it); glibc linked statically.
ENV RUSTFLAGS="-C target-cpu=x86-64-v3 -C target-feature=+crt-static"
RUN cargo build --release --target x86_64-unknown-linux-gnu \
 && cp target/x86_64-unknown-linux-gnu/release/sessions /sessions

# Run stage: nothing but the binary. The scorer runs it as uid 1000 with
# DATA_DIR, OUTPUT_DIR and NCPU set; logs go to stderr.
FROM scratch
COPY --from=build /sessions /sessions
USER 1000:1000
CMD ["/sessions"]
