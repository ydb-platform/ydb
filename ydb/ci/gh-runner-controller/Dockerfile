FROM debian:13-slim AS builder
COPY --from=ghcr.io/astral-sh/uv:0.5.24 /uv /uvx /bin/

ENV UV_PROJECT_ENVIRONMENT=/venv \
    UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1

RUN apt-get update \
    && apt-get install --no-install-suggests --no-install-recommends --yes ca-certificates python3.13 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY pyproject.toml ./
RUN uv sync --no-dev --no-editable --no-cache

FROM gcr.io/distroless/python3-debian13:nonroot
COPY --from=builder /venv /venv
COPY src/ /app
WORKDIR /app
ENTRYPOINT ["/venv/bin/python3", "app.py"]