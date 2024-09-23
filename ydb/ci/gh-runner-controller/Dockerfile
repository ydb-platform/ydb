FROM debian:12-slim AS builder
RUN apt-get update \
    && apt-get install --no-install-suggests --no-install-recommends --yes python3-venv gcc libpython3-dev \
    && python3 -m venv /venv \
    && /venv/bin/pip install --upgrade pip setuptools wheel

COPY requirements.txt /
RUN /venv/bin/pip install --disable-pip-version-check -r /requirements.txt

FROM gcr.io/distroless/python3-debian12:nonroot
COPY --from=builder /venv /venv
COPY src/ /app
WORKDIR /app
ENTRYPOINT ["/venv/bin/python3", "app.py"]