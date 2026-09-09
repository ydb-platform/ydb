ARG BASE_IMAGE=debian:bookworm-slim
FROM ${BASE_IMAGE} AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends python3 ca-certificates libaio1 \
    && rm -rf /var/lib/apt/lists/*
RUN mkdir -p /root/ydb/bin && printf '{"check_version":false}\n' > /root/ydb/bin/config.json
COPY bin/ /opt/ydb/bin/
COPY fixture/ /fixture/
ENTRYPOINT ["python3", "/fixture/entrypoint.py"]

FROM runtime AS ui-check
RUN apt-get update && apt-get install -y --no-install-recommends nodejs \
    && rm -rf /var/lib/apt/lists/*
COPY checks/ /checks/
RUN node /checks/check-legacy-ui.js /checks/viewer.js > /checks/result.txt
RUN python3 /fixture/check-genconfig.py >> /checks/result.txt

FROM runtime
COPY --from=ui-check /checks/result.txt /fixture/legacy-ui-verification.txt
ARG SOURCE_COMMIT
LABEL org.opencontainers.image.revision=${SOURCE_COMMIT}
