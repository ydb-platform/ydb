FROM ubuntu:22.04 AS build

ARG SRC_PATH
ARG BINARY_NAME
ARG REF="unknown"

RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY build/ /build/
COPY certs/ /certs/
COPY contrib/ /contrib/
COPY library/cpp/ /library/cpp/
COPY tools/ /tools/
COPY util/ /util/
COPY ydb/library/yverify_stream/ /ydb/library/yverify_stream/
COPY ydb/public/api/ /ydb/public/api/
COPY ydb/public/lib/protobuf/ /ydb/public/lib/protobuf/
COPY ydb/public/lib/validation/ /ydb/public/lib/validation/
COPY ydb/public/sdk/cpp/ /ydb/public/sdk/cpp/

COPY ./ya /

RUN ./ya make -r -D REF=${REF} ydb/public/sdk/cpp/tests/slo_workloads/${SRC_PATH} && cp ydb/public/sdk/cpp/tests/slo_workloads/${SRC_PATH}/${BINARY_NAME} /slo-workload-bin

FROM ubuntu:22.04

COPY --from=build /slo-workload-bin /
ENTRYPOINT ["/slo-workload-bin"]
