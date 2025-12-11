FROM ubuntu:22.04 AS build

ARG SRC_PATH
ARG BINARY_NAME

RUN apt-get update \
    && apt-get install -y --no-install-recommends python3

COPY . /
RUN ./ya make ydb/public/sdk/cpp/tests/slo_workloads/${SRC_PATH} -r && cp ydb/public/sdk/cpp/tests/slo_workloads/${SRC_PATH}/${BINARY_NAME} /slo-workload-bin

FROM ubuntu:22.04

COPY --from=build /slo-workload-bin /
ENTRYPOINT ["/slo-workload-bin"]
