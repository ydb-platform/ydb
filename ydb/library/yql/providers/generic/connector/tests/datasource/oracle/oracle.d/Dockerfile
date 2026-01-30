ARG BASE_IMAGE=container-registry.oracle.com/database/free:latest@sha256:83edd0756fda0e5faecc0fdf047814f0177d4224d7bf037e4900123ee3e08718
FROM $BASE_IMAGE

COPY ./init.sh /opt/oracle/scripts/startup/init.sh

HEALTHCHECK --interval=10s --timeout=10s --retries=20 --start-period=30s\
  CMD cat /tmp/start | grep 'oracle is started' || exit 1



