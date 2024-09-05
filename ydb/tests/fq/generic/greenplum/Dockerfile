FROM ghcr.io/ydb-platform/fq-connector-go_greenplum:6.25.3@sha256:0627a657b179ff73949fec05201f3e164b92639281eff248cd07669ce7247eec

# For the sake of simplicity of tests, we force Greenplum to use the same port that it uses within MDB
RUN find /data -type f -exec sed -i 's/5432/6432/' "{}" +;
