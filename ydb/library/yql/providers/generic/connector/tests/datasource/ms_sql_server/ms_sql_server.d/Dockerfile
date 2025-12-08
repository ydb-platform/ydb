FROM mcr.microsoft.com/mssql/server:2022-latest

# Create a config directory
USER root
RUN mkdir -p /usr/config
WORKDIR /usr/config

# Bundle config source
COPY . /usr/config

# Grant permissions for to our scripts to be executable
RUN chmod +x /usr/config/entrypoint.sh
RUN chmod +x /usr/config/configure-db.sh

USER mssql

ENTRYPOINT ["./entrypoint.sh"]

HEALTHCHECK --interval=10s --timeout=10s --retries=20 --start-period=30s\
  CMD cat /tmp/start | grep 'mssql is started' || exit 1