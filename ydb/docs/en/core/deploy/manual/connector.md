# Deploying connectors to external data sources

{% note warning %}

This functionality is in "Experimental" mode.

{% endnote %}

[Connectors](../../concepts/federated_query/architecture.md#connectors) are special microservices providing {{ ydb-full-name }} with a universal abstraction for accessing external data sources. Connectors act as extension points for the {{ ydb-full-name }} [federated query](../../concepts/federated_query/index.md) processing system. This guide will discuss the specifics of deploying connectors in an on-premise environment.

## fq-connector-go {#fq-connector-go}

The `fq-connector-go` connector is implemented in Go; its source code is hosted on [GitHub](https://github.com/ydb-platform/fq-connector-go). It provides access to the following data sources:

* [ClickHouse](https://clickhouse.com/)
* [PostgreSQL](https://www.postgresql.org/)

The connector can be installed using a binary distribution or a Docker image.

### Running from a binary distribution {#fq-connector-go-binary}

Use binary distributions to install the connector on a physical or virtual Linux server without container virtualization.

1. On the [releases page](https://github.com/ydb-platform/fq-connector-go/releases) of the connector, select the latest release and download the archive for your platform and architecture. The following command downloads version `v0.2.4` of the connector for the Linux platform and `amd64` architecture:
    ```bash
    mkdir /tmp/connector && cd /tmp/connector
    wget https://github.com/ydb-platform/fq-connector-go/releases/download/v0.2.4/fq-connector-go-v0.2.4-linux-amd64.tar.gz
    tar -xzf fq-connector-go-v0.2.4-linux-amd64.tar.gz
    ```

1. If {{ ydb-short-name }} nodes have not yet been deployed on the server, create directories for storing executable and configuration files:

    ```bash
    sudo mkdir -p /opt/ydb/bin /opt/ydb/cfg
    ```

1. Place the extracted executable and configuration files of the connector into the newly created directories:
    ```bash
    sudo cp fq-connector-go /opt/ydb/bin
    sudo cp fq-connector-go.yaml /opt/ydb/cfg
    ```

1. In the {% if oss %}[recommended usage mode](../../deploy/manual/deploy-ydb-federated-query.md#general-scheme){% else %}recommended usage mode{% endif %}, the connector is deployed on the same servers as the dynamic nodes of {{ ydb-short-name }}, so encryption of network connections between them *is not required*. However, if you need to enable encryption, [prepare a pair of TLS keys](../manual/deploy-ydb-on-premises.md#tls-certificates) and specify the paths to the public and private keys in the `connector_server.tls.cert` and `connector_server.tls.key` fields of the `fq-connector-go.yaml` configuration file:
    ```yaml
    connector_server:
      # ...
      tls:
        cert: "/opt/ydb/certs/fq-connector-go.crt"
        key: "/opt/ydb/certs/fq-connector-go.key"
    ```
1. If external data sources use TLS, the connector will need a root or intermediate Certificate Authority (CA) certificate that signed the sources' certificates to establish encrypted connections. Linux servers usually have some CA root certificates pre-installed. For Ubuntu OS, the list of supported CAs can be displayed with the following command:
    ```bash
    awk -v cmd='openssl x509 -noout -subject' '/BEGIN/{close(cmd)};{print | cmd}' < /etc/ssl/certs/ca-certificates.crt
    ```
    If the server lacks the required CA certificate, copy it to a special system directory and update the certificates list:
    ```bash
    sudo cp root_ca.crt /usr/local/share/ca-certificates/
    sudo update-ca-certificates
    ```

1. You can start the service manually or using `systemd`.

    {% list tabs %}

    - Manually

        Start the service from the console with the following command:
        ```bash
        /opt/ydb/bin/fq-connector-go server -c /opt/ydb/cfg/fq-connector-go.yaml
        ```

    - Using systemd

        Along with the binary distribution, fq-connector-go includes a [sample](https://github.com/ydb-platform/fq-connector-go/blob/main/examples/systemd/fq-connector-go.service) configuration file (unit) for the `systemd` initialization system. Copy the unit to the `/etc/systemd/system` directory, enable, and start the service:

        ```bash
        cd /tmp/connector
        sudo cp fq-connector-go.service /etc/systemd/system/
        sudo systemctl enable fq-connector-go.service
        sudo systemctl start fq-connector-go.service
        ```

        If successful, the service should enter the `active (running)` state. Check it with the following command:
        ```bash
        sudo systemctl status fq-connector-go
        ● fq-connector-go.service - YDB FQ Connector Go
            Loaded: loaded (/etc/systemd/system/fq-connector-go.service; enabled; vendor preset: enabled)
            Active: active (running) since Thu 2024-02-29 17:51:42 MSK; 2s ago
        ```

        Service logs can be read using the command:
        ```bash
        sudo journalctl -u fq-connector-go.service
        ```
    {% endlist %}

### Running in Docker {#fq-connector-go-docker}

1. To run the connector, use the official [Docker image](https://github.com/ydb-platform/fq-connector-go/pkgs/container/fq-connector-go). It already contains the service's [configuration file](https://github.com/ydb-platform/fq-connector-go/blob/main/app/server/config/config.prod.yaml). Start the service with default settings using the following command:

    ```bash
    docker run -d \
        --name=fq-connector-go \
        -p 2130:2130 \
        ghcr.io/ydb-platform/fq-connector-go:latest
    ```

    A listening socket of the GRPC service connector will start on port 2130 of your host's public network interface. Subsequently, the {{ ydb-short-name }} server must connect to this network address.

1. If configuration changes are needed, prepare the configuration file [based on the sample](#fq-connector-go-config) and mount it to the container:

    ```bash
    docker run -d \
        --name=fq-connector-go \
        -p 2130:2130 \
        -v /path/to/config.yaml:/opt/ydb/cfg/fq-connector-go.yaml
        ghcr.io/ydb-platform/fq-connector-go:latest
    ```

1. In the {% if oss %}[recommended usage mode](../../deploy/manual/deploy-ydb-federated-query.md#general-scheme){% else %}recommended usage mode{% endif %}, the connector is deployed on the same servers as the dynamic nodes of {{ ydb-short-name }}, so encryption of network connections between them *is not required*. However, if you need to enable encryption between {{ ydb-short-name }} and the connector, [prepare a pair of TLS keys](../manual/deploy-ydb-on-premises.md#tls-certificates) and specify the paths to the public and private keys in the `connector_server.tls.cert` and `connector_server.tls.key` fields of the configuration file:

    ```yaml
    connector_server:
      # ...
      tls:
        cert: "/opt/ydb/certs/fq-connector-go.crt"
        key: "/opt/ydb/certs/fq-connector-go.key"
    ```
    When starting the container, mount the directory with the TLS key pair inside it so that they are accessible to the `fq-connector-go` process at the paths specified in the configuration file:

    ```bash
    docker run -d \
        --name=fq-connector-go \
        -p 2130:2130 \
        -v /path/to/config.yaml:/opt/ydb/cfg/fq-connector-go.yaml
        -v /path/to/keys/:/opt/ydb/certs/
        ghcr.io/ydb-platform/fq-connector-go:latest
    ```

1. If external data sources use TLS, the connector will need a root or intermediate Certificate Authority (CA) certificate that signed the sources' certificates to establish encrypted connections. The Docker image for the connector is based on the Alpine Linux distribution image, which already contains some CA certificates. Check for the required CA in the pre-installed list with the following command:

    ```bash
    docker run -it --rm ghcr.io/ydb-platform/fq-connector-go sh
    # then in the console inside the container:
    apk add openssl
    awk -v cmd='openssl x509 -noout -subject' ' /BEGIN/{close(cmd)};{print | cmd}' < /etc/ssl/certs/ca-certificates.crt
    ```

    If the source TLS keys are issued by a CA that is not included in the trusted list, add the CA certificate to the system paths of the container with the connector. For example, build a custom Docker image based on the existing one. Prepare the following `Dockerfile`:

    ```Dockerfile
    FROM ghcr.io/ydb-platform/fq-connector-go:latest

    USER root

    RUN apk --no-cache add ca-certificates openssl
    COPY root_ca.crt /usr/local/share/ca-certificates
    RUN update-ca-certificates
    ```

    Place the `Dockerfile` and the CA root certificate in one folder, navigate to it, and build the image with the following command:
    ```bash
    docker build -t fq-connector-go_custom_ca .
    ```

    The new `fq-connector-go_custom_ca` image can be used to deploy the service using the above commands.

### Configuration {#fq-connector-go-config}

A current example of the `fq-connector-go` service configuration file can be found in the [repository](https://github.com/ydb-platform/fq-connector-go/blob/main/app/server/config/config.prod.yaml).

| Parameter | Description |
|-----------|-------------|
| `connector_server` | Required section. Contains the settings of the main GPRC server that accesses the data. |
| `connector_server.endpoint.host` | Hostname or IP address on which the service's listening socket runs. |
| `connector_server.endpoint.port` | Port number on which the service's listening socket runs. |
| `connector_server.tls` | Optional section. Filled if TLS connections are required for the main GRPC service `fq-connector-go`. By default, the service runs without TLS. |
| `connector_server.tls.key` | Full path to the private encryption key. |
| `connector_server.tls.cert` | Full path to the public encryption key. |
| `logger` | Optional section. It contains logging settings. |
| `logger.log_level` | Logging level. Valid values: `TRACE`, `DEBUG`, `INFO`, `WARN`, `ERROR`, `FATAL`. Default value: `INFO`. |
| `logger.enable_sql_query_logging` | For data sources supporting SQL, query logging  is enabled. Valid values: `true`, `false`. **IMPORTANT**: Enabling this option may result in printing confidential user data in the logs. Default value: `false`. |
| `paging` | Optional section. It contains settings for the algorithm of splitting the data stream extracted from the source into Arrow blocks. For each request, a queue of blocks prepared for sending to {{ ydb-short-name }} is created in the connector. Arrow block allocation contributes significantly to the memory consumption of the `fq-connector-go` process. The minimum memory required for the connector's operation can be roughly estimated by the formula $Mem = 2 \cdot Requests \cdot BPP \cdot PQC$, where $Requests$ is the number of concurrent requests, $BPP$ is the `paging.bytes_per_page` parameter, and $PQC$ is the `paging.prefetch_queue_capacity` parameter. |
| `paging.bytes_per_page` | Maximum number of bytes in one block. Recommended values range from 4 to 8 MiB, and the maximum is 48 MiB. Default value: 4 MiB. |
| `paging.prefetch_queue_capacity` | Number of pre-read data blocks stored in the connector's address space until YDB requests the next data block. In some scenarios, larger values of this setting can increase throughput but will also lead to higher memory consumption by the process. Recommended values - at least 2. Default value: 2. |