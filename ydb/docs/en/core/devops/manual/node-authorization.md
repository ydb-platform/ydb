# Database node authentication and authorization

Node authentication in the {{ ydb-short-name }} cluster ensures that database nodes are authenticated when making service requests to other nodes via the gRPC protocol. Node authorization ensures that the privileges required by the service requests are checked and granted during the request processing. These service calls include the database nodes registration within the cluster and access the [dynamic configuration](../../maintenance/manual/dynamic-config.md) information. The use of node authorization is recommended for all {{ ydb-short-name }} clusters, as it allows to avoid the unauthorized access to data by adding the nodes controlled by an attacker into the cluster.

Database node authentication and authorization is performed in the following order:

1. The database node being started opens a gRPC connection to one of the cluster storage nodes specified in the `--node-broker` command line option. The connection uses the TLS protocol, and the certificate of the running node is used as the client certificate for the connection.
2. The storage node and the database node perform mutual authentication checks using the TLS protocol: the certificate trust chain is checked and the host name is matched against the value of the "Subject Name" field of the certificate.
3. The storage node checks the "Subject" field of the certificate for compliance with the requirements set in the static configuration.
4. If the above checks are successful, the connection from the database node is considered as authenticated, and it gets assigned a special [SID](../../concepts/glossary.md#access-sid), determined by the settings.
5. The database node uses the established gRPC connection to register with the cluster and to obtain the dynamic configuration through the corresponding service requests.
6. The storage node checks whether the SID assigned to the connection is in the list of acceptable ones. If this check is successful, the storage node performs the requested action.

Below are the steps required to enable the node authentication and authorization feature.

## Configuration pre-requisites

1. The deployed {{ ydb-short-name }} cluster should have the [gRPC traffic encryption](../../reference/configuration/tls.md#grpc) configured for using the TLS protocol.
1. When preparing node certificates for a cluster where you plan to use the node authorization feature, the uniform rules must be used for populating the "Subject" field of the certificates, which allows to identify the certificates issued for the cluster nodes. For more information, see the [certificate verification rules documentation](../../reference/configuration/node-authentication.md).

    {% note info %}

    The proposed [example script](https://github.com/ydb-platform/ydb/blob/main/ydb/deploy/tls_cert_gen/) generates self-signed certificates for {{ ydb-short-name }} nodes and ensures that the "Subject" field is populated with the value `O=YDB` for all node certificates. The configuration examples provided below are prepared for certificates with this specific "Subject" field configuration, but feel free to use your real organization name instead.

    {% endnote %}

1. The command-line parameters for [starting database nodes](../../devops/manual/initial-deployment.md#start-dynnode) must include options that specify the paths to the trusted CA certificate, the node certificate, and the node key files. The required additional command-line options are shown in the table below.

    | **Command line option**    | **Description** |
    |----------------------------|-----------------|
    | `--grpc-ca`                | Path to trusted certification authority file `ca.crt` |
    | `--grpc-cert`              | Path to node certificate file `node.crt` |
    | `--grpc-key`               | Path to node secret key file `node.key` |

    Below is the example of the complete command to start the database node, with the extra options for gRPC TLS key and certificate files:

    ```bash
    /opt/ydb/bin/ydbd server --yaml-config  /opt/ydb/cfg/config.yaml --tenant /Root/testdb \
        --grpcs-port 2136 --grpc-ca /opt/ydb/certs/ca.crt \
        --grpc-cert /opt/ydb/certs/node.crt --grpc-key /opt/ydb/certs/node.key \
        --ic-port 19002 --ca /opt/ydb/certs/ca.crt \
        --mon-port 8766 --mon-cert /opt/ydb/certs/web.pem \
        --node-broker grpcs://<ydb1>:2135 \
        --node-broker grpcs://<ydb2>:2135 \
        --node-broker grpcs://<ydb3>:2135
    ```

## Enabling database node authentication and authorization

To enable mandatory database node authorization, add the following configuration blocks to the [static cluster configuration](../../reference/configuration/index.md) file:

1. At the root level, add the `client_certificate_authorization` block to define the requirements for the "Subject" field of trusted node certificates. For example:

    ```yaml
    client_certificate_authorization:
      request_client_certificate: true
      client_certificate_definitions:
        - member_groups: ["registerNode@cert"]
          subject_terms:
          - short_name: "O"
            values: ["YDB"]
    ```

    Add other certificate validation settings [as defined in the documentation](../../reference/configuration/node-authentication.md), if required.

    If the certificate is successfully verified and the components of the "Subject" field comply with the requirements defined in the `subject_terms` sub-block, the connection will be assigned the access subjects listed in the `member_groups` parameter. To distinguish these subjects from other user groups and accounts, their names typically have the `@cert` suffix.

1. Add the `register_dynamic_node_allowed_sids` element to the cluster authentication settings `security_config` block, and list the subjects permitted for database node registration. For internal technical reasons, the list must include the `root@builtin` element. Example:

    ```yaml
    domains_config:
        ...
        security_config:
            enforce_user_token_requirement: true
            monitoring_allowed_sids:
            ...
            administration_allowed_sids:
            ...
            viewer_allowed_sids:
            ...
            register_dynamic_node_allowed_sids:
            - "root@builtin" # required for internal technical reasons
            - "registerNode@cert"
    ```

    For more detailed information on configuring cluster authentication parameters, see the [relevant documentation section](../../reference/configuration/index.md#security-access-levels).

1. Deploy the static configuration files on all cluster nodes either manually, or [using the Ansible playbook action](../ansible/update-config.md).

1. Perform the rolling restart of storage nodes by [using ydbops](../../reference/ydbops/scenarios.md) or [Ansible playbook action](../ansible/restart.md).

1. Perform the rolling restart of database nodes through ydbops or Ansible playbooks.
