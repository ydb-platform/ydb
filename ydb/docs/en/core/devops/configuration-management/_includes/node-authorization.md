# Database Node Authentication and Authorization

Database node authentication in a {{ ydb-short-name }} cluster ensures verification of database node authenticity when making service calls to other nodes via the gRPC protocol. Node authorization ensures verification and provision of necessary permissions when processing service calls, including operations for registering starting nodes in the cluster and accessing [configuration](../configuration-v2/config-overview.md). Using database node authentication and authorization is recommended for all {{ ydb-short-name }} clusters, as it helps avoid situations of unauthorized data access through the inclusion of attacker-controlled nodes in the cluster.

Database node authentication and authorization is performed in the following order:

1. The starting database node opens a gRPC connection to one of the cluster storage nodes specified in the `--node-broker` command-line option. The connection uses the TLS protocol, and the starting node's certificate is used as the client certificate in the connection settings.
2. The storage node and database node perform mutual authenticity verification using the TLS protocol: the certificate trust chain is verified and the hostname correspondence to the "Subject Name" field value of the certificate is checked.
3. The storage node verifies that the "Subject" field of the certificate meets the requirements [established by settings](../../../reference/configuration/client_certificate_authorization.md) in the static configuration.
4. Upon successful completion of the above checks, the connection from the database node is considered authenticated, and it is assigned an access subject identifier — [SID](../../../concepts/glossary.md#access-sid), determined by the settings.
5. The database node uses the established gRPC connection to register as part of the cluster using the corresponding service call. During registration, the database node transmits its network address intended for interaction with other cluster nodes.
6. The storage node verifies that the SID assigned to the gRPC connection is in the list of allowed ones. Upon successful completion of this check, the storage node registers the database node in the cluster, mapping the received network address to the node identifier.
7. The database node joins the cluster by connecting through its network address and specifying the node identifier received during registration. Attempts to join the cluster by nodes with unknown network addresses or identifiers are blocked by other nodes.

The following describes the settings necessary to enable database node authentication and authorization.

## Configuration Prerequisites

1. The deployed {{ ydb-short-name }} cluster must have [gRPC traffic encryption configured](../../../reference/configuration/tls.md#grpc) using the TLS protocol.
2. When preparing node certificates for a cluster where database node authentication and authorization is planned to be used, it is necessary to ensure uniform rules for filling the "Subject" field of certificates, allowing identification of certificates issued for cluster nodes. More information is provided in the [certificate verification rules configuration documentation](../../../reference/configuration/client_certificate_authorization.md).

    {% note info %}

    The proposed [example script](https://github.com/ydb-platform/ydb/blob/main/ydb/deploy/tls_cert_gen/) for generating self-signed {{ ydb-short-name }} node certificates fills the "Subject" field with the value `O=YDB` for all node certificates. The configuration examples provided below are prepared for certificates with exactly this "Subject" field content.

    {% endnote %}

3. The command-line parameters for [starting database nodes](../../deployment-options/manual/initial-deployment.md#start-dynnode) must include options that specify paths to certificate files from trusted certificate authorities, a node certificate, and a node key. The list of additional options is provided in the table below.

    | **Command-line Option** | **Description** |
    |-------------------------|-----------------|
    | `--grpc-ca`             | Path to the certificate file `ca.crt` from a trusted certificate authority. |
    | `--grpc-cert`           | Path to the node certificate file `node.crt`. |
    | `--grpc-key`            | Path to the node private key file `node.key`. |

    Example command for starting a database node with options specifying paths to TLS keys and certificates for the gRPC protocol:

    ```bash
    /opt/ydb/bin/ydbd server --config-dir /opt/ydb/cfg --tenant /Root/testdb \
        --grpcs-port 2136 --grpc-ca /opt/ydb/certs/ca.crt \
        --grpc-cert /opt/ydb/certs/node.crt --grpc-key /opt/ydb/certs/node.key \
        --ic-port 19002 --ca /opt/ydb/certs/ca.crt \
        --mon-port 8766 --mon-cert /opt/ydb/certs/web.pem \
        --node-broker grpcs://<ydb1>:2135 \
        --node-broker grpcs://<ydb2>:2135 \
        --node-broker grpcs://<ydb3>:2135
    ```

## Enabling Database Node Authentication and Authorization

To enable mandatory database node authorization, the following configuration blocks must be added to the [cluster configuration file](../../../reference/configuration/index.md):

1. At the root level of the configuration, add a `client_certificate_authorization` block specifying requirements for filling the "Subject" field of trusted certificates of connecting nodes, for example:

    ```yaml
    client_certificate_authorization:
      request_client_certificate: true
      client_certificate_definitions:
        - member_groups: ["registerNode@cert"]
          subject_terms:
          - short_name: "O"
            values: ["YDB"]
    ```

    If necessary, add other certificate checks [according to the documentation](../../../reference/configuration/client_certificate_authorization.md).

    After the certificate is verified and its "Subject" field is confirmed to comply with the requirements established in the `subject_terms` block, the connection will be assigned access subjects from the `member_groups` parameter. To distinguish such access subjects, the `@cert` suffix is added to their names.

2. In the `security_config` section of the cluster configuration, add the `register_dynamic_node_allowed_sids` element with a list of access subjects allowed to register database nodes. For technical reasons, this list must also include the access subject `root@builtin`. Example:

    ```yaml
    security_config:
      enforce_user_token_requirement: true
      ...
      register_dynamic_node_allowed_sids:
      - "root@builtin" # required for technical reasons
      - "registerNode@cert"
    ```

    For more information on configuring cluster authentication parameters, see [{#T}](../../../reference/configuration/security_config.md#security-access-levels).

3. Update the static configuration files on all cluster nodes manually or using the [Ansible playbook](../../deployment-options/ansible/update-config.md).

4. Perform a rolling restart of cluster storage nodes [using ydbops](../../../reference/ydbops/rolling-restart-scenario.md) or using the [Ansible playbook](../../deployment-options/ansible/restart.md).

5. Perform a rolling restart of cluster database nodes using ydbops or using the Ansible playbook.
