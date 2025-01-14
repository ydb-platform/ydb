# Database node authorization

The node authorization feature ensures that database nodes are authenticated when they are registered as part of a {{ ydb-short-name }} cluster. The use of node authorization is recommended for all {{ ydb-short-name }} clusters, as it allows to avoid the unauthorized access to data by including the nodes controlled by an attacker into the cluster.

Below are the steps required to enable the node authorization feature.

## Enabling TLS for gRPC and preparing node certificates

Before enabling the node authorization feature, you must [configure gRPC traffic encryption](./tls.md#grpc) using the TLS protocol.

When preparing node certificates for a cluster where you plan to use the node authorization feature, ensure uniform rules for populating the "Subject" field of the certificates. During node registration, {{ ydb-short-name }} verifies that the connecting node has a trusted certificate and checks that the "Subject" field meets the requirements specified in the cluster's static configuration. The "Subject" field may contain multiple components (such as `O` - organization, `OU` - organizational unit, `C` - country, `CN` - common name), and checks can be configured against one or more of these components.

The proposed [example script](https://github.com/ydb-platform/ydb/blob/main/ydb/deploy/tls_cert_gen/) generates self-signed certificates for {{ ydb-short-name }} nodes and ensures that the "Subject" field is populated with the value `O=YDB` for all node certificates. The settings examples provided below are prepared for certificates with this specific "Subject" field configuration, but feel free to use your real organization name instead.

The command-line parameters for [starting database nodes](../../devops/manual/initial-deployment.md#start-dynnode) must include options that specify the paths to the trusted CA certificate, the node certificate, and the node key files. The required additional command-line options are shown in the table below.

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

To enable mandatory database node authorization, add the following configuration blocks to the [static cluster configuration](./index.md) file:

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

    If the certificate is successfully verified and the components of the "Subject" field comply with the requirements defined in the `subject_terms` sub-block, the connection will be assigned the access subjects listed in the `member_groups` parameter. To distinguish these subjects from other user groups and accounts, their names typically have the `@cert` suffix.

1. Add the `register_dynamic_node_allowed_sids` element to the `domains_config / security_config` block, and list the subjects permitted for database node registration. For internal technical reasons, the list must include the `root@builtin` element. Example:

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
