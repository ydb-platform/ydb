# Database node authentication

Database node authentication within the {{ ydb-short-name }} cluster ensures that service connections between cluster nodes are assigned the correct security identifiers, or [SIDs](../../concepts/glossary.md#access-sid). The process of database node authentication applies to connections that use the gRPC protocol and provide functions for registering nodes in the cluster, as well as for accessing configuration information. SIDs assigned to connections are taken into account when checking the authorization rules that apply to the corresponding gRPC service calls.

Node authentication settings are configured within the [static configuration](./index.md) of the cluster.

## client_certificate_authorization - node authentication settings

This section specified the authentication settings for database node connections, by defining the requirements for the content of "Subject" and "Subject Alternative Name" node certificates, and the list of [SID](../../concepts/glossary.md#access-sid) values being assinged to the connections.

The "Subject" field of the node certificate may contain multiple components (such as `O` - organization, `OU` - organizational unit, `C` - country, `CN` - common name), and checks can be configured against one or more of these components.

The "Subject Alternative Name" field of the node certificate is the list of network names or IP-addresses of the node. Checks can be configured to match the names specified in the certificate against the expected values.

### Syntax

```yaml
client_certificate_authorization:
  request_client_certificate: Bool
  default_group: <default SID>
  client_certificate_definitions:
    - member_groups: <SID array>
      require_same_issuer: Bool
      subject_dns:
      - suffixes: <array of allowed suffixes>
        values: <array of allowed values>
      subject_terms:
      - short_name: <Subject Name component>
        suffixes: <array of allowed suffixes>
        values: <array of allowed values>
    - member_groups: <SID array>
    ...
```

Key  | Description
---- | ---
`request_client_certificate` | Request the valid client certificate for node connections.<br/>Allowed values:<br/><ul><li>`false` — certificate is not required (used by default if the parameter is omitted);</li><li>`true` — certificate is required for all node connections.</li></ul>
`default_group` | SID being assigned to all connections providing the trusted client certificate when no explicit settings are provided in the `client_certificate_definitions` section
`client_certificate_definitions` | Section defining the requirements for database node certificates
`member_groups` | SIDs which are assigned to the connections that conform to the requirements of the current configuration block
`require_same_issuer` | Require that the value of the "Issuer" field (typically containing the Certification Authority name) should be the same for the client (database node) and server (storage node) certificates.<br/>Allowed values:<br/><ul><li>`true` — the values must be the same (used by default if the parameter is omitted);</li><li>`false` — the values can be different (which effectively means that client and server certificates may be issued by different Certification Authorities).</li></ul>
`subject_dns` | Allowed values for the "Subject Alternative Name" field, specified as allowed full values (sub-key `values` used) or allowed suffixes (sub-key `suffixes` used). The check is successful when the actual value matches any full name or any suffix specified.
`subject_terms` | The requirements for the "Subject" field value. Contains the component name (in the sub-key `short_name`), and the list of full values (sub-key `values`) or suffixes (sub-key `suffixes`). The check is successful when the actual value of each component matches either the allowed full value, or the allowed suffix.

### Examples

The next configuration fragment enables node authentication, and requires the "Subject" field to include the component `O=YDB`. Upon successful authentication, the connection will be assigned the `registerNode@cert` SID.

```yaml
client_certificate_authorization:
  request_client_certificate: true
  client_certificate_definitions:
    - member_groups: ["registerNode@cert"]
      subject_terms:
      - short_name: "O"
        values: ["YDB"]
```

The next configuration fragment enables node authentication, and requires "Subject" field to include both `OU=cluster1` and `O=YDB` components. In addition "Subject Alternative Name" field should contain the network name ending with the `.cluster1.ydb.company.net` suffix. Upon successful authentication, the connection will be assigned the `registerNode@cert` SID.

```yaml
client_certificate_authorization:
  request_client_certificate: true
  client_certificate_definitions:
    - member_groups: ["registerNode@cert"]
      subject_dns:
      - suffixes: [".cluster1.ydb.company.net"]
      subject_terms:
      - short_name: "OU"
        values: ["cluster1"]
      - short_name: "O"
        values: ["YDB"]
```
