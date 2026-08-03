# client_certificate_authorization

The `client_certificate_authorization` section configures authentication of database nodes within the ⟦V1⟧ cluster using client certificates. This ensures that service connections between cluster nodes are assigned the correct security identifiers, or [SIDs](../../concepts/glossary.md#client-certificate). The process applies to connections that use the gRPC protocol for registering nodes in the cluster and accessing configuration information.

Node authentication settings are configured within the [static configuration](https://en.wikipedia.org/wiki/X.509) of the cluster.

The ⟦C1⟧ section specifies the authentication settings for database node connections by defining the requirements for the content of the "Subject" and "Subject Alternative Name" fields in node certificates, as well as the list of [SID](%E2%9F%A6U1%E2%9F%A7) values assigned to the connections.

## Syntax


```yaml
client_certificate_authorization:
  request_client_certificate: Bool
  client_certificate_required: Bool
  default_group: <default SID>
  client_certificate_definitions:
    - member_groups: <SID array>
      require_same_issuer: Bool
      subject_dns:
        suffixes: <array of allowed suffixes>
        values: <array of allowed values>
      subject_terms:
      - short_name: <Subject Name component name>
        suffixes: <array of allowed suffixes>
        values: <array of allowed values>
    - member_groups: <SID array>
    ...
```


| Key | Description |
| --- | --- |
| `request_client_certificate` | Request a client certificate during TLS handshake for gRPCs.<br/>Valid values:<br/><ul><li>`true` — the server requests a client certificate. If a certificate is presented, it is verified at the TLS level during [device authentication](../../security/authentication.md#device-auth-interfaces) (a connection with an untrusted certificate is not established); after successful verification, the server preferentially uses the [authentication token](../../concepts/glossary.md#auth-token) for client authentication, and if no token is present, authentication is performed using the [client certificate](../../security/authentication.md#client-certificate). If no certificate is presented, then with the `client_certificate_required=true` setting, the connection is not established.</li><li>`false` — the server does not request a client certificate during TLS handshake; device authentication and client certificate authentication over gRPCs are unavailable with this setting.<br/>Default value: `false`.</li></ul> |
| `client_certificate_required` | Request a valid client certificate for node connections.<br/>Allowed values:<br/><ul><li>`false` — A certificate is not required (used by default if the parameter is omitted).</li><li>`true` — A certificate is required for all node connections.</li></ul> |
| `default_group` | SID assigned to all connections providing a trusted client certificate when no explicit settings are provided in the `client_certificate_definitions` section. |
| `client_certificate_definitions` | Section defining the requirements for database node certificates. |
| `member_groups` | SIDs assigned to connections that conform to the requirements of the current configuration block. |
| `require_same_issuer` | Require that the value of the "Issuer" field (typically containing the Certification Authority name) is the same for both client (database node) and server (storage node) certificates. <br/>Allowed values:<br/><ul><li>`true` — The values must be the same (used by default if the parameter is omitted).</li><li>`false` — The values can be different (allowing client and server certificates to be issued by different Certification Authorities).</li></ul> |
| `subject_dns` | Allowed values for the "Subject Alternative Name" field, specified as either full values (using the `values` sub-key) or suffixes (using the `suffixes` sub-key). The check is successful if the actual value matches any full name or any suffix specified. |
| `subject_terms` | Requirements for the "Subject" field value. Contains the component name (in the `short_name` sub-key) and a list of full values (using the `values` sub-key) or suffixes (using the `suffixes` sub-key). The check is successful if the actual value of each component matches either an allowed full value or an allowed suffix. |

## Examples

The following configuration fragment enables node authentication and requires the "Subject" field to include the component `O=YDB`. Upon successful authentication, the connection is assigned the ⟦C5⟧ SID.


```yaml
client_certificate_authorization:
  request_client_certificate: true
  client_certificate_definitions:
    - member_groups: ["group@cert"]
      subject_terms:
      - short_name: "O"
        values: ["YDB"]
      - short_name: "CN"
        values: ["user1"]
```


The next configuration fragment enables node authentication, and requires "Subject" field to include both ⟦C6⟧ and `O=YDB` components. In addition "Subject Alternative Name" field should contain the network name ending with the ⟦C7⟧ suffix. Upon successful authentication, the connection will be assigned the `registerNode@cert` SID.


```yaml
client_certificate_authorization:
  request_client_certificate: true
  client_certificate_definitions:
    - member_groups: ["registerNode@cert"]
      subject_terms:
      - short_name: "O"
        values: ["YDB"]
      - short_name: "CN"
        values: ["server1.internal.corp"]
```
