# client_certificate_authorization

The `client_certificate_authorization` section contains settings for requesting [client certificate](../../concepts/glossary.md#client-certificate) during [device authentication](../../security/authentication.md#device-auth-interfaces) via gRPCs and defines rules for verifying client SSL certificates and generating user [SID](../../concepts/glossary.md#access-sid) [during client certificate authentication](../../security/authentication.md#client-certificate). The settings are specified in the [static configuration](./index.md) of the cluster. The nested block `client_certificate_definitions` defines requirements for filling in the "Subject" and "Subject Alternative Name" fields of client certificates, as well as the list of assigned group SIDs.

Client certificates are described by the [X.509](https://en.wikipedia.org/wiki/X.509) standard. The "Subject" field of a certificate consists of several components (for example, `O` — organization, `OU` — organizational unit, `C` — country, `CN` — subject's common name). Checks can be configured to match one or more components of the field against expected values.

The "Subject Alternative Name" field of a certificate is a list of network names or IP addresses. A check can be configured to match the network names in the certificate against expected values.

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
| `request_client_certificate` | Requesting a client certificate during TLS handshake for gRPCs.<br/>Allowed values:<br/><ul><li>`true` — the server requests a client certificate. If a certificate is presented, it is verified at the TLS level during [device authentication](../../security/authentication.md#device-auth-interfaces) (a connection with an untrusted certificate is not established); after successful verification, the server preferentially uses [authentication token](../../concepts/glossary.md#auth-token) to authenticate the client, and if no token is present, authentication is performed using [client certificate](../../security/authentication.md#client-certificate). If no certificate is presented, then with the `client_certificate_required: true` setting, the connection is not established.</li><li>`false` — the server does not request a client certificate during TLS handshake; device authentication and client certificate authentication via gRPCs are unavailable with this setting.<br/>Default value: `false`.</li></ul> |
| `client_certificate_required` | Requiring a client certificate during TLS handshake for gRPCs.<br/>Allowed values:<br/><ul><li>`true` — the server requires a client certificate: a connection without a certificate or with an untrusted certificate is not established. `true` can only be specified together with `request_client_certificate: true`.</li><li>`false` — a client certificate is not required; the behavior when requesting a certificate is determined by the `request_client_certificate` parameter.<br/>Default value: `false`.</li></ul> |
| `default_group` | SID assigned to all connections with a trusted client certificate if no explicit settings are specified in the `client_certificate_definitions` section.<br/>Default value: `DefaultClientAuth@cert`. |
| `client_certificate_definitions` | Block of settings for client certificate requirements. |
| `member_groups` | Array of group SIDs assigned to connections whose certificates meet the requirements of this block. |
| `require_same_issuer` | Require the "Issuer" field (certification authority name) to match for client and server certificates.<br/>Valid values:<br/><ul><li>`true` — match required (applied by default if the parameter is not set);</li><li>`false` — match not required (client and server certificates may be issued by different certification authorities).</li></ul> |
| `subject_dns` | Valid values of the "Subject Alternative Name" field as an array of full values (key `values`) or an array of value suffixes (key `suffixes`). The check is considered successful if one of the field values matches any full name or matches any specified suffix. |
| `subject_terms` | Requirements for filling in the components of the "Subject" field. The component name (key `short_name`) is specified, as well as a set of full values (key `values`) or a set of value suffixes (key `suffixes`). The check is considered successful if, for each checked component of the "Subject" field, its value matches one of the allowed full values or one of the allowed suffixes. |

## Examples

The following configuration snippet requires that the "Subject" field of the client certificate contain the `O=YDB` and `CN=user1` components. For such a certificate, a user SID `O=YDB,CN=user1@cert` will be generated and the group `group@cert` will be assigned:


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


The `CN` component may specify the server network name rather than the user name. This option is advisable to use when [registering dynamic nodes](../../devops/concepts/node-authorization.md#enabling-the-node-authentication-and-authorization-mode). The following configuration snippet requires that the "Subject" field of the node's client certificate contain the `O=YDB` and `CN=server1.internal.corp` components. For such a certificate, a SID `O=YDB,CN=server1.internal.corp@cert` will be generated and the group `registerNode@cert` will be assigned:


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
