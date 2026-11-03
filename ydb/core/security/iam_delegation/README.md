# IAM delegation control service

The node service submits setup/revoke once and polls known operation IDs. The
native API distinguishes `Accepted`, `Succeeded`, `Failed` and `Unknown`.
Transport ambiguity never justifies replaying a mutation. A durable lifecycle
owner must persist the binding, request ID and accepted operation ID; that owner,
SQL admission, token minting and cleanup workers are separate changes.

`IamConfig` is a constructor-owned startup snapshot. Configure the service
identity, `ServiceControlEndpoint`, `SystemTokenName` (an existing enabled token
manager provider) and `DelegationTabletId`. Provision an `IamDelegation` tablet
independently of tenant lifetime. The feature flag can start the node service
dynamically; changing its configuration requires restart. Disabling admission
does not tear down an already running service.

All control requests share the tablet gate for their current endpoint and system
identity. Healthy requests run concurrently. One failure advances a durable
version and standard `TBackoff` deadline. A waking caller checks that version:
only one recovery request obtains the next version. Its finite reservation
expires after caller loss. Late reports cannot reset newer failures. The actor
helper coalesces waits; only the tablet authorizes dispatch. Permission refusal
is reachable service health; rejected credentials advance the shared cooldown.

Errors distinguish rejected credentials from missing rights and include fixed
method-specific requirements. Remote messages, arbitrary detail types, token
manager diagnostics and credentials are excluded from returned diagnostics.

Tests beside this component exercise native outcomes, redaction, token updates,
RPC integration, dynamic registration and exact coroutine deadlines. Tablet
scenarios live in `tablet/ut`; shared timer/cancellation scenarios in
`core/util/async_backoff_ut`.

Upgrade: provision the service tablet before enabling clients. A missing or
older backend fails closed. Protocol fields and table IDs are additive; rolling
back node clients leaves durable cooldown state in the service tablet. Deploying
new tablet types requires the hosting binaries to support that type; evacuate
such tablets before rolling tablet hosts back to versions without the factory.
