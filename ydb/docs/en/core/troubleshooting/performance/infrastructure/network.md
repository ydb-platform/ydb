# Network issues

Network performance issues, such as limited bandwidth, packet loss, and connection instability, can severely impact database performance by slowing query response times and leading to retriable errors like timeouts.

## Diagnostics

<!-- The include is added to allow partial overrides in overlays  -->
{% include notitle [network](_includes/network.md) %}

## Recommendations

Contact the responsible party for the network infrastructure the {{ ydb-short-name }} cluster uses. If you are part of a larger organization, this could be an in-house network operations team. Otherwise, contact the cloud service or hosting provider's support service.