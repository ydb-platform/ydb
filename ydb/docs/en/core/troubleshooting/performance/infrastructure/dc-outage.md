# Data center outages

Data center outages are disruptions in data center operations that could cause service or data unavailability, but {{ ydb-short-name }} has means to avoid it. Various factors, such as power failures, natural disasters, or cyberattacks, may cause these outages. A common fault-tolerant setup for {{ ydb-short-name }} spans three data centers or availability zones (AZs). In this case, {{ ydb-short-name }} can maintain uninterrupted operation even if one data center and a server rack in another are lost. However, it will initiate the relocation of tablets from the offline AZ to the remaining online nodes, temporarily leading to higher query latencies.

## Diagnostics

<!-- The include is added to allow partial overrides in overlays  -->
{% include notitle [dc-outage](_includes/dc-outage.md) %}

## Recommendations

Contact the responsible party for the affected data center to resolve the underlying issue. If you are part of a larger organization, this could be an in-house team managing low-level infrastructure. Otherwise, contact the cloud service or hosting provider's support service. Meanwhile, check the data center's status page if it has one.

Additionally, consider potential data center outages in the capacity planning process. {{ ydb-short-name }} nodes in each data center should have sufficient spare hardware resources to take over the full workload typically handled by any data center experiencing an outage.
