# Data center outages

Data center outages are disruptions in data center operations that can cause service or data unavailability. These outages may result from various factors, such as power failures, natural disasters, or cyber-attacks. A common fault-tolerant setup for {{ ydb-short-name }} spans three data centers or availability zones (AZs). {{ ydb-short-name }} can continue operating without interruption, even if one data center and a server rack in another are lost. However, it will initiate the relocation of tablets from the offline AZ to the remaining online nodes, temporarily leading to higher query latencies. Distributed transactions involving tablets that are moving to other nodes might experience increased latencies.

## Diagnostics

<!-- The include is added to allow partial overrides in overlays  -->
{% include notitle [dc-outage](_includes/dc-outage.md) %}

## Recommendations

Contact your data center support.
