---
title: "Instructions for troubleshooting issues with {{ ydb-short-name }}"
description: "The section contains code recipes for enabling diagnostics tools in different {{ ydb-short-name }} SDKs."
---

# Troubleshooting

{% include [work in progress message](_includes/addition.md) %}

When troubleshooting issues with {{ ydb-short-name }}, diagnostics tools such as logging, metrics, OpenTracing/Jaeger tracing are helpful. We strongly recommend that you enable them in advance before any problems occur. This will help see changes in the overall picture before, during, and after an issue when troubleshooting it. This greatly speeds up our investigation into incidents and lets us provide assistance much faster.

This section contains code recipes for enabling diagnostics tools in different {{ ydb-short-name }} SDKs.

Table of contents:
- [Enable logging](debug-logs.md)
- [Enable metrics in Prometheus](debug-prometheus.md)
- [Enable tracing in Jaeger](debug-jaeger.md)

