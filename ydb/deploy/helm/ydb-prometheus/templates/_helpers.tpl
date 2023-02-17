{{/*
Expand the name of the chart.
*/}}
{{- define "ydb-prometheus.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "ydb-prometheus.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "ydb-prometheus.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "ydb-prometheus.labels" -}}
helm.sh/chart: {{ include "ydb-prometheus.chart" . }}
app.kubernetes.io/name: {{ include "ydb-prometheus.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "ydb-prometheus.labels.grafana" -}}
{{- $context := . -}}
{{ $context.Values.grafanaDashboards.markerLabel.key }}: {{ $context.Values.grafanaDashboards.markerLabel.value | quote }}
{{- end -}}

{{- define "ydb-prometheus.externalCluster.jobs" }}
{{- $context := . -}}
{{- $spec := index $context 0 -}}
{{- $counters := index $context 1 -}}
{{- $specType := index $context 2 -}}
  {{- range $counter := $counters }}
    {{- $type := include "ydb-prometheus.counter.type" $counter }}
    {{- if or (eq $type "all") (eq $type $specType) }}
- job_name: {{ include "ydb-prometheus.counter.jobName" (tuple $spec.name $counter) }}
  metrics_path: {{ include "ydb-prometheus.counter.metricsPath" $counter | quote }}
  relabel_configs:
  - source_labels:
    - __address__
    target_label: instance
    regex: '([^:]+)(:[0-9]+)?'
    replacement: '${1}'
  metric_relabel_configs:
  - source_labels:
    - __name__
    target_label: __name__
    regex: (.*)
    replacement: {{ $counter.counter }}_$1
  static_configs:
  - targets:
    {{- range $host := $spec.hosts }}
      {{- printf "- %s:%d" $host ($spec.port.number | int) | nindent 4 }}
    {{- end }}
    labels:
      counter: {{ $counter.counter | quote }}
      container: ydb-dynamic
      {{- end }}
  {{- end }}
{{- end }}

{{- define "ydb-prometheus.internalCluster.serviceMonitor" -}}
{{- $context := . -}}
{{- $spec := index $context 0 -}}
{{- $counters := index $context 1 -}}
{{- $specType := index $context 2 -}}
spec:
  endpoints:
    {{- range $counter := $counters }}
      {{- $type := include "ydb-prometheus.counter.type" $counter }}
      {{- if or (eq $type "all") (eq $type $specType) }}
  - path: {{ include "ydb-prometheus.counter.metricsPath" $counter }}
    port: {{ $spec.port.name }}
    metricRelabelings:
    - sourceLabels:
      - __name__
      targetLabel: __name__
      regex: (.*)
      replacement: {{ $counter.counter }}_$1
    relabelings:
    - sourceLabels:
      - __meta_kubernetes_namespace
      targetLabel: job
      regex: (.*)
      replacement: {{ include "ydb-prometheus.counter.jobName" (tuple (printf "$1/%s" $spec.name) $counter) }}
      {{- end }}
    {{- end }}
  namespaceSelector:
    matchNames:
    - {{ $spec.namespace }}
  selector:
    matchLabels: {{ $spec.selector | toYaml | nindent 6 }}
{{- end -}}

{{- define "ydb-prometheus.counter.jobName" -}}
{{- $context := . -}}
{{- $name := index $context 0 -}}
{{- $counter := index $context 1 -}}
{{- $name := printf "ydb/%s/counter/%s" $name $counter.counter | quote -}}
{{- $name -}}
{{- end -}}

{{- define "ydb-prometheus.counter.type" -}}
{{- $counter := . -}}
{{- $type := default "all" $counter.type -}}
{{- $type -}}
{{- end -}}

{{- define "ydb-prometheus.counter.metricsPath" -}}
{{- $counter := . -}}
{{- $metricsPath := default (printf "/counters/counters=%s/prometheus" $counter.counter) $counter.metricsPath -}}
{{- $metricsPath -}}
{{- end -}}