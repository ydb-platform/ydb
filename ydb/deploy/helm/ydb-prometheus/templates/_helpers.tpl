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

{{- define "ydb-prometheus.externalCluster.config" -}}
{{- $context := . -}}
{{- $counters := .Values.ydb.counters -}}
{{- $clusters := .Values.ydb.clusters -}}
{{- $result := list -}}
{{- range $cluster := $clusters }}
  {{- if eq $cluster.type "external" }}

    {{- range $counter := $counters }}
      {{- $name := "staticNode" }}
      {{- $port := $cluster.ports.static }}
      {{- $type := default "all" $counter.type }}
      {{- if or (eq $type "static") (eq $type "all") }}
        {{- $config := include "ydb-prometheus.externalCluster.targetCounter" (tuple $cluster $counter $name $port) | fromYaml }}
        {{- $result = append $result $config }}
      {{- end }}
    {{- end }}

    {{- range $port := $cluster.ports.dynamic }}
      {{- range $counter := $counters }}
        {{- $name := printf "dynamicNode/%d" ($port | int) }}
        {{- $type := default "all" $counter.type }}
        {{- if or (eq $type "dynamic") (eq $type "all") }}
          {{- $config := include "ydb-prometheus.externalCluster.targetCounter" (tuple $cluster $counter $name $port) | fromYaml }}
          {{- $result = append $result $config }}
        {{- end }}
      {{- end }}
    {{- end }}

{{- end }}
{{- end }}

{{- $result | toYaml }}
{{- end -}}

{{- define "ydb-prometheus.externalCluster.targetCounter" }}
{{- $context := . }}
{{- $cluster := index $context 0 }}
{{- $counter := index $context 1 }}
{{- $name := index $context 2 }}
{{- $port := index $context 3 }}
{{- $metricsPath := (printf "/counters/counters=%s/prometheus" $counter.counter) }}
{{- if $counter.metricsPath }}
{{- $metricsPath = $counter.metricsPath }}
{{- end }}
job_name: {{ printf "ydb/%s/%s/counter/%s" $cluster.cluster $name $counter.counter | quote }}
metrics_path: {{ $metricsPath | quote }}
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
  {{- range $host := $cluster.hosts }}
    {{- printf "- %s:%d" $host ($port | int) | nindent 4 }}
  {{- end }}
  labels:
    project: {{ $cluster.cluster }}
    counter: {{ $counter.counter | quote }}
    container: ydb-dynamic
{{- end }}