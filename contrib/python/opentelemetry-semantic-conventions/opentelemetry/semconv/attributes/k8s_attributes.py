# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

from typing import Final

K8S_CLUSTER_NAME: Final = "k8s.cluster.name"
"""
The name of the cluster.
"""

K8S_CLUSTER_UID: Final = "k8s.cluster.uid"
"""
A pseudo-ID for the cluster, set to the UID of the `kube-system` namespace.
Note: K8s doesn't have support for obtaining a cluster ID. If this is ever
added, we will recommend collecting the `k8s.cluster.uid` through the
official APIs. In the meantime, we are able to use the `uid` of the
`kube-system` namespace as a proxy for cluster ID. Read on for the
rationale.

Every object created in a K8s cluster is assigned a distinct UID. The
`kube-system` namespace is used by Kubernetes itself and will exist
for the lifetime of the cluster. Using the `uid` of the `kube-system`
namespace is a reasonable proxy for the K8s ClusterID as it will only
change if the cluster is rebuilt. Furthermore, Kubernetes UIDs are
UUIDs as standardized by
[ISO/IEC 9834-8 and ITU-T X.667](https://www.itu.int/ITU-T/studygroups/com17/oid.html).
Which states:

> If generated according to one of the mechanisms defined in Rec.
> ITU-T X.667 | ISO/IEC 9834-8, a UUID is either guaranteed to be
> different from all other UUIDs generated before 3603 A.D., or is
> extremely likely to be different (depending on the mechanism chosen).

Therefore, UIDs between clusters should be extremely unlikely to
conflict.
"""

K8S_CONTAINER_NAME: Final = "k8s.container.name"
"""
The name of the Container from Pod specification, must be unique within a Pod. Container runtime usually uses different globally unique name (`container.name`).
"""

K8S_CONTAINER_RESTART_COUNT: Final = "k8s.container.restart_count"
"""
Number of times the container was restarted. This attribute can be used to identify a particular container (running or stopped) within a container spec.
"""

K8S_CRONJOB_ANNOTATION_TEMPLATE: Final = "k8s.cronjob.annotation"
"""
The cronjob annotation placed on the CronJob, the `<key>` being the annotation name, the value being the annotation value.
Note: Examples:

- An annotation `retries` with value `4` SHOULD be recorded as the
  `k8s.cronjob.annotation.retries` attribute with value `"4"`.
- An annotation `data` with empty string value SHOULD be recorded as
  the `k8s.cronjob.annotation.data` attribute with value `""`.
"""

K8S_CRONJOB_LABEL_TEMPLATE: Final = "k8s.cronjob.label"
"""
The label placed on the CronJob, the `<key>` being the label name, the value being the label value.
Note: Examples:

- A label `type` with value `weekly` SHOULD be recorded as the
  `k8s.cronjob.label.type` attribute with value `"weekly"`.
- A label `automated` with empty string value SHOULD be recorded as
  the `k8s.cronjob.label.automated` attribute with value `""`.
"""

K8S_CRONJOB_NAME: Final = "k8s.cronjob.name"
"""
The name of the CronJob.
"""

K8S_CRONJOB_UID: Final = "k8s.cronjob.uid"
"""
The UID of the CronJob.
"""

K8S_DAEMONSET_ANNOTATION_TEMPLATE: Final = "k8s.daemonset.annotation"
"""
The annotation placed on the DaemonSet, the `<key>` being the annotation name, the value being the annotation value, even if the value is empty.
Note: Examples:

- An annotation `replicas` with value `1` SHOULD be recorded
  as the `k8s.daemonset.annotation.replicas` attribute with value `"1"`.
- An annotation `data` with empty string value SHOULD be recorded as
  the `k8s.daemonset.annotation.data` attribute with value `""`.
"""

K8S_DAEMONSET_LABEL_TEMPLATE: Final = "k8s.daemonset.label"
"""
The label placed on the DaemonSet, the `<key>` being the label name, the value being the label value, even if the value is empty.
Note: Examples:

- A label `app` with value `guestbook` SHOULD be recorded
  as the `k8s.daemonset.label.app` attribute with value `"guestbook"`.
- A label `injected` with empty string value SHOULD be recorded as
  the `k8s.daemonset.label.injected` attribute with value `""`.
"""

K8S_DAEMONSET_NAME: Final = "k8s.daemonset.name"
"""
The name of the DaemonSet.
"""

K8S_DAEMONSET_UID: Final = "k8s.daemonset.uid"
"""
The UID of the DaemonSet.
"""

K8S_DEPLOYMENT_ANNOTATION_TEMPLATE: Final = "k8s.deployment.annotation"
"""
The annotation placed on the Deployment, the `<key>` being the annotation name, the value being the annotation value, even if the value is empty.
Note: Examples:

- An annotation `replicas` with value `1` SHOULD be recorded
  as the `k8s.deployment.annotation.replicas` attribute with value `"1"`.
- An annotation `data` with empty string value SHOULD be recorded as
  the `k8s.deployment.annotation.data` attribute with value `""`.
"""

K8S_DEPLOYMENT_LABEL_TEMPLATE: Final = "k8s.deployment.label"
"""
The label placed on the Deployment, the `<key>` being the label name, the value being the label value, even if the value is empty.
Note: Examples:

- A label `app` with value `guestbook` SHOULD be recorded
  as the `k8s.deployment.label.app` attribute with value `"guestbook"`.
- A label `injected` with empty string value SHOULD be recorded as
  the `k8s.deployment.label.injected` attribute with value `""`.
"""

K8S_DEPLOYMENT_NAME: Final = "k8s.deployment.name"
"""
The name of the Deployment.
"""

K8S_DEPLOYMENT_UID: Final = "k8s.deployment.uid"
"""
The UID of the Deployment.
"""

K8S_JOB_ANNOTATION_TEMPLATE: Final = "k8s.job.annotation"
"""
The annotation placed on the Job, the `<key>` being the annotation name, the value being the annotation value, even if the value is empty.
Note: Examples:

- An annotation `number` with value `1` SHOULD be recorded
  as the `k8s.job.annotation.number` attribute with value `"1"`.
- An annotation `data` with empty string value SHOULD be recorded as
  the `k8s.job.annotation.data` attribute with value `""`.
"""

K8S_JOB_LABEL_TEMPLATE: Final = "k8s.job.label"
"""
The label placed on the Job, the `<key>` being the label name, the value being the label value, even if the value is empty.
Note: Examples:

- A label `jobtype` with value `ci` SHOULD be recorded
  as the `k8s.job.label.jobtype` attribute with value `"ci"`.
- A label `automated` with empty string value SHOULD be recorded as
  the `k8s.job.label.automated` attribute with value `""`.
"""

K8S_JOB_NAME: Final = "k8s.job.name"
"""
The name of the Job.
"""

K8S_JOB_UID: Final = "k8s.job.uid"
"""
The UID of the Job.
"""

K8S_NAMESPACE_ANNOTATION_TEMPLATE: Final = "k8s.namespace.annotation"
"""
The annotation placed on the Namespace, the `<key>` being the annotation name, the value being the annotation value, even if the value is empty.
Note: Examples:

- An annotation `ttl` with value `0` SHOULD be recorded
  as the `k8s.namespace.annotation.ttl` attribute with value `"0"`.
- An annotation `data` with empty string value SHOULD be recorded as
  the `k8s.namespace.annotation.data` attribute with value `""`.
"""

K8S_NAMESPACE_LABEL_TEMPLATE: Final = "k8s.namespace.label"
"""
The label placed on the Namespace, the `<key>` being the label name, the value being the label value, even if the value is empty.
Note: Examples:

- A label `kubernetes.io/metadata.name` with value `default` SHOULD be recorded
  as the `k8s.namespace.label.kubernetes.io/metadata.name` attribute with value `"default"`.
- A label `data` with empty string value SHOULD be recorded as
  the `k8s.namespace.label.data` attribute with value `""`.
"""

K8S_NAMESPACE_NAME: Final = "k8s.namespace.name"
"""
The name of the namespace that the pod is running in.
"""

K8S_NODE_ANNOTATION_TEMPLATE: Final = "k8s.node.annotation"
"""
The annotation placed on the Node, the `<key>` being the annotation name, the value being the annotation value, even if the value is empty.
Note: Examples:

- An annotation `node.alpha.kubernetes.io/ttl` with value `0` SHOULD be recorded as
  the `k8s.node.annotation.node.alpha.kubernetes.io/ttl` attribute with value `"0"`.
- An annotation `data` with empty string value SHOULD be recorded as
  the `k8s.node.annotation.data` attribute with value `""`.
"""

K8S_NODE_LABEL_TEMPLATE: Final = "k8s.node.label"
"""
The label placed on the Node, the `<key>` being the label name, the value being the label value, even if the value is empty.
Note: Examples:

- A label `kubernetes.io/arch` with value `arm64` SHOULD be recorded
  as the `k8s.node.label.kubernetes.io/arch` attribute with value `"arm64"`.
- A label `data` with empty string value SHOULD be recorded as
  the `k8s.node.label.data` attribute with value `""`.
"""

K8S_NODE_NAME: Final = "k8s.node.name"
"""
The name of the Node.
"""

K8S_NODE_UID: Final = "k8s.node.uid"
"""
The UID of the Node.
"""

K8S_POD_ANNOTATION_TEMPLATE: Final = "k8s.pod.annotation"
"""
The annotation placed on the Pod, the `<key>` being the annotation name, the value being the annotation value.
Note: Examples:

- An annotation `kubernetes.io/enforce-mountable-secrets` with value `true` SHOULD be recorded as
  the `k8s.pod.annotation.kubernetes.io/enforce-mountable-secrets` attribute with value `"true"`.
- An annotation `mycompany.io/arch` with value `x64` SHOULD be recorded as
  the `k8s.pod.annotation.mycompany.io/arch` attribute with value `"x64"`.
- An annotation `data` with empty string value SHOULD be recorded as
  the `k8s.pod.annotation.data` attribute with value `""`.
"""

K8S_POD_HOSTNAME: Final = "k8s.pod.hostname"
"""
Specifies the hostname of the Pod.
Note: The K8s Pod spec has an optional hostname field, which can be used to specify a hostname.
Refer to [K8s docs](https://kubernetes.io/docs/concepts/services-networking/dns-pod-service/#pod-hostname-and-subdomain-field)
for more information about this field.

This attribute aligns with the `hostname` field of the
[K8s PodSpec](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.34/#podspec-v1-core).
"""

K8S_POD_IP: Final = "k8s.pod.ip"
"""
IP address allocated to the Pod.
Note: This attribute aligns with the `podIP` field of the
[K8s PodStatus](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.34/#podstatus-v1-core).
"""

K8S_POD_LABEL_TEMPLATE: Final = "k8s.pod.label"
"""
The label placed on the Pod, the `<key>` being the label name, the value being the label value.
Note: Examples:

- A label `app` with value `my-app` SHOULD be recorded as
  the `k8s.pod.label.app` attribute with value `"my-app"`.
- A label `mycompany.io/arch` with value `x64` SHOULD be recorded as
  the `k8s.pod.label.mycompany.io/arch` attribute with value `"x64"`.
- A label `data` with empty string value SHOULD be recorded as
  the `k8s.pod.label.data` attribute with value `""`.
"""

K8S_POD_NAME: Final = "k8s.pod.name"
"""
The name of the Pod.
"""

K8S_POD_START_TIME: Final = "k8s.pod.start_time"
"""
The start timestamp of the Pod.
Note: Date and time at which the object was acknowledged by the Kubelet.
This is before the Kubelet pulled the container image(s) for the pod.

This attribute aligns with the `startTime` field of the
[K8s PodStatus](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.34/#podstatus-v1-core),
in ISO 8601 (RFC 3339 compatible) format.
"""

K8S_POD_UID: Final = "k8s.pod.uid"
"""
The UID of the Pod.
"""

K8S_REPLICASET_ANNOTATION_TEMPLATE: Final = "k8s.replicaset.annotation"
"""
The annotation placed on the ReplicaSet, the `<key>` being the annotation name, the value being the annotation value, even if the value is empty.
Note: Examples:

- An annotation `replicas` with value `0` SHOULD be recorded
  as the `k8s.replicaset.annotation.replicas` attribute with value `"0"`.
- An annotation `data` with empty string value SHOULD be recorded as
  the `k8s.replicaset.annotation.data` attribute with value `""`.
"""

K8S_REPLICASET_LABEL_TEMPLATE: Final = "k8s.replicaset.label"
"""
The label placed on the ReplicaSet, the `<key>` being the label name, the value being the label value, even if the value is empty.
Note: Examples:

- A label `app` with value `guestbook` SHOULD be recorded
  as the `k8s.replicaset.label.app` attribute with value `"guestbook"`.
- A label `injected` with empty string value SHOULD be recorded as
  the `k8s.replicaset.label.injected` attribute with value `""`.
"""

K8S_REPLICASET_NAME: Final = "k8s.replicaset.name"
"""
The name of the ReplicaSet.
"""

K8S_REPLICASET_UID: Final = "k8s.replicaset.uid"
"""
The UID of the ReplicaSet.
"""

K8S_STATEFULSET_ANNOTATION_TEMPLATE: Final = "k8s.statefulset.annotation"
"""
The annotation placed on the StatefulSet, the `<key>` being the annotation name, the value being the annotation value, even if the value is empty.
Note: Examples:

- An annotation `replicas` with value `1` SHOULD be recorded
  as the `k8s.statefulset.annotation.replicas` attribute with value `"1"`.
- An annotation `data` with empty string value SHOULD be recorded as
  the `k8s.statefulset.annotation.data` attribute with value `""`.
"""

K8S_STATEFULSET_LABEL_TEMPLATE: Final = "k8s.statefulset.label"
"""
The label placed on the StatefulSet, the `<key>` being the label name, the value being the label value, even if the value is empty.
Note: Examples:

- A label `app` with value `guestbook` SHOULD be recorded
  as the `k8s.statefulset.label.app` attribute with value `"guestbook"`.
- A label `injected` with empty string value SHOULD be recorded as
  the `k8s.statefulset.label.injected` attribute with value `""`.
"""

K8S_STATEFULSET_NAME: Final = "k8s.statefulset.name"
"""
The name of the StatefulSet.
"""

K8S_STATEFULSET_UID: Final = "k8s.statefulset.uid"
"""
The UID of the StatefulSet.
"""
