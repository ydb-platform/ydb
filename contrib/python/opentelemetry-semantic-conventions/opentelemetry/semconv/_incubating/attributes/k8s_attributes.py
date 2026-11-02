# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

from enum import Enum
from typing import Final

K8S_CLUSTER_NAME: Final = "k8s.cluster.name"
"""
Deprecated in favor of stable :py:const:`opentelemetry.semconv.attributes.k8s_attributes.K8S_CLUSTER_NAME`.
"""

K8S_CLUSTER_UID: Final = "k8s.cluster.uid"
"""
Deprecated in favor of stable :py:const:`opentelemetry.semconv.attributes.k8s_attributes.K8S_CLUSTER_UID`.
"""

K8S_CONTAINER_EPHEMERAL_STORAGE_FS_TYPE: Final = (
    "k8s.container.ephemeral_storage.fs_type"
)
"""
The type of file system component for ephemeral storage.
Note: Eviction decisions based on ephemeral-storage resource limits are made based on the total container usage.
"""

K8S_CONTAINER_NAME: Final = "k8s.container.name"
"""
Deprecated in favor of stable :py:const:`opentelemetry.semconv.attributes.k8s_attributes.K8S_CONTAINER_NAME`.
"""

K8S_CONTAINER_RESTART_COUNT: Final = "k8s.container.restart_count"
"""
Deprecated in favor of stable :py:const:`opentelemetry.semconv.attributes.k8s_attributes.K8S_CONTAINER_RESTART_COUNT`.
"""

K8S_CONTAINER_STATUS_LAST_TERMINATED_REASON: Final = (
    "k8s.container.status.last_terminated_reason"
)
"""
Last terminated reason of the Container.
"""

K8S_CONTAINER_STATUS_REASON: Final = "k8s.container.status.reason"
"""
The reason for the container state. Corresponds to the `reason` field of the: [K8s ContainerStateWaiting](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.34/#containerstatewaiting-v1-core) or [K8s ContainerStateTerminated](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.34/#containerstateterminated-v1-core).
"""

K8S_CONTAINER_STATUS_STATE: Final = "k8s.container.status.state"
"""
The state of the container. [K8s ContainerState](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.34/#containerstate-v1-core).
"""

K8S_CRONJOB_ANNOTATION_TEMPLATE: Final = "k8s.cronjob.annotation"
"""
Deprecated in favor of stable :py:const:`opentelemetry.semconv.attributes.k8s_attributes.K8S_CRONJOB_ANNOTATION_TEMPLATE`.
"""

K8S_CRONJOB_LABEL_TEMPLATE: Final = "k8s.cronjob.label"
"""
Deprecated in favor of stable :py:const:`opentelemetry.semconv.attributes.k8s_attributes.K8S_CRONJOB_LABEL_TEMPLATE`.
"""

K8S_CRONJOB_NAME: Final = "k8s.cronjob.name"
"""
Deprecated in favor of stable :py:const:`opentelemetry.semconv.attributes.k8s_attributes.K8S_CRONJOB_NAME`.
"""

K8S_CRONJOB_UID: Final = "k8s.cronjob.uid"
"""
Deprecated in favor of stable :py:const:`opentelemetry.semconv.attributes.k8s_attributes.K8S_CRONJOB_UID`.
"""

K8S_DAEMONSET_ANNOTATION_TEMPLATE: Final = "k8s.daemonset.annotation"
"""
Deprecated in favor of stable :py:const:`opentelemetry.semconv.attributes.k8s_attributes.K8S_DAEMONSET_ANNOTATION_TEMPLATE`.
"""

K8S_DAEMONSET_LABEL_TEMPLATE: Final = "k8s.daemonset.label"
"""
Deprecated in favor of stable :py:const:`opentelemetry.semconv.attributes.k8s_attributes.K8S_DAEMONSET_LABEL_TEMPLATE`.
"""

K8S_DAEMONSET_NAME: Final = "k8s.daemonset.name"
"""
Deprecated in favor of stable :py:const:`opentelemetry.semconv.attributes.k8s_attributes.K8S_DAEMONSET_NAME`.
"""

K8S_DAEMONSET_UID: Final = "k8s.daemonset.uid"
"""
Deprecated in favor of stable :py:const:`opentelemetry.semconv.attributes.k8s_attributes.K8S_DAEMONSET_UID`.
"""

K8S_DEPLOYMENT_ANNOTATION_TEMPLATE: Final = "k8s.deployment.annotation"
"""
Deprecated in favor of stable :py:const:`opentelemetry.semconv.attributes.k8s_attributes.K8S_DEPLOYMENT_ANNOTATION_TEMPLATE`.
"""

K8S_DEPLOYMENT_LABEL_TEMPLATE: Final = "k8s.deployment.label"
"""
Deprecated in favor of stable :py:const:`opentelemetry.semconv.attributes.k8s_attributes.K8S_DEPLOYMENT_LABEL_TEMPLATE`.
"""

K8S_DEPLOYMENT_NAME: Final = "k8s.deployment.name"
"""
Deprecated in favor of stable :py:const:`opentelemetry.semconv.attributes.k8s_attributes.K8S_DEPLOYMENT_NAME`.
"""

K8S_DEPLOYMENT_UID: Final = "k8s.deployment.uid"
"""
Deprecated in favor of stable :py:const:`opentelemetry.semconv.attributes.k8s_attributes.K8S_DEPLOYMENT_UID`.
"""

K8S_HPA_METRIC_TYPE: Final = "k8s.hpa.metric.type"
"""
The type of metric source for the horizontal pod autoscaler.
Note: This attribute reflects the `type` field of spec.metrics[] in the HPA.
"""

K8S_HPA_NAME: Final = "k8s.hpa.name"
"""
The name of the horizontal pod autoscaler.
"""

K8S_HPA_SCALETARGETREF_API_VERSION: Final = (
    "k8s.hpa.scaletargetref.api_version"
)
"""
The API version of the target resource to scale for the HorizontalPodAutoscaler.
Note: This maps to the `apiVersion` field in the `scaleTargetRef` of the HPA spec.
"""

K8S_HPA_SCALETARGETREF_KIND: Final = "k8s.hpa.scaletargetref.kind"
"""
The kind of the target resource to scale for the HorizontalPodAutoscaler.
Note: This maps to the `kind` field in the `scaleTargetRef` of the HPA spec.
"""

K8S_HPA_SCALETARGETREF_NAME: Final = "k8s.hpa.scaletargetref.name"
"""
The name of the target resource to scale for the HorizontalPodAutoscaler.
Note: This maps to the `name` field in the `scaleTargetRef` of the HPA spec.
"""

K8S_HPA_UID: Final = "k8s.hpa.uid"
"""
The UID of the horizontal pod autoscaler.
"""

K8S_HUGEPAGE_SIZE: Final = "k8s.hugepage.size"
"""
The size (identifier) of the K8s huge page.
"""

K8S_JOB_ANNOTATION_TEMPLATE: Final = "k8s.job.annotation"
"""
Deprecated in favor of stable :py:const:`opentelemetry.semconv.attributes.k8s_attributes.K8S_JOB_ANNOTATION_TEMPLATE`.
"""

K8S_JOB_LABEL_TEMPLATE: Final = "k8s.job.label"
"""
Deprecated in favor of stable :py:const:`opentelemetry.semconv.attributes.k8s_attributes.K8S_JOB_LABEL_TEMPLATE`.
"""

K8S_JOB_NAME: Final = "k8s.job.name"
"""
Deprecated in favor of stable :py:const:`opentelemetry.semconv.attributes.k8s_attributes.K8S_JOB_NAME`.
"""

K8S_JOB_UID: Final = "k8s.job.uid"
"""
Deprecated in favor of stable :py:const:`opentelemetry.semconv.attributes.k8s_attributes.K8S_JOB_UID`.
"""

K8S_NAMESPACE_ANNOTATION_TEMPLATE: Final = "k8s.namespace.annotation"
"""
Deprecated in favor of stable :py:const:`opentelemetry.semconv.attributes.k8s_attributes.K8S_NAMESPACE_ANNOTATION_TEMPLATE`.
"""

K8S_NAMESPACE_LABEL_TEMPLATE: Final = "k8s.namespace.label"
"""
Deprecated in favor of stable :py:const:`opentelemetry.semconv.attributes.k8s_attributes.K8S_NAMESPACE_LABEL_TEMPLATE`.
"""

K8S_NAMESPACE_NAME: Final = "k8s.namespace.name"
"""
Deprecated in favor of stable :py:const:`opentelemetry.semconv.attributes.k8s_attributes.K8S_NAMESPACE_NAME`.
"""

K8S_NAMESPACE_PHASE: Final = "k8s.namespace.phase"
"""
The phase of the K8s namespace.
Note: This attribute aligns with the `phase` field of the
[K8s NamespaceStatus](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.34/#namespacestatus-v1-core).
"""

K8S_NODE_ANNOTATION_TEMPLATE: Final = "k8s.node.annotation"
"""
Deprecated in favor of stable :py:const:`opentelemetry.semconv.attributes.k8s_attributes.K8S_NODE_ANNOTATION_TEMPLATE`.
"""

K8S_NODE_CONDITION_STATUS: Final = "k8s.node.condition.status"
"""
The status of the condition, one of True, False, Unknown.
Note: This attribute aligns with the `status` field of the
[NodeCondition](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.34/#nodecondition-v1-core).
"""

K8S_NODE_CONDITION_TYPE: Final = "k8s.node.condition.type"
"""
The condition type of a K8s Node.
Note: K8s Node conditions as described
by [K8s documentation](https://kubernetes.io/docs/reference/node/node-status/#condition).

This attribute aligns with the `type` field of the
[NodeCondition](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.34/#nodecondition-v1-core)

The set of possible values is not limited to those listed here. Managed Kubernetes environments,
or custom controllers MAY introduce additional node condition types.
When this occurs, the exact value as reported by the Kubernetes API SHOULD be used.
"""

K8S_NODE_LABEL_TEMPLATE: Final = "k8s.node.label"
"""
Deprecated in favor of stable :py:const:`opentelemetry.semconv.attributes.k8s_attributes.K8S_NODE_LABEL_TEMPLATE`.
"""

K8S_NODE_NAME: Final = "k8s.node.name"
"""
Deprecated in favor of stable :py:const:`opentelemetry.semconv.attributes.k8s_attributes.K8S_NODE_NAME`.
"""

K8S_NODE_SYSTEM_CONTAINER_NAME: Final = "k8s.node.system_container.name"
"""
The name of the system container running on the K8s Node.
"""

K8S_NODE_UID: Final = "k8s.node.uid"
"""
Deprecated in favor of stable :py:const:`opentelemetry.semconv.attributes.k8s_attributes.K8S_NODE_UID`.
"""

K8S_PERSISTENTVOLUME_ANNOTATION_TEMPLATE: Final = (
    "k8s.persistentvolume.annotation"
)
"""
The annotation placed on the PersistentVolume, the `<key>` being the annotation name, the value being the annotation value, even if the value is empty.
Note: Examples:

- An annotation `pv.kubernetes.io/provisioned-by` with value `kubernetes.io/aws-ebs` SHOULD be recorded as
  the `k8s.persistentvolume.annotation.pv.kubernetes.io/provisioned-by` attribute with value `"kubernetes.io/aws-ebs"`.
- An annotation `data` with empty string value SHOULD be recorded as
  the `k8s.persistentvolume.annotation.data` attribute with value `""`.
"""

K8S_PERSISTENTVOLUME_LABEL_TEMPLATE: Final = "k8s.persistentvolume.label"
"""
The label placed on the PersistentVolume, the `<key>` being the label name, the value being the label value, even if the value is empty.
Note: Examples:

- A label `type` with value `ssd` SHOULD be recorded as
  the `k8s.persistentvolume.label.type` attribute with value `"ssd"`.
- A label `data` with empty string value SHOULD be recorded as
  the `k8s.persistentvolume.label.data` attribute with value `""`.
"""

K8S_PERSISTENTVOLUME_NAME: Final = "k8s.persistentvolume.name"
"""
The name of the PersistentVolume.
"""

K8S_PERSISTENTVOLUME_RECLAIM_POLICY: Final = (
    "k8s.persistentvolume.reclaim_policy"
)
"""
The reclaim policy of the PersistentVolume.
Note: This attribute aligns with the `persistentVolumeReclaimPolicy` field of the
[K8s PersistentVolumeSpec](https://kubernetes.io/docs/reference/kubernetes-api/config-and-storage-resources/persistent-volume-v1/#PersistentVolumeSpec).
"""

K8S_PERSISTENTVOLUME_STATUS_PHASE: Final = "k8s.persistentvolume.status.phase"
"""
The phase of the PersistentVolume.
Note: This attribute aligns with the `phase` field of the
[K8s PersistentVolumeStatus](https://kubernetes.io/docs/reference/kubernetes-api/config-and-storage-resources/persistent-volume-v1/#PersistentVolumeStatus).
"""

K8S_PERSISTENTVOLUME_UID: Final = "k8s.persistentvolume.uid"
"""
The UID of the PersistentVolume.
"""

K8S_PERSISTENTVOLUMECLAIM_ANNOTATION_TEMPLATE: Final = (
    "k8s.persistentvolumeclaim.annotation"
)
"""
The annotation placed on the PersistentVolumeClaim, the `<key>` being the annotation name, the value being the annotation value, even if the value is empty.
Note: Examples:

- An annotation `volume.beta.kubernetes.io/storage-provisioner` with value `kubernetes.io/aws-ebs` SHOULD be recorded as
  the `k8s.persistentvolumeclaim.annotation.volume.beta.kubernetes.io/storage-provisioner` attribute with value `"kubernetes.io/aws-ebs"`.
- An annotation `data` with empty string value SHOULD be recorded as
  the `k8s.persistentvolumeclaim.annotation.data` attribute with value `""`.
"""

K8S_PERSISTENTVOLUMECLAIM_LABEL_TEMPLATE: Final = (
    "k8s.persistentvolumeclaim.label"
)
"""
The label placed on the PersistentVolumeClaim, the `<key>` being the label name, the value being the label value, even if the value is empty.
Note: Examples:

- A label `app` with value `my-app` SHOULD be recorded as
  the `k8s.persistentvolumeclaim.label.app` attribute with value `"my-app"`.
- A label `data` with empty string value SHOULD be recorded as
  the `k8s.persistentvolumeclaim.label.data` attribute with value `""`.
"""

K8S_PERSISTENTVOLUMECLAIM_NAME: Final = "k8s.persistentvolumeclaim.name"
"""
The name of the PersistentVolumeClaim.
"""

K8S_PERSISTENTVOLUMECLAIM_STATUS_PHASE: Final = (
    "k8s.persistentvolumeclaim.status.phase"
)
"""
The phase of the PersistentVolumeClaim.
Note: This attribute aligns with the `phase` field of the
[K8s PersistentVolumeClaimStatus](https://kubernetes.io/docs/reference/kubernetes-api/config-and-storage-resources/persistent-volume-claim-v1/#PersistentVolumeClaimStatus).
"""

K8S_PERSISTENTVOLUMECLAIM_UID: Final = "k8s.persistentvolumeclaim.uid"
"""
The UID of the PersistentVolumeClaim.
"""

K8S_POD_ANNOTATION_TEMPLATE: Final = "k8s.pod.annotation"
"""
Deprecated in favor of stable :py:const:`opentelemetry.semconv.attributes.k8s_attributes.K8S_POD_ANNOTATION_TEMPLATE`.
"""

K8S_POD_HOSTNAME: Final = "k8s.pod.hostname"
"""
Deprecated in favor of stable :py:const:`opentelemetry.semconv.attributes.k8s_attributes.K8S_POD_HOSTNAME`.
"""

K8S_POD_IP: Final = "k8s.pod.ip"
"""
Deprecated in favor of stable :py:const:`opentelemetry.semconv.attributes.k8s_attributes.K8S_POD_IP`.
"""

K8S_POD_LABEL_TEMPLATE: Final = "k8s.pod.label"
"""
Deprecated in favor of stable :py:const:`opentelemetry.semconv.attributes.k8s_attributes.K8S_POD_LABEL_TEMPLATE`.
"""

K8S_POD_LABELS_TEMPLATE: Final = "k8s.pod.labels"
"""
Deprecated: Replaced by `k8s.pod.label`.
"""

K8S_POD_NAME: Final = "k8s.pod.name"
"""
Deprecated in favor of stable :py:const:`opentelemetry.semconv.attributes.k8s_attributes.K8S_POD_NAME`.
"""

K8S_POD_START_TIME: Final = "k8s.pod.start_time"
"""
Deprecated in favor of stable :py:const:`opentelemetry.semconv.attributes.k8s_attributes.K8S_POD_START_TIME`.
"""

K8S_POD_STATUS_PHASE: Final = "k8s.pod.status.phase"
"""
The phase for the pod. Corresponds to the `phase` field of the: [K8s PodStatus](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.33/#podstatus-v1-core).
"""

K8S_POD_STATUS_REASON: Final = "k8s.pod.status.reason"
"""
The reason for the pod state. Corresponds to the `reason` field of the: [K8s PodStatus](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.33/#podstatus-v1-core).
"""

K8S_POD_UID: Final = "k8s.pod.uid"
"""
Deprecated in favor of stable :py:const:`opentelemetry.semconv.attributes.k8s_attributes.K8S_POD_UID`.
"""

K8S_REPLICASET_ANNOTATION_TEMPLATE: Final = "k8s.replicaset.annotation"
"""
Deprecated in favor of stable :py:const:`opentelemetry.semconv.attributes.k8s_attributes.K8S_REPLICASET_ANNOTATION_TEMPLATE`.
"""

K8S_REPLICASET_LABEL_TEMPLATE: Final = "k8s.replicaset.label"
"""
Deprecated in favor of stable :py:const:`opentelemetry.semconv.attributes.k8s_attributes.K8S_REPLICASET_LABEL_TEMPLATE`.
"""

K8S_REPLICASET_NAME: Final = "k8s.replicaset.name"
"""
Deprecated in favor of stable :py:const:`opentelemetry.semconv.attributes.k8s_attributes.K8S_REPLICASET_NAME`.
"""

K8S_REPLICASET_UID: Final = "k8s.replicaset.uid"
"""
Deprecated in favor of stable :py:const:`opentelemetry.semconv.attributes.k8s_attributes.K8S_REPLICASET_UID`.
"""

K8S_REPLICATIONCONTROLLER_NAME: Final = "k8s.replicationcontroller.name"
"""
The name of the replication controller.
"""

K8S_REPLICATIONCONTROLLER_UID: Final = "k8s.replicationcontroller.uid"
"""
The UID of the replication controller.
"""

K8S_RESOURCEQUOTA_NAME: Final = "k8s.resourcequota.name"
"""
The name of the resource quota.
"""

K8S_RESOURCEQUOTA_RESOURCE_NAME: Final = "k8s.resourcequota.resource_name"
"""
The name of the K8s resource a resource quota defines.
Note: The value for this attribute can be either the full `count/<resource>[.<group>]` string (e.g., count/deployments.apps, count/pods), or, for certain core Kubernetes resources, just the resource name (e.g., pods, services, configmaps). Both forms are supported by Kubernetes for object count quotas. See [Kubernetes Resource Quotas documentation](https://kubernetes.io/docs/concepts/policy/resource-quotas/#quota-on-object-count) for more details.
"""

K8S_RESOURCEQUOTA_UID: Final = "k8s.resourcequota.uid"
"""
The UID of the resource quota.
"""

K8S_SERVICE_ANNOTATION_TEMPLATE: Final = "k8s.service.annotation"
"""
The annotation placed on the Service, the `<key>` being the annotation name, the value being the annotation value, even if the value is empty.
Note: Examples:

- An annotation `prometheus.io/scrape` with value `true` SHOULD be recorded as
  the `k8s.service.annotation.prometheus.io/scrape` attribute with value `"true"`.
- An annotation `data` with empty string value SHOULD be recorded as
  the `k8s.service.annotation.data` attribute with value `""`.
"""

K8S_SERVICE_ENDPOINT_ADDRESS_TYPE: Final = "k8s.service.endpoint.address_type"
"""
The address type of the service endpoint.
Note: The network address family or type of the endpoint.
This attribute aligns with the `addressType` field of the
[K8s EndpointSlice](https://kubernetes.io/docs/reference/kubernetes-api/service-resources/endpoint-slice-v1/).
It is used to differentiate metrics when a Service is backed by multiple address types
(e.g., in dual-stack clusters).
"""

K8S_SERVICE_ENDPOINT_CONDITION: Final = "k8s.service.endpoint.condition"
"""
The condition of the service endpoint.
Note: The current operational condition of the service endpoint.
An endpoint can have multiple conditions set at once (e.g., both `serving` and `terminating` during rollout).
This attribute aligns with the condition fields in the [K8s EndpointSlice](https://kubernetes.io/docs/reference/kubernetes-api/service-resources/endpoint-slice-v1/).
"""

K8S_SERVICE_ENDPOINT_ZONE: Final = "k8s.service.endpoint.zone"
"""
The zone of the service endpoint.
Note: The zone where the endpoint is located, typically corresponding to a failure domain.
This attribute aligns with the `zone` field of endpoints in the
[K8s EndpointSlice](https://kubernetes.io/docs/reference/kubernetes-api/service-resources/endpoint-slice-v1/).
It enables zone-aware monitoring of service endpoint distribution and supports
features like [Topology Aware Routing](https://kubernetes.io/docs/concepts/services-networking/topology-aware-routing/).

If the zone is not populated (e.g., nodes without the `topology.kubernetes.io/zone` label),
the attribute value will be an empty string.
"""

K8S_SERVICE_LABEL_TEMPLATE: Final = "k8s.service.label"
"""
The label placed on the Service, the `<key>` being the label name, the value being the label value, even if the value is empty.
Note: Examples:

- A label `app` with value `my-service` SHOULD be recorded as
  the `k8s.service.label.app` attribute with value `"my-service"`.
- A label `data` with empty string value SHOULD be recorded as
  the `k8s.service.label.data` attribute with value `""`.
"""

K8S_SERVICE_NAME: Final = "k8s.service.name"
"""
The name of the Service.
"""

K8S_SERVICE_PUBLISH_NOT_READY_ADDRESSES: Final = (
    "k8s.service.publish_not_ready_addresses"
)
"""
Whether the Service publishes not-ready endpoints.
Note: Whether the Service is configured to publish endpoints before the pods are ready.
This attribute is typically used to indicate that a Service (such as a headless
Service for a StatefulSet) allows peer discovery before pods pass their readiness probes.
It aligns with the `publishNotReadyAddresses` field of the
[K8s ServiceSpec](https://kubernetes.io/docs/reference/kubernetes-api/service-resources/service-v1/#ServiceSpec).
"""

K8S_SERVICE_SELECTOR_TEMPLATE: Final = "k8s.service.selector"
"""
The selector key-value pair placed on the Service, the `<key>` being the selector key, the value being the selector value.
Note: These selectors are used to correlate with pod labels. Each selector key-value pair becomes a separate attribute.

Examples:

- A selector `app=my-app` SHOULD be recorded as
  the `k8s.service.selector.app` attribute with value `"my-app"`.
- A selector `version=v1` SHOULD be recorded as
  the `k8s.service.selector.version` attribute with value `"v1"`.
"""

K8S_SERVICE_TRAFFIC_DISTRIBUTION: Final = "k8s.service.traffic_distribution"
"""
The traffic distribution policy for the Service.
Note: Specifies how traffic is distributed to endpoints for this Service.
This attribute aligns with the `trafficDistribution` field of the
[K8s ServiceSpec](https://kubernetes.io/docs/reference/networking/virtual-ips/#traffic-distribution).
Known values include `PreferSameZone` (prefer endpoints in the same zone as the client) and
`PreferSameNode` (prefer endpoints on the same node, fallback to same zone, then cluster-wide).
If this field is not set on the Service, the attribute SHOULD NOT be emitted.
When not set, Kubernetes distributes traffic evenly across all endpoints cluster-wide.
"""

K8S_SERVICE_TYPE: Final = "k8s.service.type"
"""
The type of the Kubernetes Service.
Note: This attribute aligns with the `type` field of the
[K8s ServiceSpec](https://kubernetes.io/docs/reference/kubernetes-api/service-resources/service-v1/#ServiceSpec).
"""

K8S_SERVICE_UID: Final = "k8s.service.uid"
"""
The UID of the Service.
"""

K8S_STATEFULSET_ANNOTATION_TEMPLATE: Final = "k8s.statefulset.annotation"
"""
Deprecated in favor of stable :py:const:`opentelemetry.semconv.attributes.k8s_attributes.K8S_STATEFULSET_ANNOTATION_TEMPLATE`.
"""

K8S_STATEFULSET_LABEL_TEMPLATE: Final = "k8s.statefulset.label"
"""
Deprecated in favor of stable :py:const:`opentelemetry.semconv.attributes.k8s_attributes.K8S_STATEFULSET_LABEL_TEMPLATE`.
"""

K8S_STATEFULSET_NAME: Final = "k8s.statefulset.name"
"""
Deprecated in favor of stable :py:const:`opentelemetry.semconv.attributes.k8s_attributes.K8S_STATEFULSET_NAME`.
"""

K8S_STATEFULSET_UID: Final = "k8s.statefulset.uid"
"""
Deprecated in favor of stable :py:const:`opentelemetry.semconv.attributes.k8s_attributes.K8S_STATEFULSET_UID`.
"""

K8S_STORAGECLASS_NAME: Final = "k8s.storageclass.name"
"""
The name of K8s [StorageClass](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.34/#storageclass-v1-storage-k8s-io) object.
"""

K8S_VOLUME_NAME: Final = "k8s.volume.name"
"""
The name of the K8s volume.
"""

K8S_VOLUME_TYPE: Final = "k8s.volume.type"
"""
The type of the K8s volume.
"""


class K8sContainerEphemeralStorageFsTypeValues(Enum):
    ROOTFS = "rootfs"
    """For the container's writable layer usage."""
    LOGS = "logs"
    """For the container's log files usage (stdout/stderr)."""


class K8sContainerStatusReasonValues(Enum):
    CONTAINER_CREATING = "ContainerCreating"
    """The container is being created."""
    CRASH_LOOP_BACK_OFF = "CrashLoopBackOff"
    """The container is in a crash loop back off state."""
    CREATE_CONTAINER_CONFIG_ERROR = "CreateContainerConfigError"
    """There was an error creating the container configuration."""
    ERR_IMAGE_PULL = "ErrImagePull"
    """There was an error pulling the container image."""
    IMAGE_PULL_BACK_OFF = "ImagePullBackOff"
    """The container image pull is in back off state."""
    OOM_KILLED = "OOMKilled"
    """The container was killed due to out of memory."""
    COMPLETED = "Completed"
    """The container has completed execution."""
    ERROR = "Error"
    """There was an error with the container."""
    CONTAINER_CANNOT_RUN = "ContainerCannotRun"
    """The container cannot run."""


class K8sContainerStatusStateValues(Enum):
    TERMINATED = "terminated"
    """The container has terminated."""
    RUNNING = "running"
    """The container is running."""
    WAITING = "waiting"
    """The container is waiting."""


class K8sNamespacePhaseValues(Enum):
    ACTIVE = "active"
    """Active namespace phase as described by [K8s API](https://pkg.go.dev/k8s.io/api@v0.31.3/core/v1#NamespacePhase)."""
    TERMINATING = "terminating"
    """Terminating namespace phase as described by [K8s API](https://pkg.go.dev/k8s.io/api@v0.31.3/core/v1#NamespacePhase)."""


class K8sNodeConditionStatusValues(Enum):
    CONDITION_TRUE = "true"
    """condition_true."""
    CONDITION_FALSE = "false"
    """condition_false."""
    CONDITION_UNKNOWN = "unknown"
    """condition_unknown."""


class K8sNodeConditionTypeValues(Enum):
    READY = "Ready"
    """The node is healthy and ready to accept pods."""
    DISK_PRESSURE = "DiskPressure"
    """Pressure exists on the disk size—that is, if the disk capacity is low."""
    MEMORY_PRESSURE = "MemoryPressure"
    """Pressure exists on the node memory—that is, if the node memory is low."""
    PID_PRESSURE = "PIDPressure"
    """Pressure exists on the processes—that is, if there are too many processes on the node."""
    NETWORK_UNAVAILABLE = "NetworkUnavailable"
    """The network for the node is not correctly configured."""


class K8sPersistentvolumeReclaimPolicyValues(Enum):
    DELETE = "Delete"
    """The volume will be deleted when released from its claim."""
    RECYCLE = "Recycle"
    """The volume will be recycled (basic scrub) when released from its claim."""
    RETAIN = "Retain"
    """The volume will be retained when released from its claim."""


class K8sPersistentvolumeStatusPhaseValues(Enum):
    AVAILABLE = "Available"
    """The volume is available and not yet bound to a claim."""
    BOUND = "Bound"
    """The volume is bound to a claim."""
    FAILED = "Failed"
    """The volume has failed its automatic reclamation."""
    PENDING = "Pending"
    """The volume is being provisioned."""
    RELEASED = "Released"
    """The claim has been deleted but the volume is not yet available."""


class K8sPersistentvolumeclaimStatusPhaseValues(Enum):
    BOUND = "Bound"
    """The claim is bound to a volume."""
    LOST = "Lost"
    """The claim has lost its underlying volume (the volume does not exist anymore)."""
    PENDING = "Pending"
    """The claim has not yet been bound to a volume."""


class K8sPodStatusPhaseValues(Enum):
    PENDING = "Pending"
    """The pod has been accepted by the system, but one or more of the containers has not been started. This includes time before being bound to a node, as well as time spent pulling images onto the host."""
    RUNNING = "Running"
    """The pod has been bound to a node and all of the containers have been started. At least one container is still running or is in the process of being restarted."""
    SUCCEEDED = "Succeeded"
    """All containers in the pod have voluntarily terminated with a container exit code of 0, and the system is not going to restart any of these containers."""
    FAILED = "Failed"
    """All containers in the pod have terminated, and at least one container has terminated in a failure (exited with a non-zero exit code or was stopped by the system)."""
    UNKNOWN = "Unknown"
    """For some reason the state of the pod could not be obtained, typically due to an error in communicating with the host of the pod."""


class K8sPodStatusReasonValues(Enum):
    EVICTED = "Evicted"
    """The pod is evicted."""
    NODE_AFFINITY = "NodeAffinity"
    """The pod is in a status because of its node affinity."""
    NODE_LOST = "NodeLost"
    """The reason on a pod when its state cannot be confirmed as kubelet is unresponsive on the node it is (was) running."""
    SHUTDOWN = "Shutdown"
    """The node is shutdown."""
    UNEXPECTED_ADMISSION_ERROR = "UnexpectedAdmissionError"
    """The pod was rejected admission to the node because of an error during admission that could not be categorized."""


class K8sServiceEndpointAddressTypeValues(Enum):
    IPV4 = "IPv4"
    """IPv4 address type."""
    IPV6 = "IPv6"
    """IPv6 address type."""
    FQDN = "FQDN"
    """FQDN address type."""


class K8sServiceEndpointConditionValues(Enum):
    READY = "ready"
    """The endpoint is ready to receive new connections."""
    SERVING = "serving"
    """The endpoint is currently handling traffic."""
    TERMINATING = "terminating"
    """The endpoint is in the process of shutting down."""


class K8sServiceTypeValues(Enum):
    CLUSTER_IP = "ClusterIP"
    """ClusterIP service type."""
    NODE_PORT = "NodePort"
    """NodePort service type."""
    LOAD_BALANCER = "LoadBalancer"
    """LoadBalancer service type."""
    EXTERNAL_NAME = "ExternalName"
    """ExternalName service type."""


class K8sVolumeTypeValues(Enum):
    PERSISTENT_VOLUME_CLAIM = "persistentVolumeClaim"
    """A [persistentVolumeClaim](https://kubernetes.io/docs/concepts/storage/volumes/#persistentvolumeclaim) volume."""
    CONFIG_MAP = "configMap"
    """A [configMap](https://kubernetes.io/docs/concepts/storage/volumes/#configmap) volume."""
    DOWNWARD_API = "downwardAPI"
    """A [downwardAPI](https://kubernetes.io/docs/concepts/storage/volumes/#downwardapi) volume."""
    EMPTY_DIR = "emptyDir"
    """An [emptyDir](https://kubernetes.io/docs/concepts/storage/volumes/#emptydir) volume."""
    SECRET = "secret"
    """A [secret](https://kubernetes.io/docs/concepts/storage/volumes/#secret) volume."""
    LOCAL = "local"
    """A [local](https://kubernetes.io/docs/concepts/storage/volumes/#local) volume."""
