# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

from enum import Enum
from typing import Final

from typing_extensions import deprecated

CONTAINER_COMMAND: Final = "container.command"
"""
The command used to run the container (i.e. the command name).
Note: If using embedded credentials or sensitive data, it is recommended to remove them to prevent potential leakage.
"""

CONTAINER_COMMAND_ARGS: Final = "container.command_args"
"""
All the command arguments (including the command/executable itself) run by the container.
"""

CONTAINER_COMMAND_LINE: Final = "container.command_line"
"""
The full command run by the container as a single string representing the full command.
"""

CONTAINER_CPU_STATE: Final = "container.cpu.state"
"""
Deprecated: Replaced by `cpu.mode`.
"""

CONTAINER_CSI_PLUGIN_NAME: Final = "container.csi.plugin.name"
"""
The name of the CSI ([Container Storage Interface](https://github.com/container-storage-interface/spec)) plugin used by the volume.
Note: This can sometimes be referred to as a "driver" in CSI implementations. This should represent the `name` field of the GetPluginInfo RPC.
"""

CONTAINER_CSI_VOLUME_ID: Final = "container.csi.volume.id"
"""
The unique volume ID returned by the CSI ([Container Storage Interface](https://github.com/container-storage-interface/spec)) plugin.
Note: This can sometimes be referred to as a "volume handle" in CSI implementations. This should represent the `Volume.volume_id` field in CSI spec.
"""

CONTAINER_ID: Final = "container.id"
"""
Deprecated in favor of stable :py:const:`opentelemetry.semconv.attributes.container_attributes.CONTAINER_ID`.
"""

CONTAINER_IMAGE_ID: Final = "container.image.id"
"""
Runtime specific image identifier. Usually a hash algorithm followed by a UUID.
Note: Docker defines a sha256 of the image id; `container.image.id` corresponds to the `Image` field from the Docker container inspect [API](https://docs.docker.com/reference/api/engine/version/v1.52/#tag/Container/operation/ContainerInspect) endpoint.
K8s defines a link to the container registry repository with digest `"imageID": "registry.azurecr.io /namespace/service/dockerfile@sha256:bdeabd40c3a8a492eaf9e8e44d0ebbb84bac7ee25ac0cf8a7159d25f62555625"`.
The ID is assigned by the container runtime and can vary in different environments. Consider using `oci.manifest.digest` if it is important to identify the same image in different environments/runtimes.
"""

CONTAINER_IMAGE_NAME: Final = "container.image.name"
"""
Deprecated in favor of stable :py:const:`opentelemetry.semconv.attributes.container_attributes.CONTAINER_IMAGE_NAME`.
"""

CONTAINER_IMAGE_REPO_DIGESTS: Final = "container.image.repo_digests"
"""
Deprecated in favor of stable :py:const:`opentelemetry.semconv.attributes.container_attributes.CONTAINER_IMAGE_REPO_DIGESTS`.
"""

CONTAINER_IMAGE_TAGS: Final = "container.image.tags"
"""
Deprecated in favor of stable :py:const:`opentelemetry.semconv.attributes.container_attributes.CONTAINER_IMAGE_TAGS`.
"""

CONTAINER_LABEL_TEMPLATE: Final = "container.label"
"""
Container labels, `<key>` being the label name, the value being the label value.
Note: For example, a docker container label `app` with value `nginx` SHOULD be recorded as the `container.label.app` attribute with value `"nginx"`.
"""

CONTAINER_LABELS_TEMPLATE: Final = "container.labels"
"""
Deprecated: Replaced by `container.label`.
"""

CONTAINER_NAME: Final = "container.name"
"""
Container name used by container runtime.
"""

CONTAINER_RUNTIME: Final = "container.runtime"
"""
Deprecated: Replaced by `container.runtime.name`.
"""

CONTAINER_RUNTIME_DESCRIPTION: Final = "container.runtime.description"
"""
A description about the runtime which could include, for example details about the CRI/API version being used or other customizations.
"""

CONTAINER_RUNTIME_NAME: Final = "container.runtime.name"
"""
The container runtime managing this container.
"""

CONTAINER_RUNTIME_VERSION: Final = "container.runtime.version"
"""
The version of the runtime of this process, as returned by the runtime without modification.
"""


@deprecated(
    "The attribute container.cpu.state is deprecated - Replaced by `cpu.mode`"
)
class ContainerCpuStateValues(Enum):
    USER = "user"
    """When tasks of the cgroup are in user mode (Linux). When all container processes are in user mode (Windows)."""
    SYSTEM = "system"
    """When CPU is used by the system (host OS)."""
    KERNEL = "kernel"
    """When tasks of the cgroup are in kernel mode (Linux). When all container processes are in kernel mode (Windows)."""
