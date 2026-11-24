# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

from typing import Final

CONTAINER_ID: Final = "container.id"
"""
Container ID. Usually a UUID, as for example used to [identify Docker containers](https://docs.docker.com/engine/containers/run/#container-identification). The UUID might be abbreviated.
"""

CONTAINER_IMAGE_NAME: Final = "container.image.name"
"""
Name of the image the container was built on.
"""

CONTAINER_IMAGE_REPO_DIGESTS: Final = "container.image.repo_digests"
"""
Repo digests of the container image as provided by the container runtime.
Note: [Docker](https://docs.docker.com/reference/api/engine/version/v1.52/#tag/Image/operation/ImageInspect) and [CRI](https://github.com/kubernetes/cri-api/blob/c75ef5b473bbe2d0a4fc92f82235efd665ea8e9f/pkg/apis/runtime/v1/api.proto#L1237-L1238) report those under the `RepoDigests` field.
"""

CONTAINER_IMAGE_TAGS: Final = "container.image.tags"
"""
Container image tags. An example can be found in [Docker Image Inspect](https://docs.docker.com/reference/api/engine/version/v1.52/#tag/Image/operation/ImageInspect). Should be only the `<tag>` section of the full name for example from `registry.example.com/my-org/my-image:<tag>`.
"""
