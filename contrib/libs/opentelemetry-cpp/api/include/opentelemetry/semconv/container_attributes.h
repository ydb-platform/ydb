/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

/*
 * DO NOT EDIT, this is an Auto-generated file from:
 * buildscripts/semantic-convention/templates/registry/semantic_attributes-h.j2
 */

#pragma once

#include "opentelemetry/common/macros.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace semconv
{
namespace container
{

/**
  Container ID. Usually a UUID, as for example used to <a
  href="https://docs.docker.com/engine/containers/run/#container-identification">identify Docker
  containers</a>. The UUID might be abbreviated.
 */
static constexpr const char *kContainerId = "container.id";

/**
  Name of the image the container was built on.
 */
static constexpr const char *kContainerImageName = "container.image.name";

/**
  Repo digests of the container image as provided by the container runtime.
  <p>
  <a
  href="https://docs.docker.com/reference/api/engine/version/v1.52/#tag/Image/operation/ImageInspect">Docker</a>
  and <a
  href="https://github.com/kubernetes/cri-api/blob/c75ef5b473bbe2d0a4fc92f82235efd665ea8e9f/pkg/apis/runtime/v1/api.proto#L1237-L1238">CRI</a>
  report those under the @code RepoDigests @endcode field.
 */
static constexpr const char *kContainerImageRepoDigests = "container.image.repo_digests";

/**
  Container image tags. An example can be found in <a
  href="https://docs.docker.com/reference/api/engine/version/v1.52/#tag/Image/operation/ImageInspect">Docker
  Image Inspect</a>. Should be only the @code <tag> @endcode section of the full name for example
  from @code registry.example.com/my-org/my-image:<tag> @endcode.
 */
static constexpr const char *kContainerImageTags = "container.image.tags";

}  // namespace container
}  // namespace semconv
OPENTELEMETRY_END_NAMESPACE
