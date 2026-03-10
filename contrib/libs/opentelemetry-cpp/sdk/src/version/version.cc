// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// Please DONOT touch this file.
// Any changes done here would be overwritten by pre-commit git hook

#include "opentelemetry/sdk/version/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace version
{
const int major_version    = 1;
const int minor_version    = 25;
const int patch_version    = 0;
const char *pre_release    = "";
const char *build_metadata = "none";
const char *short_version  = "1.25.0";
const char *full_version   = "1.25.0";
const char *build_date     = "Sat Feb  7 10:29:31 AM UTC 2026";
}  // namespace version
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
