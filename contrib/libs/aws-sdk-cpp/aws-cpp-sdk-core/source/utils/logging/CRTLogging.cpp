/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/core/utils/logging/CRTLogging.h>
#include <aws/common/logging.h>
#include <memory>

using namespace Aws::Utils;
using namespace Aws::Utils::Logging;

namespace Aws
{
namespace Utils
{
namespace Logging {

static std::shared_ptr<CRTLogSystemInterface> CRTLogSystem(nullptr);

void InitializeCRTLogging(const std::shared_ptr<CRTLogSystemInterface>& crtLogSystem) {
    CRTLogSystem = crtLogSystem;
}

void ShutdownCRTLogging() {
    CRTLogSystem = nullptr;
}

} // namespace Logging
} // namespace Utils
} // namespace Aws
