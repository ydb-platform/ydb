/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/core/platform/Security.h>

#include <windows.h>

namespace Aws
{
namespace Security
{

void SecureMemClear(unsigned char *data, size_t length)
{
    SecureZeroMemory(data, length);
}

} // namespace Security
} // namespace Aws
