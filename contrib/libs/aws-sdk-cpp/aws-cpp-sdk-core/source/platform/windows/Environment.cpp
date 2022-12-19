/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/core/platform/Environment.h>

#include <stdio.h>
#include <utility>

namespace Aws
{
namespace Environment
{

/*
using std::getenv generates a warning on windows so we use _dupenv_s instead.  The character array returned by this function is our responsibility to clean up, so rather than returning raw strings
that would need to be manually freed in all the client functions, just copy it into a Aws::String instead, freeing it here.
*/
Aws::String GetEnv(const char *variableName)
{
    char* variableValue = nullptr;
    std::size_t valueSize = 0;
    auto queryResult = _dupenv_s(&variableValue, &valueSize, variableName);

    Aws::String result;
    if(queryResult == 0 && variableValue != nullptr && valueSize > 0)
    {
        result.assign(variableValue, valueSize - 1);  // don't copy the c-string terminator byte
        free(variableValue);
    }

    return result;
}

} // namespace Environment
} // namespace Aws
