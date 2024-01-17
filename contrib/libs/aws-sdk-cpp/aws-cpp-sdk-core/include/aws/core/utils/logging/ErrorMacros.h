/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once

#include <aws/core/Core_EXPORTS.h>

#include <aws/core/utils/logging/LogLevel.h>
#include <aws/core/utils/logging/AWSLogging.h>
#include <aws/core/utils/logging/LogMacros.h>

#define AWS_OPERATION_CHECK_PTR(PTR, OPERATION, ERROR_TYPE, ERROR) \
do { \
  if (PTR == nullptr) \
  { \
    AWS_LOGSTREAM_FATAL(#OPERATION, "Unexpected nullptr: " #PTR); \
    return OPERATION##Outcome(Aws::Client::AWSError<ERROR_TYPE>(ERROR, #ERROR, "Unexpected nullptr: " #PTR, false)); \
  } \
} while (0)

#define AWS_CHECK(LOG_TAG, CONDITION, ERROR_MESSAGE, RETURN) \
do { \
  if (!(CONDITION)) \
  { \
    AWS_LOGSTREAM_ERROR(LOG_TAG, ERROR_MESSAGE); \
    return RETURN; \
  } \
} while (0)

#define AWS_CHECK_PTR(LOG_TAG, PTR) \
do { \
  if (PTR == nullptr) \
  { \
    AWS_LOGSTREAM_FATAL(LOG_TAG, "Unexpected nullptr: " #PTR); \
    return; \
  } \
} while (0)

#define AWS_OPERATION_CHECK_SUCCESS(OUTCOME, OPERATION, ERROR_TYPE, ERROR, ERROR_MESSAGE) \
do { \
  if (!OUTCOME.IsSuccess()) \
  { \
    AWS_LOGSTREAM_ERROR(#OPERATION, ERROR_MESSAGE); \
    return OPERATION##Outcome(Aws::Client::AWSError<ERROR_TYPE>(ERROR, #ERROR, ERROR_MESSAGE, false)); \
  } \
} while (0)

#define AWS_OPERATION_CHECK_PARAMETER_PRESENT(REQUEST, FIELD, OPERATION, CLIENT_NAMESPACE) \
do { \
  if (!REQUEST##.##FIELD##HasBeenSet()) \
  { \
    AWS_LOGSTREAM_ERROR(#OPERATION, "Required field: "#FIELD" is not set"); \
    return OPERATION##Outcome(Aws::Client::AWSError<CLIENT_NAMESPACE##Errors>(CLIENT_NAMESPACE##Errors::MISSING_PARAMETER, "MISSING_PARAMETER", "Missing required field ["#FIELD"]", false)); \
  } \
} while (0)
