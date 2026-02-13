/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once

#include <aws/core/client/AWSError.h>
#include <aws/core/client/CoreErrors.h>
#include <aws/sqs/SQS_EXPORTS.h>

namespace Aws
{
namespace SQS
{
enum class SQSErrors
{
  //From Core//
  //////////////////////////////////////////////////////////////////////////////////////////
  INCOMPLETE_SIGNATURE = 0,
  INTERNAL_FAILURE = 1,
  INVALID_ACTION = 2,
  INVALID_CLIENT_TOKEN_ID = 3,
  INVALID_PARAMETER_COMBINATION = 4,
  INVALID_QUERY_PARAMETER = 5,
  INVALID_PARAMETER_VALUE = 6,
  MISSING_ACTION = 7, // SDK should never allow
  MISSING_AUTHENTICATION_TOKEN = 8, // SDK should never allow
  MISSING_PARAMETER = 9, // SDK should never allow
  OPT_IN_REQUIRED = 10,
  REQUEST_EXPIRED = 11,
  SERVICE_UNAVAILABLE = 12,
  THROTTLING = 13,
  VALIDATION = 14,
  ACCESS_DENIED = 15,
  RESOURCE_NOT_FOUND = 16,
  UNRECOGNIZED_CLIENT = 17,
  MALFORMED_QUERY_STRING = 18,
  SLOW_DOWN = 19,
  REQUEST_TIME_TOO_SKEWED = 20,
  INVALID_SIGNATURE = 21,
  SIGNATURE_DOES_NOT_MATCH = 22,
  INVALID_ACCESS_KEY_ID = 23,
  REQUEST_TIMEOUT = 24,
  NETWORK_CONNECTION = 99,

  UNKNOWN = 100,
  ///////////////////////////////////////////////////////////////////////////////////////////

  BATCH_ENTRY_IDS_NOT_DISTINCT= static_cast<int>(Aws::Client::CoreErrors::SERVICE_EXTENSION_START_RANGE) + 1,
  BATCH_REQUEST_TOO_LONG,
  EMPTY_BATCH_REQUEST,
  INVALID_ATTRIBUTE_NAME,
  INVALID_BATCH_ENTRY_ID,
  INVALID_ID_FORMAT,
  INVALID_MESSAGE_CONTENTS,
  MESSAGE_NOT_INFLIGHT,
  OVER_LIMIT,
  PURGE_QUEUE_IN_PROGRESS,
  QUEUE_DELETED_RECENTLY,
  QUEUE_DOES_NOT_EXIST,
  QUEUE_NAME_EXISTS,
  RECEIPT_HANDLE_IS_INVALID,
  TOO_MANY_ENTRIES_IN_BATCH_REQUEST,
  UNSUPPORTED_OPERATION
};

class AWS_SQS_API SQSError : public Aws::Client::AWSError<SQSErrors>
{
public:
  SQSError() {}
  SQSError(const Aws::Client::AWSError<Aws::Client::CoreErrors>& rhs) : Aws::Client::AWSError<SQSErrors>(rhs) {}
  SQSError(Aws::Client::AWSError<Aws::Client::CoreErrors>&& rhs) : Aws::Client::AWSError<SQSErrors>(rhs) {}
  SQSError(const Aws::Client::AWSError<SQSErrors>& rhs) : Aws::Client::AWSError<SQSErrors>(rhs) {}
  SQSError(Aws::Client::AWSError<SQSErrors>&& rhs) : Aws::Client::AWSError<SQSErrors>(rhs) {}

  template <typename T>
  T GetModeledError();
};

namespace SQSErrorMapper
{
  AWS_SQS_API Aws::Client::AWSError<Aws::Client::CoreErrors> GetErrorForName(const char* errorName);
}

} // namespace SQS
} // namespace Aws
