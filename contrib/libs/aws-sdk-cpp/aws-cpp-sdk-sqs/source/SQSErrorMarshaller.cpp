/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/core/client/AWSError.h>
#include <aws/sqs/SQSErrorMarshaller.h>
#include <aws/sqs/SQSErrors.h>

using namespace Aws::Client;
using namespace Aws::SQS;

AWSError<CoreErrors> SQSErrorMarshaller::FindErrorByName(const char* errorName) const
{
  AWSError<CoreErrors> error = SQSErrorMapper::GetErrorForName(errorName);
  if(error.GetErrorType() != CoreErrors::UNKNOWN)
  {
    return error;
  }

  return AWSErrorMarshaller::FindErrorByName(errorName);
}