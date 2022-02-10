/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/core/client/AWSError.h>
#include <aws/core/utils/HashingUtils.h>
#include <aws/s3/S3Errors.h>
#include <aws/s3/model/InvalidObjectState.h>

using namespace Aws::Client;
using namespace Aws::Utils;
using namespace Aws::S3;
using namespace Aws::S3::Model;

namespace Aws
{
namespace S3
{
template<> AWS_S3_API InvalidObjectState S3Error::GetModeledError()
{
  assert(this->GetErrorType() == S3Errors::INVALID_OBJECT_STATE);
  return InvalidObjectState(this->GetXmlPayload().GetRootElement());
}

namespace S3ErrorMapper
{

static const int NO_SUCH_UPLOAD_HASH = HashingUtils::HashString("NoSuchUpload");
static const int BUCKET_ALREADY_OWNED_BY_YOU_HASH = HashingUtils::HashString("BucketAlreadyOwnedByYou");
static const int OBJECT_ALREADY_IN_ACTIVE_TIER_HASH = HashingUtils::HashString("ObjectAlreadyInActiveTierError");
static const int NO_SUCH_BUCKET_HASH = HashingUtils::HashString("NoSuchBucket");
static const int NO_SUCH_KEY_HASH = HashingUtils::HashString("NoSuchKey");
static const int OBJECT_NOT_IN_ACTIVE_TIER_HASH = HashingUtils::HashString("ObjectNotInActiveTierError");
static const int BUCKET_ALREADY_EXISTS_HASH = HashingUtils::HashString("BucketAlreadyExists");
static const int INVALID_OBJECT_STATE_HASH = HashingUtils::HashString("InvalidObjectState");


AWSError<CoreErrors> GetErrorForName(const char* errorName)
{
  int hashCode = HashingUtils::HashString(errorName);

  if (hashCode == NO_SUCH_UPLOAD_HASH)
  {
    return AWSError<CoreErrors>(static_cast<CoreErrors>(S3Errors::NO_SUCH_UPLOAD), false);
  }
  else if (hashCode == BUCKET_ALREADY_OWNED_BY_YOU_HASH)
  {
    return AWSError<CoreErrors>(static_cast<CoreErrors>(S3Errors::BUCKET_ALREADY_OWNED_BY_YOU), false);
  }
  else if (hashCode == OBJECT_ALREADY_IN_ACTIVE_TIER_HASH)
  {
    return AWSError<CoreErrors>(static_cast<CoreErrors>(S3Errors::OBJECT_ALREADY_IN_ACTIVE_TIER), false);
  }
  else if (hashCode == NO_SUCH_BUCKET_HASH)
  {
    return AWSError<CoreErrors>(static_cast<CoreErrors>(S3Errors::NO_SUCH_BUCKET), false);
  }
  else if (hashCode == NO_SUCH_KEY_HASH)
  {
    return AWSError<CoreErrors>(static_cast<CoreErrors>(S3Errors::NO_SUCH_KEY), false);
  }
  else if (hashCode == OBJECT_NOT_IN_ACTIVE_TIER_HASH)
  {
    return AWSError<CoreErrors>(static_cast<CoreErrors>(S3Errors::OBJECT_NOT_IN_ACTIVE_TIER), false);
  }
  else if (hashCode == BUCKET_ALREADY_EXISTS_HASH)
  {
    return AWSError<CoreErrors>(static_cast<CoreErrors>(S3Errors::BUCKET_ALREADY_EXISTS), false);
  }
  else if (hashCode == INVALID_OBJECT_STATE_HASH)
  {
    return AWSError<CoreErrors>(static_cast<CoreErrors>(S3Errors::INVALID_OBJECT_STATE), false);
  }
  return AWSError<CoreErrors>(CoreErrors::UNKNOWN, false);
}

} // namespace S3ErrorMapper
} // namespace S3
} // namespace Aws
