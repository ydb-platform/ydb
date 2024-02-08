/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once
#include <aws/s3/S3_EXPORTS.h>
#include <aws/s3/model/LoggingEnabled.h>
#include <utility>

namespace Aws
{
template<typename RESULT_TYPE>
class AmazonWebServiceResult;

namespace Utils
{
namespace Xml
{
  class XmlDocument;
} // namespace Xml
} // namespace Utils
namespace S3
{
namespace Model
{
  class GetBucketLoggingResult
  {
  public:
    AWS_S3_API GetBucketLoggingResult();
    AWS_S3_API GetBucketLoggingResult(const Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>& result);
    AWS_S3_API GetBucketLoggingResult& operator=(const Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>& result);


    
    inline const LoggingEnabled& GetLoggingEnabled() const{ return m_loggingEnabled; }

    
    inline void SetLoggingEnabled(const LoggingEnabled& value) { m_loggingEnabled = value; }

    
    inline void SetLoggingEnabled(LoggingEnabled&& value) { m_loggingEnabled = std::move(value); }

    
    inline GetBucketLoggingResult& WithLoggingEnabled(const LoggingEnabled& value) { SetLoggingEnabled(value); return *this;}

    
    inline GetBucketLoggingResult& WithLoggingEnabled(LoggingEnabled&& value) { SetLoggingEnabled(std::move(value)); return *this;}

  private:

    LoggingEnabled m_loggingEnabled;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
