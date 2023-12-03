/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once
#include <aws/s3/S3_EXPORTS.h>
#include <aws/s3/model/ReplicationConfiguration.h>
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
  class GetBucketReplicationResult
  {
  public:
    AWS_S3_API GetBucketReplicationResult();
    AWS_S3_API GetBucketReplicationResult(const Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>& result);
    AWS_S3_API GetBucketReplicationResult& operator=(const Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>& result);


    
    inline const ReplicationConfiguration& GetReplicationConfiguration() const{ return m_replicationConfiguration; }

    
    inline void SetReplicationConfiguration(const ReplicationConfiguration& value) { m_replicationConfiguration = value; }

    
    inline void SetReplicationConfiguration(ReplicationConfiguration&& value) { m_replicationConfiguration = std::move(value); }

    
    inline GetBucketReplicationResult& WithReplicationConfiguration(const ReplicationConfiguration& value) { SetReplicationConfiguration(value); return *this;}

    
    inline GetBucketReplicationResult& WithReplicationConfiguration(ReplicationConfiguration&& value) { SetReplicationConfiguration(std::move(value)); return *this;}

  private:

    ReplicationConfiguration m_replicationConfiguration;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
