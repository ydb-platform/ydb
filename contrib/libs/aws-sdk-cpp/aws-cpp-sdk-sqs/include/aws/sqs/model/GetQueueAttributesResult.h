/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once
#include <aws/sqs/SQS_EXPORTS.h>
#include <aws/core/utils/memory/stl/AWSMap.h>
#include <aws/sqs/model/ResponseMetadata.h>
#include <aws/sqs/model/QueueAttributeName.h>
#include <aws/core/utils/memory/stl/AWSString.h>
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
namespace SQS
{
namespace Model
{
  /**
   * <p>A list of returned queue attributes.</p><p><h3>See Also:</h3>   <a
   * href="http://docs.aws.amazon.com/goto/WebAPI/sqs-2012-11-05/GetQueueAttributesResult">AWS
   * API Reference</a></p>
   */
  class GetQueueAttributesResult
  {
  public:
    AWS_SQS_API GetQueueAttributesResult();
    AWS_SQS_API GetQueueAttributesResult(const Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>& result);
    AWS_SQS_API GetQueueAttributesResult& operator=(const Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>& result);


    /**
     * <p>A map of attributes to their respective values.</p>
     */
    inline const Aws::Map<QueueAttributeName, Aws::String>& GetAttributes() const{ return m_attributes; }

    /**
     * <p>A map of attributes to their respective values.</p>
     */
    inline void SetAttributes(const Aws::Map<QueueAttributeName, Aws::String>& value) { m_attributes = value; }

    /**
     * <p>A map of attributes to their respective values.</p>
     */
    inline void SetAttributes(Aws::Map<QueueAttributeName, Aws::String>&& value) { m_attributes = std::move(value); }

    /**
     * <p>A map of attributes to their respective values.</p>
     */
    inline GetQueueAttributesResult& WithAttributes(const Aws::Map<QueueAttributeName, Aws::String>& value) { SetAttributes(value); return *this;}

    /**
     * <p>A map of attributes to their respective values.</p>
     */
    inline GetQueueAttributesResult& WithAttributes(Aws::Map<QueueAttributeName, Aws::String>&& value) { SetAttributes(std::move(value)); return *this;}

    /**
     * <p>A map of attributes to their respective values.</p>
     */
    inline GetQueueAttributesResult& AddAttributes(const QueueAttributeName& key, const Aws::String& value) { m_attributes.emplace(key, value); return *this; }

    /**
     * <p>A map of attributes to their respective values.</p>
     */
    inline GetQueueAttributesResult& AddAttributes(QueueAttributeName&& key, const Aws::String& value) { m_attributes.emplace(std::move(key), value); return *this; }

    /**
     * <p>A map of attributes to their respective values.</p>
     */
    inline GetQueueAttributesResult& AddAttributes(const QueueAttributeName& key, Aws::String&& value) { m_attributes.emplace(key, std::move(value)); return *this; }

    /**
     * <p>A map of attributes to their respective values.</p>
     */
    inline GetQueueAttributesResult& AddAttributes(QueueAttributeName&& key, Aws::String&& value) { m_attributes.emplace(std::move(key), std::move(value)); return *this; }

    /**
     * <p>A map of attributes to their respective values.</p>
     */
    inline GetQueueAttributesResult& AddAttributes(QueueAttributeName&& key, const char* value) { m_attributes.emplace(std::move(key), value); return *this; }

    /**
     * <p>A map of attributes to their respective values.</p>
     */
    inline GetQueueAttributesResult& AddAttributes(const QueueAttributeName& key, const char* value) { m_attributes.emplace(key, value); return *this; }


    
    inline const ResponseMetadata& GetResponseMetadata() const{ return m_responseMetadata; }

    
    inline void SetResponseMetadata(const ResponseMetadata& value) { m_responseMetadata = value; }

    
    inline void SetResponseMetadata(ResponseMetadata&& value) { m_responseMetadata = std::move(value); }

    
    inline GetQueueAttributesResult& WithResponseMetadata(const ResponseMetadata& value) { SetResponseMetadata(value); return *this;}

    
    inline GetQueueAttributesResult& WithResponseMetadata(ResponseMetadata&& value) { SetResponseMetadata(std::move(value)); return *this;}

  private:

    Aws::Map<QueueAttributeName, Aws::String> m_attributes;

    ResponseMetadata m_responseMetadata;
  };

} // namespace Model
} // namespace SQS
} // namespace Aws
