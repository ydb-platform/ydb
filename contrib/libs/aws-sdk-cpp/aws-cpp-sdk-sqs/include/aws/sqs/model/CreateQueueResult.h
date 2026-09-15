/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once
#include <aws/sqs/SQS_EXPORTS.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/sqs/model/ResponseMetadata.h>
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
   * <p>Returns the <code>QueueUrl</code> attribute of the created
   * queue.</p><p><h3>See Also:</h3>   <a
   * href="http://docs.aws.amazon.com/goto/WebAPI/sqs-2012-11-05/CreateQueueResult">AWS
   * API Reference</a></p>
   */
  class CreateQueueResult
  {
  public:
    AWS_SQS_API CreateQueueResult();
    AWS_SQS_API CreateQueueResult(const Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>& result);
    AWS_SQS_API CreateQueueResult& operator=(const Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>& result);


    /**
     * <p>The URL of the created Amazon SQS queue.</p>
     */
    inline const Aws::String& GetQueueUrl() const{ return m_queueUrl; }

    /**
     * <p>The URL of the created Amazon SQS queue.</p>
     */
    inline void SetQueueUrl(const Aws::String& value) { m_queueUrl = value; }

    /**
     * <p>The URL of the created Amazon SQS queue.</p>
     */
    inline void SetQueueUrl(Aws::String&& value) { m_queueUrl = std::move(value); }

    /**
     * <p>The URL of the created Amazon SQS queue.</p>
     */
    inline void SetQueueUrl(const char* value) { m_queueUrl.assign(value); }

    /**
     * <p>The URL of the created Amazon SQS queue.</p>
     */
    inline CreateQueueResult& WithQueueUrl(const Aws::String& value) { SetQueueUrl(value); return *this;}

    /**
     * <p>The URL of the created Amazon SQS queue.</p>
     */
    inline CreateQueueResult& WithQueueUrl(Aws::String&& value) { SetQueueUrl(std::move(value)); return *this;}

    /**
     * <p>The URL of the created Amazon SQS queue.</p>
     */
    inline CreateQueueResult& WithQueueUrl(const char* value) { SetQueueUrl(value); return *this;}


    
    inline const ResponseMetadata& GetResponseMetadata() const{ return m_responseMetadata; }

    
    inline void SetResponseMetadata(const ResponseMetadata& value) { m_responseMetadata = value; }

    
    inline void SetResponseMetadata(ResponseMetadata&& value) { m_responseMetadata = std::move(value); }

    
    inline CreateQueueResult& WithResponseMetadata(const ResponseMetadata& value) { SetResponseMetadata(value); return *this;}

    
    inline CreateQueueResult& WithResponseMetadata(ResponseMetadata&& value) { SetResponseMetadata(std::move(value)); return *this;}

  private:

    Aws::String m_queueUrl;

    ResponseMetadata m_responseMetadata;
  };

} // namespace Model
} // namespace SQS
} // namespace Aws
