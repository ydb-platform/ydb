/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once
#include <aws/sqs/SQS_EXPORTS.h>
#include <aws/core/utils/memory/stl/AWSVector.h>
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
   * <p>A list of your queues.</p><p><h3>See Also:</h3>   <a
   * href="http://docs.aws.amazon.com/goto/WebAPI/sqs-2012-11-05/ListQueuesResult">AWS
   * API Reference</a></p>
   */
  class ListQueuesResult
  {
  public:
    AWS_SQS_API ListQueuesResult();
    AWS_SQS_API ListQueuesResult(const Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>& result);
    AWS_SQS_API ListQueuesResult& operator=(const Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>& result);


    /**
     * <p>A list of queue URLs, up to 1,000 entries, or the value of MaxResults that
     * you sent in the request.</p>
     */
    inline const Aws::Vector<Aws::String>& GetQueueUrls() const{ return m_queueUrls; }

    /**
     * <p>A list of queue URLs, up to 1,000 entries, or the value of MaxResults that
     * you sent in the request.</p>
     */
    inline void SetQueueUrls(const Aws::Vector<Aws::String>& value) { m_queueUrls = value; }

    /**
     * <p>A list of queue URLs, up to 1,000 entries, or the value of MaxResults that
     * you sent in the request.</p>
     */
    inline void SetQueueUrls(Aws::Vector<Aws::String>&& value) { m_queueUrls = std::move(value); }

    /**
     * <p>A list of queue URLs, up to 1,000 entries, or the value of MaxResults that
     * you sent in the request.</p>
     */
    inline ListQueuesResult& WithQueueUrls(const Aws::Vector<Aws::String>& value) { SetQueueUrls(value); return *this;}

    /**
     * <p>A list of queue URLs, up to 1,000 entries, or the value of MaxResults that
     * you sent in the request.</p>
     */
    inline ListQueuesResult& WithQueueUrls(Aws::Vector<Aws::String>&& value) { SetQueueUrls(std::move(value)); return *this;}

    /**
     * <p>A list of queue URLs, up to 1,000 entries, or the value of MaxResults that
     * you sent in the request.</p>
     */
    inline ListQueuesResult& AddQueueUrls(const Aws::String& value) { m_queueUrls.push_back(value); return *this; }

    /**
     * <p>A list of queue URLs, up to 1,000 entries, or the value of MaxResults that
     * you sent in the request.</p>
     */
    inline ListQueuesResult& AddQueueUrls(Aws::String&& value) { m_queueUrls.push_back(std::move(value)); return *this; }

    /**
     * <p>A list of queue URLs, up to 1,000 entries, or the value of MaxResults that
     * you sent in the request.</p>
     */
    inline ListQueuesResult& AddQueueUrls(const char* value) { m_queueUrls.push_back(value); return *this; }


    /**
     * <p>Pagination token to include in the next request. Token value is
     * <code>null</code> if there are no additional results to request, or if you did
     * not set <code>MaxResults</code> in the request.</p>
     */
    inline const Aws::String& GetNextToken() const{ return m_nextToken; }

    /**
     * <p>Pagination token to include in the next request. Token value is
     * <code>null</code> if there are no additional results to request, or if you did
     * not set <code>MaxResults</code> in the request.</p>
     */
    inline void SetNextToken(const Aws::String& value) { m_nextToken = value; }

    /**
     * <p>Pagination token to include in the next request. Token value is
     * <code>null</code> if there are no additional results to request, or if you did
     * not set <code>MaxResults</code> in the request.</p>
     */
    inline void SetNextToken(Aws::String&& value) { m_nextToken = std::move(value); }

    /**
     * <p>Pagination token to include in the next request. Token value is
     * <code>null</code> if there are no additional results to request, or if you did
     * not set <code>MaxResults</code> in the request.</p>
     */
    inline void SetNextToken(const char* value) { m_nextToken.assign(value); }

    /**
     * <p>Pagination token to include in the next request. Token value is
     * <code>null</code> if there are no additional results to request, or if you did
     * not set <code>MaxResults</code> in the request.</p>
     */
    inline ListQueuesResult& WithNextToken(const Aws::String& value) { SetNextToken(value); return *this;}

    /**
     * <p>Pagination token to include in the next request. Token value is
     * <code>null</code> if there are no additional results to request, or if you did
     * not set <code>MaxResults</code> in the request.</p>
     */
    inline ListQueuesResult& WithNextToken(Aws::String&& value) { SetNextToken(std::move(value)); return *this;}

    /**
     * <p>Pagination token to include in the next request. Token value is
     * <code>null</code> if there are no additional results to request, or if you did
     * not set <code>MaxResults</code> in the request.</p>
     */
    inline ListQueuesResult& WithNextToken(const char* value) { SetNextToken(value); return *this;}


    
    inline const ResponseMetadata& GetResponseMetadata() const{ return m_responseMetadata; }

    
    inline void SetResponseMetadata(const ResponseMetadata& value) { m_responseMetadata = value; }

    
    inline void SetResponseMetadata(ResponseMetadata&& value) { m_responseMetadata = std::move(value); }

    
    inline ListQueuesResult& WithResponseMetadata(const ResponseMetadata& value) { SetResponseMetadata(value); return *this;}

    
    inline ListQueuesResult& WithResponseMetadata(ResponseMetadata&& value) { SetResponseMetadata(std::move(value)); return *this;}

  private:

    Aws::Vector<Aws::String> m_queueUrls;

    Aws::String m_nextToken;

    ResponseMetadata m_responseMetadata;
  };

} // namespace Model
} // namespace SQS
} // namespace Aws
