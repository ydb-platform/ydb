/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once
#include <aws/sqs/SQS_EXPORTS.h>
#include <aws/sqs/SQSRequest.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <utility>

namespace Aws
{
namespace SQS
{
namespace Model
{

  /**
   * <p/><p><h3>See Also:</h3>   <a
   * href="http://docs.aws.amazon.com/goto/WebAPI/sqs-2012-11-05/ListQueuesRequest">AWS
   * API Reference</a></p>
   */
  class ListQueuesRequest : public SQSRequest
  {
  public:
    AWS_SQS_API ListQueuesRequest();

    // Service request name is the Operation name which will send this request out,
    // each operation should has unique request name, so that we can get operation's name from this request.
    // Note: this is not true for response, multiple operations may have the same response name,
    // so we can not get operation's name from response.
    inline virtual const char* GetServiceRequestName() const override { return "ListQueues"; }

    AWS_SQS_API Aws::String SerializePayload() const override;

  protected:
    AWS_SQS_API void DumpBodyToUrl(Aws::Http::URI& uri ) const override;

  public:

    /**
     * <p>A string to use for filtering the list results. Only those queues whose name
     * begins with the specified string are returned.</p> <p>Queue URLs and names are
     * case-sensitive.</p>
     */
    inline const Aws::String& GetQueueNamePrefix() const{ return m_queueNamePrefix; }

    /**
     * <p>A string to use for filtering the list results. Only those queues whose name
     * begins with the specified string are returned.</p> <p>Queue URLs and names are
     * case-sensitive.</p>
     */
    inline bool QueueNamePrefixHasBeenSet() const { return m_queueNamePrefixHasBeenSet; }

    /**
     * <p>A string to use for filtering the list results. Only those queues whose name
     * begins with the specified string are returned.</p> <p>Queue URLs and names are
     * case-sensitive.</p>
     */
    inline void SetQueueNamePrefix(const Aws::String& value) { m_queueNamePrefixHasBeenSet = true; m_queueNamePrefix = value; }

    /**
     * <p>A string to use for filtering the list results. Only those queues whose name
     * begins with the specified string are returned.</p> <p>Queue URLs and names are
     * case-sensitive.</p>
     */
    inline void SetQueueNamePrefix(Aws::String&& value) { m_queueNamePrefixHasBeenSet = true; m_queueNamePrefix = std::move(value); }

    /**
     * <p>A string to use for filtering the list results. Only those queues whose name
     * begins with the specified string are returned.</p> <p>Queue URLs and names are
     * case-sensitive.</p>
     */
    inline void SetQueueNamePrefix(const char* value) { m_queueNamePrefixHasBeenSet = true; m_queueNamePrefix.assign(value); }

    /**
     * <p>A string to use for filtering the list results. Only those queues whose name
     * begins with the specified string are returned.</p> <p>Queue URLs and names are
     * case-sensitive.</p>
     */
    inline ListQueuesRequest& WithQueueNamePrefix(const Aws::String& value) { SetQueueNamePrefix(value); return *this;}

    /**
     * <p>A string to use for filtering the list results. Only those queues whose name
     * begins with the specified string are returned.</p> <p>Queue URLs and names are
     * case-sensitive.</p>
     */
    inline ListQueuesRequest& WithQueueNamePrefix(Aws::String&& value) { SetQueueNamePrefix(std::move(value)); return *this;}

    /**
     * <p>A string to use for filtering the list results. Only those queues whose name
     * begins with the specified string are returned.</p> <p>Queue URLs and names are
     * case-sensitive.</p>
     */
    inline ListQueuesRequest& WithQueueNamePrefix(const char* value) { SetQueueNamePrefix(value); return *this;}


    /**
     * <p>Pagination token to request the next set of results.</p>
     */
    inline const Aws::String& GetNextToken() const{ return m_nextToken; }

    /**
     * <p>Pagination token to request the next set of results.</p>
     */
    inline bool NextTokenHasBeenSet() const { return m_nextTokenHasBeenSet; }

    /**
     * <p>Pagination token to request the next set of results.</p>
     */
    inline void SetNextToken(const Aws::String& value) { m_nextTokenHasBeenSet = true; m_nextToken = value; }

    /**
     * <p>Pagination token to request the next set of results.</p>
     */
    inline void SetNextToken(Aws::String&& value) { m_nextTokenHasBeenSet = true; m_nextToken = std::move(value); }

    /**
     * <p>Pagination token to request the next set of results.</p>
     */
    inline void SetNextToken(const char* value) { m_nextTokenHasBeenSet = true; m_nextToken.assign(value); }

    /**
     * <p>Pagination token to request the next set of results.</p>
     */
    inline ListQueuesRequest& WithNextToken(const Aws::String& value) { SetNextToken(value); return *this;}

    /**
     * <p>Pagination token to request the next set of results.</p>
     */
    inline ListQueuesRequest& WithNextToken(Aws::String&& value) { SetNextToken(std::move(value)); return *this;}

    /**
     * <p>Pagination token to request the next set of results.</p>
     */
    inline ListQueuesRequest& WithNextToken(const char* value) { SetNextToken(value); return *this;}


    /**
     * <p>Maximum number of results to include in the response. Value range is 1 to
     * 1000. You must set <code>MaxResults</code> to receive a value for
     * <code>NextToken</code> in the response.</p>
     */
    inline int GetMaxResults() const{ return m_maxResults; }

    /**
     * <p>Maximum number of results to include in the response. Value range is 1 to
     * 1000. You must set <code>MaxResults</code> to receive a value for
     * <code>NextToken</code> in the response.</p>
     */
    inline bool MaxResultsHasBeenSet() const { return m_maxResultsHasBeenSet; }

    /**
     * <p>Maximum number of results to include in the response. Value range is 1 to
     * 1000. You must set <code>MaxResults</code> to receive a value for
     * <code>NextToken</code> in the response.</p>
     */
    inline void SetMaxResults(int value) { m_maxResultsHasBeenSet = true; m_maxResults = value; }

    /**
     * <p>Maximum number of results to include in the response. Value range is 1 to
     * 1000. You must set <code>MaxResults</code> to receive a value for
     * <code>NextToken</code> in the response.</p>
     */
    inline ListQueuesRequest& WithMaxResults(int value) { SetMaxResults(value); return *this;}

  private:

    Aws::String m_queueNamePrefix;
    bool m_queueNamePrefixHasBeenSet = false;

    Aws::String m_nextToken;
    bool m_nextTokenHasBeenSet = false;

    int m_maxResults;
    bool m_maxResultsHasBeenSet = false;
  };

} // namespace Model
} // namespace SQS
} // namespace Aws
