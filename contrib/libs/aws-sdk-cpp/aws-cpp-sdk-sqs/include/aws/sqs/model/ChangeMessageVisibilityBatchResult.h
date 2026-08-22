/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once
#include <aws/sqs/SQS_EXPORTS.h>
#include <aws/core/utils/memory/stl/AWSVector.h>
#include <aws/sqs/model/ResponseMetadata.h>
#include <aws/sqs/model/ChangeMessageVisibilityBatchResultEntry.h>
#include <aws/sqs/model/BatchResultErrorEntry.h>
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
   * <p>For each message in the batch, the response contains a <code>
   * <a>ChangeMessageVisibilityBatchResultEntry</a> </code> tag if the message
   * succeeds or a <code> <a>BatchResultErrorEntry</a> </code> tag if the message
   * fails.</p><p><h3>See Also:</h3>   <a
   * href="http://docs.aws.amazon.com/goto/WebAPI/sqs-2012-11-05/ChangeMessageVisibilityBatchResult">AWS
   * API Reference</a></p>
   */
  class ChangeMessageVisibilityBatchResult
  {
  public:
    AWS_SQS_API ChangeMessageVisibilityBatchResult();
    AWS_SQS_API ChangeMessageVisibilityBatchResult(const Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>& result);
    AWS_SQS_API ChangeMessageVisibilityBatchResult& operator=(const Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>& result);


    /**
     * <p>A list of <code> <a>ChangeMessageVisibilityBatchResultEntry</a> </code>
     * items.</p>
     */
    inline const Aws::Vector<ChangeMessageVisibilityBatchResultEntry>& GetSuccessful() const{ return m_successful; }

    /**
     * <p>A list of <code> <a>ChangeMessageVisibilityBatchResultEntry</a> </code>
     * items.</p>
     */
    inline void SetSuccessful(const Aws::Vector<ChangeMessageVisibilityBatchResultEntry>& value) { m_successful = value; }

    /**
     * <p>A list of <code> <a>ChangeMessageVisibilityBatchResultEntry</a> </code>
     * items.</p>
     */
    inline void SetSuccessful(Aws::Vector<ChangeMessageVisibilityBatchResultEntry>&& value) { m_successful = std::move(value); }

    /**
     * <p>A list of <code> <a>ChangeMessageVisibilityBatchResultEntry</a> </code>
     * items.</p>
     */
    inline ChangeMessageVisibilityBatchResult& WithSuccessful(const Aws::Vector<ChangeMessageVisibilityBatchResultEntry>& value) { SetSuccessful(value); return *this;}

    /**
     * <p>A list of <code> <a>ChangeMessageVisibilityBatchResultEntry</a> </code>
     * items.</p>
     */
    inline ChangeMessageVisibilityBatchResult& WithSuccessful(Aws::Vector<ChangeMessageVisibilityBatchResultEntry>&& value) { SetSuccessful(std::move(value)); return *this;}

    /**
     * <p>A list of <code> <a>ChangeMessageVisibilityBatchResultEntry</a> </code>
     * items.</p>
     */
    inline ChangeMessageVisibilityBatchResult& AddSuccessful(const ChangeMessageVisibilityBatchResultEntry& value) { m_successful.push_back(value); return *this; }

    /**
     * <p>A list of <code> <a>ChangeMessageVisibilityBatchResultEntry</a> </code>
     * items.</p>
     */
    inline ChangeMessageVisibilityBatchResult& AddSuccessful(ChangeMessageVisibilityBatchResultEntry&& value) { m_successful.push_back(std::move(value)); return *this; }


    /**
     * <p>A list of <code> <a>BatchResultErrorEntry</a> </code> items.</p>
     */
    inline const Aws::Vector<BatchResultErrorEntry>& GetFailed() const{ return m_failed; }

    /**
     * <p>A list of <code> <a>BatchResultErrorEntry</a> </code> items.</p>
     */
    inline void SetFailed(const Aws::Vector<BatchResultErrorEntry>& value) { m_failed = value; }

    /**
     * <p>A list of <code> <a>BatchResultErrorEntry</a> </code> items.</p>
     */
    inline void SetFailed(Aws::Vector<BatchResultErrorEntry>&& value) { m_failed = std::move(value); }

    /**
     * <p>A list of <code> <a>BatchResultErrorEntry</a> </code> items.</p>
     */
    inline ChangeMessageVisibilityBatchResult& WithFailed(const Aws::Vector<BatchResultErrorEntry>& value) { SetFailed(value); return *this;}

    /**
     * <p>A list of <code> <a>BatchResultErrorEntry</a> </code> items.</p>
     */
    inline ChangeMessageVisibilityBatchResult& WithFailed(Aws::Vector<BatchResultErrorEntry>&& value) { SetFailed(std::move(value)); return *this;}

    /**
     * <p>A list of <code> <a>BatchResultErrorEntry</a> </code> items.</p>
     */
    inline ChangeMessageVisibilityBatchResult& AddFailed(const BatchResultErrorEntry& value) { m_failed.push_back(value); return *this; }

    /**
     * <p>A list of <code> <a>BatchResultErrorEntry</a> </code> items.</p>
     */
    inline ChangeMessageVisibilityBatchResult& AddFailed(BatchResultErrorEntry&& value) { m_failed.push_back(std::move(value)); return *this; }


    
    inline const ResponseMetadata& GetResponseMetadata() const{ return m_responseMetadata; }

    
    inline void SetResponseMetadata(const ResponseMetadata& value) { m_responseMetadata = value; }

    
    inline void SetResponseMetadata(ResponseMetadata&& value) { m_responseMetadata = std::move(value); }

    
    inline ChangeMessageVisibilityBatchResult& WithResponseMetadata(const ResponseMetadata& value) { SetResponseMetadata(value); return *this;}

    
    inline ChangeMessageVisibilityBatchResult& WithResponseMetadata(ResponseMetadata&& value) { SetResponseMetadata(std::move(value)); return *this;}

  private:

    Aws::Vector<ChangeMessageVisibilityBatchResultEntry> m_successful;

    Aws::Vector<BatchResultErrorEntry> m_failed;

    ResponseMetadata m_responseMetadata;
  };

} // namespace Model
} // namespace SQS
} // namespace Aws
