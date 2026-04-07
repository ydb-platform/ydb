/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once
#include <aws/sqs/SQS_EXPORTS.h>
#include <aws/core/utils/memory/stl/AWSVector.h>
#include <aws/sqs/model/ResponseMetadata.h>
#include <aws/sqs/model/DeleteMessageBatchResultEntry.h>
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
   * <a>DeleteMessageBatchResultEntry</a> </code> tag if the message is deleted or a
   * <code> <a>BatchResultErrorEntry</a> </code> tag if the message can't be
   * deleted.</p><p><h3>See Also:</h3>   <a
   * href="http://docs.aws.amazon.com/goto/WebAPI/sqs-2012-11-05/DeleteMessageBatchResult">AWS
   * API Reference</a></p>
   */
  class DeleteMessageBatchResult
  {
  public:
    AWS_SQS_API DeleteMessageBatchResult();
    AWS_SQS_API DeleteMessageBatchResult(const Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>& result);
    AWS_SQS_API DeleteMessageBatchResult& operator=(const Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>& result);


    /**
     * <p>A list of <code> <a>DeleteMessageBatchResultEntry</a> </code> items.</p>
     */
    inline const Aws::Vector<DeleteMessageBatchResultEntry>& GetSuccessful() const{ return m_successful; }

    /**
     * <p>A list of <code> <a>DeleteMessageBatchResultEntry</a> </code> items.</p>
     */
    inline void SetSuccessful(const Aws::Vector<DeleteMessageBatchResultEntry>& value) { m_successful = value; }

    /**
     * <p>A list of <code> <a>DeleteMessageBatchResultEntry</a> </code> items.</p>
     */
    inline void SetSuccessful(Aws::Vector<DeleteMessageBatchResultEntry>&& value) { m_successful = std::move(value); }

    /**
     * <p>A list of <code> <a>DeleteMessageBatchResultEntry</a> </code> items.</p>
     */
    inline DeleteMessageBatchResult& WithSuccessful(const Aws::Vector<DeleteMessageBatchResultEntry>& value) { SetSuccessful(value); return *this;}

    /**
     * <p>A list of <code> <a>DeleteMessageBatchResultEntry</a> </code> items.</p>
     */
    inline DeleteMessageBatchResult& WithSuccessful(Aws::Vector<DeleteMessageBatchResultEntry>&& value) { SetSuccessful(std::move(value)); return *this;}

    /**
     * <p>A list of <code> <a>DeleteMessageBatchResultEntry</a> </code> items.</p>
     */
    inline DeleteMessageBatchResult& AddSuccessful(const DeleteMessageBatchResultEntry& value) { m_successful.push_back(value); return *this; }

    /**
     * <p>A list of <code> <a>DeleteMessageBatchResultEntry</a> </code> items.</p>
     */
    inline DeleteMessageBatchResult& AddSuccessful(DeleteMessageBatchResultEntry&& value) { m_successful.push_back(std::move(value)); return *this; }


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
    inline DeleteMessageBatchResult& WithFailed(const Aws::Vector<BatchResultErrorEntry>& value) { SetFailed(value); return *this;}

    /**
     * <p>A list of <code> <a>BatchResultErrorEntry</a> </code> items.</p>
     */
    inline DeleteMessageBatchResult& WithFailed(Aws::Vector<BatchResultErrorEntry>&& value) { SetFailed(std::move(value)); return *this;}

    /**
     * <p>A list of <code> <a>BatchResultErrorEntry</a> </code> items.</p>
     */
    inline DeleteMessageBatchResult& AddFailed(const BatchResultErrorEntry& value) { m_failed.push_back(value); return *this; }

    /**
     * <p>A list of <code> <a>BatchResultErrorEntry</a> </code> items.</p>
     */
    inline DeleteMessageBatchResult& AddFailed(BatchResultErrorEntry&& value) { m_failed.push_back(std::move(value)); return *this; }


    
    inline const ResponseMetadata& GetResponseMetadata() const{ return m_responseMetadata; }

    
    inline void SetResponseMetadata(const ResponseMetadata& value) { m_responseMetadata = value; }

    
    inline void SetResponseMetadata(ResponseMetadata&& value) { m_responseMetadata = std::move(value); }

    
    inline DeleteMessageBatchResult& WithResponseMetadata(const ResponseMetadata& value) { SetResponseMetadata(value); return *this;}

    
    inline DeleteMessageBatchResult& WithResponseMetadata(ResponseMetadata&& value) { SetResponseMetadata(std::move(value)); return *this;}

  private:

    Aws::Vector<DeleteMessageBatchResultEntry> m_successful;

    Aws::Vector<BatchResultErrorEntry> m_failed;

    ResponseMetadata m_responseMetadata;
  };

} // namespace Model
} // namespace SQS
} // namespace Aws
