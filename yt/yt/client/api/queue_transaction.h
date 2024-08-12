#pragma once

#include "client_common.h"

#include <yt/yt/client/queue_client/public.h>

#include <yt/yt/core/ypath/public.h>

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

struct TAdvanceQueueConsumerOptions
    : public TTimeoutOptions
{ };

struct TPushQueueProducerOptions
    : public TTimeoutOptions
{
    //! Sequence number of the first row in the batch.
    /*!
     * Rows in the batch will have such sequence numbers: SequenceNumber, SequenceNumber+1, SequenceNumber+2, ...
     * If the option is not set the $sequence_number column must be present in each row.
     */
    std::optional<NQueueClient::TQueueProducerSequenceNumber> SequenceNumber;

    //! Any yson data which will be saved for the session
    /*!
     * It can be retrieved later and used, for example, to understand
     * what data should be written after fail of the user process.
     */
    NYTree::INodePtr UserMeta;
};

struct TPushQueueProducerResult
{
    //! Sequence number of the last row in the written batch.
    /*!
     * All rows with greater sequence number will be ignored in future calls of PushQueueProducer.
     */
    NQueueClient::TQueueProducerSequenceNumber LastSequenceNumber{-1};
    //! Number of rows which were skipped because of their sequence number.
    /*!
     * Skipped rows were written before.
     */
    i64 SkippedRowCount = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IQueueTransaction
{
    virtual ~IQueueTransaction() = default;

    // TODO(nadya73): Remove it: YT-20712
    virtual void AdvanceConsumer(
        const NYPath::TRichYPath& consumerPath,
        const NYPath::TRichYPath& queuePath,
        int partitionIndex,
        std::optional<i64> oldOffset,
        i64 newOffset) = 0;

    //! Advances the consumer's offset for partition with index #partitionIndex, setting it to #newOffset.
    /*!
     *  If #oldOffset is specified, the current offset is read inside this transaction and compared with #oldOffset.
     *  If they are equal, the new offset is written, otherwise an exception is thrown.
     */
    virtual TFuture<void> AdvanceQueueConsumer(
        const NYPath::TRichYPath& consumerPath,
        const NYPath::TRichYPath& queuePath,
        int partitionIndex,
        std::optional<i64> oldOffset,
        i64 newOffset,
        const TAdvanceQueueConsumerOptions& options) = 0;

    //! Writes rows in the queue with checking their sequence number.
    /*!
     * If row sequence number is less than sequence number saved in producer table, then this row will not be written.
     * #sessionId - an identificator of write session, for example, `<host>-<filename>`.
     * #epoch - a number of producer epoch. All calls with an epoch less than the current epoch will fail.
     */
    virtual TFuture<TPushQueueProducerResult> PushQueueProducer(
        const NYPath::TRichYPath& producerPath,
        const NYPath::TRichYPath& queuePath,
        const NQueueClient::TQueueProducerSessionId& sessionId,
        NQueueClient::TQueueProducerEpoch epoch,
        NTableClient::TNameTablePtr nameTable,
        TSharedRange<NTableClient::TUnversionedRow> rows,
        const TPushQueueProducerOptions& options = {}) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi

