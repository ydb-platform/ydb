#include "producer_client.h"

#include "private.h"

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/wire_protocol.h>

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt_proto/yt/client/api/rpc_proxy/proto/api_service.pb.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/periodic_executor.h>

namespace NYT::NQueueClient {

using namespace NApi;
using namespace NConcurrency;
using namespace NCrypto;
using namespace NTableClient;
using namespace NThreading;
using namespace NTransactionClient;
using namespace NYPath;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = QueueClientLogger;

////////////////////////////////////////////////////////////////////////////////

class TProducerSession
    : public IProducerSession
{
public:
    TProducerSession(
        IClientPtr client,
        TRichYPath producerPath,
        TRichYPath queuePath,
        TNameTablePtr nameTable,
        TQueueProducerSessionId sessionId,
        TCreateQueueProducerSessionResult createSessionResult,
        TProducerSessionOptions options,
        IInvokerPtr invoker)
        : Client_(std::move(client))
        , ProducerPath_(std::move(producerPath))
        , QueuePath_(std::move(queuePath))
        , NameTable_(std::move(nameTable))
        , SessionId_(std::move(sessionId))
        , Invoker_(std::move(invoker))
        , Options_(std::move(options))
        , Epoch_(createSessionResult.Epoch)
        , LastSequenceNumber_(createSessionResult.SequenceNumber)
        , UserMeta_(std::move(createSessionResult.UserMeta))
    {
        if (Options_.BackgroundFlushPeriod) {
            if (!Invoker_) {
                THROW_ERROR_EXCEPTION("Cannot create producer session with background flush without invoker");
            }

            FlushExecutor_ = New<TPeriodicExecutor>(
                Invoker_,
                BIND(&TProducerSession::OnFlush, NYT::MakeWeak(this)),
                *Options_.BackgroundFlushPeriod);
        } else {
            if (!Options_.BatchOptions.RowCount && !Options_.BatchOptions.ByteSize) {
                YT_LOG_DEBUG("None of batch row count or batch byte size are specified, batch byte size will be equal to 16 MB");
                Options_.BatchOptions.ByteSize = 16_MB;
            }
        }
    }

    // TODO(nadya73): add possibility to pass user meta.

    TQueueProducerSequenceNumber GetLastSequenceNumber() const override
    {
        return LastSequenceNumber_.load();
    }

    const INodePtr& GetUserMeta() const override
    {
        return UserMeta_;
    }

    bool Write(TRange<TUnversionedRow> rows) override
    {
        auto guard = Guard(SpinLock_);

        if (!Started_) {
            if (FlushExecutor_) {
                FlushExecutor_->Start();
            }
            Started_ = true;
        }

        if (Closed_ || Canceled_) {
            return false;
        }

        for (const auto& row : rows) {
            CurrentBufferWriter_->WriteUnversionedRow(row);
            ++CurrentBufferRowCount_;
        }
        if (IsCurrentBufferFull()) {
            if (!FlushExecutor_) {
                return false;
            }
            RotateBuffer();
            FlushExecutor_->ScheduleOutOfBand();
        }
        return true;
    }

    TFuture<void> GetReadyEvent() override
    {
        if (FlushExecutor_) {
            return VoidFuture;
        }

        {
            auto guard = Guard(SpinLock_);
            if (Closed_) {
                return MakeFuture<void>(TError("Producer session was closed"));
            }
        }
        return TryToFlush();
    }

    TFuture<void> Flush() override
    {
        if (FlushExecutor_) {
            FlushExecutor_->ScheduleOutOfBand();
            return FlushExecutor_->GetExecutedEvent();
        }

        return TryToFlush(/*checkIfFlushNeeded*/ false);
    }

    void Cancel() override
    {
        auto guard = Guard(SpinLock_);

        CurrentBufferWriter_ = CreateWireProtocolWriter();
        CurrentBufferRowCount_ = 0;

        BufferQueue_.clear();

        Canceled_ = true;
    }

    TFuture<void> Close() override
    {
        {
            auto guard = Guard(SpinLock_);
            Closed_ = true;
        }

        // Run one last flush will finish writing the remaining items and
        // eventually lead to the stop promise being set.
        // A single flush is enough since it is guaranteed that no new messages are added to the queue after the
        // critical section above.

        if (!FlushExecutor_) {
            return TryToFlush();
        }

        FlushExecutor_->ScheduleOutOfBand();

        return StoppedPromise_.ToFuture()
            .Apply(BIND([this, this_ = MakeStrong(this)] {
                return FlushExecutor_->Stop();
            }));
    }

    std::optional<TMD5Hash> GetDigest() const override
    {
        return std::nullopt;
    }

private:
    const IClientPtr Client_;
    const TRichYPath ProducerPath_;
    const TRichYPath QueuePath_;
    const TNameTablePtr NameTable_;
    const TQueueProducerSessionId SessionId_;
    const IInvokerPtr Invoker_;

    TProducerSessionOptions Options_;

    TQueueProducerEpoch Epoch_ = TQueueProducerEpoch{0};
    std::atomic<TQueueProducerSequenceNumber> LastSequenceNumber_ = TQueueProducerSequenceNumber{0};
    INodePtr UserMeta_;

    std::unique_ptr<IWireProtocolWriter> CurrentBufferWriter_ = CreateWireProtocolWriter();
    i64 CurrentBufferRowCount_ = 0;

    struct TBuffer
    {
        std::vector<TSharedRef> SerializedRows;
        i64 RowCount;
    };

    std::deque<TBuffer> BufferQueue_;

    bool Started_ = false;
    TPeriodicExecutorPtr FlushExecutor_;

    bool Canceled_ = false;
    bool Closed_ = false;
    TPromise<void> StoppedPromise_ = NewPromise<void>();

    YT_DECLARE_SPIN_LOCK(TSpinLock, SpinLock_);

    bool IsCurrentBufferFull() const
    {
        YT_ASSERT_SPINLOCK_AFFINITY(SpinLock_);

        return (Options_.BatchOptions.ByteSize && static_cast<i64>(CurrentBufferWriter_->GetByteSize()) >= *Options_.BatchOptions.ByteSize)
            || (Options_.BatchOptions.RowCount && CurrentBufferRowCount_ >= *Options_.BatchOptions.RowCount);
    }

    bool IsFlushNeeded() const
    {
        YT_ASSERT_SPINLOCK_AFFINITY(SpinLock_);

        return IsCurrentBufferFull() || !BufferQueue_.empty();
    }

    std::optional<TBuffer> GetBufferToFlush()
    {
        YT_ASSERT_SPINLOCK_AFFINITY(SpinLock_);

        RotateBuffer();

        if (BufferQueue_.empty()) {
            return std::nullopt;
        }
        auto buffer = std::optional<TBuffer>{std::move(BufferQueue_.front())};
        BufferQueue_.pop_front();

        return buffer;
    }

    TFuture<void> TryToFlush(bool checkIfFlushNeeded = true)
    {
        std::optional<TBuffer> buffer;
        {
            auto guard = Guard(SpinLock_);
            if ((checkIfFlushNeeded && !IsFlushNeeded()) && !Closed_) {
                return VoidFuture;
            }
            buffer = GetBufferToFlush();
        }
        if (!buffer) {
            return VoidFuture;
        }
        return FlushImpl(std::move(*buffer))
            .Apply(BIND([this, this_ = MakeStrong(this), checkIfFlushNeeded] {
                return TryToFlush(checkIfFlushNeeded);
            }));
    }

    void RotateBuffer()
    {
        YT_ASSERT_SPINLOCK_AFFINITY(SpinLock_);

        if (CurrentBufferRowCount_ <= 0) {
            return;
        }

        auto writer = CreateWireProtocolWriter();
        writer->WriteSerializedRowset(CurrentBufferRowCount_, CurrentBufferWriter_->Finish());

        BufferQueue_.push_back(TBuffer{
            .SerializedRows = writer->Finish(),
            .RowCount = CurrentBufferRowCount_,
        });

        CurrentBufferWriter_ = CreateWireProtocolWriter();
        CurrentBufferRowCount_ = 0;
    }

    void OnFlush()
    {
        std::optional<TBuffer> buffer;
        {
            auto guard = Guard(SpinLock_);

            if (Canceled_) {
                YT_LOG_DEBUG("Producer session was canceled, flush nothing");
                StoppedPromise_.TrySet();
                return;
            }

            buffer = GetBufferToFlush();
        }

        TError error;

        if (buffer) {
            auto& backoffStrategy = Options_.BackoffStrategy;
            backoffStrategy.Restart();
            while (backoffStrategy.Next()) {
                if (Canceled_) {
                    YT_LOG_DEBUG("Producer session was canceled, flush nothing");
                    StoppedPromise_.TrySet();
                    return;
                }

                // TODO(nadya73): Fix copying.
                error = WaitFor(FlushImpl(*buffer));
                if (error.IsOK()) {
                    break;
                }

                TDelayedExecutor::WaitForDuration(backoffStrategy.GetBackoff());
            }
        } else {
            YT_LOG_DEBUG("No buffer to flush, do nothing");
        }

        bool isStopped = false;
        {
            auto guard = Guard(SpinLock_);
            if (Closed_ && CurrentBufferRowCount_ == 0 && BufferQueue_.empty() || !error.IsOK()) {
                isStopped = true;
            }
        }

        if (isStopped) {
            StoppedPromise_.TrySet(error);
        }
    }

    TFuture<void> FlushImpl(TBuffer buffer)
    {
        YT_LOG_DEBUG("Trying to flush %v rows", buffer.RowCount);

        return Client_->StartTransaction(ETransactionType::Tablet)
            .Apply(BIND([buffer = std::move(buffer), this, this_ = MakeStrong(this)] (const ITransactionPtr& transaction) {
                TPushQueueProducerOptions pushQueueProducerOptions;
                if (Options_.AutoSequenceNumber) {
                    pushQueueProducerOptions.SequenceNumber = TQueueProducerSequenceNumber{LastSequenceNumber_.load().Underlying() + 1};
                }
                pushQueueProducerOptions.RequireSyncReplica = Options_.RequireSyncReplica;

                return transaction->PushQueueProducer(ProducerPath_, QueuePath_, SessionId_, Epoch_, NameTable_, buffer.SerializedRows, pushQueueProducerOptions)
                    .Apply(BIND([=, this, this_ = MakeStrong(this)] (const TPushQueueProducerResult& pushQueueProducerResult) {
                        LastSequenceNumber_.store(pushQueueProducerResult.LastSequenceNumber);
                        return transaction->Commit();
                    }))
                    .Apply(BIND([this, this_ = MakeStrong(this)] (const TTransactionCommitResult&) {
                        if (Options_.AckCallback) {
                            Options_.AckCallback(LastSequenceNumber_);
                        }
                    }));
            })).AsVoid();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TProducerClient
    : public IProducerClient
{
public:
    TProducerClient(IClientPtr client, TRichYPath producerPath)
        : Client_(std::move(client))
        , ProducerPath_(std::move(producerPath))
    { }

    TFuture<IProducerSessionPtr> CreateSession(
        const TRichYPath& queuePath,
        const TNameTablePtr& nameTable,
        const TQueueProducerSessionId& sessionId,
        const TProducerSessionOptions& options,
        const IInvokerPtr& invoker) override
    {
        return Client_->CreateQueueProducerSession(ProducerPath_, queuePath, sessionId)
            .Apply(BIND([=, this, this_ = MakeStrong(this)]
                        (const TCreateQueueProducerSessionResult& createSessionResult) -> IProducerSessionPtr {
                return New<TProducerSession>(Client_, ProducerPath_, queuePath, nameTable, sessionId, createSessionResult, options, invoker);
            }));
    }

private:
    const IClientPtr Client_;
    const TRichYPath ProducerPath_;
};

////////////////////////////////////////////////////////////////////////////////

IProducerClientPtr CreateProducerClient(
    const IClientPtr& client,
    const TRichYPath& producerPath)
{
    return New<TProducerClient>(client, producerPath);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueClient
