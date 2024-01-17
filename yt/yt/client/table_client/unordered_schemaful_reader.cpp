#include "unordered_schemaful_reader.h"

#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/unversioned_reader.h>
#include <yt/yt/client/table_client/row_batch.h>

#include <yt/yt/core/actions/cancelable_context.h>

#include <library/cpp/yt/threading/spin_lock.h>

#include <util/random/shuffle.h>

namespace NYT::NTableClient {

using namespace NConcurrency;
using namespace NChunkClient::NProto;

////////////////////////////////////////////////////////////////////////////////

// 1. Sequential prefetch
//    - 0
//    - 1
//    - all
// 2. Unordered
//    - full concurrency and prefetch

////////////////////////////////////////////////////////////////////////////////

class TUnorderedSchemafulReader
    : public ISchemafulUnversionedReader
{
public:
    TUnorderedSchemafulReader(
        std::function<ISchemafulUnversionedReaderPtr()> getNextReader,
        int concurrency)
        : GetNextReader_(std::move(getNextReader))
    {
        Sessions_.reserve(concurrency);
        for (int index = 0; index < concurrency; ++index) {
            auto reader = GetNextReader_();
            if (!reader) {
                Exhausted_ = true;
                break;
            }
            Sessions_.emplace_back(std::move(reader));
        }
    }

    ~TUnorderedSchemafulReader()
    {
        CancelableContext_->Cancel(TError("Reader destroyed"));
    }

    IUnversionedRowBatchPtr Read(const TRowBatchReadOptions& options) override
    {
        bool hasPending = false;

        for (auto& session : Sessions_) {
            if (session.Exhausted) {
                continue;
            }

            if (session.ReadyEvent) {
                if (!session.ReadyEvent->IsSet()) {
                    hasPending = true;
                    continue;
                }

                const auto& error = session.ReadyEvent->Get();
                if (!error.IsOK()) {
                    auto guard = WriterGuard(SpinLock_);
                    ReadyEvent_ = MakePromise<void>(error);
                    return CreateEmptyUnversionedRowBatch();
                }

                session.ReadyEvent->Reset();
            }

            // TODO(babenko): consider adjusting options w.r.t. concurrency.
            auto batch = session.Reader->Read(options);
            if (!batch) {
                session.Exhausted = true;
                if (RefillSession(session)) {
                    hasPending = true;
                }
                continue;
            }

            if (!batch->IsEmpty()) {
                return batch;
            }

            YT_ASSERT(!session.ReadyEvent);
            UpdateSession(session);
            hasPending = true;
        }

        if (!hasPending) {
            return nullptr;
        }

        auto readyEvent = NewPromise<void>();
        {
            auto guard = WriterGuard(SpinLock_);
            ReadyEvent_ = readyEvent;
        }

        for (auto& session : Sessions_) {
            if (session.ReadyEvent) {
                readyEvent.TrySetFrom(*session.ReadyEvent);
            }
        }

        readyEvent.OnCanceled(BIND(&TUnorderedSchemafulReader::OnCanceled, MakeWeak(this)));

        return CreateEmptyUnversionedRowBatch();
    }

    TFuture<void> GetReadyEvent() const override
    {
        return DoGetReadyEvent();
    }

    TDataStatistics GetDataStatistics() const override
    {
        auto guard = ReaderGuard(SpinLock_);
        auto dataStatistics = DataStatistics_;
        for (const auto& session : Sessions_) {
            if (session.Reader) {
                dataStatistics += session.Reader->GetDataStatistics();
            }
        }
        return dataStatistics;
    }

    NChunkClient::TCodecStatistics GetDecompressionStatistics() const override
    {
        auto guard = ReaderGuard(SpinLock_);
        auto result = DecompressionStatistics_;
        for (const auto& session : Sessions_) {
            if (session.Reader) {
                result += session.Reader->GetDecompressionStatistics();
            }
        }
        return result;
    }

    bool IsFetchingCompleted() const override
    {
        auto guard = ReaderGuard(SpinLock_);
        for (const auto& session : Sessions_) {
            if (session.Reader && !session.Reader->IsFetchingCompleted()) {
                return false;
            }
        }
        return true;
    }

    std::vector<NChunkClient::TChunkId> GetFailedChunkIds() const override
    {
        auto guard = ReaderGuard(SpinLock_);
        auto result = FailedChunkIds_;
        for (const auto& session : Sessions_) {
            if (session.Reader) {
                auto failedChunkIds = session.Reader->GetFailedChunkIds();
                result.insert(result.end(), failedChunkIds.begin(), failedChunkIds.end());
            }
        }
        return result;
    }

private:
    const std::function<ISchemafulUnversionedReaderPtr()> GetNextReader_;

    struct TSession
    {
        explicit TSession(ISchemafulUnversionedReaderPtr reader)
            : Reader(std::move(reader))
        { }

        ISchemafulUnversionedReaderPtr Reader;
        TFutureHolder<void> ReadyEvent;
        bool Exhausted = false;
    };

    std::vector<TSession> Sessions_;
    bool Exhausted_ = false;
    TDataStatistics DataStatistics_;
    NChunkClient::TCodecStatistics DecompressionStatistics_;
    std::vector<NChunkClient::TChunkId> FailedChunkIds_;

    TPromise<void> ReadyEvent_ = MakePromise<void>(TError());
    const TCancelableContextPtr CancelableContext_ = New<TCancelableContext>();
    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, SpinLock_);

    TPromise<void> DoGetReadyEvent() const
    {
        auto guard = ReaderGuard(SpinLock_);
        return ReadyEvent_;
    }

    void UpdateSession(TSession& session)
    {
        session.ReadyEvent = session.Reader->GetReadyEvent();
        session.ReadyEvent->Subscribe(BIND(&TUnorderedSchemafulReader::OnReady, MakeStrong(this)));
        CancelableContext_->PropagateTo(*session.ReadyEvent);
    }

    bool RefillSession(TSession& session)
    {
        auto dataStatistics = session.Reader->GetDataStatistics();
        auto cpuCompressionStatistics = session.Reader->GetDecompressionStatistics();
        {
            auto guard = WriterGuard(SpinLock_);
            DataStatistics_ += dataStatistics;
            DecompressionStatistics_ += cpuCompressionStatistics;
            auto failedChunkIds = session.Reader->GetFailedChunkIds();
            FailedChunkIds_.insert(FailedChunkIds_.end(), failedChunkIds.begin(), failedChunkIds.end());
            session.Reader.Reset();
        }

        if (Exhausted_) {
            return false;
        }

        auto reader = GetNextReader_();
        if (!reader) {
            Exhausted_ = true;
            return false;
        }

        {
            auto guard = WriterGuard(SpinLock_);
            session.Exhausted = false;
            session.Reader = std::move(reader);
        }

        UpdateSession(session);
        return true;
    }

    void OnReady(const TError& value)
    {
        DoGetReadyEvent().TrySet(value);
    }

    void OnCanceled(const TError& error)
    {
        DoGetReadyEvent().TrySet(TError(NYT::EErrorCode::Canceled, "Table reader canceled")
            << error);
        CancelableContext_->Cancel(error);
    }
};

ISchemafulUnversionedReaderPtr CreateUnorderedSchemafulReader(
    std::function<ISchemafulUnversionedReaderPtr()> getNextReader,
    int concurrency)
{
    return New<TUnorderedSchemafulReader>(
        std::move(getNextReader),
        concurrency);
}

////////////////////////////////////////////////////////////////////////////////

ISchemafulUnversionedReaderPtr CreateOrderedSchemafulReader(
    std::function<ISchemafulUnversionedReaderPtr()> getNextReader)
{
    return CreateUnorderedSchemafulReader(getNextReader, 1);
}

ISchemafulUnversionedReaderPtr CreatePrefetchingOrderedSchemafulReader(
    std::function<ISchemafulUnversionedReaderPtr()> getNextReader)
{
    auto nextReader = getNextReader();
    auto readerGenerator = [
        nextReader = std::move(nextReader),
        getNextReader = std::move(getNextReader)
    ] () mutable -> ISchemafulUnversionedReaderPtr {
        auto currentReader = nextReader;
        if (currentReader) {
            nextReader = getNextReader();
        }
        return currentReader;
    };

    return CreateUnorderedSchemafulReader(readerGenerator, 1);
}

ISchemafulUnversionedReaderPtr CreateFullPrefetchingOrderedSchemafulReader(
    std::function<ISchemafulUnversionedReaderPtr()> getNextReader)
{
    std::vector<ISchemafulUnversionedReaderPtr> readers;

    while (auto nextReader = getNextReader()) {
        readers.push_back(nextReader);
    }

    auto readerGenerator = [
        index = 0,
        readers = std::move(readers)
    ] () mutable -> ISchemafulUnversionedReaderPtr {
        if (index == std::ssize(readers)) {
            return nullptr;
        }
        return readers[index++];
    };

    return CreateUnorderedSchemafulReader(readerGenerator, 1);
}

ISchemafulUnversionedReaderPtr CreateFullPrefetchingShufflingSchemafulReader(
    std::function<ISchemafulUnversionedReaderPtr()> getNextReader)
{
    std::vector<ISchemafulUnversionedReaderPtr> readers;

    while (auto nextReader = getNextReader()) {
        readers.push_back(std::move(nextReader));
    }

    Shuffle(readers.begin(), readers.end());

    auto readerGenerator = [
        index = 0,
        readers = std::move(readers)
    ] () mutable -> ISchemafulUnversionedReaderPtr {
        if (index == std::ssize(readers)) {
            return nullptr;
        }
        return readers[index++];
    };

    return CreateUnorderedSchemafulReader(readerGenerator, 1);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
