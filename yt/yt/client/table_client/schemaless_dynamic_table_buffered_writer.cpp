#include "schemaless_dynamic_table_buffered_writer.h"
#include "config.h"

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_writer.h>

#include <yt/yt/client/table_client/schemaless_dynamic_table_writer.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/misc/backoff_strategy.h>

#include <yt/yt/core/concurrency/nonblocking_batcher.h>

#include <yt/yt/core/utilex/random.h>

namespace NYT::NTableClient {

using namespace NApi;
using namespace NConcurrency;
using namespace NTransactionClient;
using namespace NYPath;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

class TSchemalessBufferedDynamicTableWriter
    : public IUnversionedWriter
{
public:
    TSchemalessBufferedDynamicTableWriter(
        TYPath path,
        IClientPtr client,
        IInvokerPtr invoker,
        TSchemalessBufferedDynamicTableWriterConfigPtr config)
        : Path_(std::move(path))
        , Client_(std::move(client))
        , Config_(std::move(config))
        , Batcher_(New<TNonblockingBatcher<TUnversionedRow>>(TBatchSizeLimiter(Config_->MaxBatchSize), Config_->FlushPeriod))
        , SchemalessDynamicTableWriter_(CreateSchemalessDynamicTableWriter(Path_, Client_))
        , WriteExecutor_(New<TPeriodicExecutor>(
            invoker,
            BIND(&TSchemalessBufferedDynamicTableWriter::DoWrite, MakeWeak(this))))
    {
        Logger.AddTag("Path: %v", Path_);

        WriteExecutor_->Start();
    }

    void UpdateConfig(const TSchemalessBufferedDynamicTableWriterConfigPtr& newConfig)
    {
        Batcher_->UpdateSettings(
            newConfig->FlushPeriod,
            TBatchSizeLimiter(newConfig->MaxBatchSize),
            /*allowEmptyBatches*/ false);

        Config_ = std::move(newConfig);
    }

    bool Write(TRange<TUnversionedRow> rows) override
    {
        for (auto row : rows) {
            Batcher_->Enqueue(std::move(row));
        }
        return false;
    }

    TFuture<void> GetReadyEvent() override
    {
        GetReadyPromise_ = NewPromise<void>();
        return GetReadyPromise_.ToFuture();
    }

    TFuture<void> Close() override
    {
        auto guard = TGuard(PromiseLock_);
        ClosePromise_ = NewPromise<void>();
        return ClosePromise_.ToFuture();
    }

    const TTableSchemaPtr& GetSchema() const override
    {
        return Schema_;
    }

    const TNameTablePtr& GetNameTable() const override
    {
        return NameTable_;
    }

private:
    const TYPath Path_;
    const IClientPtr Client_;
    TSchemalessBufferedDynamicTableWriterConfigPtr Config_;
    TNonblockingBatcherPtr<TUnversionedRow> Batcher_;

    IUnversionedWriterPtr SchemalessDynamicTableWriter_;

    TPeriodicExecutorPtr WriteExecutor_;

    NThreading::TSpinLock PromiseLock_;
    TPromise<void> ClosePromise_;

    TPromise<void> GetReadyPromise_ = NewPromise<void>();

    std::atomic<int> PendingCount_ = 0;
    std::atomic<int> DroppedCount_ = 0;
    std::atomic<int> WriteFailuresCount_ = 0;

    const TNameTablePtr NameTable_ = New<TNameTable>();
    const TTableSchemaPtr Schema_ = New<TTableSchema>();

    NLogging::TLogger Logger = NLogging::TLogger("SchemalessBufferedDynamicTableWriter");

private:
    void DoWrite()
    {
        auto batch = [&] {
            auto guard = TGuard(PromiseLock_);
            if (!ClosePromise_) {
                auto asyncBatch = Batcher_->DequeueBatch();
                return WaitForUnique(asyncBatch)
                    .ValueOrThrow();
            }

            ClosePromise_.Set();
            auto batches = Batcher_->Drain();

            std::vector<TUnversionedRow> result;
            for (auto& batch : batches) {
                result.insert(result.end(), batch.begin(), batch.end());
            }
            return result;
        }();

        if (batch.empty()) {
            return;
        }

        WriteBatchWithExpBackoff(batch);

        GetReadyPromise_.Set();
    }

    void WriteBatchWithExpBackoff(const std::vector<TUnversionedRow>& batch)
    {
        TBackoffStrategy backoffStrategy(Config_->ExponentialBackoffOptions);
        while (true) {
            if (TryHandleBatch(batch)) {
                return;
            }
            YT_LOG_WARNING("Failed to upload rows (RetryDelay: %v, PendingRows: %v)",
                backoffStrategy.GetBackoff().Seconds(),
                GetPendingCount());
            TDelayedExecutor::WaitForDuration(backoffStrategy.GetBackoff());
            backoffStrategy.Next();
        }
    }

    bool TryHandleBatch(const std::vector<TUnversionedRow>& batch)
    {
        YT_LOG_DEBUG("Table transaction starting (Items: %v, PendingItems: %v)",
            batch.size(),
            GetPendingCount());

        if (!SchemalessDynamicTableWriter_->Write(batch)) {
            return false;
        }

        YT_LOG_DEBUG("Table transaction committed (CommittedItems: %v)",
            batch.size());

        return true;
    }

    int GetPendingCount() const
    {
        return PendingCount_.load();
    }
};

////////////////////////////////////////////////////////////////////////////////

IUnversionedWriterPtr CreateSchemalessBufferedDynamicTableWriter(TYPath path, IClientPtr client, IInvokerPtr invoker, TSchemalessBufferedDynamicTableWriterConfigPtr config)
{
    return New<TSchemalessBufferedDynamicTableWriter>(std::move(path), std::move(client), std::move(invoker), std::move(config));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
