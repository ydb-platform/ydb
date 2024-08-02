#include "schemaless_buffered_dynamic_table_writer.h"
#include "schemaless_dynamic_table_writer.h"
#include "unversioned_writer.h"
#include "config.h"

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/core/misc/backoff_strategy.h>

#include <yt/yt/core/concurrency/nonblocking_batcher.h>

namespace NYT::NTableClient {

using namespace NApi;
using namespace NConcurrency;
using namespace NTransactionClient;
using namespace NYPath;
using namespace NProfiling;
using namespace NCrypto;

////////////////////////////////////////////////////////////////////////////////

// TODO(eshcherin): Make it reconfigurable.
class TSchemalessBufferedDynamicTableWriter
    : public IUnversionedWriter
{
public:
    TSchemalessBufferedDynamicTableWriter(
        TYPath path,
        IClientPtr client,
        TSchemalessBufferedDynamicTableWriterConfigPtr config,
        IInvokerPtr invoker)
        : Writer_(CreateSchemalessDynamicTableWriter(path, std::move(client)))
        , Config_(std::move(config))
        , Batcher_(New<TNonblockingBatcher<TUnversionedRow>>(
            TBatchSizeLimiter(Config_->MaxBatchSize),
            Config_->FlushPeriod))
        , RetryBackoffStrategy_(Config_->RetryBackoff)
    {
        invoker->Invoke(BIND(&TSchemalessBufferedDynamicTableWriter::Loop, MakeWeak(this)));
    }

    bool Write(TRange<TUnversionedRow> rows) override
    {
        for (auto row : rows) {
            Batcher_->Enqueue(row);
        }

        return true;
    }

    TFuture<void> GetReadyEvent() override
    {
        return VoidFuture;
    }

    TFuture<void> Close() override
    {
        Closed_ = true;
        return ClosePromise_.ToFuture();
    }

    const TTableSchemaPtr& GetSchema() const override
    {
        return Writer_->GetSchema();
    }

    const TNameTablePtr& GetNameTable() const override
    {
        return Writer_->GetNameTable();
    }

    std::optional<TMD5Hash> GetDigest() const override
    {
        return std::nullopt;
    }

private:
    const IUnversionedWriterPtr Writer_;
    const TSchemalessBufferedDynamicTableWriterConfigPtr Config_;
    const TNonblockingBatcherPtr<TUnversionedRow> Batcher_;

    TBackoffStrategy RetryBackoffStrategy_;

    TPromise<void> ClosePromise_ = NewPromise<void>();
    std::atomic<bool> Closed_ = false;

    void Loop() {
        while (!Closed_) {
            auto asyncBatch = Batcher_->DequeueBatch();
            auto batch = WaitForUnique(asyncBatch)
                .ValueOrThrow();

            if (batch.empty()) {
                continue;
            }

            WriteBatchWithRetries(batch);
        }

        auto batches = Batcher_->Drain();
        for (const auto& batch : batches) {
            WriteBatchWithRetries(batch);
        }

        ClosePromise_.Set();
    }

    // TODO(eshcherbin): Add logging.
    void WriteBatchWithRetries(const std::vector<TUnversionedRow>& batch)
    {
        RetryBackoffStrategy_.Restart();
        while (RetryBackoffStrategy_.GetInvocationCount() < RetryBackoffStrategy_.GetInvocationCount()) {
            if (TryWriteBatch(batch)) {
                break;
            }

            TDelayedExecutor::WaitForDuration(RetryBackoffStrategy_.GetBackoff());
            RetryBackoffStrategy_.Next();
        }
    }

    bool TryWriteBatch(const std::vector<TUnversionedRow>& batch)
    {
        try {
            Y_UNUSED(Writer_->Write(batch));
        } catch (const std::exception& ex) {
            return false;
        }

        return true;
    }
};

////////////////////////////////////////////////////////////////////////////////

IUnversionedWriterPtr CreateSchemalessBufferedDynamicTableWriter(
    TYPath path,
    IClientPtr client,
    TSchemalessBufferedDynamicTableWriterConfigPtr config,
    IInvokerPtr invoker)
{
    return New<TSchemalessBufferedDynamicTableWriter>(
        std::move(path),
        std::move(client),
        std::move(config),
        std::move(invoker));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
