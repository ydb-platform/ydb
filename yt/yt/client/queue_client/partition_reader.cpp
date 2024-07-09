#include "partition_reader.h"
#include "private.h"
#include "queue_rowset.h"
#include "common.h"
#include "consumer_client.h"

#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/core/profiling/timing.h>

namespace NYT::NQueueClient {

using namespace NApi;
using namespace NConcurrency;
using namespace NProfiling;
using namespace NTableClient;
using namespace NTransactionClient;
using namespace NYPath;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TMultiQueueConsumerPartitionReader
    : public IPartitionReader
{
public:
    TMultiQueueConsumerPartitionReader(
        TPartitionReaderConfigPtr config,
        IClientPtr client,
        TRichYPath consumerPath,
        TRichYPath queuePath,
        int partitionIndex)
        : Config_(std::move(config))
        , Client_(std::move(client))
        , ConsumerPath_(std::move(consumerPath))
        , QueuePath_(std::move(queuePath))
        , PartitionIndex_(partitionIndex)
        , RowBatchReadOptions_({Config_->MaxRowCount, Config_->MaxDataWeight, Config_->DataWeightPerRowHint})
        , Logger(QueueClientLogger().WithTag("Consumer: %v, Queue: %v, Partition: %v", ConsumerPath_, QueuePath_, PartitionIndex_))
    {
        PullQueueConsumerOptions_.UseNativeTabletNodeApi = Config_->UseNativeTabletNodeApi;
    }

    TFuture<void> Open() override
    {
        return BIND(&TMultiQueueConsumerPartitionReader::DoOpen, MakeStrong(this))
            .AsyncVia(GetCurrentInvoker())
            .Run();
    }

    TFuture<IPersistentQueueRowsetPtr> Read() override
    {
        YT_VERIFY(Opened_);

        return BIND(&TMultiQueueConsumerPartitionReader::DoRead, MakeStrong(this))
            .AsyncVia(GetCurrentInvoker())
            .Run();
    }

private:
    const TPartitionReaderConfigPtr Config_;
    const IClientPtr Client_;
    const TRichYPath ConsumerPath_;
    const TRichYPath QueuePath_;
    const int PartitionIndex_;
    const TQueueRowBatchReadOptions RowBatchReadOptions_;
    const NLogging::TLogger Logger;

    TPullQueueConsumerOptions PullQueueConsumerOptions_;

    ISubConsumerClientPtr ConsumerClient_;
    bool Opened_ = false;

    class TPersistentQueueRowset
        : public IPersistentQueueRowset
    {
    public:
        TPersistentQueueRowset(
            IQueueRowsetPtr rowset,
            TWeakPtr<TMultiQueueConsumerPartitionReader> partitionReader,
            i64 currentOffset)
            : Rowset_(std::move(rowset))
            , PartitionReader_(std::move(partitionReader))
            , CurrentOffset_(currentOffset)
        { }

        const NTableClient::TTableSchemaPtr& GetSchema() const override
        {
            return Rowset_->GetSchema();
        }

        const NTableClient::TNameTablePtr& GetNameTable() const override
        {
            return Rowset_->GetNameTable();
        }

        TSharedRange<NTableClient::TUnversionedRow> GetRows() const override
        {
            return Rowset_->GetRows();
        }

        i64 GetStartOffset() const override
        {
            return Rowset_->GetStartOffset();
        }

        i64 GetFinishOffset() const override
        {
            return Rowset_->GetFinishOffset();
        }

        void Commit(const NApi::ITransactionPtr& transaction) override
        {
            YT_VERIFY(transaction);

            if (auto partitionReader = PartitionReader_.Lock()) {
                // TODO(achulkov2): Check that this is the first uncommitted batch returned to the user and crash otherwise.
                // Will be much easier to do once we figure out how prefetching & batch storage will look like.

                // TODO(achulkov2): Mark this batch as committed in the partition reader.

                transaction->AdvanceConsumer(
                    partitionReader->ConsumerPath_.GetPath(),
                    partitionReader->QueuePath_,
                    partitionReader->PartitionIndex_,
                    CurrentOffset_,
                    GetFinishOffset());
            } else {
                THROW_ERROR_EXCEPTION("Partition reader destroyed");
            }
        }

    private:
        const IQueueRowsetPtr Rowset_;
        const TWeakPtr<TMultiQueueConsumerPartitionReader> PartitionReader_;
        const i64 CurrentOffset_;
    };

    IPersistentQueueRowsetPtr DoRead()
    {
        YT_LOG_DEBUG("Reading rowset");
        TWallTimer timer;

        auto currentOffset = FetchCurrentOffset();

        YT_LOG_DEBUG(
            "Pulling from queue (Offset: %v, MaxRowCount: %v, MaxDataWeight: %v, DataWeightPerRowHint: %v)",
            currentOffset,
            RowBatchReadOptions_.MaxRowCount,
            RowBatchReadOptions_.MaxDataWeight,
            RowBatchReadOptions_.DataWeightPerRowHint);
        auto asyncRowset = (Config_->UsePullQueueConsumer
            ? Client_->PullQueueConsumer(
                ConsumerPath_,
                QueuePath_,
                currentOffset,
                PartitionIndex_,
                RowBatchReadOptions_,
                PullQueueConsumerOptions_)
            : Client_->PullQueue(
                QueuePath_,
                currentOffset,
                PartitionIndex_,
                RowBatchReadOptions_,
                PullQueueConsumerOptions_));
        auto rowset = WaitFor(asyncRowset)
            .ValueOrThrow();

        HandleRowset(rowset);

        YT_LOG_DEBUG("Rowset read (WallTime: %v)", timer.GetElapsedTime());

        return New<TPersistentQueueRowset>(rowset, MakeWeak(this), currentOffset);

    }

    void HandleRowset(const IQueueRowsetPtr& rowset)
    {
        // TODO(achulkov2): When prefetching is implemented we'll have some sort of struct for holding the batch + stats.
        // TODO(achulkov2): Don't burn CPU here and get the data weight from PullQueue somehow.
        auto dataWeight = static_cast<i64>(GetDataWeight(rowset->GetRows()));
        i64 rowCount = std::ssize(rowset->GetRows());

        YT_LOG_DEBUG(
            "Rowset obtained (RowCount: %v, DataWeight: %v, StartOffset: %v, FinishOffset: %v)",
            rowCount,
            dataWeight,
            rowset->GetStartOffset(),
            rowset->GetFinishOffset());
    }

    // NB: Can throw.
    i64 FetchCurrentOffset() const
    {
        TWallTimer timer;

        std::vector<int> partitionIndexesToFetch{PartitionIndex_};
        auto partitions = WaitFor(ConsumerClient_->CollectPartitions(partitionIndexesToFetch))
            .ValueOrThrow();

        YT_VERIFY(partitions.size() <= 1);

        i64 currentOffset = 0;

        if (!partitions.empty()) {
            YT_VERIFY(partitions[0].PartitionIndex == PartitionIndex_);
            currentOffset = partitions[0].NextRowIndex;
        }

        YT_LOG_DEBUG("Fetched current offset (Offset: %v, WallTime: %v)", currentOffset, timer.GetElapsedTime());
        return currentOffset;
    }

    void DoOpen()
    {
        YT_LOG_DEBUG("Opening partition reader");

        auto queueCluster = QueuePath_.GetCluster();
        if (!queueCluster) {
            THROW_ERROR_EXCEPTION("Queue cluster must be specified");
        }

        ConsumerClient_ = CreateConsumerClient(Client_, ConsumerPath_.GetPath())->GetSubConsumerClient(
            Client_,
            {
                .Cluster = *queueCluster,
                .Path = QueuePath_.GetPath(),
            });

        Opened_ = true;

        YT_LOG_DEBUG("Partition reader opened");
    }
};

IPartitionReaderPtr CreateMultiQueueConsumerPartitionReader(
    TPartitionReaderConfigPtr config,
    IClientPtr client,
    TRichYPath consumerPath,
    TRichYPath queuePath,
    int partitionIndex)
{
    return New<TMultiQueueConsumerPartitionReader>(
        std::move(config),
        std::move(client),
        std::move(consumerPath),
        std::move(queuePath),
        partitionIndex);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueClient
