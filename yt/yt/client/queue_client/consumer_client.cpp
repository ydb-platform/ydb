#include "consumer_client.h"
#include "private.h"

#include <yt/yt/client/table_client/config.h>
#include <yt/yt/client/table_client/comparator.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/check_schema_compatibility.h>

#include <yt/yt/client/tablet_client/table_mount_cache.h>

#include <yt/yt/client/api/rowset.h>
#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/transaction_client/helpers.h>

#include <yt/yt/client/ypath/rich.h>

#include <library/cpp/iterator/functools.h>

#include <util/string/join.h>

namespace NYT::NQueueClient {

using namespace NApi;
using namespace NConcurrency;
using namespace NHiveClient;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NTransactionClient;
using namespace NYPath;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = QueueClientLogger;

////////////////////////////////////////////////////////////////////////////////

static const TTableSchemaPtr YTConsumerTableSchema = New<TTableSchema>(std::vector<TColumnSchema>{
    TColumnSchema("queue_cluster", EValueType::String, ESortOrder::Ascending).SetRequired(true),
    TColumnSchema("queue_path", EValueType::String, ESortOrder::Ascending).SetRequired(true),
    TColumnSchema("partition_index", EValueType::Uint64, ESortOrder::Ascending).SetRequired(true),
    TColumnSchema("offset", EValueType::Uint64).SetRequired(true),
}, /*strict*/ true, /*uniqueKeys*/ true);

static const TTableSchemaPtr BigRTConsumerTableSchema = New<TTableSchema>(std::vector<TColumnSchema>{
    TColumnSchema("ShardId", EValueType::Uint64, ESortOrder::Ascending),
    TColumnSchema("Offset", EValueType::Uint64),
}, /*strict*/ true, /*uniqueKeys*/ true);

class TGenericConsumerClient
    : public ISubConsumerClient
{
public:
    TGenericConsumerClient(
        TYPath path,
        TUnversionedOwningRow rowPrefix,
        TStringBuf partitionIndexColumnName,
        TStringBuf offsetColumnName,
        bool decrementOffset,
        const TTableSchemaPtr& tableSchema)
        : Path_(std::move(path))
        , RowPrefix_(std::move(rowPrefix))
        , PartitionIndexColumnName_(std::move(partitionIndexColumnName))
        , OffsetColumnName_(std::move(offsetColumnName))
        , TableSchema_(tableSchema)
        , NameTable_(TNameTable::FromSchema(*TableSchema_))
        , PartitionIndexColumnId_(NameTable_->GetId(PartitionIndexColumnName_))
        , OffsetColumnId_(NameTable_->GetId(OffsetColumnName_))
        , SubConsumerColumnFilter_{PartitionIndexColumnId_, OffsetColumnId_}
        , DecrementOffset_(decrementOffset)
        , SubConsumerSchema_(New<TTableSchema>(std::vector<TColumnSchema>{
            TColumnSchema(TString(PartitionIndexColumnName_), EValueType::Uint64, ESortOrder::Ascending),
            TColumnSchema(TString(OffsetColumnName_), EValueType::Uint64),
        }, /*strict*/ true, /*uniqueKeys*/ true))
    {
        if (RowPrefix_.GetCount() == 0) {
            RowPrefixCondition_ = "1 = 1";
        } else {
            TStringBuilder builder;
            builder.AppendChar('(');
            for (int index = 0; index < RowPrefix_.GetCount(); ++index) {
                if (index != 0) {
                    builder.AppendString(", ");
                }
                builder.AppendFormat("[%v]", TableSchema_->Columns()[index].Name());
            }
            builder.AppendString(") = (");
            for (int index = 0; index < RowPrefix_.GetCount(); ++index) {
                if (index != 0) {
                    builder.AppendString(", ");
                }
                YT_VERIFY(RowPrefix_[index].Type == EValueType::String);
                builder.AppendFormat("\"%v\"", RowPrefix_[index].AsStringBuf());
            }
            builder.AppendChar(')');
            RowPrefixCondition_ = builder.Flush();
        }
    }

    void Advance(
        const ITransactionPtr& transaction,
        int partitionIndex,
        std::optional<i64> oldOffset,
        i64 newOffset) const override
    {
        if (oldOffset) {
            TUnversionedRowsBuilder keyRowsBuilder;
            TUnversionedRowBuilder rowBuilder;
            for (const auto& value : RowPrefix_) {
                rowBuilder.AddValue(value);
            }
            rowBuilder.AddValue(MakeUnversionedUint64Value(partitionIndex, PartitionIndexColumnId_));
            keyRowsBuilder.AddRow(rowBuilder.GetRow());

            TVersionedLookupRowsOptions options;
            options.RetentionConfig = New<TRetentionConfig>();
            options.RetentionConfig->MaxDataVersions = 1;

            auto partitionRowset = WaitFor(transaction->VersionedLookupRows(Path_, NameTable_, keyRowsBuilder.Build(), options))
                .ValueOrThrow();
            const auto& rows = partitionRowset->GetRows();

            // XXX(max42): should we use name table from the rowset, or it coincides with our own name table?
            auto offsetRowsetColumnId = partitionRowset->GetNameTable()->GetIdOrThrow(OffsetColumnName_);

            THROW_ERROR_EXCEPTION_UNLESS(
                std::ssize(partitionRowset->GetRows()) <= 1,
                "The table for consumer %Qv should contain at most one row for partition %v when an old offset is specified",
                Path_,
                partitionIndex);

            i64 currentOffset = 0;
            TTimestamp offsetTimestamp = 0;
            // If the key doesn't exist, or the offset value is null, the offset is -1 in BigRT terms and 0 in ours.
            if (!rows.empty()) {
                const auto& offsetValue = rows[0].Values()[0];
                YT_VERIFY(offsetValue.Id == offsetRowsetColumnId);
                offsetTimestamp = offsetValue.Timestamp;
                if (offsetValue.Type != EValueType::Null) {
                    currentOffset = FromUnversionedValue<i64>(offsetValue);
                    if (DecrementOffset_) {
                        // We need to add 1, since BigRT stores the offset of the last read row.
                        ++currentOffset;
                    }
                }

                YT_LOG_DEBUG(
                    "Read current offset (Consumer: %v, PartitionIndex: %v, Offset: %v, Timestamp: %v)",
                    Path_,
                    partitionIndex,
                    currentOffset,
                    offsetTimestamp);
            }

            if (currentOffset != *oldOffset) {
                THROW_ERROR_EXCEPTION(
                    EErrorCode::ConsumerOffsetConflict,
                    "Offset conflict at partition %v of consumer %v: expected offset %v, found offset %v",
                    partitionIndex,
                    Path_,
                    *oldOffset,
                    currentOffset)
                        << TErrorAttribute("partition", partitionIndex)
                        << TErrorAttribute("consumer", Path_)
                        << TErrorAttribute("expected_offset", *oldOffset)
                        << TErrorAttribute("current_offset", currentOffset)
                        << TErrorAttribute("current_offset_timestamp", offsetTimestamp);
            }
        }

        TUnversionedRowsBuilder rowsBuilder;
        TUnversionedRowBuilder rowBuilder;
        for (const auto& value : RowPrefix_) {
            rowBuilder.AddValue(value);
        }
        rowBuilder.AddValue(MakeUnversionedUint64Value(partitionIndex, PartitionIndexColumnId_));
        if (DecrementOffset_) {
            if (newOffset >= 1) {
                // We need to subtract 1, since BigRT stores the offset of the last read row.
                rowBuilder.AddValue(MakeUnversionedUint64Value(newOffset - 1, OffsetColumnId_));
            } else {
                // For BigRT consumers we store 0 (in our terms) by storing null.
                rowBuilder.AddValue(MakeUnversionedNullValue(OffsetColumnId_));
            }
        } else {
            rowBuilder.AddValue(MakeUnversionedUint64Value(newOffset, OffsetColumnId_));
        }
        rowsBuilder.AddRow(rowBuilder.GetRow());

        YT_LOG_DEBUG(
            "Advancing consumer offset (Path: %v, Partition: %v, Offset: %v -> %v)",
            Path_,
            partitionIndex,
            oldOffset,
            newOffset);
        transaction->WriteRows(Path_, NameTable_, rowsBuilder.Build());
    }

    TFuture<std::vector<TPartitionInfo>> CollectPartitions(
        const IClientPtr& client,
        int expectedPartitionCount,
        bool withLastConsumeTime = false) const override
    {
        if (expectedPartitionCount <= 0) {
            return MakeFuture(std::vector<TPartitionInfo>{});
        }

        auto selectQuery = Format(
            "[%v], [%v] from [%v] where ([%v] between 0 and %v) and (%v)",
            PartitionIndexColumnName_,
            OffsetColumnName_,
            Path_,
            PartitionIndexColumnName_,
            expectedPartitionCount - 1,
            RowPrefixCondition_);
        return BIND(
            &TGenericConsumerClient::DoCollectPartitions,
            MakeStrong(this),
            client,
            selectQuery,
            withLastConsumeTime)
            .AsyncVia(GetCurrentInvoker())
            .Run()
            .Apply(BIND([expectedPartitionCount] (const std::vector<TPartitionInfo>& partitionInfos) {
                YT_VERIFY(std::ssize(partitionInfos) <= expectedPartitionCount);

                std::vector<TPartitionInfo> result(expectedPartitionCount);
                for (int partitionIndex = 0; partitionIndex < expectedPartitionCount; ++partitionIndex) {
                    result[partitionIndex] = {.PartitionIndex = partitionIndex, .NextRowIndex = 0};
                }
                for (const auto& partitionInfo : partitionInfos) {
                    result[partitionInfo.PartitionIndex] = partitionInfo;
                }
                return result;
            }));
    }

    TFuture<std::vector<TPartitionInfo>> CollectPartitions(
        const IClientPtr& client,
        const std::vector<int>& partitionIndexes,
        bool withLastConsumeTime) const override
    {
        auto selectQuery = Format(
            "[%v], [%v] from [%v] where ([%v] in (%v)) and (%v)",
            PartitionIndexColumnName_,
            OffsetColumnName_,
            Path_,
            PartitionIndexColumnName_,
            JoinSeq(",", partitionIndexes),
            RowPrefixCondition_);
        return BIND(
            &TGenericConsumerClient::DoCollectPartitions,
            MakeStrong(this),
            client,
            selectQuery,
            withLastConsumeTime)
            .AsyncVia(GetCurrentInvoker())
            .Run()
            .Apply(BIND([partitionIndexes] (const std::vector<TPartitionInfo>& partitionInfos) {
                THashMap<int, TPartitionInfo> indexToPartitionInfo;
                for (auto partitionIndex : partitionIndexes) {
                    indexToPartitionInfo[partitionIndex] = {
                        .PartitionIndex = partitionIndex,
                        .NextRowIndex = 0,
                    };
                }
                for (const auto& partitionInfo : partitionInfos) {
                    indexToPartitionInfo[partitionInfo.PartitionIndex] = partitionInfo;
                }

                std::vector<TPartitionInfo> result;
                result.reserve(partitionIndexes.size());
                for (auto partitionIndex : partitionIndexes) {
                    result.push_back(indexToPartitionInfo[partitionIndex]);
                }

                return result;
            }));
    }

    TFuture<TCrossClusterReference> FetchTargetQueue(const IClientPtr& client) const override
    {
        return client->GetNode(Path_ + "/@target_queue")
            .Apply(BIND([] (const TYsonString& ysonString) {
                return TCrossClusterReference::FromString(ConvertTo<TString>(ysonString));
            }));
    }

    TFuture<TPartitionStatistics> FetchPartitionStatistics(
        const IClientPtr& client,
        const TYPath& queue,
        int partitionIndex) const override
    {
        return client->GetNode(queue + "/@tablets")
            .Apply(BIND([queue, partitionIndex] (const TYsonString& ysonString) -> TPartitionStatistics {
                auto tabletList = ConvertToNode(ysonString)->AsList();

                for (const auto& tablet : tabletList->GetChildren()) {
                    const auto& tabletMapNode = tablet->AsMap();
                    auto tabletIndex = ConvertTo<i64>(tabletMapNode->FindChild("index"));

                    if (partitionIndex == tabletIndex) {
                        auto flushedDataWeight = ConvertTo<i64>(tabletMapNode->FindChild("statistics")->AsMap()->FindChild("uncompressed_data_size"));
                        auto flushedRowCount = ConvertTo<i64>(tabletMapNode->FindChild("flushed_row_count"));

                        return {.FlushedDataWeight = flushedDataWeight, .FlushedRowCount = flushedRowCount};
                    }
                }

                THROW_ERROR_EXCEPTION("Queue %Qv has no tablet with index %v", queue, partitionIndex);
            }));
    }

private:
    const TYPath Path_;
    const TUnversionedOwningRow RowPrefix_;
    //! A condition of form ([ColumnName0], [ColumnName1], ...) = (RowPrefix_[0], RowPrefix_[1], ...)
    //! defining this subconsumer.
    TString RowPrefixCondition_;
    const TStringBuf PartitionIndexColumnName_;
    const TStringBuf OffsetColumnName_;
    const TTableSchemaPtr& TableSchema_;
    const TNameTablePtr NameTable_;
    const int PartitionIndexColumnId_;
    const int OffsetColumnId_;
    //! A column filter consisting of PartitionIndexColumnName_ and OffsetColumnName_.
    const TColumnFilter SubConsumerColumnFilter_;

    // COMPAT(achulkov2): Remove this once we drop support for legacy BigRT consumers.
    //! Controls whether the offset is decremented before being written to the offset table.
    //! BigRT stores the offset of the last read row, so for legacy BigRT consumers this option
    //! should be set to true.
    bool DecrementOffset_ = false;

    TTableSchemaPtr SubConsumerSchema_;

    std::vector<TPartitionInfo> DoCollectPartitions(
        const IClientPtr& client,
        const TString& selectQuery,
        bool withLastConsumeTime) const
    {
        std::vector<TPartitionInfo> result;

        YT_LOG_DEBUG("Collecting partitions (Query: %v)", selectQuery);

        TSelectRowsOptions selectRowsOptions;
        selectRowsOptions.ReplicaConsistency = EReplicaConsistency::Sync;
        auto selectRowsResult = WaitFor(client->SelectRows(selectQuery, selectRowsOptions))
            .ValueOrThrow();

        // Note that after table construction table schema may have changed.
        // We must be prepared for that.

        auto partitionIndexRowsetColumnId =
            selectRowsResult.Rowset->GetNameTable()->FindId(PartitionIndexColumnName_);
        auto offsetRowsetColumnId = selectRowsResult.Rowset->GetNameTable()->FindId(OffsetColumnName_);

        if (!partitionIndexRowsetColumnId || !offsetRowsetColumnId) {
            THROW_ERROR_EXCEPTION(
                "Table must have columns %Qv and %Qv",
                PartitionIndexColumnName_,
                OffsetColumnName_);
        }

        std::vector<ui64> partitionIndices;
        for (auto row : selectRowsResult.Rowset->GetRows()) {

            YT_VERIFY(row.GetCount() == 2);

            const auto& partitionIndexValue = row[*partitionIndexRowsetColumnId];
            if (partitionIndexValue.Type == EValueType::Null) {
                // This is a weird row for partition Null. Just ignore it.
                continue;
            }
            YT_VERIFY(partitionIndexValue.Type == EValueType::Uint64);

            partitionIndices.push_back(FromUnversionedValue<ui64>(partitionIndexValue));

            const auto& offsetValue = row[*offsetRowsetColumnId];

            i64 offset;
            if (offsetValue.Type == EValueType::Uint64) {
                offset = FromUnversionedValue<i64>(offsetValue);
                if (DecrementOffset_) {
                    ++offset;
                }
            } else if (offsetValue.Type == EValueType::Null) {
                offset = 0;
            } else {
                YT_ABORT();
            }

            // NB: in BigRT offsets encode the last read row, while we operate with the first unread row.
            result.emplace_back(TPartitionInfo{
                .PartitionIndex = FromUnversionedValue<i64>(partitionIndexValue),
                .NextRowIndex = offset,
            });
        }

        if (!withLastConsumeTime) {
            return result;
        }

        // Now do versioned lookups in order to obtain timestamps.

        TUnversionedRowsBuilder builder;
        for (ui64 partitionIndex : partitionIndices) {
            TUnversionedRowBuilder rowBuilder;
            for (const auto& value : RowPrefix_) {
                rowBuilder.AddValue(value);
            }
            rowBuilder.AddValue(MakeUnversionedUint64Value(partitionIndex, PartitionIndexColumnId_));
            builder.AddRow(rowBuilder.GetRow());
        }

        TVersionedLookupRowsOptions options;
        // This allows easier detection of key set change during the query.
        options.KeepMissingRows = true;
        options.RetentionConfig = New<TRetentionConfig>();
        options.RetentionConfig->MaxDataVersions = 1;
        options.ReplicaConsistency = EReplicaConsistency::Sync;

        auto versionedRowset = WaitFor(client->VersionedLookupRows(
            Path_,
            NameTable_,
            builder.Build(),
            options))
            .ValueOrThrow();

        YT_VERIFY(versionedRowset->GetRows().size() == partitionIndices.size());

        for (const auto& [index, versionedRow] : Enumerate(versionedRowset->GetRows())) {
            if (versionedRow.GetWriteTimestampCount() < 1) {
                THROW_ERROR_EXCEPTION("Partition set changed during collection");
            }
            auto timestamp = versionedRow.WriteTimestamps()[0];
            result[index].LastConsumeTime = TimestampToInstant(timestamp).first;
        }

        return result;
    }
};

////////////////////////////////////////////////////////////////////////////////

ISubConsumerClientPtr CreateBigRTConsumerClient(
    const TYPath& path,
    const TTableSchema& schema)
{
    if (!schema.IsUniqueKeys()) {
        THROW_ERROR_EXCEPTION("Consumer schema must have unique keys, schema does not")
            << TErrorAttribute("actual_schema", schema);
    }

    if (schema == *BigRTConsumerTableSchema) {
        return New<TGenericConsumerClient>(
            path,
            TUnversionedOwningRow(),
            "ShardId",
            "Offset",
            /*decrementOffset*/ true,
            BigRTConsumerTableSchema);
    } else {
        THROW_ERROR_EXCEPTION("Table schema is not recognized as a valid BigRT consumer schema")
            << TErrorAttribute("expected_schema", *BigRTConsumerTableSchema)
            << TErrorAttribute("actual_schema", schema);
    }
}

ISubConsumerClientPtr CreateBigRTConsumerClient(
    const IClientPtr& client,
    const TYPath& path)
{
    auto tableInfo = WaitFor(client->GetTableMountCache()->GetTableInfo(path))
        .ValueOrThrow();
    auto schema = tableInfo->Schemas[ETableSchemaKind::Primary];

    return CreateBigRTConsumerClient(path, *schema);
}

////////////////////////////////////////////////////////////////////////////////

class TYTConsumerClient
    : public IConsumerClient
{
public:
    explicit TYTConsumerClient(TYPath path)
        : Path_(std::move(path))
    { }

    ISubConsumerClientPtr GetSubConsumerClient(const TCrossClusterReference& queue) const override
    {
        TUnversionedOwningRowBuilder builder;
        builder.AddValue(MakeUnversionedStringValue(queue.Cluster, QueueClusterColumnId_));
        builder.AddValue(MakeUnversionedStringValue(queue.Path, QueuePathColumnId_));
        auto row = builder.FinishRow();

        auto subConsumerClient = New<TGenericConsumerClient>(
            Path_,
            std::move(row),
            "partition_index",
            "offset",
            /*decrementOffset*/ false,
            YTConsumerTableSchema);

        return subConsumerClient;
    }

private:
    TYPath Path_;
    static const TNameTablePtr NameTable_;
    static const int QueueClusterColumnId_;
    static const int QueuePathColumnId_;
};

const TNameTablePtr TYTConsumerClient::NameTable_ = TNameTable::FromSchema(*YTConsumerTableSchema);
const int TYTConsumerClient::QueueClusterColumnId_ = TYTConsumerClient::NameTable_->GetId("queue_cluster");
const int TYTConsumerClient::QueuePathColumnId_ = TYTConsumerClient::NameTable_->GetId("queue_path");

////////////////////////////////////////////////////////////////////////////////

IConsumerClientPtr CreateConsumerClient(
    const TYPath& path,
    const TTableSchema& schema)
{
    if (!schema.IsUniqueKeys()) {
        THROW_ERROR_EXCEPTION("Consumer schema must have unique keys, schema does not")
            << TErrorAttribute("actual_schema", schema);
    }

    if (schema == *YTConsumerTableSchema) {
        return New<TYTConsumerClient>(path);
    } else {
        THROW_ERROR_EXCEPTION("Table schema is not recognized as a valid consumer schema")
            << TErrorAttribute("expected_schema", *YTConsumerTableSchema)
            << TErrorAttribute("actual_schema", schema);
    }
}

IConsumerClientPtr CreateConsumerClient(
    const IClientPtr& client,
    const TYPath& path)
{
    auto tableInfo = WaitFor(client->GetTableMountCache()->GetTableInfo(path))
        .ValueOrThrow();
    auto schema = tableInfo->Schemas[ETableSchemaKind::Primary];

    return CreateConsumerClient(path, *schema);
}

ISubConsumerClientPtr CreateSubConsumerClient(
    const IClientPtr& client,
    const TYPath& consumerPath,
    TRichYPath queuePath)
{
    auto queueCluster = queuePath.GetCluster();
    if (!queueCluster) {
        if (auto clientCluster = client->GetClusterName()) {
            queueCluster = *clientCluster;
        }
    }

    if (!queueCluster) {
        THROW_ERROR_EXCEPTION(
            "Cannot create subconsumer for %Qv, could not infer cluster for queue %Qv from attributes or client",
            consumerPath,
            queuePath);
    }

    TCrossClusterReference queueRef;
    queueRef.Cluster = *queueCluster;
    queueRef.Path = queuePath.GetPath();

    return CreateConsumerClient(client, consumerPath)->GetSubConsumerClient(queueRef);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueClient
