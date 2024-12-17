#include "consumer_client.h"

#include "common.h"
#include "helpers.h"
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

static constexpr auto& Logger = QueueClientLogger;

////////////////////////////////////////////////////////////////////////////////

static constexpr TStringBuf YTConsumerMetaColumnName = "meta";

static const TTableSchemaPtr YTConsumerWithoutMetaTableSchema = New<TTableSchema>(std::vector<TColumnSchema>{
    TColumnSchema("queue_cluster", EValueType::String, ESortOrder::Ascending).SetRequired(true),
    TColumnSchema("queue_path", EValueType::String, ESortOrder::Ascending).SetRequired(true),
    TColumnSchema("partition_index", EValueType::Uint64, ESortOrder::Ascending).SetRequired(true),
    TColumnSchema("offset", EValueType::Uint64).SetRequired(true),
}, /*strict*/ true, /*uniqueKeys*/ true);

static const TTableSchemaPtr YTConsumerTableSchema = New<TTableSchema>(std::vector<TColumnSchema>{
    TColumnSchema("queue_cluster", EValueType::String, ESortOrder::Ascending).SetRequired(true),
    TColumnSchema("queue_path", EValueType::String, ESortOrder::Ascending).SetRequired(true),
    TColumnSchema("partition_index", EValueType::Uint64, ESortOrder::Ascending).SetRequired(true),
    TColumnSchema("offset", EValueType::Uint64).SetRequired(true),
    TColumnSchema("meta", EValueType::Any).SetRequired(false),
}, /*strict*/ true, /*uniqueKeys*/ true);

////////////////////////////////////////////////////////////////////////////////

void TConsumerMeta::Register(TRegistrar registrar)
{
    registrar.Parameter("cumulative_data_weight", &TThis::CumulativeDataWeight)
        .Default();
    registrar.Parameter("offset_timestamp", &TThis::OffsetTimestamp)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

class TGenericConsumerClient
    : public ISubConsumerClient
{
public:
    TGenericConsumerClient(
        IClientPtr consumerClusterClient,
        IClientPtr queueClusterClient,
        TYPath consumerPath,
        std::optional<TCrossClusterReference> queueRef,
        TUnversionedOwningRow rowPrefix,
        TStringBuf partitionIndexColumnName,
        TStringBuf offsetColumnName,
        bool decrementOffset,
        const TTableSchemaPtr& consumerTableSchema,
        const TTableSchemaPtr& queueTableSchema)
        : ConsumerClusterClient_(std::move(consumerClusterClient))
        , QueueClusterClient_(std::move(queueClusterClient))
        , ConsumerPath_(std::move(consumerPath))
        , QueueRef_(std::move(queueRef))
        , RowPrefix_(std::move(rowPrefix))
        , PartitionIndexColumnName_(std::move(partitionIndexColumnName))
        , OffsetColumnName_(std::move(offsetColumnName))
        , ConsumerTableSchema_(consumerTableSchema)
        , ConsumerNameTable_(TNameTable::FromSchema(*ConsumerTableSchema_))
        , QueueTableSchema_(queueTableSchema)
        , PartitionIndexColumnId_(ConsumerNameTable_->GetId(PartitionIndexColumnName_))
        , OffsetColumnId_(ConsumerNameTable_->GetId(OffsetColumnName_))
        , MetaColumnId_(ConsumerNameTable_->FindId(YTConsumerMetaColumnName))
        , SubConsumerColumnFilter_{PartitionIndexColumnId_, OffsetColumnId_}
        , DecrementOffset_(decrementOffset)
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
                builder.AppendFormat("[%v]", ConsumerTableSchema_->Columns()[index].Name());
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
        const ITransactionPtr& consumerTransaction,
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

            auto partitionRowset = WaitFor(consumerTransaction->VersionedLookupRows(ConsumerPath_, ConsumerNameTable_, keyRowsBuilder.Build(), options))
                .ValueOrThrow()
                .Rowset;
            const auto& rows = partitionRowset->GetRows();

            // XXX(max42): should we use name table from the rowset, or it coincides with our own name table?
            auto offsetRowsetColumnId = partitionRowset->GetNameTable()->GetIdOrThrow(OffsetColumnName_);

            THROW_ERROR_EXCEPTION_UNLESS(
                std::ssize(partitionRowset->GetRows()) <= 1,
                "The table for consumer %Qv should contain at most one row for partition %v when an old offset is specified",
                ConsumerPath_,
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
                    ConsumerPath_,
                    partitionIndex,
                    currentOffset,
                    offsetTimestamp);
            }

            if (currentOffset != *oldOffset) {
                THROW_ERROR_EXCEPTION(
                    EErrorCode::ConsumerOffsetConflict,
                    "Offset conflict at partition %v of consumer %v: expected offset %v, found offset %v",
                    partitionIndex,
                    ConsumerPath_,
                    *oldOffset,
                    currentOffset)
                        << TErrorAttribute("partition", partitionIndex)
                        << TErrorAttribute("consumer", ConsumerPath_)
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

        TYsonString metaYsonString;
        if (MetaColumnId_) {
            auto metaValue = MakeUnversionedNullValue(*MetaColumnId_);
            if (QueueRef_ && QueueClusterClient_) {
                auto meta = GetConsumerMeta(partitionIndex, newOffset);
                if (meta) {
                    metaYsonString = ConvertToYsonString(*meta);
                    metaValue = MakeUnversionedAnyValue(metaYsonString.AsStringBuf(), *MetaColumnId_);
                }
            } else {
                YT_LOG_DEBUG("Consumer meta was not calculated due to unknown queue path or cluster client");
            }

            rowBuilder.AddValue(std::move(metaValue));
        }

        rowsBuilder.AddRow(RowBuffer_->CaptureRow(rowBuilder.GetRow()));

        YT_LOG_DEBUG(
            "Advancing consumer offset (Path: %v, Partition: %v, Offset: %v -> %v)",
            ConsumerPath_,
            partitionIndex,
            oldOffset,
            newOffset);
        consumerTransaction->WriteRows(ConsumerPath_, ConsumerNameTable_, rowsBuilder.Build());
        RowBuffer_->Clear();
    }

    TFuture<std::vector<TPartitionInfo>> CollectPartitions(
        int expectedPartitionCount,
        bool withLastConsumeTime = false) const override
    {
        if (expectedPartitionCount <= 0) {
            return MakeFuture(std::vector<TPartitionInfo>{});
        }

        TStringBuilder queryBuilder;
        queryBuilder.AppendFormat("[%v], [%v]",
            PartitionIndexColumnName_,
            OffsetColumnName_);

        bool hasMetaColumn = ConsumerTableSchema_->FindColumn(YTConsumerMetaColumnName);
        if (hasMetaColumn) {
            queryBuilder.AppendFormat(", [%v]", YTConsumerMetaColumnName);
        }

        queryBuilder.AppendFormat(
            " from [%v] where ([%v] between 0 and %v) and (%v)",

            ConsumerPath_,
            PartitionIndexColumnName_,
            expectedPartitionCount - 1,
            RowPrefixCondition_);

        auto selectQuery = queryBuilder.Flush();

        return BIND(
            &TGenericConsumerClient::DoCollectPartitions,
            MakeStrong(this),
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
        const std::vector<int>& partitionIndexes,
        bool withLastConsumeTime) const override
    {
        auto selectQuery = Format(
            "[%v], [%v] from [%v] where ([%v] in (%v)) and (%v)",
            PartitionIndexColumnName_,
            OffsetColumnName_,
            ConsumerPath_,
            PartitionIndexColumnName_,
            JoinSeq(",", partitionIndexes),
            RowPrefixCondition_);
        return BIND(
            &TGenericConsumerClient::DoCollectPartitions,
            MakeStrong(this),
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

    TFuture<TPartitionStatistics> FetchPartitionStatistics(
        const TYPath& queuePath,
        int partitionIndex) const override
    {
        return QueueClusterClient_->GetNode(queuePath + "/@tablets")
            .Apply(BIND([queuePath, partitionIndex] (const TYsonString& ysonString) -> TPartitionStatistics {
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

                THROW_ERROR_EXCEPTION("Queue %v has no tablet with index %v",
                    queuePath,
                    partitionIndex);
            }));
    }

private:
    const IClientPtr ConsumerClusterClient_;
    const IClientPtr QueueClusterClient_;
    const TYPath ConsumerPath_;
    const std::optional<TCrossClusterReference> QueueRef_;
    const TUnversionedOwningRow RowPrefix_;
    //! A condition of form ([ColumnName0], [ColumnName1], ...) = (RowPrefix_[0], RowPrefix_[1], ...)
    //! defining this subconsumer.
    TString RowPrefixCondition_;
    const TStringBuf PartitionIndexColumnName_;
    const TStringBuf OffsetColumnName_;
    const TTableSchemaPtr ConsumerTableSchema_;
    const TNameTablePtr ConsumerNameTable_;
    const TTableSchemaPtr QueueTableSchema_;
    const int PartitionIndexColumnId_;
    const int OffsetColumnId_;
    const std::optional<int> MetaColumnId_;
    //! A column filter consisting of PartitionIndexColumnName_ and OffsetColumnName_.
    const TColumnFilter SubConsumerColumnFilter_;
    const TRowBufferPtr RowBuffer_ = New<TRowBuffer>();

    // COMPAT(achulkov2): Remove this once we drop support for legacy BigRT consumers.
    //! Controls whether the offset is decremented before being written to the offset table.
    //! BigRT stores the offset of the last read row, so for legacy BigRT consumers this option
    //! should be set to true.
    bool DecrementOffset_ = false;

    std::vector<TPartitionInfo> DoCollectPartitions(
        const TString& selectQuery,
        bool withLastConsumeTime) const
    {
        std::vector<TPartitionInfo> result;

        YT_LOG_DEBUG("Collecting partitions (Query: %v)", selectQuery);

        TSelectRowsOptions selectRowsOptions;
        selectRowsOptions.ReplicaConsistency = EReplicaConsistency::Sync;
        auto selectRowsResult = WaitFor(ConsumerClusterClient_->SelectRows(selectQuery, selectRowsOptions))
            .ValueOrThrow();

        // Note that after table construction table schema may have changed.
        // We must be prepared for that.

        auto partitionIndexRowsetColumnId =
            selectRowsResult.Rowset->GetNameTable()->FindId(PartitionIndexColumnName_);
        auto offsetRowsetColumnId = selectRowsResult.Rowset->GetNameTable()->FindId(OffsetColumnName_);
        auto metaColumnId = selectRowsResult.Rowset->GetNameTable()->FindId(YTConsumerMetaColumnName);

        if (!partitionIndexRowsetColumnId || !offsetRowsetColumnId) {
            THROW_ERROR_EXCEPTION(
                "Table must have columns %Qv and %Qv",
                PartitionIndexColumnName_,
                OffsetColumnName_);
        }
        int expectedColumnsCount = 2 + (metaColumnId ? 1 : 0);

        std::vector<ui64> partitionIndices;

        for (auto row : selectRowsResult.Rowset->GetRows()) {
            YT_VERIFY(static_cast<int>(row.GetCount()) == expectedColumnsCount);

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
            auto partitionInfo = TPartitionInfo{
                .PartitionIndex = FromUnversionedValue<i64>(partitionIndexValue),
                .NextRowIndex = offset,
            };

            if (metaColumnId) {
                const auto& metaValue = row[*metaColumnId];
                YT_VERIFY(metaValue.Type == EValueType::Any || metaValue.Type == EValueType::Null);
                if (metaValue.Type == EValueType::Any) {
                    partitionInfo.ConsumerMeta = ConvertTo<TConsumerMeta>(FromUnversionedValue<TYsonString>(metaValue));
                }
            }

            result.push_back(std::move(partitionInfo));
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

        auto versionedRowset = WaitFor(ConsumerClusterClient_->VersionedLookupRows(
            ConsumerPath_,
            ConsumerNameTable_,
            builder.Build(),
            options))
            .ValueOrThrow()
            .Rowset;

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

    std::optional<TConsumerMeta> GetConsumerMeta(
        int partitionIndex,
        i64 offset) const
    {
        if (!QueueRef_ || !QueueClusterClient_ || !QueueTableSchema_) {
            return {};
        }

        auto params = TCollectPartitionRowInfoParams{
            .HasCumulativeDataWeightColumn = static_cast<bool>(QueueTableSchema_->FindColumn(CumulativeDataWeightColumnName)),
            .HasTimestampColumn = static_cast<bool>(QueueTableSchema_->FindColumn(TimestampColumnName)),
        };

        std::vector<std::pair<int, i64>> tabletAndRowIndices = {{partitionIndex, offset}};
        if (offset > 0) {
            tabletAndRowIndices.push_back({partitionIndex, offset - 1});
        }

        auto partitionRowInfosOrError = WaitFor(CollectPartitionRowInfos(
            QueueRef_->Path,
            QueueClusterClient_,
            std::move(tabletAndRowIndices),
            params,
            Logger()));

        if (!partitionRowInfosOrError.IsOK()) {
            YT_LOG_DEBUG(partitionRowInfosOrError, "Failed to get partition row infos (Path: %v)",
                QueueRef_->Path);
            return {};
        }

        auto partitionRowInfos = std::move(partitionRowInfosOrError).Value();

        auto partitionIt = partitionRowInfos.find(partitionIndex);
        if (partitionIt == partitionRowInfos.end()) {
            YT_LOG_DEBUG("Failed to collect row info for partition (Path: %v, PartitionIndex: %v)",
                QueueRef_->Path,
                partitionIndex);
            return {};
        }

        TConsumerMeta meta;

        auto partitionRowIt = partitionIt->second.find(offset);
        if (partitionRowIt != partitionIt->second.end()) {
            meta.OffsetTimestamp = partitionRowIt->second.Timestamp;
        } else {
            YT_LOG_DEBUG("Failed to collect consumer offset timestamp (Path: %v, PartitionIndex: %v, Offset: %v)",
                QueueRef_->Path,
                partitionIndex,
                offset);
        }

        if (offset > 0) {
            auto partitionRowIt = partitionIt->second.find(offset - 1);
            if (partitionRowIt != partitionIt->second.end()) {
                meta.CumulativeDataWeight = partitionRowIt->second.CumulativeDataWeight;
            } else {
                YT_LOG_DEBUG("Failed to collect consumer cumulative data weight (Path: %v, PartitionIndex: %v, Offset: %v)",
                    QueueRef_->Path,
                    partitionIndex,
                    offset - 1);
            }
        }

        return meta;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TYTConsumerClient
    : public IConsumerClient
{
public:
    TYTConsumerClient(IClientPtr consumerClusterClient, TYPath consumerPath, TTableSchemaPtr consumerTableSchema)
        : ConsumerClusterClient_(std::move(consumerClusterClient))
        , ConsumerPath_(std::move(consumerPath))
        , ConsumerTableSchema_(std::move(consumerTableSchema))
    { }

    ISubConsumerClientPtr GetSubConsumerClient(const IClientPtr& queueClusterClient, const TCrossClusterReference& queueRef) const override
    {
        TUnversionedOwningRowBuilder builder;
        builder.AddValue(MakeUnversionedStringValue(queueRef.Cluster, QueueClusterColumnId_));
        builder.AddValue(MakeUnversionedStringValue(queueRef.Path, QueuePathColumnId_));
        auto row = builder.FinishRow();

        TTableSchemaPtr queueTableSchema;
        if (queueClusterClient) {
            auto queueTableInfo = WaitFor(queueClusterClient->GetTableMountCache()->GetTableInfo(queueRef.Path))
                .ValueOrThrow();
            queueTableSchema = queueTableInfo->Schemas[ETableSchemaKind::Primary];
        }

        auto subConsumerClient = New<TGenericConsumerClient>(
            ConsumerClusterClient_,
            queueClusterClient,
            ConsumerPath_,
            std::optional<TCrossClusterReference>{queueRef},
            std::move(row),
            "partition_index",
            "offset",
            /*decrementOffset*/ false,
            ConsumerTableSchema_,
            queueTableSchema);

        return subConsumerClient;
    }

private:
    const IClientPtr ConsumerClusterClient_;
    const TYPath ConsumerPath_;
    const TTableSchemaPtr ConsumerTableSchema_;

    static const TNameTablePtr ConsumerNameTable_;
    static const int QueueClusterColumnId_;
    static const int QueuePathColumnId_;
};

const TNameTablePtr TYTConsumerClient::ConsumerNameTable_ = TNameTable::FromSchema(*YTConsumerTableSchema);
const int TYTConsumerClient::QueueClusterColumnId_ = TYTConsumerClient::ConsumerNameTable_->GetId("queue_cluster");
const int TYTConsumerClient::QueuePathColumnId_ = TYTConsumerClient::ConsumerNameTable_->GetId("queue_path");

////////////////////////////////////////////////////////////////////////////////

IConsumerClientPtr CreateConsumerClient(
    const IClientPtr& consumerClusterClient,
    const TYPath& consumerPath,
    const TTableSchema& consumerSchema)
{
    if (!consumerSchema.IsUniqueKeys()) {
        THROW_ERROR_EXCEPTION("Consumer schema must have unique keys, schema does not")
            << TErrorAttribute("actual_schema", consumerSchema);
    }

    if (consumerSchema == *YTConsumerTableSchema) {
        return New<TYTConsumerClient>(consumerClusterClient, consumerPath, YTConsumerTableSchema);
    } else if (consumerSchema == *YTConsumerWithoutMetaTableSchema) {
        return New<TYTConsumerClient>(consumerClusterClient, consumerPath, YTConsumerWithoutMetaTableSchema);
    } else {
        THROW_ERROR_EXCEPTION("Table schema is not recognized as a valid consumer schema")
            << TErrorAttribute("expected_schema", *YTConsumerTableSchema)
            << TErrorAttribute("actual_schema", consumerSchema);
    }
}

IConsumerClientPtr CreateConsumerClient(
    const IClientPtr& consumerClusterClient,
    const TYPath& consumerPath)
{
    auto tableInfo = WaitFor(consumerClusterClient->GetTableMountCache()->GetTableInfo(consumerPath))
        .ValueOrThrow();
    auto schema = tableInfo->Schemas[ETableSchemaKind::Primary];

    return CreateConsumerClient(consumerClusterClient, consumerPath, *schema);
}

ISubConsumerClientPtr CreateSubConsumerClient(
    const IClientPtr& consumerClusterClient,
    const IClientPtr& queueClusterClient,
    const TYPath& consumerPath,
    TRichYPath queuePath)
{
    auto queueCluster = queuePath.GetCluster();
    if (!queueCluster) {
        if (auto clientCluster = consumerClusterClient->GetClusterName()) {
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

    return CreateConsumerClient(consumerClusterClient, consumerPath)->GetSubConsumerClient(queueClusterClient, queueRef);
}

const TTableSchemaPtr& GetConsumerSchema()
{
    return YTConsumerTableSchema;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueClient
