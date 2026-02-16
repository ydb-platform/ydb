#include "describe.h"
#include "colors.h"
#include "pretty_table.h"
#include "print_utils.h"

#include <ydb/public/lib/json_value/ydb_json_value.h>
#include <ydb/public/lib/ydb_cli/dump/util/util.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/coordination/coordination.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/draft/ydb_replication.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/draft/ydb_view.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/draft/accessor.h>

#include <util/generic/hash.h>
#include <util/stream/format.h>
#include <util/string/join.h>

namespace NYdb::NConsoleClient {

namespace {

// Helper function to format duration in human-readable format
// Returns empty string for zero duration, otherwise returns duration in format like "2h", "5m 30s", etc.
TString HumanReadableDurationOrEmpty(TDuration duration) {
    if (duration == TDuration::Zero()) {
        return "";
    }
    return ToString(HumanReadable(duration));
}

TString FormatCodecs(const std::vector<NYdb::NTopic::ECodec>& codecs) {
    return JoinSeq(", ", codecs);
}

void PrintTopicConsumers(const std::vector<NYdb::NTopic::TConsumer>& consumers, IOutputStream& out) {
    if (consumers.empty()) {
        return;
    }
    TPrettyTable table({ "ConsumerName", "SupportedCodecs", "ReadFrom", "Important", "Availability period", });
    for (const auto& c: consumers) {
        table.AddRow()
            .Column(0, c.GetConsumerName())
            .Column(1, FormatCodecs(c.GetSupportedCodecs()))
            .Column(2, c.GetReadFrom().ToRfc822StringLocal())
            .Column(3, c.GetImportant() ? "Yes" : "No")
            .Column(4, HumanReadableDurationOrEmpty(c.GetAvailabilityPeriod()));
    }
    out << Endl << "Consumers: " << Endl;
    out << table;
}

void PrintStatistics(const NTopic::TTopicDescription& topicDescription, IOutputStream& out) {
    out << Endl << "Topic stats:" << Endl;
    auto& topicStats = topicDescription.GetTopicStats();
    out << "Approximate size of topic: " << PrettySize(topicStats.GetStoreSizeBytes()) << Endl;
    out << "Max partitions write time lag: " << FormatDuration(topicStats.GetMaxWriteTimeLag()) << Endl;
    out << "Min partitions last write time: " << FormatTime(topicStats.GetMinLastWriteTime()) << Endl;
    out << "Written size per minute: " << PrettySize(topicStats.GetBytesWrittenPerMinute()) << Endl;
    out << "Written size per hour: " << PrettySize(topicStats.GetBytesWrittenPerHour()) << Endl;
    out << "Written size per day: " << PrettySize(topicStats.GetBytesWrittenPerDay()) << Endl;
}

void PrintMain(const NTopic::TTopicDescription& topicDescription, IOutputStream& out) {
    out << Endl << "Main:";
    out << Endl << "RetentionPeriod: " << HumanReadable(topicDescription.GetRetentionPeriod());
    if (topicDescription.GetRetentionStorageMb().has_value()) {
        out << Endl << "StorageRetention: " << *topicDescription.GetRetentionStorageMb() << " MB";
    }
    out << Endl << "PartitionsCount: " << topicDescription.GetTotalPartitionsCount();
    out << Endl << "PartitionWriteSpeed: " << topicDescription.GetPartitionWriteSpeedBytesPerSecond() / 1_KB << " KB";
    out << Endl << "MeteringMode: " << (TStringBuilder() << topicDescription.GetMeteringMode());
    if (topicDescription.GetMetricsLevel().has_value()) {
        out << Endl << "MetricsLevel: " << *topicDescription.GetMetricsLevel();
    }
    if (!topicDescription.GetSupportedCodecs().empty()) {
        out << Endl << "SupportedCodecs: " << FormatCodecs(topicDescription.GetSupportedCodecs()) << Endl;
    } else {
        out << Endl;
    }
}

// TODO: Deduplicate with ydb_service_scheme.cpp
THashMap<NTopic::EAutoPartitioningStrategy, TString> AutoPartitioningStrategiesStrs = {
    std::pair<NTopic::EAutoPartitioningStrategy, TString>(NTopic::EAutoPartitioningStrategy::Disabled, "disabled"),
    std::pair<NTopic::EAutoPartitioningStrategy, TString>(NTopic::EAutoPartitioningStrategy::ScaleUp, "up"),
    std::pair<NTopic::EAutoPartitioningStrategy, TString>(NTopic::EAutoPartitioningStrategy::ScaleUpAndDown, "up-and-down"),
    std::pair<NTopic::EAutoPartitioningStrategy, TString>(NTopic::EAutoPartitioningStrategy::Paused, "paused"),
};

void PrintAutopartitioning(const NTopic::TTopicDescription& topicDescription, IOutputStream& out) {
    auto autoPartitioningStrategyIt = AutoPartitioningStrategiesStrs.find(topicDescription.GetPartitioningSettings().GetAutoPartitioningSettings().GetStrategy());
    if (!autoPartitioningStrategyIt.IsEnd()) {
        out << Endl << "AutoPartitioning:";
        out << Endl << "Strategy: " << autoPartitioningStrategyIt->second;
        if (topicDescription.GetPartitioningSettings().GetAutoPartitioningSettings().GetStrategy() != NTopic::EAutoPartitioningStrategy::Disabled) {
            out << Endl << "MinActivePartitions: " << topicDescription.GetPartitioningSettings().GetMinActivePartitions();
            out << Endl << "MaxActivePartitions: " << topicDescription.GetPartitioningSettings().GetMaxActivePartitions();
            out << Endl << "DownUtilizationPercent: " << topicDescription.GetPartitioningSettings().GetAutoPartitioningSettings().GetDownUtilizationPercent();
            out << Endl << "UpUtilizationPercent: " << topicDescription.GetPartitioningSettings().GetAutoPartitioningSettings().GetUpUtilizationPercent();
            out << Endl << "StabilizationWindowSeconds: " << topicDescription.GetPartitioningSettings().GetAutoPartitioningSettings().GetStabilizationWindow().Seconds() << Endl;
        } else {
            out << Endl;
        }
    }
}

void PrintPartitionStatistics(const NTopic::TTopicDescription& topicDescription, IOutputStream& out) {
    out << Endl << "Topic partitions stats:" << Endl;

    TVector<TString> columnNames = { "#" };
    columnNames.push_back("Active");
    columnNames.push_back("Start offset");
    columnNames.push_back("End offset");
    columnNames.push_back("Size");
    columnNames.push_back("Last write time");
    columnNames.push_back("Max write time lag");
    columnNames.push_back("Written size per minute");
    columnNames.push_back("Written size per hour");
    columnNames.push_back("Written size per day");

    TPrettyTable table(columnNames);
    for (const auto& part : topicDescription.GetPartitions()) {
        auto& row = table.AddRow();
        row.Column(0, part.GetPartitionId());
        row.Column(1, part.GetActive());
        const auto& partStats = part.GetPartitionStats();
        if (partStats) {
            row.Column(2, partStats->GetStartOffset());
            row.Column(3, partStats->GetEndOffset());
            row.Column(4, PrettySize(partStats->GetStoreSizeBytes()));
            row.Column(5, FormatTime(partStats->GetLastWriteTime()));
            row.Column(6, FormatDuration(partStats->GetMaxWriteTimeLag()));
            row.Column(7, PrettySize(partStats->GetBytesWrittenPerMinute()));
            row.Column(8, PrettySize(partStats->GetBytesWrittenPerHour()));
            row.Column(9, PrettySize(partStats->GetBytesWrittenPerDay()));
        }
    }
    out << table;
}

} // namespace

int PrintPrettyDescribeConsumerResult(const NYdb::NTopic::TConsumerDescription& description, bool withPartitionsStats, IOutputStream& out) {
    // Consumer info
    const NYdb::NTopic::TConsumer& consumer = description.GetConsumer();
    out << "Consumer " << consumer.GetConsumerName() << ": " << Endl;
    out << "Important: " << (consumer.GetImportant() ? "Yes" : "No") << Endl;
    if (const auto availabilityPeriodStr = HumanReadableDurationOrEmpty(consumer.GetAvailabilityPeriod())) {
        out << "Availability period: " << availabilityPeriodStr << Endl;
    }
    if (const TInstant& readFrom = consumer.GetReadFrom()) {
        out << "Read from: " << readFrom.ToRfc822StringLocal() << Endl;
    } else {
        out << "Read from: 0" << Endl;
    }
    out << "Supported codecs: " << FormatCodecs(consumer.GetSupportedCodecs()) << Endl;

    if (const auto& attrs = consumer.GetAttributes(); !attrs.empty()) {
        TPrettyTable attrTable({ "Attribute", "Value" }, TPrettyTableConfig().WithoutRowDelimiters());
        for (const auto& [k, v] : attrs) {
            attrTable.AddRow()
                .Column(0, k)
                .Column(1, v);
        }
        out << "Attributes:" << Endl << attrTable;
    }

    // Partitions
    TVector<TString> columnNames = {
        "#",
        "Active",
        "ChildIds",
        "ParentIds"
    };

    size_t statsBase = columnNames.size();
    if (withPartitionsStats) {
        columnNames.insert(columnNames.end(),
            {
                "Start offset",
                "End offset",
                "Size",
                "Last write time",
                "Max write time lag",
                "Written size per minute",
                "Written size per hour",
                "Written size per day",
                "Committed offset",
                "Last read offset",
                "Reader name",
                "Read session id"
            }
        );
    }

    TPrettyTable partitionsTable(columnNames, TPrettyTableConfig().WithoutRowDelimiters());
    for (const NYdb::NTopic::TPartitionInfo& partition : description.GetPartitions()) {
        auto& row = partitionsTable.AddRow();
        row
            .Column(0, partition.GetPartitionId())
            .Column(1, partition.GetActive())
            .Column(2, JoinSeq(",", partition.GetChildPartitionIds()))
            .Column(3, JoinSeq(",", partition.GetParentPartitionIds()));
        if (withPartitionsStats) {
            if (const auto& maybeStats = partition.GetPartitionStats()) {
                row
                    .Column(statsBase + 0, maybeStats->GetStartOffset())
                    .Column(statsBase + 1, maybeStats->GetEndOffset())
                    .Column(statsBase + 2, PrettySize(maybeStats->GetStoreSizeBytes()))
                    .Column(statsBase + 3, FormatTime(maybeStats->GetLastWriteTime()))
                    .Column(statsBase + 4, FormatDuration(maybeStats->GetMaxWriteTimeLag()))
                    .Column(statsBase + 5, PrettySize(maybeStats->GetBytesWrittenPerMinute()))
                    .Column(statsBase + 6, PrettySize(maybeStats->GetBytesWrittenPerHour()))
                    .Column(statsBase + 7, PrettySize(maybeStats->GetBytesWrittenPerDay()));
            }

            if (const auto& maybeStats = partition.GetPartitionConsumerStats()) {
                row
                    .Column(statsBase + 8, maybeStats->GetCommittedOffset())
                    .Column(statsBase + 9, maybeStats->GetLastReadOffset())
                    .Column(statsBase + 10, maybeStats->GetReaderName())
                    .Column(statsBase + 11, maybeStats->GetReadSessionId());
            }
        }
    }
    out << "Partitions:" << Endl << partitionsTable;
    return EXIT_SUCCESS;
}

namespace {

template<typename TTableLikeObjectDescription>
void PrintColumns(const TTableLikeObjectDescription& tableDescription, IOutputStream& out) {
    if (!tableDescription.GetTableColumns().size()) {
        return;
    }
    out << Endl;
    TPrettyTable table({ "Name", "Type", "Family", "Key" }, TPrettyTableConfig().WithoutRowDelimiters());

    const std::vector<std::string>& keyColumns = tableDescription.GetPrimaryKeyColumns();
    for (const NTable::TTableColumn& column : tableDescription.GetTableColumns()) {
        TString key = "";
        auto itKey = std::find(keyColumns.begin(), keyColumns.end(), column.Name);
        if (itKey != keyColumns.end()) {
            key = TStringBuilder() << "K" << itKey - keyColumns.begin();
        }
        TString columnType;
        try {
            columnType = FormatType(column.Type);
        } catch (yexception) {
            columnType = "<unknown_type>";
        }
        table.AddRow()
            .Column(0, column.Name)
            .Column(1, columnType)
            .Column(2, column.Family)
            .Column(3, key);
    }

    out << "Columns:" << Endl << table;
}

void PrintIndexes(const NTable::TTableDescription& tableDescription, IOutputStream& out) {
    const std::vector<NTable::TIndexDescription>& indexes = tableDescription.GetIndexDescriptions();
    if (!indexes.size()) {
        return;
    }

    TPrettyTable table({ "Name", "Type", "Index columns", "Cover columns" },
        TPrettyTableConfig().WithoutRowDelimiters());

    for (const auto& index : indexes) {
        table.AddRow()
            .Column(0, index.GetIndexName())
            .Column(1, index.GetIndexType())
            .Column(2, JoinSeq(",", index.GetIndexColumns()))
            .Column(3, JoinSeq(",", index.GetDataColumns()));
    }

    out << Endl << "Indexes:" << Endl << table;
}

void PrintChangefeeds(const NTable::TTableDescription& tableDescription, IOutputStream& out) {
    const auto& changefeeds = tableDescription.GetChangefeedDescriptions();
    if (changefeeds.empty()) {
        return;
    }

    TPrettyTable table({ "Name", "Mode", "Format", "State", "VirtualTimestamps" },
        TPrettyTableConfig().WithoutRowDelimiters());

    for (const auto& changefeed : changefeeds) {
        auto& row = table.AddRow()
            .Column(0, changefeed.GetName())
            .Column(1, changefeed.GetMode())
            .Column(2, changefeed.GetFormat())
            .Column(4, changefeed.GetVirtualTimestamps() ? "on" : "off");
        if (changefeed.GetState() == NTable::EChangefeedState::InitialScan && changefeed.GetInitialScanProgress()) {
            const float percentage = changefeed.GetInitialScanProgress()->GetProgress();
            row.Column(3, TStringBuilder() << changefeed.GetState()
                << " (" << FloatToString(percentage, PREC_POINT_DIGITS, 2) << "%)");
        } else {
            row.Column(3, changefeed.GetState());
        }
    }

    out << Endl << "Changefeeds:" << Endl << table;
}

void PrintStorageSettings(const NTable::TTableDescription& tableDescription, IOutputStream& out) {
    const NTable::TStorageSettings& settings = tableDescription.GetStorageSettings();
    const auto commitLog0 = settings.GetTabletCommitLog0();
    const auto commitLog1 = settings.GetTabletCommitLog1();
    const auto external = settings.GetExternal();
    const auto storeExternalBlobs = settings.GetStoreExternalBlobs();
    const auto externalDataChannelsCount = settings.GetExternalDataChannelsCount();
    if (!commitLog0 && !commitLog1 && !external && !storeExternalBlobs.has_value() && !externalDataChannelsCount.has_value()) {
        return;
    }
    out << Endl << "Storage settings: " << Endl;
    if (commitLog0) {
        out << "Internal channel 0 commit log storage pool: " << commitLog0.value() << Endl;
    }
    if (commitLog1) {
        out << "Internal channel 1 commit log storage pool: " << commitLog1.value() << Endl;
    }
    if (external) {
        out << "External blobs storage pool: " << external.value() << Endl;
    }
    if (storeExternalBlobs) {
        out << "Store large values in \"external blobs\": "
            << (storeExternalBlobs.value() ? "true" : "false") << Endl;
    }
    if (externalDataChannelsCount) {
        out << "External data channels: " << externalDataChannelsCount.value() << Endl;
    }
}

void PrintColumnFamilies(const NTable::TTableDescription& tableDescription, IOutputStream& out) {
    if (tableDescription.GetColumnFamilies().empty()) {
        return;
    }
    TPrettyTable table({ "Name", "Data", "Compression", "Cache mode" },
        TPrettyTableConfig().WithoutRowDelimiters());

    for (const NTable::TColumnFamilyDescription& family : tableDescription.GetColumnFamilies()) {
        std::optional<std::string> data = family.GetData();
        TString compression;
        if (family.GetCompression()) {
            switch (family.GetCompression().value()) {
            case NTable::EColumnFamilyCompression::None:
                compression = "None";
                break;
            case NTable::EColumnFamilyCompression::LZ4:
                compression = "LZ4";
                break;
            default:
                compression = TStringBuilder() << "unknown("
                    << static_cast<size_t>(family.GetCompression().value()) << ")";
            }
        }
        TStringBuilder cacheMode;
        if (family.GetCacheMode().has_value()) {
            cacheMode << family.GetCacheMode().value();
        }
        table.AddRow()
            .Column(0, family.GetName())
            .Column(1, data ? data.value() : "")
            .Column(2, compression)
            .Column(3, cacheMode);
    }
    out << Endl << "Column families: " << Endl;
    out << table;
}

template<typename TTableLikeObjectDescription>
void PrintAttributes(const TTableLikeObjectDescription& tableDescription, IOutputStream& out) {
    if (tableDescription.GetAttributes().empty()) {
        return;
    }
    TPrettyTable table({ "Name", "Value" }, TPrettyTableConfig().WithoutRowDelimiters());

    for (const auto& [name, value] : tableDescription.GetAttributes()) {
        table.AddRow()
            .Column(0, name)
            .Column(1, value);
    }
    out << Endl << "Attributes: " << Endl;
    out << table;
}

void PrintTtlSettings(const NTable::TTableDescription& tableDescription, IOutputStream& out) {
    const auto& settings = tableDescription.GetTtlSettings();
    if (!settings) {
        return;
    }

    out << Endl << "Ttl settings ";
    switch (settings->GetMode()) {
    case NTable::TTtlSettings::EMode::DateTypeColumn:
    {
        out << "(date type column):" << Endl;
        const auto& dateTypeColumn = settings->GetDateTypeColumn();
        out << "Column name: " << dateTypeColumn.GetColumnName() << Endl;
        out << "Expire after: " << dateTypeColumn.GetExpireAfter() << Endl;
        break;
    }
    case NTable::TTtlSettings::EMode::ValueSinceUnixEpoch:
    {
        out << "(value since unix epoch):" << Endl;
        const auto& valueSinceEpoch = settings->GetValueSinceUnixEpoch();
        out << "Column name: " << valueSinceEpoch.GetColumnName() << Endl;
        out << "Column unit: " << valueSinceEpoch.GetColumnUnit() << Endl;
        out << "Expire after: " << valueSinceEpoch.GetExpireAfter() << Endl;
        break;
    }
    default:
        NColorizer::TColors colors = NConsoleClient::AutoColors(out);
        out << "(unknown):" << Endl
            << colors.RedColor() << "Unknown ttl settings mode. Please update your version of YDB cli"
            << colors.OldColor() << Endl;
    }

    if (settings->GetRunInterval()) {
        out << "Run interval: " << settings->GetRunInterval() << Endl;
    }
}

void PrintPartitioningSettings(const NTable::TTableDescription& tableDescription, IOutputStream& out) {
    const auto& settings = tableDescription.GetPartitioningSettings();
    const auto partBySize = settings.GetPartitioningBySize();
    const auto partByLoad = settings.GetPartitioningByLoad();
    if (!partBySize.has_value() && !partByLoad.has_value()) {
        return;
    }
    const auto partitionSizeMb = settings.GetPartitionSizeMb();
    const auto minPartitions = settings.GetMinPartitionsCount();
    const auto maxPartitions = settings.GetMaxPartitionsCount();
    out << Endl << "Auto partitioning settings: " << Endl;
    out << "Partitioning by size: " << (partBySize.value() ? "true" : "false") << Endl;
    out << "Partitioning by load: " << (partByLoad.value() ? "true" : "false") << Endl;
    if (partBySize.has_value() && partitionSizeMb) {
        out << "Preferred partition size (Mb): " << partitionSizeMb << Endl;
    }
    if (minPartitions) {
        out << "Min partitions count: " << minPartitions << Endl;
    }
    if (maxPartitions) {
        out << "Max partitions count: " << maxPartitions << Endl;
    }
}

void PrintReadReplicasSettings(const NTable::TTableDescription& tableDescription, IOutputStream& out) {
    const auto& settings = tableDescription.GetReadReplicasSettings();
    if (!settings) {
        return;
    }
    out << Endl << "Read replicas settings: " << Endl;
    switch (settings->GetMode()) {
    case NTable::TReadReplicasSettings::EMode::PerAz:
        out << "Read replicas count in each AZ: " << settings->GetReadReplicasCount() << Endl;
        break;
    case NTable::TReadReplicasSettings::EMode::AnyAz:
        out << "Read replicas total count in all AZs: " << settings->GetReadReplicasCount() << Endl;
        break;
    default:
        NColorizer::TColors colors = NConsoleClient::AutoColors(out);
        out << colors.RedColor() << "Unknown read replicas settings mode. Please update your version of YDB cli"
            << colors.OldColor() << Endl;
    }
}

void PrintStatistics(const NTable::TTableDescription& tableDescription, IOutputStream& out) {
    out << Endl << "Table stats:" << Endl;
    out << "Partitions count: " << tableDescription.GetPartitionsCount() << Endl;
    out << "Approximate number of rows: " << tableDescription.GetTableRows() << Endl;
    out << "Approximate size of table: " << PrettySize(tableDescription.GetTableSize()) << Endl;
    out << "Last modified: " << FormatTime(tableDescription.GetModificationTime()) << Endl;
    out << "Created: " << FormatTime(tableDescription.GetCreationTime()) << Endl;
}

void PrintPartitionInfo(const NTable::TTableDescription& tableDescription, bool showBoundaries, bool showStats, IOutputStream& out) {
    const std::vector<NTable::TKeyRange>& ranges = tableDescription.GetKeyRanges();
    const std::vector<NTable::TPartitionStats>& stats = tableDescription.GetPartitionStats();
    if (showBoundaries) {
        if (showStats) {
            out << Endl << "Partitions info:" << Endl;
            if (ranges.empty() && stats.empty()) {
                out << "No data." << Endl;
                return;
            }
        } else {
            out << Endl << "Partitions key boundaries:" << Endl;
            if (ranges.empty()) {
                out << "No data." << Endl;
                return;
            }
        }
    } else {
        out << Endl << "Partitions stats:" << Endl;
        if (stats.empty()) {
            out << "No data." << Endl;
            return;
        }
    }
    size_t rowsCount;
    if (showBoundaries && showStats && ranges.size() != stats.size()) {
        out << "(!) Warning: partitions key boundaries size (" << ranges.size()
            << ") mismatches partitions stats size (" << stats.size() << ")." << Endl;
        rowsCount = Min(ranges.size(), stats.size());
    } else {
        rowsCount = Max(ranges.size(), stats.size());
    }
    TVector<TString> columnNames = { "#" };
    if (showBoundaries) {
        columnNames.push_back("");
        columnNames.push_back("From");
        columnNames.push_back("To");
        columnNames.push_back("");
    }
    if (showStats) {
        columnNames.push_back("Rows");
        columnNames.push_back("Size");
    }
    TPrettyTable table(columnNames, TPrettyTableConfig().WithoutRowDelimiters());
    for (size_t i = 0; i < rowsCount; ++i) {
        auto& row = table.AddRow();
        size_t j = 0;
        row.Column(j++, i + 1);
        if (showBoundaries) {
            const NTable::TKeyRange& keyRange = ranges[i];
            const std::optional<NTable::TKeyBound>& from = keyRange.From();
            const std::optional<NTable::TKeyBound>& to = keyRange.To();
            if (from.has_value()) {
                const NTable::TKeyBound& bound = from.value();
                if (bound.IsInclusive()) {
                    row.Column(j++, "[");
                } else {
                    row.Column(j++, "(");
                }
                row.Column(j++, FormatValueJson(bound.GetValue(), EBinaryStringEncoding::Unicode));
            } else {
                row.Column(j++, "(");
                row.Column(j++, "-Inf");
            }
            if (to.has_value()) {
                const NTable::TKeyBound& bound = to.value();
                row.Column(j++, FormatValueJson(bound.GetValue(), EBinaryStringEncoding::Unicode));
                if (bound.IsInclusive()) {
                    row.Column(j++, "]");
                } else {
                    row.Column(j++, ")");
                }
            } else {
                row.Column(j++, "+Inf");
                row.Column(j++, ")");
            }
        }
        if (showStats) {
            const NTable::TPartitionStats& partStats = stats[i];
            row.Column(j++, partStats.Rows);
            row.Column(j++, PrettySize(partStats.Size));
        }
    }
    out << table;
}

template <typename T, typename U>
static TString ValueOr(const std::optional<T>& value, const U& orValue) {
    if (value) {
        return TStringBuilder() << *value;
    } else {
        return TStringBuilder() << orValue;
    }
}

template <typename U>
static TString ProgressOr(const std::optional<float>& value, const U& orValue) {
    if (value) {
        return TStringBuilder() << FloatToString(*value, PREC_POINT_DIGITS, 2) << "%";
    } else {
        return TStringBuilder() << orValue;
    }
}

static TStringBuf SkipDatabasePrefix(TStringBuf value, TStringBuf prefix) {
    if (value.SkipPrefix(prefix)) {
        value.Skip(1); // skip '/'
    }
    return value;
}

void PrintConnectionParams(const NReplication::TConnectionParams& connParams, IOutputStream& out) {
    bool isLocal = connParams.GetDiscoveryEndpoint().empty();
    if (isLocal) {
        return;
    }

    out << Endl << "Endpoint: " << connParams.GetDiscoveryEndpoint();
    out << Endl << "Database: " << connParams.GetDatabase();

    switch (connParams.GetCredentials()) {
    case NReplication::TConnectionParams::ECredentials::Static:
        out << Endl << "User: " << connParams.GetStaticCredentials().User;
        out << Endl << "Password (SECRET): " << connParams.GetStaticCredentials().PasswordSecretName;
        break;
    case NReplication::TConnectionParams::ECredentials::OAuth:
        out << Endl << "OAuth token (SECRET): " << connParams.GetOAuthCredentials().TokenSecretName;
        break;
    }
}

void PrintViewQuery(const std::string& query, IOutputStream& out) {
    out << "\nQuery text:\n" << query << Endl;
}

// Helpers for PrintDescription template
template <typename TCommand, typename TValue>
using TPrettyPrinter = int(TCommand::*)(const TValue&, const TDescribeOptions&) const;

template <typename TCommand, typename TValue>
static int PrintDescription(TCommand* self, EDataFormat format, const TValue& value, const TDescribeOptions& options, TPrettyPrinter<TCommand, TValue> prettyFunc) {
    switch (format) {
        case EDataFormat::Default:
        case EDataFormat::Pretty:
            return std::invoke(prettyFunc, self, value, options);
        case EDataFormat::Json:
            Cerr << "Warning! Option --json is deprecated and will be removed soon. "
                 << "Use \"--format proto-json-base64\" option instead." << Endl;
            [[fallthrough]];
        case EDataFormat::ProtoJsonBase64:
            return PrintProtoJsonBase64(NDraft::TProtoAccessor::GetProto(value), self->GetOutputStream());
        default:
            // Should verify this earlier
            return EXIT_FAILURE;
    }
}

} // namespace

TDescribeLogic::TDescribeLogic(TDriver driver, IOutputStream& out)
    : Driver(driver)
    , Out(out)
{}

int TDescribeLogic::Describe(const TString& path, const TDescribeOptions& options, EDataFormat format) {
    return DescribePath(path, options, format);
}

int TDescribeLogic::DescribePath(const TString& path, const TDescribeOptions& options, EDataFormat format) {
    NScheme::TSchemeClient client(Driver);
    NScheme::TDescribePathResult result = client.DescribePath(
        path,
        NScheme::TDescribePathSettings()
    ).GetValueSync();

    if (!result.IsSuccess()) {
        return TryTopicConsumerDescribeOrFail(path, result, options, format);
    }

    // NStatusHelpers::ThrowOnErrorOrPrintIssues(result); // We can just print issues inside
    if (!result.IsSuccess()) {
        Out << result.GetIssues().ToString() << Endl;
        return EXIT_FAILURE;
    }

    return PrintPathResponse(path, result, options, format);
}

int TDescribeLogic::PrintPathResponse(const TString& path, const NScheme::TDescribePathResult& result, const TDescribeOptions& options, EDataFormat format) {
    if (format == EDataFormat::Default || format == EDataFormat::Pretty) {
        NScheme::TSchemeEntry entry = result.GetEntry();
        Out << "<" << EntryTypeToString(entry.Type) << "> " << entry.Name << Endl;
    }
    NScheme::TSchemeEntry entry = result.GetEntry();
    switch (entry.Type) {
    case NScheme::ESchemeEntryType::Table:
        return DescribeTable(path, options, format);
    case NScheme::ESchemeEntryType::ColumnTable:
        return DescribeColumnTable(path, options, format);
    case NScheme::ESchemeEntryType::PqGroup:
    case NScheme::ESchemeEntryType::Topic:
        return DescribeTopic(path, options, format);
    case NScheme::ESchemeEntryType::CoordinationNode:
        return DescribeCoordinationNode(path, format);
    case NScheme::ESchemeEntryType::Replication:
        return DescribeReplication(path, options, format);
    case NScheme::ESchemeEntryType::Transfer:
        return DescribeTransfer(path, format);
    case NScheme::ESchemeEntryType::View:
        return DescribeView(path);
    case NScheme::ESchemeEntryType::ExternalDataSource:
        return DescribeExternalDataSource(path, format);
    case NScheme::ESchemeEntryType::ExternalTable:
        return DescribeExternalTable(path, format);
    case NScheme::ESchemeEntryType::SysView:
        return DescribeSystemView(path, format);
    default:
        return DescribeEntryDefault(entry, options);
    }
}

int TDescribeLogic::DescribeEntryDefault(NScheme::TSchemeEntry entry, const TDescribeOptions& options) {
    if (options.ShowPermissions) {
        Out << Endl;
        PrintAllPermissions(entry.Owner, entry.Permissions, entry.EffectivePermissions, Out);
    }
    WarnAboutTableOptions(TString(entry.Name), options, EDataFormat::Default); // Assuming default format for check
    return EXIT_SUCCESS;
}

int TDescribeLogic::DescribeTable(const TString& path, const TDescribeOptions& options, EDataFormat format) {
    NTable::TTableClient client(Driver);
    NTable::TCreateSessionResult sessionResult = client.GetSession(NTable::TCreateSessionSettings()).GetValueSync();
    if (!sessionResult.IsSuccess()) {
        Out << sessionResult.GetIssues().ToString() << Endl;
        return EXIT_FAILURE;
    }

    NTable::TDescribeTableResult result = sessionResult.GetSession().DescribeTable(
        path,
        NTable::TDescribeTableSettings()
        .WithKeyShardBoundary(options.ShowKeyShardBoundaries)
        .WithTableStatistics(options.ShowStats || options.ShowPartitionStats)
        .WithPartitionStatistics(options.ShowPartitionStats)
    ).GetValueSync();

    if (!result.IsSuccess()) {
        Out << result.GetIssues().ToString() << Endl;
        return EXIT_FAILURE;
    }

    auto desc = result.GetTableDescription();
    return PrintDescription(this, format, desc, options, &TDescribeLogic::PrintTableResponsePretty);
}

int TDescribeLogic::DescribeColumnTable(const TString& path, const TDescribeOptions& options, EDataFormat format) {
    NTable::TTableClient client(Driver);
    NTable::TCreateSessionResult sessionResult = client.GetSession(NTable::TCreateSessionSettings()).GetValueSync();
    if (!sessionResult.IsSuccess()) {
        Out << sessionResult.GetIssues().ToString() << Endl;
        return EXIT_FAILURE;
    }

    NTable::TDescribeTableResult result = sessionResult.GetSession().DescribeTable(
        path,
        NTable::TDescribeTableSettings()
        .WithTableStatistics(options.ShowStats)
    ).GetValueSync();

    if (!result.IsSuccess()) {
        Out << result.GetIssues().ToString() << Endl;
        return EXIT_FAILURE;
    }

    auto desc = result.GetTableDescription();
    return PrintDescription(this, format, desc, options, &TDescribeLogic::PrintTableResponsePretty);
}

int TDescribeLogic::PrintTableResponsePretty(const NTable::TTableDescription& tableDescription, const TDescribeOptions& options) const {
    PrintColumns(tableDescription, Out);
    PrintIndexes(tableDescription, Out);
    PrintChangefeeds(tableDescription, Out);
    PrintStorageSettings(tableDescription, Out);
    PrintColumnFamilies(tableDescription, Out);
    PrintAttributes(tableDescription, Out);
    PrintTtlSettings(tableDescription, Out);
    PrintPartitioningSettings(tableDescription, Out);
    if (tableDescription.GetKeyBloomFilter().has_value()) {
        Out << Endl << "Bloom filter by key: "
            << (tableDescription.GetKeyBloomFilter().value() ? "true" : "false") << Endl;
    }
    PrintReadReplicasSettings(tableDescription, Out);
    PrintPermissionsIfNeeded(tableDescription, options);
    if (options.ShowStats) {
        PrintStatistics(tableDescription, Out);
    }
    if (options.ShowKeyShardBoundaries || options.ShowPartitionStats) {
        PrintPartitionInfo(tableDescription, options.ShowKeyShardBoundaries, options.ShowPartitionStats, Out);
    }

    return EXIT_SUCCESS;
}

void TDescribeLogic::WarnAboutTableOptions(const TString& path, const TDescribeOptions& options, EDataFormat format) {
    if (options.ShowKeyShardBoundaries || options.ShowStats || options.ShowPartitionStats || format != EDataFormat::Default) {
        TVector<TString> opts;
        if (options.ShowKeyShardBoundaries) {
            opts.emplace_back("\"partition-boundaries\"(\"shard-boundaries\")");
        }
        if (options.ShowStats) {
            opts.emplace_back("\"stats\"");
        }
        if (options.ShowPartitionStats) {
            opts.emplace_back("\"partition-stats\"");
        }
        if (format != EDataFormat::Default) {
            opts.emplace_back("\"json\"");
        }
        Cerr << "Note: \"" << path << "\" is not a table. Option";
        if (opts.size() > 1) {
            Cerr << 's';
        }
        for (auto& option : opts) {
            if (option != *opts.begin()) {
                Cerr << ',';
            }
            Cerr << ' ' << option;
        }
        Cerr << (opts.size() > 1 ? " are" : " is")
            << " used only for tables and thus "
            << (opts.size() > 1 ? "have" : "has")
            << " no effect for this command." << Endl;
    }
}

int TDescribeLogic::DescribeTopic(const TString& path, const TDescribeOptions& options, EDataFormat format) {
    NYdb::NTopic::TTopicClient topicClient(Driver);
    NYdb::NTopic::TDescribeTopicSettings settings;
    settings.IncludeStats(options.ShowStats || options.ShowPartitionStats);

    auto result = topicClient.DescribeTopic(path, settings).GetValueSync();
    if (!result.IsSuccess()) {
        Out << result.GetIssues().ToString() << Endl;
        return EXIT_FAILURE;
    }

    const auto& desc = result.GetTopicDescription();
    return PrintDescription(this, format, desc, options, &TDescribeLogic::PrintTopicResponsePretty);
}

int TDescribeLogic::PrintTopicResponsePretty(const NYdb::NTopic::TTopicDescription& description, const TDescribeOptions& options) const {
    PrintMain(description, Out);
    PrintAutopartitioning(description, Out);
    PrintTopicConsumers(description.GetConsumers(), Out);
    PrintPermissionsIfNeeded(description, options);
    if (options.ShowStats) {
        PrintStatistics(description, Out);
    }
    if (options.ShowPartitionStats){
        PrintPartitionStatistics(description, Out);
    }

    return EXIT_SUCCESS;
}

int TDescribeLogic::DescribeCoordinationNode(const TString& path, EDataFormat format) {
    NCoordination::TClient client(Driver);
    auto result = client.DescribeNode(path).GetValueSync();
    if (!result.IsSuccess()) {
        Out << result.GetIssues().ToString() << Endl;
        return EXIT_FAILURE;
    }

    const auto& desc = result.GetResult();
    if (format == EDataFormat::Pretty || format == EDataFormat::Default) {
        return PrintCoordinationNodeResponsePretty(desc);
    } else if (format == EDataFormat::Json) {
        Cerr << "Warning! Option --json is deprecated and will be removed soon. "
             << "Use \"--format proto-json-base64\" option instead." << Endl;
        return PrintProtoJsonBase64(NDraft::TProtoAccessor::GetProto(desc), Out);
    } else {
        return PrintProtoJsonBase64(NDraft::TProtoAccessor::GetProto(desc), Out);
    }
}

int TDescribeLogic::PrintCoordinationNodeResponsePretty(const NYdb::NCoordination::TNodeDescription& result) const {
    Out << Endl << "AttachConsistencyMode: " << result.GetAttachConsistencyMode() << Endl;
    Out << "ReadConsistencyMode: " << result.GetReadConsistencyMode() << Endl;
    if (result.GetSessionGracePeriod().has_value()) {
        Out << "SessionGracePeriod: " << result.GetSessionGracePeriod().value() << Endl;
    }
    if (result.GetSelfCheckPeriod().has_value()) {
        Out << "SelfCheckPeriod: " << result.GetSelfCheckPeriod().value() << Endl;
    }
    Out << "RatelimiterCountersMode: " << result.GetRateLimiterCountersMode() << Endl;
    return EXIT_SUCCESS;
}

int TDescribeLogic::DescribeReplication(const TString& path, const TDescribeOptions& options, EDataFormat format) {
    NReplication::TReplicationClient client(Driver);
    auto settings = NReplication::TDescribeReplicationSettings()
        .IncludeStats(options.ShowStats);

    auto result = client.DescribeReplication(path, settings).ExtractValueSync();
    if (!result.IsSuccess()) {
        Out << result.GetIssues().ToString() << Endl;
        return EXIT_FAILURE;
    }

    if (format == EDataFormat::Pretty || format == EDataFormat::Default) {
        return PrintReplicationResponsePretty(result, options);
    } else if (format == EDataFormat::Json) {
        Cerr << "Warning! Option --json is deprecated and will be removed soon. "
             << "Use \"--format proto-json-base64\" option instead." << Endl;
        return PrintProtoJsonBase64(NDraft::TProtoAccessor::GetProto(result), Out);
    } else {
        return PrintProtoJsonBase64(NDraft::TProtoAccessor::GetProto(result), Out);
    }
}

int TDescribeLogic::PrintReplicationResponsePretty(const NYdb::NReplication::TDescribeReplicationResult& result, const TDescribeOptions& options) const {
    const auto& desc = result.GetReplicationDescription();

    Out << Endl << "State: ";
    switch (desc.GetState()) {
    case NReplication::TReplicationDescription::EState::Running:
        if (const auto& stats = desc.GetRunningState().GetStats(); options.ShowStats) {
            if (const auto& progress = stats.GetInitialScanProgress(); progress && *progress < 100) {
                Out << "Initial scan (" << FloatToString(*progress, PREC_POINT_DIGITS, 2) << "%)";
            } else if (const auto& lag = stats.GetLag()) {
                Out << "Standby (lag: " << *lag << ")";
            } else {
                Out << desc.GetState();
            }
        } else {
            Out << desc.GetState();
        }
        break;
    case NReplication::TReplicationDescription::EState::Error:
        Out << "Error: " << desc.GetErrorState().GetIssues().ToOneLineString();
        break;
    default:
        break;
    }

    const auto& connParams = desc.GetConnectionParams();
    const auto& srcDatabase = connParams.GetDatabase();
    const auto& dstDatabase = options.Database;

    PrintConnectionParams(connParams, Out);

    Out << Endl << "Consistency level: " << desc.GetConsistencyLevel();
    switch (desc.GetConsistencyLevel()) {
    case NReplication::TReplicationDescription::EConsistencyLevel::Row:
        break;
    case NReplication::TReplicationDescription::EConsistencyLevel::Global:
        Out << Endl << "Commit interval: " << desc.GetGlobalConsistency().GetCommitInterval();
        break;
    }

    if (const auto& items = desc.GetItems(); !items.empty()) {
        TVector<TString> columnNames = { "#", "Source", "Destination", "Changefeed" };
        if (options.ShowStats) {
            columnNames.push_back("Lag");
            columnNames.push_back("Progress");
        }

        TPrettyTable table(columnNames, TPrettyTableConfig().WithoutRowDelimiters());
        for (const auto& item : items) {
            auto& row = table.AddRow()
                .Column(0, item.Id)
                .Column(1, SkipDatabasePrefix(TStringBuf(item.SrcPath), TStringBuf(srcDatabase)))
                .Column(2, SkipDatabasePrefix(TStringBuf(item.DstPath), TStringBuf(dstDatabase)))
                .Column(3, ValueOr(item.SrcChangefeedName, "n/a"));
            if (options.ShowStats) {
                row
                    .Column(4, ValueOr(item.Stats.GetLag(), "n/a"))
                    .Column(5, ProgressOr(item.Stats.GetInitialScanProgress(), "n/a"));
            }
        }
        Out << Endl << "Items:" << Endl << table;
    }

    Out << Endl;
    return EXIT_SUCCESS;
}

int TDescribeLogic::DescribeTransfer(const TString& path, EDataFormat format) {
    NReplication::TReplicationClient client(Driver);

    auto result = client.DescribeTransfer(path).ExtractValueSync();
    if (!result.IsSuccess()) {
        Out << result.GetIssues().ToString() << Endl;
        return EXIT_FAILURE;
    }

    if (format == EDataFormat::Pretty || format == EDataFormat::Default) {
        return PrintTransferResponsePretty(result);
    } else if (format == EDataFormat::Json) {
        Cerr << "Warning! Option --json is deprecated and will be removed soon. "
             << "Use \"--format proto-json-base64\" option instead." << Endl;
        return PrintProtoJsonBase64(NDraft::TProtoAccessor::GetProto(result), Out);
    } else {
        return PrintProtoJsonBase64(NDraft::TProtoAccessor::GetProto(result), Out);
    }
}

int TDescribeLogic::PrintTransferResponsePretty(const NYdb::NReplication::TDescribeTransferResult& result) const {
    const auto& desc = result.GetTransferDescription();

    Out << Endl << "State: ";
    switch (desc.GetState()) {
    case NReplication::TTransferDescription::EState::Running:
    case NReplication::TTransferDescription::EState::Paused:
        Out << desc.GetState();
        break;
    case NReplication::TTransferDescription::EState::Error:
        Out << "Error: " << desc.GetErrorState().GetIssues().ToOneLineString();
        break;
    default:
        break;
    }

    const auto& connParams = desc.GetConnectionParams();
    PrintConnectionParams(connParams, Out);

    Out << Endl << "Source path: " << desc.GetSrcPath();
    Out << Endl << "Destination path: " << desc.GetDstPath();
    Out << Endl << "Consumer: " << desc.GetConsumerName();
    Out << Endl << "Transformation lambda: " << desc.GetTransformationLambda();
    Out << Endl << "Batch size, bytes: " << desc.GetBatchingSettings().SizeBytes;
    Out << Endl << "Batch flush interval: " << desc.GetBatchingSettings().FlushInterval;

    Out << Endl;
    return EXIT_SUCCESS;
}

int TDescribeLogic::DescribeView(const TString& path) {
    TString query;
    auto status = NDump::DescribeViewQuery(Driver, path, query);
    if (status.IsSuccess()) {
        PrintViewQuery(query, Out);
        return EXIT_SUCCESS;
    }
    Out << status.GetIssues().ToString() << Endl;
    return EXIT_FAILURE;
}

int TDescribeLogic::DescribeExternalDataSource(const TString& path, EDataFormat format) {
    NTable::TTableClient client(Driver);
    const auto sessionResult = client.CreateSession().ExtractValueSync();
    if (!sessionResult.IsSuccess()) {
        Out << sessionResult.GetIssues().ToString() << Endl;
        return EXIT_FAILURE;
    }
    const auto description = sessionResult.GetSession().DescribeExternalDataSource(path).ExtractValueSync();
    if (!description.IsSuccess()) {
        Out << description.GetIssues().ToString() << Endl;
        return EXIT_FAILURE;
    }

    const auto& desc = description.GetExternalDataSourceDescription();
    if (format == EDataFormat::Pretty || format == EDataFormat::Default) {
        return PrintExternalDataSourceResponsePretty(desc);
    } else if (format == EDataFormat::Json) {
        Cerr << "Warning! Option --json is deprecated and will be removed soon. "
             << "Use \"--format proto-json-base64\" option instead." << Endl;
        return PrintProtoJsonBase64(NDraft::TProtoAccessor::GetProto(desc), Out);
    } else {
        return PrintProtoJsonBase64(NDraft::TProtoAccessor::GetProto(desc), Out);
    }
}

int TDescribeLogic::PrintExternalDataSourceResponsePretty(const NYdb::NTable::TExternalDataSourceDescription& /*result*/) const {
    // to do
    return EXIT_SUCCESS;
}

int TDescribeLogic::DescribeExternalTable(const TString& path, EDataFormat format) {
    NTable::TTableClient client(Driver);
    const auto sessionResult = client.CreateSession().ExtractValueSync();
    if (!sessionResult.IsSuccess()) {
        Out << sessionResult.GetIssues().ToString() << Endl;
        return EXIT_FAILURE;
    }
    const auto result = sessionResult.GetSession().DescribeExternalTable(path).ExtractValueSync();
    if (!result.IsSuccess()) {
        Out << result.GetIssues().ToString() << Endl;
        return EXIT_FAILURE;
    }

    const auto& desc = result.GetExternalTableDescription();
    if (format == EDataFormat::Pretty || format == EDataFormat::Default) {
        return PrintExternalTableResponsePretty(desc);
    } else if (format == EDataFormat::Json) {
        Cerr << "Warning! Option --json is deprecated and will be removed soon. "
             << "Use \"--format proto-json-base64\" option instead." << Endl;
        return PrintProtoJsonBase64(NDraft::TProtoAccessor::GetProto(desc), Out);
    } else {
        return PrintProtoJsonBase64(NDraft::TProtoAccessor::GetProto(desc), Out);
    }
}

int TDescribeLogic::PrintExternalTableResponsePretty(const NYdb::NTable::TExternalTableDescription& /*result*/) const {
    // to do
    return EXIT_SUCCESS;
}

int TDescribeLogic::DescribeSystemView(const TString& path, EDataFormat format) {
    NTable::TTableClient client(Driver);
    const auto sessionResult = client.CreateSession().ExtractValueSync();
    if (!sessionResult.IsSuccess()) {
        Out << sessionResult.GetIssues().ToString() << Endl;
        return EXIT_FAILURE;
    }
    const auto result = sessionResult.GetSession().DescribeSystemView(path).ExtractValueSync();
    if (!result.IsSuccess()) {
        Out << result.GetIssues().ToString() << Endl;
        return EXIT_FAILURE;
    }

    const auto desc = result.GetSystemViewDescription();
    if (format == EDataFormat::Pretty || format == EDataFormat::Default) {
        return PrintSystemViewResponsePretty(desc);
    } else if (format == EDataFormat::Json) {
        Cerr << "Warning! Option --json is deprecated and will be removed soon. "
             << "Use \"--format proto-json-base64\" option instead." << Endl;
        return PrintProtoJsonBase64(NDraft::TProtoAccessor::GetProto(desc), Out);
    } else {
        return PrintProtoJsonBase64(NDraft::TProtoAccessor::GetProto(desc), Out);
    }
}

int TDescribeLogic::PrintSystemViewResponsePretty(const NYdb::NTable::TSystemViewDescription& result) const {
    Out << "Id: "  << result.GetSysViewId() << " (" << result.GetSysViewName() <<  ")" << Endl;
    PrintColumns(result, Out);
    PrintAttributes(result, Out);

    return EXIT_SUCCESS;
}

std::pair<TString, TString> TDescribeLogic::ParseTopicConsumer(const TString& path) const {
    const size_t slashPos = path.find_last_of('/');
    std::pair<TString, TString> result;
    if (slashPos != TString::npos && slashPos != path.size() - 1) {
        result.first = path.substr(0, slashPos);
        result.second = path.substr(slashPos + 1);
    }
    return result;
}

int TDescribeLogic::TryTopicConsumerDescribeOrFail(const TString& path, const NScheme::TDescribePathResult& result, const TDescribeOptions& options, EDataFormat format) {
    auto [topic, consumer] = ParseTopicConsumer(path);
    if (!topic || !consumer) {
        Out << result.GetIssues().ToString() << Endl;
        return EXIT_FAILURE;
    }

    NScheme::TSchemeClient client(Driver);
    NScheme::TDescribePathResult topicDescribeResult = client.DescribePath(
        topic,
        NScheme::TDescribePathSettings()
    ).GetValueSync();
    if (!topicDescribeResult.IsSuccess() || (topicDescribeResult.GetEntry().Type != NScheme::ESchemeEntryType::Topic && topicDescribeResult.GetEntry().Type != NScheme::ESchemeEntryType::PqGroup)) {
        Out << result.GetIssues().ToString() << Endl;
        return EXIT_FAILURE;
    }

    // OK, this is topic, check the consumer
    NYdb::NTopic::TTopicClient topicClient(Driver);
    auto consumerDescription = topicClient.DescribeConsumer(topic, consumer, NYdb::NTopic::TDescribeConsumerSettings().IncludeStats(options.ShowPartitionStats)).GetValueSync();
    if (!consumerDescription.IsSuccess()) {
        Out << consumerDescription.GetIssues().ToString() << Endl;
        return EXIT_FAILURE;
    }

    return PrintDescription(this, format, consumerDescription.GetConsumerDescription(), options, &TDescribeLogic::PrintConsumerResponsePretty);
}

int TDescribeLogic::PrintConsumerResponsePretty(const NYdb::NTopic::TConsumerDescription& description, const TDescribeOptions& options) const {
    return PrintPrettyDescribeConsumerResult(description, options.ShowPartitionStats, Out);
}

template<typename TDescriptionType>
void TDescribeLogic::PrintPermissionsIfNeeded(const TDescriptionType& description, const TDescribeOptions& options) const {
    if (options.ShowPermissions) {
        Out << Endl;
        PrintAllPermissions(
            description.GetOwner(),
            description.GetPermissions(),
            description.GetEffectivePermissions(),
            Out
        );
    }
}

} // namespace NYdb::NConsoleClient

