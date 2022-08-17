#include "ydb_service_scheme.h"

#include <ydb/public/lib/json_value/ydb_json_value.h>
#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/public/lib/ydb_cli/common/tabbed_table.h>
#include <ydb/public/lib/ydb_cli/common/print_utils.h>
#include <ydb/public/sdk/cpp/client/ydb_persqueue_public/persqueue.h>

namespace NYdb {
namespace NConsoleClient {

TCommandScheme::TCommandScheme()
    : TClientCommandTree("scheme", {}, "Scheme service operations")
{
    AddCommand(std::make_unique<TCommandMakeDirectory>());
    AddCommand(std::make_unique<TCommandRemoveDirectory>());
    AddCommand(std::make_unique<TCommandDescribe>());
    AddCommand(std::make_unique<TCommandList>());
    AddCommand(std::make_unique<TCommandPermissions>());
}

TCommandMakeDirectory::TCommandMakeDirectory()
    : TYdbOperationCommand("mkdir", std::initializer_list<TString>(), "Make directory")
{}

void TCommandMakeDirectory::Config(TConfig& config) {
    TYdbOperationCommand::Config(config);

    config.SetFreeArgsNum(1);
    SetFreeArgTitle(0, "<path>", "Path to create");
}

void TCommandMakeDirectory::Parse(TConfig& config) {
    TClientCommand::Parse(config);
    ParsePath(config, 0);
}

int TCommandMakeDirectory::Run(TConfig& config) {
    NScheme::TSchemeClient client(CreateDriver(config));
    ThrowOnError(
        client.MakeDirectory(
            Path,
            FillSettings(NScheme::TMakeDirectorySettings())
        ).GetValueSync()
    );
    return EXIT_SUCCESS;
}

TCommandRemoveDirectory::TCommandRemoveDirectory()
    : TYdbOperationCommand("rmdir", std::initializer_list<TString>(), "Remove directory")
{}

void TCommandRemoveDirectory::Config(TConfig& config) {
    TYdbOperationCommand::Config(config);

    config.SetFreeArgsNum(1);
    SetFreeArgTitle(0, "<path>", "Path to remove");
}

void TCommandRemoveDirectory::Parse(TConfig& config) {
    TClientCommand::Parse(config);
    ParsePath(config, 0);
}

int TCommandRemoveDirectory::Run(TConfig& config) {
    NScheme::TSchemeClient client(CreateDriver(config));
    ThrowOnError(
        client.RemoveDirectory(
            Path,
            FillSettings(NScheme::TRemoveDirectorySettings())
        ).GetValueSync()
    );
    return EXIT_SUCCESS;
}

namespace {
    TString EntryTypeToString(NScheme::ESchemeEntryType entry) {
        switch (entry) {
        case  NScheme::ESchemeEntryType::Directory:
            return "dir";
        case  NScheme::ESchemeEntryType::Table:
            return "table";
        case  NScheme::ESchemeEntryType::ColumnTable:
            return "column-table";
        case  NScheme::ESchemeEntryType::PqGroup:
            return "pq-group";
        case  NScheme::ESchemeEntryType::Topic:
            return "topic";
        case  NScheme::ESchemeEntryType::SubDomain:
            return "sub-domain";
        case  NScheme::ESchemeEntryType::RtmrVolume:
            return "rtmr-volume";
        case  NScheme::ESchemeEntryType::BlockStoreVolume:
            return "block-store-volume";
        case  NScheme::ESchemeEntryType::CoordinationNode:
            return "coordination-node";
        case  NScheme::ESchemeEntryType::Unknown:
        case  NScheme::ESchemeEntryType::Sequence:
        case  NScheme::ESchemeEntryType::Replication:
            return "unknown";
        }
    }

    void PrintPermissions(const TVector<NScheme::TPermissions>& permissions) {
        if (permissions.size()) {
            for (const NScheme::TPermissions& permission : permissions) {
                Cout << permission.Subject << ":";
                for (const TString& name : permission.PermissionNames) {
                    if (name != *permission.PermissionNames.begin()) {
                        Cout << ",";
                    }
                    Cout << name;
                }
                Cout << Endl;
            }
        } else {
            Cout << "none" << Endl;
        }
    }

    void PrintAllPermissions(
        const TString& owner,
        const TVector<NScheme::TPermissions>& permissions,
        const TVector<NScheme::TPermissions>& effectivePermissions
    ) {
        Cout << "Owner: " << owner << Endl << Endl << "Permissions: " << Endl;
        PrintPermissions(permissions);
        Cout << Endl << "Effective permissions: " << Endl;
        PrintPermissions(effectivePermissions);
    }

    void PrintEntryVerbose(const NScheme::TSchemeEntry& entry, bool permissions) {
        Cout << "<" << EntryTypeToString(entry.Type) << "> " << entry.Name << Endl;
        if (permissions) {
            Cout << Endl;
            PrintAllPermissions(entry.Owner, entry.Permissions, entry.EffectivePermissions);
        }
    }
}

TCommandDescribe::TCommandDescribe()
    : TYdbOperationCommand("describe", std::initializer_list<TString>(), "Show information about object at given object")
{}

void TCommandDescribe::Config(TConfig& config) {
    TYdbOperationCommand::Config(config);
    // Common options
    config.Opts->AddLongOption("permissions", "Show owner and permissions").StoreTrue(&ShowPermissions);

    // Table options
    config.Opts->AddLongOption("partition-boundaries", "[Table] Show partition key boundaries").StoreTrue(&ShowKeyShardBoundaries)
        .AddLongName("shard-boundaries");
    config.Opts->AddLongOption("stats", "[Table] Show table statistics").StoreTrue(&ShowTableStats);
    config.Opts->AddLongOption("partition-stats", "[Table] Show partition statistics").StoreTrue(&ShowPartitionStats);

    AddJsonOption(config, "(Deprecated, will be removed soon. Use --format option instead) [Table] Output in json format");
    AddFormats(config, { EOutputFormat::Pretty, EOutputFormat::ProtoJsonBase64 });
    config.Opts->MutuallyExclusive("json", "format");

    config.SetFreeArgsNum(1);
    SetFreeArgTitle(0, "<path>", "Path to an object to describe");
}

void TCommandDescribe::Parse(TConfig& config) {
    TClientCommand::Parse(config);
    ParseFormats();
    ParsePath(config, 0);
}

int TCommandDescribe::Run(TConfig& config) {
    TDriver driver = CreateDriver(config);
    NScheme::TSchemeClient client(driver);
    NScheme::TDescribePathResult result = client.DescribePath(
        Path,
        FillSettings(NScheme::TDescribePathSettings())
    ).GetValueSync();
    ThrowOnError(result);
    return PrintPathResponse(driver, result);
}

int TCommandDescribe::PrintPathResponse(TDriver& driver, const NScheme::TDescribePathResult& result) {
    NScheme::TSchemeEntry entry = result.GetEntry();
    switch (entry.Type) {
    case NScheme::ESchemeEntryType::Table:
        return DescribeTable(driver);
    case NScheme::ESchemeEntryType::PqGroup:
    case NScheme::ESchemeEntryType::Topic:
        return DescribeStream(driver);
    default:
        WarnAboutTableOptions();
        PrintEntryVerbose(entry, ShowPermissions);
    }
    return EXIT_SUCCESS;
}

namespace {
    TString FormatCodecs(const TVector<NYdb::NPersQueue::ECodec> codecs) {
        if (codecs.empty()) {
            return "";
        }

        TStringBuilder builder = TStringBuilder();
        for (unsigned int i = 0; i < codecs.size() - 1; ++i) {
            builder << codecs[i] << ", ";
        }
        builder << codecs[codecs.size() - 1];
        return ToString(builder);
    }

    void PrintStreamReadRules(
            const TVector<NYdb::NPersQueue::TDescribeTopicResult::
                                  TTopicSettings::TReadRule>& readRules) {
        if (readRules.empty()) {
            return;
        }
        TPrettyTable table(
                {"ConsumerName", "SupportedCodecs",
                 "StartingMessageTimestamp", "Important",
                 "ServiceType", "SupportedFormat", "Version"});
        for (const auto& rule: readRules) {
            table.AddRow()
                .Column(0, rule.ConsumerName())
                .Column(1, FormatCodecs(rule.SupportedCodecs()))
                .Column(2, rule.StartingMessageTimestamp().ToRfc822StringLocal())
                .Column(3, rule.Important())
                .Column(4, rule.ServiceType())
                .Column(5, rule.SupportedFormat())
                .Column(6, rule.Version());
        }
        Cout << Endl << "ReadRules: " << Endl;
        Cout << table;
    }
}

int TCommandDescribe::PrintStreamResponsePretty(const NYdb::NPersQueue::TDescribeTopicResult::TTopicSettings &settings) {
    Cout << Endl << "RetentionPeriod: " << settings.RetentionPeriod().Hours() << " hours";
    Cout << Endl << "PartitionsCount: " << settings.PartitionsCount();
    Cout << Endl << "SupportedFormat: " << settings.SupportedFormat();
    if (!settings.SupportedCodecs().empty()) {
        Cout << Endl << "SupportedCodecs: " << FormatCodecs(settings.SupportedCodecs()) << Endl;
    }
    PrintStreamReadRules(settings.ReadRules());
    return EXIT_SUCCESS;
}

int TCommandDescribe::PrintStreamResponseProtoJsonBase64(
        const NYdb::NPersQueue::
                TDescribeTopicResult& result) {
    TString json;
    google::protobuf::util::JsonPrintOptions jsonOpts;
    jsonOpts.preserve_proto_field_names = true;
    auto convertStatus = google::protobuf::util::MessageToJsonString(
            TProtoAccessor::GetProto(result),
            &json,
            jsonOpts
    );
    if (convertStatus.ok()) {
        Cout << json << Endl;
    } else {
        Cerr << "Error occurred while converting result proto to json" << Endl;
        return EXIT_FAILURE;
    }
    return EXIT_SUCCESS;
}

int TCommandDescribe::PrintStreamResponse(const NYdb::NPersQueue::TDescribeTopicResult& result) {
    switch (OutputFormat) {
        case EOutputFormat::Default:
        case EOutputFormat::Pretty:
            PrintStreamResponsePretty(result.TopicSettings());
            break;
        case EOutputFormat::Json:
            Cerr << "Warning! Option --json is deprecated and will be removed soon. "
                 << "Use \"--format proto-json-base64\" option instead." << Endl;
        case EOutputFormat::ProtoJsonBase64:
            return PrintStreamResponseProtoJsonBase64(result);
        default:
            throw TMissUseException() << "This command doesn't support " << OutputFormat << " output format";
    }
    return EXIT_SUCCESS;
}

int TCommandDescribe::DescribeStream(TDriver& driver) {
    NYdb::NPersQueue::TPersQueueClient persQueueClient(driver);
    auto describeResult = persQueueClient.DescribeTopic(Path).GetValueSync();
    ThrowOnError(describeResult);
    return PrintStreamResponse(describeResult);
}

int TCommandDescribe::DescribeTable(TDriver& driver) {
    NTable::TTableClient client(driver);
    NTable::TCreateSessionResult sessionResult = client.GetSession(NTable::TCreateSessionSettings()).GetValueSync();
    ThrowOnError(sessionResult);
    NTable::TDescribeTableResult result = sessionResult.GetSession().DescribeTable(
        Path,
        FillSettings(
            NTable::TDescribeTableSettings()
            .WithKeyShardBoundary(ShowKeyShardBoundaries)
            .WithTableStatistics(ShowTableStats || ShowPartitionStats)
            .WithPartitionStatistics(ShowPartitionStats)
        )
    ).GetValueSync();
    ThrowOnError(result);
    return PrintTableResponse(result);
}

namespace {
    void PrintColumns(const NTable::TTableDescription& tableDescription) {
        if (!tableDescription.GetTableColumns().size()) {
            return;
        }
        TPrettyTable table({ "Name", "Type", "Family", "Key" }, TPrettyTableConfig().WithoutRowDelimiters());

        const TVector<TString>& keyColumns = tableDescription.GetPrimaryKeyColumns();
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

        Cout << table;
    }

    // Temporary hack until KIKIMR-8635 goes to prod
    TInstant CorrectTime(const TInstant& time) {
        ui64 timeUs = time.MicroSeconds();
        if (timeUs < 2000000000000) {
            timeUs *= 1000;
        }
        return TInstant::MicroSeconds(timeUs);
    }

    void PrintIndexes(const NTable::TTableDescription& tableDescription) {
        const TVector<NTable::TIndexDescription>& indexes = tableDescription.GetIndexDescriptions();
        if (!indexes.size()) {
            return;
        }
        Cout << Endl << "Indexes: " << Endl;
        for (const auto& index : indexes) {
            Cout << index.GetIndexName() << " [" << index.GetIndexType() << "] Index columns: (";
            const auto& columns = index.GetIndexColumns();
            for (auto colIt = columns.begin(); colIt != columns.end();) {
                Cout << (*colIt);
                if (++colIt != columns.end()) {
                    Cout << ",";
                }
            }

            const auto& cover = index.GetDataColumns();
            if (!cover) {
                Cout << ")" << Endl;
            } else {
                Cout << ") Cover columns: (";
                for (auto colIt = cover.begin(); colIt != cover.end();) {
                    Cout << (*colIt);
                    if (++colIt != cover.end()) {
                        Cout << ",";
                    }
                }
                Cout << ")" << Endl;
            }
        }
    }

    void PrintStorageSettings(const NTable::TTableDescription& tableDescription) {
        const NTable::TStorageSettings& settings = tableDescription.GetStorageSettings();
        const auto commitLog0 = settings.GetTabletCommitLog0();
        const auto commitLog1 = settings.GetTabletCommitLog1();
        const auto external = settings.GetExternal();
        const auto storeExternalBlobs = settings.GetStoreExternalBlobs();
        if (!commitLog0 && !commitLog1 && !external && !storeExternalBlobs.Defined()) {
            return;
        }
        Cout << Endl << "Storage settings: " << Endl;
        if (commitLog0) {
            Cout << "Internal channel 0 commit log storage pool: " << commitLog0.GetRef() << Endl;
        }
        if (commitLog1) {
            Cout << "Internal channel 1 commit log storage pool: " << commitLog1.GetRef() << Endl;
        }
        if (external) {
            Cout << "External blobs storage pool: " << external.GetRef() << Endl;
        }
        if (storeExternalBlobs) {
            Cout << "Store large values in \"external blobs\": "
                << (storeExternalBlobs.GetRef() ? "true" : "false") << Endl;
        }
    }

    void PrintColumnFamilies(const NTable::TTableDescription& tableDescription) {
        if (!tableDescription.GetColumnFamilies()) {
            return;
        }
        TPrettyTable table({ "Name", "Data", "Compression", "Keep in memory" },
            TPrettyTableConfig().WithoutRowDelimiters());

        for (const NTable::TColumnFamilyDescription& family : tableDescription.GetColumnFamilies()) {
            TMaybe<TString> data = family.GetData();
            TString compression;
            if (family.GetCompression()) {
                switch (family.GetCompression().GetRef()) {
                case NTable::EColumnFamilyCompression::None:
                    compression = "None";
                    break;
                case NTable::EColumnFamilyCompression::LZ4:
                    compression = "LZ4";
                    break;
                default:
                    compression = TStringBuilder() << "unknown("
                        << static_cast<size_t>(family.GetCompression().GetRef()) << ")";
                }
            }
            TStringBuilder keepInMemory;
            if (family.GetKeepInMemory().Defined()) {
                keepInMemory << keepInMemory << family.GetKeepInMemory().GetRef();
            }
            table.AddRow()
                .Column(0, family.GetName())
                .Column(1, data ? data.GetRef() : "")
                .Column(2, compression)
                .Column(3, keepInMemory);
        }
        Cout << Endl << "Column families: " << Endl;
        Cout << table;
    }

    void PrintAttributes(const NTable::TTableDescription& tableDescription) {
        if (!tableDescription.GetAttributes().size()) {
            return;
        }
        TPrettyTable table({ "Name", "Value" }, TPrettyTableConfig().WithoutRowDelimiters());

        for (const auto& [name, value] : tableDescription.GetAttributes()) {
            table.AddRow()
                .Column(0, name)
                .Column(1, value);
        }
        Cout << Endl << "Attributes: " << Endl;
        Cout << table;
    }

    void PrintTtlSettings(const NTable::TTableDescription& tableDescription) {
        const auto& settings = tableDescription.GetTtlSettings();
        if (!settings) {
            return;
        }
        Cout << Endl << "Ttl settings ";
        switch (settings->GetMode()) {
        case NTable::TTtlSettings::EMode::DateTypeColumn:
        {
            Cout << "(date type column):" << Endl;
            const auto& dateTypeColumn = settings->GetDateTypeColumn();
            Cout << "Column name: " << dateTypeColumn.GetColumnName() << Endl;
            Cout << "Expire after: " << dateTypeColumn.GetExpireAfter() << Endl;
            break;
        }
        case NTable::TTtlSettings::EMode::ValueSinceUnixEpoch:
        {
            Cout << "(value since unix epoch):" << Endl;
            const auto& valueSinceEpoch = settings->GetValueSinceUnixEpoch();
            Cout << "Column name: " << valueSinceEpoch.GetColumnName() << Endl;
            Cout << "Column unit: " << valueSinceEpoch.GetColumnUnit() << Endl;
            Cout << "Expire after: " << valueSinceEpoch.GetExpireAfter() << Endl;
            break;
        }
        default:
            NColorizer::TColors colors = NColorizer::AutoColors(Cout);
            Cout << "(unknown):" << Endl
                << colors.RedColor() << "Unknown ttl settings mode. Please update your version of YDB cli"
                << colors.OldColor() << Endl;
        }
    }

    void PrintPartitioningSettings(const NTable::TTableDescription& tableDescription) {
        const auto& settings = tableDescription.GetPartitioningSettings();
        const auto partBySize = settings.GetPartitioningBySize();
        const auto partByLoad = settings.GetPartitioningByLoad();
        if (!partBySize.Defined() && !partByLoad.Defined()) {
            return;
        }
        const auto partitionSizeMb = settings.GetPartitionSizeMb();
        const auto minPartitions = settings.GetMinPartitionsCount();
        const auto maxPartitions = settings.GetMaxPartitionsCount();
        Cout << Endl << "Auto partitioning settings: " << Endl;
        Cout << "Partitioning by size: " << (partBySize.GetRef() ? "true" : "false") << Endl;
        Cout << "Partitioning by load: " << (partByLoad.GetRef() ? "true" : "false") << Endl;
        if (partBySize.Defined() && partitionSizeMb) {
            Cout << "Preferred partition size (Mb): " << partitionSizeMb << Endl;
        }
        if (minPartitions) {
            Cout << "Min partitions count: " << minPartitions << Endl;
        }
        if (maxPartitions) {
            Cout << "Max partitions count: " << maxPartitions << Endl;
        }
    }

    void PrintReadReplicasSettings(const NTable::TTableDescription& tableDescription) {
        const auto& settings = tableDescription.GetReadReplicasSettings();
        if (!settings) {
            return;
        }
        Cout << Endl << "Read replicas settings: " << Endl;
        switch (settings->GetMode()) {
        case NTable::TReadReplicasSettings::EMode::PerAz:
            Cout << "Read replicas count in each AZ: " << settings->GetReadReplicasCount() << Endl;
            break;
        case NTable::TReadReplicasSettings::EMode::AnyAz:
            Cout << "Read replicas total count in all AZs: " << settings->GetReadReplicasCount() << Endl;
            break;
        default:
            NColorizer::TColors colors = NColorizer::AutoColors(Cout);
            Cout << colors.RedColor() << "Unknown read replicas settings mode. Please update your version of YDB cli"
                << colors.OldColor() << Endl;
        }
    }

    void PrintStatistics(const NTable::TTableDescription& tableDescription) {
        Cout << Endl << "Table stats:" << Endl;
        Cout << "Partitions count: " << tableDescription.GetPartitionsCount() << Endl;
        Cout << "Approximate number of rows: " << tableDescription.GetTableRows() << Endl;
        Cout << "Approximate size of table: " << PrettySize(tableDescription.GetTableSize()) << Endl;
        Cout << "Last modified: " << FormatTime(tableDescription.GetModificationTime()) << Endl;
        Cout << "Created: " << FormatTime(CorrectTime(tableDescription.GetCreationTime())) << Endl;
    }

    void PrintPartitionInfo(const NTable::TTableDescription& tableDescription, bool showBoundaries, bool showStats) {
        const TVector<NTable::TKeyRange>& ranges = tableDescription.GetKeyRanges();
        const TVector<NTable::TPartitionStats>& stats = tableDescription.GetPartitionStats();
        if (showBoundaries) {
            if (showStats) {
                Cout << Endl << "Partitions info:" << Endl;
                if (ranges.empty() && stats.empty()) {
                    Cout << "No data." << Endl;
                    return;
                }
            } else {
                Cout << Endl << "Partitions key boundaries:" << Endl;
                if (ranges.empty()) {
                    Cout << "No data." << Endl;
                    return;
                }
            }
        } else {
            Cout << Endl << "Partitions stats:" << Endl;
            if (stats.empty()) {
                Cout << "No data." << Endl;
                return;
            }
        }
        size_t rowsCount;
        if (showBoundaries && showStats && ranges.size() != stats.size()) {
            Cerr << "(!) Warning: partitions key boundaries size (" << ranges.size()
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
                const TMaybe<NTable::TKeyBound>& from = keyRange.From();
                const TMaybe<NTable::TKeyBound>& to = keyRange.To();
                if (from.Defined()) {
                    const NTable::TKeyBound& bound = from.GetRef();
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
                if (to.Defined()) {
                    const NTable::TKeyBound& bound = to.GetRef();
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
        Cout << table;
    }
}

int TCommandDescribe::PrintTableResponse(NTable::TDescribeTableResult& result) {
    NTable::TTableDescription tableDescription = result.GetTableDescription();
    switch (OutputFormat) {
    case EOutputFormat::Default:
    case EOutputFormat::Pretty:
        PrintResponsePretty(tableDescription);
        break;
    case EOutputFormat::Json:
        Cerr << "Warning! Option --json is deprecated and will be removed soon. "
            << "Use \"--format proto-json-base64\" option instead." << Endl;
    case EOutputFormat::ProtoJsonBase64:
        return PrintResponseProtoJsonBase64(tableDescription);
    default:
        throw TMissUseException() << "This command doesn't support " << OutputFormat << " output format";
    }
    return EXIT_SUCCESS;
}

void TCommandDescribe::PrintResponsePretty(const NTable::TTableDescription& tableDescription) {
    PrintColumns(tableDescription);
    PrintIndexes(tableDescription);
    PrintStorageSettings(tableDescription);
    PrintColumnFamilies(tableDescription);
    PrintAttributes(tableDescription);
    PrintTtlSettings(tableDescription);
    PrintPartitioningSettings(tableDescription);
    if (tableDescription.GetKeyBloomFilter().Defined()) {
        Cout << Endl << "Bloom filter by key: "
            << (tableDescription.GetKeyBloomFilter().GetRef() ? "true" : "false") << Endl;
    }
    PrintReadReplicasSettings(tableDescription);
    if (ShowPermissions) {
        if (tableDescription.GetColumns().size()) {
            Cout << Endl;
        }
        PrintAllPermissions(
            tableDescription.GetOwner(),
            tableDescription.GetPermissions(),
            tableDescription.GetEffectivePermissions()
        );
    }
    if (ShowTableStats) {
        PrintStatistics(tableDescription);
    }
    if (ShowKeyShardBoundaries || ShowPartitionStats) {
        PrintPartitionInfo(tableDescription, ShowKeyShardBoundaries, ShowPartitionStats);
    }
}

int TCommandDescribe::PrintResponseProtoJsonBase64(const NTable::TTableDescription& tableDescription) {
    TString json;
    google::protobuf::util::JsonPrintOptions jsonOpts;
    jsonOpts.preserve_proto_field_names = true;
    auto convertStatus = google::protobuf::util::MessageToJsonString(
        NYdb::TProtoAccessor::GetProto(tableDescription),
        &json,
        jsonOpts
    );
    if (convertStatus.ok()) {
        Cout << json << Endl;
    } else {
        Cerr << "Error occurred while converting result proto to json" << Endl;
        return EXIT_FAILURE;
    }
    return EXIT_SUCCESS;
}

void TCommandDescribe::WarnAboutTableOptions() {
    if (ShowKeyShardBoundaries || ShowTableStats || ShowPartitionStats || OutputFormat != EOutputFormat::Default) {
        TVector<TString> options;
        if (ShowKeyShardBoundaries) {
            options.emplace_back("\"partition-boundaries\"(\"shard-boundaries\")");
        }
        if (ShowTableStats) {
            options.emplace_back("\"stats\"");
        }
        if (ShowPartitionStats) {
            options.emplace_back("\"partition-stats\"");
        }
        if (OutputFormat != EOutputFormat::Default) {
            options.emplace_back("\"json\"");
        }
        Cerr << "Note: \"" << Path << "\" is not a table. Option";
        if (options.size() > 1) {
            Cerr << 's';
        }
        for (auto& option : options) {
            if (option != *options.begin()) {
                Cerr << ',';
            }
            Cerr << ' ' << option;
        }
        Cerr << (options.size() > 1 ? " are" : " is")
            << " used only for tables and thus "
            << (options.size() > 1 ? "have" : "has")
            << " no effect for this command." << Endl;
    }
}

TCommandList::TCommandList()
    : TYdbOperationCommand("ls", std::initializer_list<TString>(), "Show information about objects inside given directory")
{}

void TCommandList::Config(TConfig& config) {
    TYdbOperationCommand::Config(config);

    config.Opts->AddCharOption('l', "List objects with detailed information")
        .StoreTrue(&AdvancedMode);
    config.Opts->AddCharOption('R', "List subdirectories recursively")
        .StoreTrue(&Recursive);

    config.SetFreeArgsMax(1);
    SetFreeArgTitle(0, "<path>", "Path to list");
}

void TCommandList::Parse(TConfig& config) {
    TClientCommand::Parse(config);
    ParsePath(config, 0, true);
}

int TCommandList::Run(TConfig& config) {
    TDriver driver = CreateDriver(config);
    NScheme::TSchemeClient client(driver);
    NScheme::TListDirectoryResult result = client.ListDirectory(
        Path,
        FillSettings(NScheme::TListDirectorySettings())
    ).GetValueSync();
    ThrowOnError(result);
    if (AdvancedMode) {
        PrintResponseAdvanced(result, driver);
    } else {
        PrintResponse(result, Path, client);
    }
    Y_UNUSED(Recursive);
    return EXIT_SUCCESS;
}

void TCommandList::PrintResponse(
    NScheme::TListDirectoryResult& result,
    const TString& path,
    NScheme::TSchemeClient& client
) {
    TVector<NScheme::TSchemeEntry> children = result.GetChildren();
    if (children.size()) {
        if (Recursive) {
            Cout << path << ":" << Endl;
        }
        TAdaptiveTabbedTable table(children);
        Cout << table;
        if (Recursive) {
            for (const auto& child : children) {
                TString childPath = path + '/' + child.Name;
                if (child.Type == NScheme::ESchemeEntryType::Directory) {
                    NScheme::TListDirectoryResult child_result = client.ListDirectory(
                        childPath,
                        FillSettings(NScheme::TListDirectorySettings())
                    ).GetValueSync();
                    ThrowOnError(child_result);
                    Cout << Endl;
                    PrintResponse(child_result, childPath, client);
                }
            }
        }
    } else {
        NScheme::TSchemeEntry entry = result.GetEntry();
        if (Recursive) {
            Cout << path << ":" << Endl;
        }
        if (entry.Type != NScheme::ESchemeEntryType::Directory) {
            NColorizer::TColors colors = NColorizer::AutoColors(Cout);
            PrintSchemeEntry(Cout, entry, colors);
            Cout << Endl;
        }
    }
}

void TCommandList::PrintResponseAdvanced(NScheme::TListDirectoryResult& result, TDriver& driver) {
    TVector<NScheme::TSchemeEntry> entries = result.GetChildren();
    bool oneExactEntry = false;
    NTable::TTableClient tableClient(driver);
    NScheme::TSchemeClient schemeClient(driver);
    if (!entries.size()) {
        if (result.GetEntry().Type != NScheme::ESchemeEntryType::Directory) {
            entries.push_back(result.GetEntry());
            oneExactEntry = true;
        } else {
            return;
        }
    }
    TPrettyTable table(
        { "Type", "Owner", "Size", "Created", "Modified", "Name" },
        TPrettyTableConfig().WithoutRowDelimiters()
    );

    AddEntriesRecursive(TString(), entries, 0, table, oneExactEntry, tableClient, schemeClient);

    Cout << table;
}

void TCommandList::AddEntriesRecursive(
    const TString& path,
    const TVector<NScheme::TSchemeEntry> entries,
    size_t depth,
    TPrettyTable& table,
    bool oneExactEntry,
    NTable::TTableClient& tableClient,
    NScheme::TSchemeClient& schemeClient
) {
    for (const auto& entry : entries) {
        TString type = EntryTypeToString(entry.Type);
        TString childRalativePath = path;
        if (!oneExactEntry) {
            childRalativePath += (childRalativePath ? "/" : "") + entry.Name;
        }
        TString childFullPath = Path + (childRalativePath ? "/" + childRalativePath : "");
        if (oneExactEntry) {
            childRalativePath = entry.Name;
        }
        switch (entry.Type) {
        case NScheme::ESchemeEntryType::Table:
        {
            NTable::TCreateSessionResult sessionResult = tableClient.GetSession(
                NTable::TCreateSessionSettings()
            ).GetValueSync();
            ThrowOnError(sessionResult);

            NTable::TDescribeTableResult tableResult = sessionResult.GetSession().DescribeTable(
                childFullPath,
                FillSettings(
                    NTable::TDescribeTableSettings().WithTableStatistics(true)
                )
            ).GetValueSync();
            ThrowOnError(tableResult);
            NTable::TTableDescription tableDescription = tableResult.GetTableDescription();

            table.AddRow()
                .Column(0, EntryTypeToString(entry.Type))
                .Column(1, entry.Owner)
                .Column(2, PrettySize(tableDescription.GetTableSize()))
                .Column(3, FormatTime(CorrectTime(tableDescription.GetCreationTime())))
                .Column(4, FormatTime(tableDescription.GetModificationTime()))
                .Column(5, childRalativePath);
            break;
        }
        default:
        {
            table.AddRow()
                .Column(0, EntryTypeToString(entry.Type))
                .Column(1, entry.Owner)
                .Column(2, "")
                .Column(3, "")
                .Column(4, "")
                .Column(5, childRalativePath);

            if (Recursive && entry.Type == NScheme::ESchemeEntryType::Directory) {
                NScheme::TListDirectoryResult child_result = schemeClient.ListDirectory(
                    childFullPath,
                    FillSettings(NScheme::TListDirectorySettings())
                ).GetValueSync();
                ThrowOnError(child_result);
                AddEntriesRecursive(childRalativePath, child_result.GetChildren(), depth + 1, table, false,
                    tableClient, schemeClient);
            }
            break;
        } // default block
        } // switch
    } // for
}

TCommandPermissions::TCommandPermissions()
    : TClientCommandTree("permissions", {}, "Modify permissions")
{
    AddCommand(std::make_unique<TCommandPermissionGrant>());
    AddCommand(std::make_unique<TCommandPermissionRevoke>());
    AddCommand(std::make_unique<TCommandPermissionSet>());
    AddCommand(std::make_unique<TCommandChangeOwner>());
    AddCommand(std::make_unique<TCommandPermissionClear>());
}

TCommandPermissionGrant::TCommandPermissionGrant()
    : TYdbOperationCommand("grant", { "add" }, "Grant permission")
{}

void TCommandPermissionGrant::Config(TConfig& config) {
    TYdbOperationCommand::Config(config);

    config.SetFreeArgsNum(2);
    SetFreeArgTitle(0, "<path>", "Path to grant permissions to");
    SetFreeArgTitle(1, "<subject>", "Subject to grant permissions");

    config.Opts->AddLongOption('p', "permission", "[At least one] Permission(s) to grant")
        .RequiredArgument("NAME").AppendTo(&PermissionsToGrant);
}

void TCommandPermissionGrant::Parse(TConfig& config) {
    TClientCommand::Parse(config);
    ParsePath(config, 0);
    Subject = config.ParseResult->GetFreeArgs()[1];
    if (!Subject) {
        throw TMissUseException() << "Missing required argument <subject>";
    }
    if (!PermissionsToGrant.size()) {
        throw TMissUseException() << "At least one permission to grant should be provided";
    }
}

int TCommandPermissionGrant::Run(TConfig& config) {
    NScheme::TSchemeClient client(CreateDriver(config));
    ThrowOnError(
        client.ModifyPermissions(
            Path,
            FillSettings(
                NScheme::TModifyPermissionsSettings()
                .AddGrantPermissions({ Subject, PermissionsToGrant })
            )
        ).GetValueSync()
    );
    return EXIT_SUCCESS;
}

TCommandPermissionRevoke::TCommandPermissionRevoke()
    : TYdbOperationCommand("revoke", { "remove" }, "Revoke permission")
{}

void TCommandPermissionRevoke::Config(TConfig& config) {
    TYdbOperationCommand::Config(config);

    config.SetFreeArgsNum(2);
    SetFreeArgTitle(0, "<path>", "Path to revoke permissions to");
    SetFreeArgTitle(1, "<subject>", "Subject to revoke permissions");

    config.Opts->AddLongOption('p', "permission", "[At least one] Permission(s) to revoke")
        .RequiredArgument("NAME").AppendTo(&PermissionsToRevoke);
}

void TCommandPermissionRevoke::Parse(TConfig& config) {
    TClientCommand::Parse(config);
    ParsePath(config, 0);
    Subject = config.ParseResult->GetFreeArgs()[1];
    if (!Subject) {
        throw TMissUseException() << "Missing required argument <subject>";
    }
    if (!PermissionsToRevoke.size()) {
        throw TMissUseException() << "At least one permission to revoke should be provided";
    }
}

int TCommandPermissionRevoke::Run(TConfig& config) {
    NScheme::TSchemeClient client(CreateDriver(config));
    ThrowOnError(
        client.ModifyPermissions(
            Path,
            FillSettings(
                NScheme::TModifyPermissionsSettings()
                .AddRevokePermissions({ Subject, PermissionsToRevoke })
            )
        ).GetValueSync()
    );
    return EXIT_SUCCESS;
}

TCommandPermissionSet::TCommandPermissionSet()
    : TYdbOperationCommand("set", std::initializer_list<TString>(), "Set permissions")
{}

void TCommandPermissionSet::Config(TConfig& config) {
    TYdbOperationCommand::Config(config);

    config.SetFreeArgsNum(2);
    SetFreeArgTitle(0, "<path>", "Path to set permissions to");
    SetFreeArgTitle(1, "<subject>", "Subject to set permissions");

    config.Opts->AddLongOption('p', "permission", "[At least one] Permission(s) to set")
        .RequiredArgument("NAME").AppendTo(&PermissionsToSet);
}

void TCommandPermissionSet::Parse(TConfig& config) {
    TClientCommand::Parse(config);
    ParsePath(config, 0);
    Subject = config.ParseResult->GetFreeArgs()[1];
    if (!Subject) {
        throw TMissUseException() << "Missing required argument <subject>";
    }
    if (!PermissionsToSet.size()) {
        throw TMissUseException() << "At least one permission to set should be provided";
    }
}

int TCommandPermissionSet::Run(TConfig& config) {
    NScheme::TSchemeClient client(CreateDriver(config));
    ThrowOnError(
        client.ModifyPermissions(
            Path,
            FillSettings(
                NScheme::TModifyPermissionsSettings()
                .AddSetPermissions({ Subject, PermissionsToSet })
            )
        ).GetValueSync()
    );
    return EXIT_SUCCESS;
}

TCommandChangeOwner::TCommandChangeOwner()
    : TYdbOperationCommand("chown", std::initializer_list<TString>(), "Change owner")
{}

void TCommandChangeOwner::Config(TConfig& config) {
    TYdbOperationCommand::Config(config);

    config.SetFreeArgsNum(2);
    SetFreeArgTitle(0, "<path>", "Path to change owner for");
    SetFreeArgTitle(1, "<owner>", "Owner to set");
}

void TCommandChangeOwner::Parse(TConfig& config) {
    TClientCommand::Parse(config);
    ParsePath(config, 0);
    Owner = config.ParseResult->GetFreeArgs()[1];
    if (!Owner){
        throw TMissUseException() << "Missing required argument <owner>";
    }
}

int TCommandChangeOwner::Run(TConfig& config) {
    NScheme::TSchemeClient client(CreateDriver(config));
    ThrowOnError(
        client.ModifyPermissions(
            Path,
            FillSettings(
                NScheme::TModifyPermissionsSettings()
                .AddChangeOwner(Owner)
            )
        ).GetValueSync()
    );
    return EXIT_SUCCESS;
}

TCommandPermissionClear::TCommandPermissionClear()
    : TYdbOperationCommand("clear", std::initializer_list<TString>(), "Clear permissions")
{}

void TCommandPermissionClear::Config(TConfig& config) {
    TYdbOperationCommand::Config(config);

    config.SetFreeArgsNum(1);
    SetFreeArgTitle(0, "<path>", "Path to clear permissions to");
}

void TCommandPermissionClear::Parse(TConfig& config) {
    TClientCommand::Parse(config);
    ParsePath(config, 0);
}

int TCommandPermissionClear::Run(TConfig& config) {
    NScheme::TSchemeClient client(CreateDriver(config));
    ThrowOnError(
        client.ModifyPermissions(
            Path,
            FillSettings(
                NScheme::TModifyPermissionsSettings()
                .AddClearAcl()
            )
        ).GetValueSync()
    );
    return EXIT_SUCCESS;
}

}
}
