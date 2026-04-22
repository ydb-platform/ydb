#include "describe_tool.h"
#include "tool_base.h"

#include <ydb/public/lib/ydb_cli/commands/interactive/common/json_utils.h>
#include <ydb/public/lib/ydb_cli/common/describe.h>
#include <ydb/public/lib/ydb_cli/common/ftxui.h>
#include <ydb/public/lib/ydb_cli/common/log.h>
#include <ydb/public/lib/ydb_cli/common/ydb_path.h>

#include <util/stream/str.h>
#include <util/string/strip.h>

namespace NYdb::NConsoleClient::NAi {

namespace {

class TDescribeTool final : public TDatabaseToolBase {
    using TBase = TDatabaseToolBase;

    static constexpr char DESCRIPTION[] = R"(
Displays comprehensive meta-information about a database object at the specified path.
This is a read-only operation.

Information provided by object type:
- **Table**: Schema (column names, types, families, keys), storage settings, column families (compression, etc.), TTL settings, read replicas settings, indexes, changefeeds.
  - Optional: Auto-partitioning settings, partition key boundaries, table statistics (row count, size, partitions count, creation/modification time), partition statistics.
- **Topic**: Retention settings, supported codecs, partitioning settings, consumer rules.
  - Optional: Topic statistics (size, write time lag, write speed), partition statistics.
- **Directory**: List of children.
- **Coordination Node**: Consistency mode, session grace period, rate limiter settings.
- **Replication**: State, connection parameters, consistency level,lag/progress stats.
- **Transfer**: Source/Destination paths, consumer, transform lambda.
- **External Table/Data Source**: Configuration.
- **View**: Query text.

Use this tool to inspect table schema before executing `INSERT`, `SELECT`, `UPDATE`, `DELETE` or other SQL queries that need the knowledge of the schema.
NEVER guess column names, types or keys without verifying them with this tool first.

**Path Format**:
- **Relative** (Preferred): Path relative to the database root (e.g., `my_dir/my_table`). Do NOT start with `/`.
- **Absolute**: Full path starting with `/` and including the database prefix (e.g., `/ru/home/mydb/my_dir/my_table`).
)";

    static constexpr char PATH_PROPERTY[] = "path";
    static constexpr char PERMISSIONS_PROPERTY[] = "permissions";
    static constexpr char PARTITION_BOUNDARIES_PROPERTY[] = "partition_boundaries";
    static constexpr char STATS_PROPERTY[] = "stats";
    static constexpr char PARTITION_STATS_PROPERTY[] = "partition_stats";

public:
    explicit TDescribeTool(const TDescribeToolSettings& settings)
        : TBase(settings.Database, CreateParametersSchema(), DESCRIPTION)
        , Driver(settings.Driver)
    {}

protected:
    void ParseParameters(const NJson::TJsonValue& parameters) final {
        TJsonParser parser(parameters);

        Path = CanonizePath(parser.GetKey(PATH_PROPERTY).GetString());

        if (auto p = parser.MaybeKey(PERMISSIONS_PROPERTY)) {
            Options.ShowPermissions = p->GetBooleanSafe(false);
        }

        if (auto p = parser.MaybeKey(PARTITION_BOUNDARIES_PROPERTY)) {
            Options.ShowKeyShardBoundaries = p->GetBooleanSafe(false);
        }

        if (auto p = parser.MaybeKey(STATS_PROPERTY)) {
            Options.ShowStats = p->GetBooleanSafe(false);
        }

        if (auto p = parser.MaybeKey(PARTITION_STATS_PROPERTY)) {
            Options.ShowPartitionStats = p->GetBooleanSafe(false);
        }

        Options.Database = Database;
    }

    bool AskPermissions() final {
        PrintFtxuiMessage("", TStringBuilder() << "Describing path " << Path, ftxui::Color::Green);
        return true;
    }

    TResponse DoExecute() final {
        TStringStream outputStream;
        TDescribeLogic describeLogic(Driver, outputStream);

        int status = describeLogic.Describe(Path, Options, EDataFormat::ProtoJsonBase64);

        if (GetGlobalLogger().IsVerbose()) {
            if (status == EXIT_SUCCESS) {
                TStringStream prettyStream;
                TDescribeLogic prettyLogic(Driver, prettyStream);
                if (prettyLogic.Describe(Path, Options, EDataFormat::Pretty) == EXIT_SUCCESS) {
                    Cout << Endl << prettyStream.Str() << Endl;
                } else {
                    Cout << Endl << FormatJsonValue(outputStream.Str()) << Endl;
                }
            } else {
                Cout << Endl << FormatJsonValue(outputStream.Str()) << Endl;
            }
        }

        if (status != EXIT_SUCCESS) {
            Cout << Endl << Colors.Red() << "Describe path \"" << Path << "\" failed: " << Strip(outputStream.Str()) << Colors.OldColor() << Endl;
            outputStream << "\nCommand failed.";
            return TResponse::Error(outputStream.Str());
        }

        return TResponse::Success(outputStream.Str());
    }

private:
    static NJson::TJsonValue CreateParametersSchema() {
        return TJsonSchemaBuilder()
            .Property(PATH_PROPERTY)
                .Type(TJsonSchemaBuilder::EType::String)
                .Description("Path to the object to describe")
                .Done()
            .Property(PERMISSIONS_PROPERTY, false)
                .Type(TJsonSchemaBuilder::EType::Boolean)
                .Description("Show owner and permissions")
                .Done()
            .Property(PARTITION_BOUNDARIES_PROPERTY, false)
                .Type(TJsonSchemaBuilder::EType::Boolean)
                .Description("[Table] Show partition key boundaries")
                .Done()
            .Property(STATS_PROPERTY, false)
                .Type(TJsonSchemaBuilder::EType::Boolean)
                .Description("[Table|Topic|Replication] Show statistics")
                .Done()
            .Property(PARTITION_STATS_PROPERTY, false)
                .Type(TJsonSchemaBuilder::EType::Boolean)
                .Description("[Table|Topic|Consumer] Show partition statistics")
                .Done()
            .Build();
    }

private:
    TDriver Driver;

    TString Path;
    TDescribeOptions Options;
};

} // anonymous namespace

ITool::TPtr CreateDescribeTool(const TDescribeToolSettings& settings) {
    return std::make_shared<TDescribeTool>(settings);
}

} // namespace NYdb::NConsoleClient::NAi
