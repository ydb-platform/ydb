#include "client.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/scheme/scheme.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <util/generic/yexception.h>
#include <util/string/cast.h>
#include <util/string/split.h>

#include <unordered_set>

namespace NYdb::NConsoleClient {
namespace {

using namespace NYdbWorkload::NScheme;

constexpr auto RequestTimeout = TDuration::Minutes(2);

void Check(const TStatus& status, TStringBuf operation, const std::string& path) {
    Y_ENSURE(status.IsSuccess(), operation << " " << path << ": " << status.GetStatus() << " " << status.GetIssues().ToString()
        << ". Workload stopped; timed-out operations may still complete on the server.");
}

bool CheckBatch(const std::unordered_set<std::string>& names, const std::string& prefix, const TObject& object) {
    Y_ENSURE(names.empty() || names.size() == object.AlterBatchSize, "Unexpected or partial ALTER batch at " << object.Path);
    for (ui32 i = 0; i < names.size(); ++i) {
        Y_ENSURE(names.contains(prefix + std::to_string(i)), "Unexpected schema member at " << object.Path);
    }
    return !names.empty();
}

class TSdkClient final : public IClient {
public:
    TSdkClient(const TDriver& driver, const TString& database, ui32 concurrency)
        : Database(database)
        , Scheme(driver)
        , Tables(driver, NTable::TClientSettings().SessionPoolSettings(NTable::TSessionPoolSettings().MaxActiveSessions(concurrency)))
        , Topics(driver)
    {
        while (!Database.empty() && Database.back() == '/') {
            Database.pop_back();
        }
    }

    void MakeDirectory(const TString& path) override {
        std::string fullPath = Database;
        for (const auto& part : StringSplitter(path).Split('/')) {
            fullPath += "/" + std::string(part.Token());
            const auto result = Scheme.DescribePath(fullPath, NScheme::TDescribePathSettings().ClientTimeout(RequestTimeout)).GetValueSync();
            if (result.IsSuccess()) {
                Y_ENSURE(result.GetEntry().Type == NScheme::ESchemeEntryType::Directory, "Expected directory at " << fullPath);
            } else {
                if (result.GetStatus() != EStatus::SCHEME_ERROR) {
                    Check(result, "DescribePath", fullPath);
                }
                Check(Scheme.MakeDirectory(fullPath, NScheme::TMakeDirectorySettings().ClientTimeout(RequestTimeout)).GetValueSync(), "MakeDirectory", fullPath);
            }
        }
    }

    void Create(const TObject& object) override {
        const auto path = FullPath(object);
        const auto existing = Scheme.DescribePath(path, NScheme::TDescribePathSettings().ClientTimeout(RequestTimeout)).GetValueSync();
        Y_ENSURE(!existing.IsSuccess(), "Object already exists: " << path);
        if (existing.GetStatus() != EStatus::SCHEME_ERROR) {
            Check(existing, "DescribePath", path);
        }
        if (object.Type == EObjectType::Table) {
            auto description = NTable::TTableBuilder()
                .AddNonNullableColumn("id", EPrimitiveType::Uint64)
                .SetPrimaryKeyColumn("id")
                .SetUniformPartitions(object.Partitions)
                .SetPartitioningSettings(NTable::TPartitioningSettingsBuilder()
                    .SetPartitioningBySize(false).SetPartitioningByLoad(false)
                    .SetMinPartitionsCount(object.Partitions).SetMaxPartitionsCount(object.Partitions).Build())
                .Build();
            Check(Session().CreateTable(path, std::move(description), NTable::TCreateTableSettings().ClientTimeout(RequestTimeout)).GetValueSync(), "CreateTable", path);
        } else {
            auto settings = NTopic::TCreateTopicSettings()
                .PartitioningSettings(object.Partitions, object.Partitions)
                .ClientTimeout(RequestTimeout);
            Check(Topics.CreateTopic(path, settings).GetValueSync(), "CreateTopic", path);
        }
    }

    bool Inspect(const TObject& object) override {
        const auto path = FullPath(object);
        std::unordered_set<std::string> names;
        if (object.Type == EObjectType::Table) {
            const auto result = Session().DescribeTable(path, NTable::TDescribeTableSettings().ClientTimeout(RequestTimeout)).GetValueSync();
            Check(result, "DescribeTable", path);
            const auto& description = result.GetTableDescription();
            Y_ENSURE(description.GetPrimaryKeyColumns() == std::vector<std::string>{"id"}, "Unexpected primary key at " << path);
            const auto& partitions = description.GetPartitioningSettings();
            Y_ENSURE(partitions.GetMinPartitionsCount() == object.Partitions
                && partitions.GetMaxPartitionsCount() == object.Partitions
                && partitions.GetPartitioningBySize() == false
                && partitions.GetPartitioningByLoad() == false, "Partition settings do not match config at " << path);
            const auto keyType = TTypeBuilder().Primitive(EPrimitiveType::Uint64).Build().ToString();
            const auto extraType = TTypeBuilder().BeginOptional().Primitive(EPrimitiveType::Utf8).EndOptional().Build().ToString();
            for (const auto& column : description.GetColumns()) {
                if (column.Name == "id") {
                    Y_ENSURE(column.Type.ToString() == keyType, "Unexpected key type at " << path);
                } else {
                    Y_ENSURE(column.Type.ToString() == extraType, "Unexpected column type at " << path);
                    names.insert(column.Name);
                }
            }
            return CheckBatch(names, "extra_", object);
        }
        const auto result = Topics.DescribeTopic(path, NTopic::TDescribeTopicSettings().ClientTimeout(RequestTimeout)).GetValueSync();
        Check(result, "DescribeTopic", path);
        const auto& description = result.GetTopicDescription();
        // A disabled strategy may not preserve min/max settings in DescribeTopic.
        Y_ENSURE(description.GetPartitions().size() == object.Partitions
            && description.GetPartitioningSettings().GetAutoPartitioningSettings().GetStrategy() == NTopic::EAutoPartitioningStrategy::Disabled,
            "Partition settings do not match config at " << path);
        for (const auto& consumer : description.GetConsumers()) {
            names.insert(consumer.GetConsumerName());
        }
        return CheckBatch(names, "reader_", object);
    }

    void Alter(const TObject& object, bool add) override {
        const auto path = FullPath(object);
        if (object.Type == EObjectType::Table) {
            auto settings = NTable::TAlterTableSettings().ClientTimeout(RequestTimeout);
            const auto type = TTypeBuilder().BeginOptional().Primitive(EPrimitiveType::Utf8).EndOptional().Build();
            for (ui32 i = 0; i < object.AlterBatchSize; ++i) {
                const auto name = "extra_" + std::to_string(i);
                if (add) {
                    settings.AppendAddColumns(NTable::TTableColumn(name, type));
                } else {
                    settings.AppendDropColumns(name);
                }
            }
            Check(Session().AlterTable(path, settings).GetValueSync(), add ? "AddColumns" : "DropColumns", path);
        } else {
            auto settings = NTopic::TAlterTopicSettings().ClientTimeout(RequestTimeout);
            for (ui32 i = 0; i < object.AlterBatchSize; ++i) {
                const auto name = "reader_" + std::to_string(i);
                if (add) {
                    settings.BeginAddConsumer(name).EndAddConsumer();
                } else {
                    settings.AppendDropConsumers(name);
                }
            }
            Check(Topics.AlterTopic(path, settings).GetValueSync(), add ? "AddConsumers" : "DropConsumers", path);
        }
    }

private:
    std::string FullPath(const TObject& object) const {
        return Database + "/" + std::string(object.Path);
    }

    NTable::TSession Session() {
        auto result = Tables.GetSession(NTable::TCreateSessionSettings().ClientTimeout(RequestTimeout)).GetValueSync();
        Check(result, "GetSession", Database);
        return result.GetSession();
    }

    std::string Database;
    NScheme::TSchemeClient Scheme;
    NTable::TTableClient Tables;
    NTopic::TTopicClient Topics;
};

} // namespace

std::unique_ptr<NYdbWorkload::NScheme::IClient> CreateSchemeWorkloadClient(
    const TDriver& driver, const TString& database, ui32 concurrency)
{
    return std::make_unique<TSdkClient>(driver, database, concurrency);
}

} // namespace NYdb::NConsoleClient
