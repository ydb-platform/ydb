#include "client.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/scheme/scheme.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <util/generic/yexception.h>
#include <util/string/cast.h>
#include <util/string/split.h>

#include <unordered_set>
#include <unordered_map>

namespace NYdb::NConsoleClient {
namespace {

using namespace NYdbWorkload::NScheme;

using NThreading::TFuture;

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
        : Database_(database)
        , Scheme_(driver)
        , Tables_(driver, NTable::TClientSettings().SessionPoolSettings(NTable::TSessionPoolSettings().MaxActiveSessions(concurrency)))
        , Topics_(driver)
    {
        while (!Database_.empty() && Database_.back() == '/') {
            Database_.pop_back();
        }
    }

    void PrepareDirectory(const TString& path) override {
        std::string fullPath = Database_;
        for (const auto& part : StringSplitter(path).Split('/')) {
            fullPath += "/" + std::string(part.Token());
            const auto result = Scheme_.DescribePath(fullPath, NScheme::TDescribePathSettings().ClientTimeout(RequestTimeout)).GetValueSync();
            if (result.IsSuccess()) {
                Y_ENSURE(result.GetEntry().Type == NScheme::ESchemeEntryType::Directory, "Expected directory at " << fullPath);
            } else {
                if (result.GetStatus() != EStatus::SCHEME_ERROR) {
                    Check(result, "DescribePath", fullPath);
                }
                Check(Scheme_.MakeDirectory(fullPath, NScheme::TMakeDirectorySettings().ClientTimeout(RequestTimeout)).GetValueSync(), "MakeDirectory", fullPath);
            }
        }
        // SCHEME_ERROR from DescribePath can also mean a scheme failure. Determine absence
        // from a successful directory listing, and propagate every error when inspecting existing objects.
        const auto listing = Scheme_.ListDirectory(fullPath, NScheme::TListDirectorySettings().ClientTimeout(RequestTimeout)).GetValueSync();
        Check(listing, "ListDirectory", fullPath);
        Entries_.clear();
        for (const auto& entry : listing.GetChildren()) {
            Entries_.emplace(entry.Name, entry.Type);
        }
    }

    TFuture<void> Create(const TObject& object) override {
        auto path = FullPath(object);
        auto existing = Scheme_.DescribePath(path, NScheme::TDescribePathSettings().ClientTimeout(RequestTimeout));
        return existing.Apply([this, object, path = std::move(path)](const auto& result) {
            const auto& status = result.GetValue();
            Y_ENSURE(!status.IsSuccess(), "Object already exists: " << path);
            if (status.GetStatus() != EStatus::SCHEME_ERROR) {
                Check(status, "DescribePath", path);
            }
            return CreateObject(object, path);
        });
    }

    TFuture<EObjectState> Inspect(const TObject& object) override {
        const auto path = FullPath(object);
        const auto entry = Entries_.find(path.substr(path.rfind('/') + 1));
        if (entry == Entries_.end()) {
            return NThreading::MakeFuture(EObjectState::Missing);
        }
        const auto expectedType = object.Type == EObjectType::Table ? NScheme::ESchemeEntryType::Table : NScheme::ESchemeEntryType::Topic;
        Y_ENSURE(entry->second == expectedType, "Unexpected object type at " << path);
        if (object.Type == EObjectType::Table) {
            return WithSession([path](auto& session) {
                return session.DescribeTable(path, NTable::TDescribeTableSettings()
                    .WithKeyShardBoundary(true).ClientTimeout(RequestTimeout));
            }).Apply([object, path](const auto& future) {
                const auto& result = future.GetValue();
                Check(result, "DescribeTable", path);
                std::unordered_set<std::string> names;
                const auto& description = result.GetTableDescription();
                Y_ENSURE(description.GetPrimaryKeyColumns() == std::vector<std::string>{"id"}, "Unexpected primary key at " << path);
                Y_ENSURE(description.GetIndexDescriptions().empty(), "Table indexes are not supported at " << path);
                Y_ENSURE(description.GetKeyRanges().size() == object.Partitions,
                    "Partition count does not match config at " << path << ": expected " << object.Partitions
                    << ", actual " << description.GetKeyRanges().size());
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
                return CheckBatch(names, "extra_", object) ? EObjectState::WithBatch : EObjectState::WithoutBatch;
            });
        }
        return Topics_.DescribeTopic(path, NTopic::TDescribeTopicSettings().ClientTimeout(RequestTimeout))
            .Apply([object, path](const auto& future) {
                const auto& result = future.GetValue();
                Check(result, "DescribeTopic", path);
                std::unordered_set<std::string> names;
                const auto& description = result.GetTopicDescription();
                // A disabled strategy may not preserve min/max settings in DescribeTopic.
                Y_ENSURE(description.GetPartitions().size() == object.Partitions
                    && description.GetPartitioningSettings().GetAutoPartitioningSettings().GetStrategy() == NTopic::EAutoPartitioningStrategy::Disabled,
                    "Partition settings do not match config at " << path);
                for (const auto& consumer : description.GetConsumers()) {
                    names.insert(consumer.GetConsumerName());
                }
                return CheckBatch(names, "reader_", object) ? EObjectState::WithBatch : EObjectState::WithoutBatch;
            });
    }

    TFuture<void> Alter(const TObject& object, bool add) override {
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
            return WithSession([path, settings = std::move(settings)](auto& session) {
                return session.AlterTable(path, settings);
            }).Apply([path, add](const auto& result) {
                Check(result.GetValue(), add ? "AddColumns" : "DropColumns", path);
            });
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
            return Topics_.AlterTopic(path, settings).Apply([path, add](const auto& result) {
                Check(result.GetValue(), add ? "AddConsumers" : "DropConsumers", path);
            });
        }
    }

private:
    TFuture<void> CreateObject(const TObject& object, const std::string& path) {
        if (object.Type == EObjectType::Table) {
            auto description = NTable::TTableBuilder()
                .AddNonNullableColumn("id", EPrimitiveType::Uint64)
                .SetPrimaryKeyColumn("id")
                .SetUniformPartitions(object.Partitions)
                .SetPartitioningSettings(NTable::TPartitioningSettingsBuilder()
                    .SetPartitioningBySize(false).SetPartitioningByLoad(false)
                    .SetMinPartitionsCount(object.Partitions).SetMaxPartitionsCount(object.Partitions).Build())
                .Build();
            return WithSession([path, description = std::move(description)](auto& session) mutable {
                return session.CreateTable(path, std::move(description), NTable::TCreateTableSettings().ClientTimeout(RequestTimeout));
            }).Apply([path](const auto& result) {
                Check(result.GetValue(), "CreateTable", path);
            });
        } else {
            auto settings = NTopic::TCreateTopicSettings()
                .PartitioningSettings(object.Partitions, object.Partitions)
                .ClientTimeout(RequestTimeout);
            return Topics_.CreateTopic(path, settings).Apply([path](const auto& result) {
                Check(result.GetValue(), "CreateTopic", path);
            });
        }
    }

    std::string FullPath(const TObject& object) const {
        return Database_ + "/" + std::string(object.Path);
    }

    template <typename TRequest>
    auto WithSession(TRequest request) -> decltype(request(std::declval<NTable::TSession&>())) {
        return Tables_.GetSession(NTable::TCreateSessionSettings().ClientTimeout(RequestTimeout))
            .Apply([this, request = std::move(request)](const auto& future) mutable {
                const auto& result = future.GetValue();
                Check(result, "GetSession", Database_);
                auto session = result.GetSession();
                auto response = request(session);
                return response.Apply([session = std::move(session)](auto completed) mutable {
                    // Return the session to the pool before publishing the operation result.
                    NTable::TSession keepAlive = std::move(session);
                    return completed.ExtractValue();
                });
            });
    }

private:
    std::string Database_;
    NScheme::TSchemeClient Scheme_;
    NTable::TTableClient Tables_;
    NTopic::TTopicClient Topics_;
    std::unordered_map<std::string, NScheme::ESchemeEntryType> Entries_;
};

} // namespace

std::unique_ptr<NYdbWorkload::NScheme::IClient> CreateSchemeWorkloadClient(
    const TDriver& driver, const TString& database, ui32 concurrency)
{
    return std::make_unique<TSdkClient>(driver, database, concurrency);
}

} // namespace NYdb::NConsoleClient
