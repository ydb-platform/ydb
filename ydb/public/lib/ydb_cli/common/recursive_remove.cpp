#include "interactive.h"
#include "recursive_remove.h"

#include <ydb/public/lib/ydb_cli/common/recursive_list.h>
#include <ydb/public/lib/ydb_cli/common/retry_func.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/coordination/coordination.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/draft/ydb_keyvalue.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <util/string/builder.h>
#include <util/system/type_name.h>

#include <algorithm>
#include <map>
#include <mutex>
#include <optional>
#include <thread>

namespace NYdb::NConsoleClient {

using namespace NScheme;
using namespace NTable;
using namespace NTopic;

namespace {

TStatus RemoveDirectory(TSchemeClient& client, const TString& path, const TRemoveDirectorySettings& settings) {
    return RetryFunction([&]() -> TStatus {
        return client.RemoveDirectory(path, settings).ExtractValueSync();
    });
}

TStatus RemoveTable(TTableClient& client, const TString& path, const TDropTableSettings& settings) {
    return client.RetryOperationSync([path, settings](TSession session) {
        return session.DropTable(path, settings).ExtractValueSync();
    });
}

TStatus DropSchemeObject(const TString& objectType, NQuery::TQueryClient& client, const TString& path, const TRemoveDirectorySettings& settings) {
    return client.RetryQuerySync([&](NQuery::TSession session) {
        const auto executionSettings = NQuery::TExecuteQuerySettings()
            .TraceId(settings.TraceId_)
            .RequestType(settings.RequestType_)
            .Header(settings.Header_)
            .ClientTimeout(settings.ClientTimeout_);
        const auto query = TStringBuilder() << "DROP " << objectType << " `" << path << "`";
        return session.ExecuteQuery(query, NQuery::TTxControl::NoTx(), executionSettings).ExtractValueSync();
    });
}

TStatus RemoveThrowQuery(const TString& entry, TTableClient& client, const TString& path, const TRemoveDirectorySettings& settings) {
    // This is temporary solution, safe deleting of columnstore is impossible now
    return client.RetryOperationSync([path, settings, entry](TSession session) {
        auto execSettings = TExecSchemeQuerySettings().UseClientTimeoutForOperation(settings.UseClientTimeoutForOperation_)
            .ClientTimeout(settings.ClientTimeout_).OperationTimeout(settings.OperationTimeout_).CancelAfter(settings.CancelAfter_)
            .Header(settings.Header_).ReportCostInfo(settings.ReportCostInfo_).RequestType(settings.RequestType_).TraceId(settings.TraceId_);
        return session.ExecuteSchemeQuery("DROP " + entry + " `" + path + "`", execSettings).ExtractValueSync();
    });
}

TStatus RemoveColumnStore(TTableClient& client, const TString& path, const TRemoveDirectorySettings& settings) {
    return RemoveThrowQuery("TABLESTORE", client, path, settings);
}

TStatus RemoveExternalDataSource(TTableClient& client, const TString& path, const TRemoveDirectorySettings& settings) {
    return RemoveThrowQuery("EXTERNAL DATA SOURCE", client, path, settings);
}

TStatus RemoveExternalTable(TTableClient& client, const TString& path, const TRemoveDirectorySettings& settings) {
    return RemoveThrowQuery("EXTERNAL TABLE", client, path, settings);
}

TStatus RemoveView(NQuery::TQueryClient& client, const TString& path, const TRemoveDirectorySettings& settings) {
    return DropSchemeObject("VIEW", client, path, settings);
}

TStatus RemoveTopic(TTopicClient& client, const TString& path, const TDropTopicSettings& settings) {
    return RetryFunction([&]() -> TStatus {
        return client.DropTopic(path, settings).ExtractValueSync();
    });
}

TStatus RemoveCoordinationNode(NCoordination::TClient& client, const TString& path, const NCoordination::TDropNodeSettings& settings) {
    return RetryFunction([&]() -> TStatus {
        return client.DropNode(path, settings).ExtractValueSync();
    });
}

TStatus RemoveReplication(NQuery::TQueryClient& client, const TString& path, const TRemoveDirectorySettings& settings) {
    return DropSchemeObject("ASYNC REPLICATION", client, path, settings);
}

TStatus RemoveTransfer(NQuery::TQueryClient& client, const TString& path, const TRemoveDirectorySettings& settings) {
    return DropSchemeObject("TRANSFER", client, path, settings);
}

TStatus RemoveStreamingQuery(NQuery::TQueryClient& client, const TString& path, const TRemoveDirectorySettings& settings) {
    return DropSchemeObject("STREAMING QUERY", client, path, settings);
}

TStatus RemoveSecret(NQuery::TQueryClient& client, const TString& path, const TRemoveDirectorySettings& settings) {
    return DropSchemeObject("SECRET", client, path, settings);
}

TStatus RemoveSolomonVolume(NKeyValue::TKeyValueClient& client, const TString& path, const NKeyValue::TDropVolumeSettings& settings) {
    return RetryFunction([&]() -> TStatus {
        return client.DropVolume(path, settings).ExtractValueSync();
    });
}

TStatus RemoveResourcePool(NQuery::TQueryClient& client, const TString& path, const TRemoveDirectorySettings& settings) {
    // Resource pool DDL accepts a pool name rather than its path in .metadata.
    return DropSchemeObject("RESOURCE POOL", client, TString(TStringBuf(path).RNextTok('/')), settings);
}

TStatus RemoveBackupCollection(NQuery::TQueryClient& client, const TString& path, const TRemoveDirectorySettings& settings) {
    // Backup collection DDL resolves the name under .backups/collections.
    return DropSchemeObject("BACKUP COLLECTION", client, TString(TStringBuf(path).RNextTok('/')), settings);
}

NYdb::NIssue::TIssues MakeIssues(const TString& error) {
    NYdb::NIssue::TIssues issues;
    issues.AddIssue(NYdb::NIssue::TIssue(error));
    return issues;
}

} // anonymous namespace

bool Prompt(const TString& path, ESchemeEntryType type) {
    return AskYesOrNo(TStringBuilder() << "Remove " << to_lower(ToString(type)) << " '" << path << "'?", /* defaultAnswer */ false);
}

bool Prompt(ERecursiveRemovePrompt mode, const TString& path, NScheme::ESchemeEntryType type, bool first) {
    switch (mode) {
        case ERecursiveRemovePrompt::Always:
            return Prompt(path, type);
        case ERecursiveRemovePrompt::Once:
            if (first) {
                return Prompt(path, type);
            } else {
                return true;
            }
        case ERecursiveRemovePrompt::Never:
            return true;
    }
}

template <typename TClient, typename TSettings>
using TRemoveFunc = TStatus(*)(TClient&, const TString&, const TSettings&);

template <typename TClient, typename TSettings>
TStatus Remove(
    TRemoveFunc<TClient, TSettings> func,
    TSchemeClient& schemeClient,
    TClient& specializedClient,
    const ESchemeEntryType type,
    const TString& path,
    ERecursiveRemovePrompt prompt,
    const TRemoveDirectorySettings& settings
) {
    if (Prompt(prompt, path, type, false)) {
        auto status = func(specializedClient, path, TSettings(settings));
        if (status.GetStatus() == EStatus::SCHEME_ERROR && schemeClient.DescribePath(path).ExtractValueSync().GetStatus() == EStatus::SCHEME_ERROR) {
            Cerr << "WARNING: Couldn't delete path: \'" << path << "\'. It was probably already deleted in another process" << Endl;
            return TStatus(EStatus::SUCCESS, {});
        }
        return status;
    } else {
        return TStatus(EStatus::SUCCESS, {});
    }
}

TStatus Remove(
    TSchemeClient& schemeClient,
    TTableClient& tableClient,
    TTopicClient& topicClient,
    NQuery::TQueryClient& queryClient,
    NCoordination::TClient& coordinationClient,
    NKeyValue::TKeyValueClient& keyValueClient,
    const ESchemeEntryType type,
    const TString& path,
    ERecursiveRemovePrompt prompt,
    const TRemoveDirectorySettings& settings
) {
    switch (type) {
    case ESchemeEntryType::Directory:
        return Remove(&RemoveDirectory, schemeClient, schemeClient, type, path, prompt, settings);

    case ESchemeEntryType::ColumnStore:
        return Remove(&RemoveColumnStore, schemeClient, tableClient, type, path, prompt, settings);

    case ESchemeEntryType::ColumnTable:
    case ESchemeEntryType::Table:
        return Remove(&RemoveTable, schemeClient, tableClient, type, path, prompt, settings);

    case ESchemeEntryType::PqGroup:
    case ESchemeEntryType::Topic:
        return Remove(&RemoveTopic, schemeClient, topicClient, type, path, prompt, settings);
    case ESchemeEntryType::ExternalDataSource:
        return Remove(&RemoveExternalDataSource, schemeClient, tableClient, type, path, prompt, settings);
    case ESchemeEntryType::ExternalTable:
        return Remove(&RemoveExternalTable, schemeClient, tableClient, type, path, prompt, settings);
    case ESchemeEntryType::View:
        return Remove(&RemoveView, schemeClient, queryClient, type, path, prompt, settings);
    case ESchemeEntryType::CoordinationNode:
        return Remove(&RemoveCoordinationNode, schemeClient, coordinationClient, type, path, prompt, settings);
    case ESchemeEntryType::SubDomain:
        // continue silently
        return TStatus(EStatus::SUCCESS, {});
    case ESchemeEntryType::Replication:
        return Remove(&RemoveReplication, schemeClient, queryClient, type, path, prompt, settings);
    case ESchemeEntryType::Transfer:
        return Remove(&RemoveTransfer, schemeClient, queryClient, type, path, prompt, settings);
    case ESchemeEntryType::StreamingQuery:
        return Remove(&RemoveStreamingQuery, schemeClient, queryClient, type, path, prompt, settings);
    case ESchemeEntryType::Secret:
        return Remove(&RemoveSecret, schemeClient, queryClient, type, path, prompt, settings);
    case ESchemeEntryType::SolomonVolume:
        return Remove(&RemoveSolomonVolume, schemeClient, keyValueClient, type, path, prompt, settings);
    case ESchemeEntryType::ResourcePool:
        return Remove(&RemoveResourcePool, schemeClient, queryClient, type, path, prompt, settings);
    case ESchemeEntryType::BackupCollection:
        return Remove(&RemoveBackupCollection, schemeClient, queryClient, type, path, prompt, settings);

    default:
        return TStatus(EStatus::UNSUPPORTED, MakeIssues(TStringBuilder()
            << "Unsupported entry type: " << type));
    }
}

TStatus RemoveDirectoryRecursive(
    const TDriver& driver,
    const TString& path,
    const TRemoveDirectoryRecursiveSettings& settings
) {
    TSchemeClient schemeClient(driver);
    const auto listingSettings = TRecursiveListSettings()
        .SkipMode(settings.SkipMode_)
        .Filter([](const TSchemeEntry& entry) {
            // explicitly skip subdomains
            return entry.Type != ESchemeEntryType::SubDomain;
        });
    auto recursiveListResult = RecursiveList(schemeClient, path, listingSettings, settings.RemoveSelf_);
    if (!recursiveListResult.Status.IsSuccess()) {
        return recursiveListResult.Status;
    }

    if (settings.Prompt_ == ERecursiveRemovePrompt::Once) {
        if (!Prompt(path, ESchemeEntryType::Directory)) {
            return TStatus(EStatus::SUCCESS, {});
        }
    }

    // RecursiveList outputs elements in pre-order: root, recurse(children)...
    // We need to reverse it to delete scheme objects before directories.
    auto remover = NInternal::CreateDefaultRemover(driver, settings);
    return RemovePathsRecursive(
        recursiveListResult.Entries.rbegin(),
        recursiveListResult.Entries.rend(),
        remover,
        settings
    );
}

TStatus RemovePathRecursive(
    const TDriver& driver,
    const TSchemeEntry& entry,
    const TRemoveDirectoryRecursiveSettings& settings
) {
    auto remover = NInternal::CreateDefaultRemover(driver, settings);
    return remover(entry);
}

TStatus RemovePathRecursive(
    const TDriver& driver,
    const TString& path,
    const TRemoveDirectoryRecursiveSettings& settings
) {
    TSchemeClient schemeClient(driver);
    const auto entity = schemeClient.DescribePath(path).ExtractValueSync();
    if (!entity.IsSuccess()) {
        if (settings.NotExistsIsOk_ && entity.GetStatus() == EStatus::SCHEME_ERROR && entity.GetIssues().ToString().find("Path not found") != TString::npos) {
            return TStatus(EStatus::SUCCESS, {});
        }
        return entity;
    }

    auto entry = entity.GetEntry();
    entry.Name = path;
    switch (entry.Type) {
        case NYdb::NScheme::ESchemeEntryType::ColumnStore:
        case NYdb::NScheme::ESchemeEntryType::BackupCollection:
        case NYdb::NScheme::ESchemeEntryType::Directory:
            return RemoveDirectoryRecursive(driver, path, settings);
        default:
            return RemovePathRecursive(driver, entry, settings);
    }
}

namespace NInternal {

    TStatus RemoveInParallel(
        const TVector<const NScheme::TSchemeEntry*>& entries,
        TRemover& remover,
        TProgressBar* progressBar,
        ui32 threads
    ) {
        std::mutex mutex;
        size_t nextIndex = 0;
        std::optional<TStatus> firstError;
        std::exception_ptr firstException;

        // Returns nullptr when all entries are taken or after the first failure.
        auto takeNextEntry = [&]() -> const NScheme::TSchemeEntry* {
            std::lock_guard lock(mutex);
            if (firstError || firstException || nextIndex == entries.size()) {
                return nullptr;
            }
            return entries[nextIndex++];
        };

        auto removeEntries = [&]() {
            while (const auto* entry = takeNextEntry()) {
                try {
                    auto result = remover(*entry);
                    std::lock_guard lock(mutex);
                    if (!result.IsSuccess() && !firstError) {
                        firstError = std::move(result);
                    } else if (result.IsSuccess() && progressBar) {
                        progressBar->AddProgress(1);
                    }
                } catch (...) {
                    // An exception must not escape a worker thread: it would terminate the process.
                    std::lock_guard lock(mutex);
                    if (!firstException) {
                        firstException = std::current_exception();
                    }
                }
            }
        };

        // The current thread is one of the workers, so threads == 1 removes the entries in their original order.
        const size_t threadCount = std::min<size_t>(std::max<ui32>(threads, 1), entries.size());
        // std::jthread joins in its destructor, so the started workers are joined even if starting the next one throws.
        std::vector<std::jthread> workers;
        for (size_t i = 1; i < threadCount; ++i) {
            workers.emplace_back(removeEntries);
        }
        removeEntries();
        for (auto& worker : workers) {
            worker.join();
        }

        if (firstException) {
            std::rethrow_exception(firstException);
        }
        return firstError.value_or(TStatus(EStatus::SUCCESS, {}));
    }

    TVector<TVector<const NScheme::TSchemeEntry*>> SplitIntoRemoveBatches(
        const TVector<const NScheme::TSchemeEntry*>& entries
    ) {
        TVector<const NScheme::TSchemeEntry*> withoutDependents;
        // Replications and transfers might need a secret to finish their removal, so secrets go after them.
        TVector<const NScheme::TSchemeEntry*> secrets;
        // The name of a descendant is "<parent name>/<child name>...", so it always contains more '/' than its parent.
        std::map<size_t, TVector<const NScheme::TSchemeEntry*>, std::greater<size_t>> withDependentsBySlashCount;
        for (const auto* entry : entries) {
            if (MightHaveDependents(entry->Type)) {
                const size_t slashCount = std::count(entry->Name.begin(), entry->Name.end(), '/');
                withDependentsBySlashCount[slashCount].push_back(entry);
            } else if (entry->Type == ESchemeEntryType::Secret) {
                secrets.push_back(entry);
            } else {
                withoutDependents.push_back(entry);
            }
        }

        TVector<TVector<const NScheme::TSchemeEntry*>> batches;
        if (!withoutDependents.empty()) {
            batches.push_back(std::move(withoutDependents));
        }
        if (!secrets.empty()) {
            batches.push_back(std::move(secrets));
        }
        for (auto& [slashCount, batch] : withDependentsBySlashCount) {
            batches.push_back(std::move(batch));
        }
        return batches;
    }

    TRemover CreateDefaultRemover(
        const TDriver& driver,
        const TRemoveDirectoryRecursiveSettings& settings
    ) {
        // Every removing thread holds at most one session at a time.
        return [
            schemeClient = TSchemeClient(driver),
            tableClient = TTableClient(driver, NTable::TClientSettings()
                .SessionPoolSettings(NTable::TSessionPoolSettings().MaxActiveSessions(settings.Threads_))),
            topicClient = TTopicClient(driver),
            queryClient = NQuery::TQueryClient(driver, NQuery::TClientSettings()
                .SessionPoolSettings(NQuery::TSessionPoolSettings().MaxActiveSessions(settings.Threads_))),
            coordinationClient = NCoordination::TClient(driver),
            keyValueClient = NKeyValue::TKeyValueClient(driver),
            settings
        ](const TSchemeEntry& entry) mutable {
            return Remove(schemeClient, tableClient, topicClient, queryClient, coordinationClient, keyValueClient, entry.Type, TString(entry.Name), settings.Prompt_, settings);
        };
    }

    bool MightHaveDependents(ESchemeEntryType type) {
        switch (type) {
        // containers can be removed only after the objects inside them
        case ESchemeEntryType::Directory:
        case ESchemeEntryType::ColumnStore:
        case ESchemeEntryType::BackupCollection:
        // external data sources might have dependent objects (i.e. external tables)
        case ESchemeEntryType::ExternalDataSource:
            return true;
        default:
            return false;
        }
    }

}

} // namespace NYdb::NConsoleClient
