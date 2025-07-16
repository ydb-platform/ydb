#include "interactive.h"
#include "recursive_remove.h"

#include <ydb/public/lib/ydb_cli/common/recursive_list.h>
#include <ydb/public/lib/ydb_cli/common/retry_func.h>

#include <util/string/builder.h>
#include <util/system/type_name.h>

namespace NYdb::NConsoleClient {

using namespace NScheme;
using namespace NTable;
using namespace NTopic;

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

NYdb::NIssue::TIssues MakeIssues(const TString& error) {
    NYdb::NIssue::TIssues issues;
    issues.AddIssue(NYdb::NIssue::TIssue(error));
    return issues;
}

bool Prompt(const TString& path, ESchemeEntryType type) {
    Cout << "Remove " << to_lower(ToString(type)) << " '" << path << "' (y/n)? ";
    return AskYesOrNo();
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

    default:
        return TStatus(EStatus::UNSUPPORTED, MakeIssues(TStringBuilder()
            << "Unsupported entry type: " << type));
    }
}

TStatus RemoveDirectoryRecursive(
    TSchemeClient& schemeClient,
    TTableClient& tableClient,
    TTopicClient& topicClient,
    NQuery::TQueryClient& queryClient,
    NCoordination::TClient& coordinationClient,
    const TString& path,
    const TRemoveDirectoryRecursiveSettings& settings
) {
    const auto listingSettings = TRecursiveListSettings()
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
    return RemovePathsRecursive(
        schemeClient,
        tableClient,
        topicClient,
        queryClient,
        coordinationClient,
        recursiveListResult.Entries.rbegin(),
        recursiveListResult.Entries.rend(),
        settings
    );
}

TStatus RemoveDirectoryRecursive(
    const TDriver& driver,
    const TString& path,
    const TRemoveDirectoryRecursiveSettings& settings
) {
    TSchemeClient schemeClient(driver);
    TTableClient tableClient(driver);
    TTopicClient topicClient(driver);
    NQuery::TQueryClient queryClient(driver);
    NCoordination::TClient coordinationClient(driver);
    return RemoveDirectoryRecursive(schemeClient, tableClient, topicClient, queryClient, coordinationClient, path, settings);
}

TStatus RemovePathRecursive(
    const TDriver& driver,
    const TSchemeEntry& entry,
    const TRemoveDirectoryRecursiveSettings& settings
) {
    TSchemeClient schemeClient(driver);
    TTableClient tableClient(driver);
    TTopicClient topicClient(driver);
    NQuery::TQueryClient queryClient(driver);
    NCoordination::TClient coordinationClient(driver);
    auto remover = NInternal::CreateDefaultRemover(schemeClient, tableClient, topicClient, queryClient, coordinationClient, settings);
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
        case NYdb::NScheme::ESchemeEntryType::Directory:
            return RemoveDirectoryRecursive(driver, path, settings);
        default:
            return RemovePathRecursive(driver, entry, settings);
    }
}

namespace NInternal {

    TRemover CreateDefaultRemover(
        NScheme::TSchemeClient& schemeClient,
        NTable::TTableClient& tableClient,
        NTopic::TTopicClient& topicClient,
        NQuery::TQueryClient& queryClient,
        NCoordination::TClient& coordinationClient,
        const TRemoveDirectoryRecursiveSettings& settings
    ) {
        return [&, settings](const TSchemeEntry& entry) {
            return Remove(schemeClient, tableClient, topicClient, queryClient, coordinationClient, entry.Type, TString(entry.Name), settings.Prompt_, settings);
        };
    }

    bool MightHaveDependents(ESchemeEntryType type) {
        switch (type) {
        // directories might contain external data sources, which might have dependent objects (i.e. external tables)
        case ESchemeEntryType::Directory:
        case ESchemeEntryType::ExternalDataSource:
            return true;
        default:
            return false;
        }
    }

}

}
