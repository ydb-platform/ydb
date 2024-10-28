#include "interactive.h"
#include "progress_bar.h"
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

TStatus RemoveTopic(TTopicClient& client, const TString& path, const TDropTopicSettings& settings) {
    return RetryFunction([&]() -> TStatus {
        return client.DropTopic(path, settings).ExtractValueSync();
    });
}

NYql::TIssues MakeIssues(const TString& error) {
    NYql::TIssues issues;
    issues.AddIssue(NYql::TIssue(error));
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
TStatus Remove(TRemoveFunc<TClient, TSettings> func, TSchemeClient& schemeClient, TClient* client, const ESchemeEntryType type,
        const TString& path, ERecursiveRemovePrompt prompt, const TRemoveDirectorySettings& settings)
{
    if (!client) {
        return TStatus(EStatus::GENERIC_ERROR, MakeIssues(TStringBuilder()
            << TypeName<TClient>() << " not specified"));
    }

    if (Prompt(prompt, path, type, false)) {
        auto status = func(*client, path, TSettings(settings));
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
    TSchemeClient& schemeClient, TTableClient* tableClient, TTopicClient* topicClient, const ESchemeEntryType type,
    const TString& path, ERecursiveRemovePrompt prompt, const TRemoveDirectorySettings& settings)
{
    switch (type) {
    case ESchemeEntryType::Directory:
        return Remove(&RemoveDirectory, schemeClient, &schemeClient, type, path, prompt, settings);

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

    default:
        return TStatus(EStatus::UNSUPPORTED, MakeIssues(TStringBuilder()
            << "Unsupported entry type: " << type));
    }
}

TStatus RemoveDirectoryRecursive(
        TSchemeClient& schemeClient,
        TTableClient* tableClient,
        TTopicClient* topicClient,
        const TString& path,
        ERecursiveRemovePrompt prompt,
        const TRemoveDirectorySettings& settings,
        bool removeSelf,
        bool createProgressBar)
{
    auto recursiveListResult = RecursiveList(schemeClient, path, {}, removeSelf);
    if (!recursiveListResult.Status.IsSuccess()) {
        return recursiveListResult.Status;
    }

    if (prompt == ERecursiveRemovePrompt::Once) {
        if (!Prompt(path, ESchemeEntryType::Directory)) {
            return TStatus(EStatus::SUCCESS, {});
        }
    }
    
    std::unique_ptr<TProgressBar> bar;
    if (createProgressBar) {
        bar = std::make_unique<TProgressBar>(recursiveListResult.Entries.size());
    }
    // output order is: Root, Recursive(children)...
    // we need to reverse it to delete recursively
    for (auto it = recursiveListResult.Entries.rbegin(); it != recursiveListResult.Entries.rend(); ++it) {
        if (auto result = Remove(schemeClient, tableClient, topicClient, it->Type, it->Name, prompt, settings); !result.IsSuccess()) {
            return result;
        }
        if (createProgressBar) {
            bar->AddProgress(1);
        }
    }

    return TStatus(EStatus::SUCCESS, {});
}

TStatus RemoveDirectoryRecursive(
        TSchemeClient& schemeClient,
        TTableClient& tableClient,
        const TString& path,
        const TRemoveDirectorySettings& settings,
        bool removeSelf,
        bool createProgressBar)
{
    return RemoveDirectoryRecursive(schemeClient, &tableClient, nullptr, path, ERecursiveRemovePrompt::Never, settings, removeSelf, createProgressBar);
}

TStatus RemoveDirectoryRecursive(
        TSchemeClient& schemeClient,
        TTableClient& tableClient,
        TTopicClient& topicClient,
        const TString& path,
        ERecursiveRemovePrompt prompt,
        const TRemoveDirectorySettings& settings,
        bool removeSelf,
        bool createProgressBar)
{
    return RemoveDirectoryRecursive(schemeClient, &tableClient, &topicClient, path, prompt, settings, removeSelf, createProgressBar);
}

NYdb::TStatus RemovePathRecursive(NScheme::TSchemeClient& schemeClient, NTable::TTableClient& tableClient, NTopic::TTopicClient& topicClient, const TString& path, ERecursiveRemovePrompt prompt, const TRemovePathRecursiveSettings& settings /*= {}*/, bool createProgressBar /*= true*/) {
    auto entity = schemeClient.DescribePath(path).ExtractValueSync();
    if (!entity.IsSuccess()) {
        if (settings.NotExistsIsOk_ && entity.GetStatus() == EStatus::SCHEME_ERROR && entity.GetIssues().ToString().find("Path not found") != TString::npos) {
            return TStatus(EStatus::SUCCESS, {});
        }
        return entity;
    }
    switch (entity.GetEntry().Type) {
    case ESchemeEntryType::Directory:
    case ESchemeEntryType::ColumnStore:
        return RemoveDirectoryRecursive(schemeClient, tableClient, topicClient, path, prompt, settings, true, createProgressBar);
    default:
        return Remove(schemeClient, &tableClient, &topicClient, entity.GetEntry().Type, path, prompt, settings);
    }
}
}
