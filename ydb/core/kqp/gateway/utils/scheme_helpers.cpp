#include "scheme_helpers.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/path.h>
#include <ydb/core/base/table_index.h>
#include <ydb/core/protos/auth.pb.h>
#include <ydb/core/protos/external_sources.pb.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/protos/subdomains.pb.h>

namespace NKikimr::NKqp::NSchemeHelpers {

using namespace NKikimrSchemeOp;
using namespace NKikimrExternalSources;
using namespace NYql;

TString CanonizePath(const TString& path) {
    if (path.empty()) {
        return "/";
    }

    if (path[0] != '/') {
        return "/" + path;
    }

    return path;
}

bool TrySplitTablePath(const TString& path, std::pair<TString, TString>& result, TString& error) {
    auto parts = NKikimr::SplitPath(path);

    if (parts.size() < 2) {
        error = TString("Missing scheme root in table path: ") + path;
        return false;
    }

    result = std::make_pair(
        CombinePath(parts.begin(), parts.end() - 1),
        parts.back());

    return true;
}

bool SplitTablePath(const TString& tableName, const TString& database, std::pair<TString, TString>& pathPair,
    TString& error, bool createDir)
{
    if (createDir) {
        return TrySplitPathByDb(tableName, database, pathPair, error);
    } else {
        return TrySplitTablePath(tableName, pathPair, error);
    }
}

TVector<TString> CreateIndexTablePath(const TString& tableName, const NYql::TIndexDescription& index) {
    const auto implTables = index.GetImplTables();
    TVector<TString> paths;
    paths.reserve(implTables.size());
    for (const auto& implTable : implTables) {
        paths.emplace_back(TStringBuilder() << tableName << "/" << index.Name << "/" << implTable);
    }
    return paths;
}

TString GetDomainDatabase(const TAppData* appData) {
    if (appData->DomainsInfo && appData->DomainsInfo->Domain) {
        if (const auto& name = appData->DomainsInfo->GetDomain()->Name) {
            return "/" + name;
        }
    }
    return {};
}

// DomainLoginOnly setting determine what database should handle user|group administration operations (AlterLogin).
// DomainLoginOnly = false -- database where request is directed to
// DomainLoginOnly = true -- domain (root) database
TString SelectDatabaseForAlterLoginOperations(const TAppData* appData, const TString& requestDatabase) {
    if (appData->AuthConfig.GetDomainLoginOnly()) {
        return GetDomainDatabase(appData);
    } else {
        return requestDatabase;
    }
}

void FillCreateExternalTableColumnDesc(NKikimrSchemeOp::TExternalTableDescription& externalTableDesc,
                                       const TString& name,
                                       bool replaceIfExists,
                                       const TCreateExternalTableSettings& settings)
{
    externalTableDesc.SetName(name);
    externalTableDesc.SetDataSourcePath(settings.DataSourcePath);
    externalTableDesc.SetLocation(settings.Location);
    externalTableDesc.SetSourceType("General");
    externalTableDesc.SetReplaceIfExists(replaceIfExists);

    Y_ENSURE(settings.ColumnOrder.size() == settings.Columns.size());
    for (const auto& name : settings.ColumnOrder) {
        auto columnIt = settings.Columns.find(name);
        Y_ENSURE(columnIt != settings.Columns.end());

        TColumnDescription& columnDesc = *externalTableDesc.AddColumns();
        columnDesc.SetName(columnIt->second.Name);
        columnDesc.SetType(columnIt->second.Type);
        columnDesc.SetNotNull(columnIt->second.NotNull);
    }
    NKikimrExternalSources::TGeneral general;
    general.set_location(settings.Location);
    auto& attributes = *general.mutable_attributes();
    for (const auto& [key, value]: settings.SourceTypeParameters) {
        attributes.insert({key, value});
    }
    externalTableDesc.SetContent(general.SerializeAsString());
}

bool Validate(const TAlterDatabaseSettings& settings, TIssue& error) {
    const int settingsToAlter = (settings.Owner ? 1 : 0) + (settings.SchemeLimits ? 1 : 0);
    if (settingsToAlter > 1) {
        error.SetMessage("Multiple setting classes cannot be altered simultaneously.");
        error.SetCode(TIssuesIds_EIssueCode_KIKIMR_BAD_REQUEST, TSeverityIds_ESeverityId_S_ERROR);
        return false;
    }
    if (settingsToAlter == 0) {
        error.SetMessage("No settings to alter.");
        error.SetCode(TIssuesIds_EIssueCode_KIKIMR_BAD_REQUEST, TSeverityIds_ESeverityId_S_ERROR);
        return false;
    }
    return true;
}

void FillAlterDatabaseOwner(TModifyScheme& modifyScheme, const TString& name, const TString& newOwner) {
    modifyScheme.SetOperationType(ESchemeOpModifyACL);
    auto& modifyACL = *modifyScheme.MutableModifyACL();
    modifyACL.SetNewOwner(newOwner);
    modifyACL.SetName(name);

    auto* condition = modifyScheme.AddApplyIf();
    condition->AddPathTypes(EPathType::EPathTypeSubDomain);
    condition->AddPathTypes(EPathType::EPathTypeExtSubDomain);
}

void FillAlterDatabaseSchemeLimits(TModifyScheme& modifyScheme, const TString& name, const NKikimrSubDomains::TSchemeLimits& in) {
    modifyScheme.SetOperationType(ESchemeOpAlterExtSubDomain);
    auto& subdomain = *modifyScheme.MutableSubDomain();
    subdomain.SetName(name);
    *subdomain.MutableSchemeLimits() = in;
}

std::pair<TString, TString> SplitPathByDirAndBaseNames(const TString& path) {
    auto splitPos = path.find_last_of('/');
    if (splitPos == path.npos || splitPos + 1 == path.size()) {
        ythrow yexception() << "wrong path format '" << path << "'";
    }
    return {path.substr(0, splitPos), path.substr(splitPos + 1)};
}

} // namespace NKikimr::NKqp::NSchemeHelpers
