#include "scheme_helpers.h"

#include <ydb/core/base/path.h>
#include <ydb/core/base/table_index.h>
#include <ydb/core/protos/external_sources.pb.h>

namespace NKikimr::NKqp::NSchemeHelpers {

using namespace NKikimrSchemeOp;
using namespace NKikimrExternalSources;

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

TVector<TString> CreateIndexTablePath(const TString& tableName, NYql::TIndexDescription::EType indexType, const TString& indexName) {
    auto implTables = NTableIndex::GetImplTables(NYql::TIndexDescription::ConvertIndexType(indexType));
    TVector<TString> paths;
    paths.reserve(implTables.size());
    for (const auto& implTable : implTables) {
        paths.emplace_back(TStringBuilder() << tableName << "/" << indexName << "/" << implTable);
    }
    return paths;
}

bool SetDatabaseForLoginOperation(TString& result, bool getDomainLoginOnly, TMaybe<TString> domainName,
    const TString& database)
{
    if (getDomainLoginOnly && !domainName) {
        return false;
    }
    result = domainName ? "/" + *domainName : database;
    return true;
}

void FillCreateExternalTableColumnDesc(NKikimrSchemeOp::TExternalTableDescription& externalTableDesc,
                                       const TString& name,
                                       bool replaceIfExists,
                                       const NYql::TCreateExternalTableSettings& settings)
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
    auto& attributes = *general.mutable_attributes();
    for (const auto& [key, value]: settings.SourceTypeParameters) {
        attributes.insert({key, value});
    }
    attributes.insert({"location", settings.Location});
    externalTableDesc.SetContent(general.SerializeAsString());
}

std::pair<TString, TString> SplitPathByDirAndBaseNames(const TString& path) {
    auto splitPos = path.find_last_of('/');
    if (splitPos == path.npos || splitPos + 1 == path.size()) {
        ythrow yexception() << "wrong path format '" << path << "'";
    }
    return {path.substr(0, splitPos), path.substr(splitPos + 1)};
}

} // namespace NKikimr::NKqp::NSchemeHelpers
