#pragma once
#include <ydb/core/kqp/provider/yql_kikimr_gateway.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/base/appdata_fwd.h>

#include <util/generic/string.h>
#include <util/string/join.h>

namespace NKikimr::NKqp::NSchemeHelpers {

TString CanonizePath(const TString& path);

template <typename TIter>
TString CombinePath(TIter begin, TIter end, bool canonize = true) {
    auto path = JoinRange("/", begin, end);
    return canonize
        ? CanonizePath(path)
        : path;
}

bool TrySplitTablePath(const TString& path, std::pair<TString, TString>& result, TString& error);

bool SplitTablePath(const TString& tableName, const TString& database, std::pair<TString, TString>& pathPair,
    TString& error, bool createDir);

TVector<TString> CreateIndexTablePath(const TString& tableName, const NYql::TIndexDescription& index);

TString GetDomainDatabase(const TAppData* appData);

TString SelectDatabaseForAlterLoginOperations(const TAppData* appData, const TString& requestDatabase);

void FillCreateExternalTableColumnDesc(NKikimrSchemeOp::TExternalTableDescription& externalTableDesc,
                                       const TString& name,
                                       bool replaceIfExists,
                                       const NYql::TCreateExternalTableSettings& settings);

bool Validate(const NYql::TAlterDatabaseSettings& settings, NYql::TIssue& error);

void FillAlterDatabaseOwner(NKikimrSchemeOp::TModifyScheme& modifyScheme, const TString& name, const TString& newOwner);

void FillAlterDatabaseSchemeLimits(NKikimrSchemeOp::TModifyScheme& modifyScheme, const TString& name, const NKikimrSubDomains::TSchemeLimits& in);

std::pair<TString, TString> SplitPathByDirAndBaseNames(const TString& path);

} // namespace NKikimr::NKqp::NSchemeHelpers
