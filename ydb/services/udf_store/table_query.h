#pragma once

#include "metadata_subscription/udf_meta.h"

#include <ydb/public/api/protos/ydb_table.pb.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NKikimr::NUdfStore::NTableQuery {

struct TWasmSourceRow {
    TString Md5;
    ui64 Version = 0;
    TString Body;
};

struct TLibrarySourceRow {
    TString Name;
    TString Md5;
    ui64 Version = 0;
    TString Body;
    ECompileStatus CompileStatus = ECompileStatus::Pending;
    TString CompileError;
};

struct TWasmArtifactRow {
    TString Id;
    TString Kind;
    TString SourceMd5;
    ui64 Version = 0;
    TString Format;
    TString WasmData;
    TString ObjectCode;
};

TString BuildSelectWasmSourceQuery(const TString& tablePath);
void SetSelectWasmSourceParams(Ydb::Table::ExecuteDataQueryRequest& request, const TString& md5);
bool ParseWasmSourceResponse(const Ydb::Table::ExecuteDataQueryResponse& response, TWasmSourceRow& row);

TString BuildSelectLibrarySourceQuery(const TString& tablePath);
void SetSelectLibrarySourceParams(Ydb::Table::ExecuteDataQueryRequest& request, const TString& name);
bool ParseLibrarySourceResponse(const Ydb::Table::ExecuteDataQueryResponse& response, TLibrarySourceRow& row);

TString BuildSelectArtifactQuery(const TString& tablePath);
void SetSelectArtifactParams(
    Ydb::Table::ExecuteDataQueryRequest& request,
    const TString& id,
    const TString& kind);
bool ParseArtifactResponse(const Ydb::Table::ExecuteDataQueryResponse& response, TWasmArtifactRow& row);

TString BuildUpsertArtifactQuery(const TString& tablePath);
void SetUpsertArtifactParams(
    Ydb::Table::ExecuteDataQueryRequest& request,
    const TWasmArtifactRow& row);

TString BuildUpdateLibraryCompileStatusQuery(const TString& tablePath);
void SetUpdateLibraryCompileStatusParams(
    Ydb::Table::ExecuteDataQueryRequest& request,
    const TString& name,
    const TString& status,
    const TString& errorMessage);

TString BuildUpdateCompileStatusQuery(const TString& tablePath);
void SetUpdateCompileStatusParams(
    Ydb::Table::ExecuteDataQueryRequest& request,
    const TString& md5,
    const TString& status,
    const TString& errorMessage);

bool ExtractQueryResult(
    const Ydb::Table::ExecuteDataQueryResponse& response,
    Ydb::Table::ExecuteQueryResult& result);

} // namespace NKikimr::NUdfStore::NTableQuery
