#pragma once

#include "metadata_subscription/udf_module.h"

#include <ydb/public/api/protos/ydb_table.pb.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NKikimr::NUdfStore::NTableQuery {

struct TModuleSourceRow {
    TString Uid;
    TString Md5;
    TString Name;
    TString Type;
    ui64 Version = 0;
    ui64 Size = 0;
    ui64 ChunkCount = 0;
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
    ui64 WasmDataSize = 0;
    ui64 WasmDataChunkCount = 0;
    ui64 ObjectCodeSize = 0;
    ui64 ObjectCodeChunkCount = 0;
    TString WasmData;
    TString ObjectCode;
};

struct TPendingChunkWrite {
    TString BlobKind;
    ui64 ChunkIdx = 0;
    TString Data;
};

TString BuildSelectModuleByNameQuery(const TString& tablePath);
void SetSelectModuleByNameParams(
    Ydb::Table::ExecuteDataQueryRequest& request,
    const TString& name,
    const TString& type);

bool ParseModuleSourceResponse(const Ydb::Table::ExecuteDataQueryResponse& response, TModuleSourceRow& row);

TString BuildSelectSourceChunksQuery(const TString& tablePath);
void SetSelectSourceChunksParams(Ydb::Table::ExecuteDataQueryRequest& request, const TString& ownerKey);
bool ParseSourceChunksResponse(const Ydb::Table::ExecuteDataQueryResponse& response, TVector<TString>& chunks);

TString BuildSelectArtifactQuery(const TString& tablePath);
void SetSelectArtifactParams(
    Ydb::Table::ExecuteDataQueryRequest& request,
    const TString& id,
    const TString& kind);
bool ParseArtifactResponse(const Ydb::Table::ExecuteDataQueryResponse& response, TWasmArtifactRow& row);

TString BuildSelectArtifactChunksQuery(const TString& tablePath);
void SetSelectArtifactChunksParams(
    Ydb::Table::ExecuteDataQueryRequest& request,
    const TString& id,
    const TString& kind,
    const TString& blobKind);
bool ParseArtifactChunksResponse(const Ydb::Table::ExecuteDataQueryResponse& response, TVector<TString>& chunks);

TString BuildUpsertArtifactQuery(const TString& tablePath);
void SetUpsertArtifactParams(
    Ydb::Table::ExecuteDataQueryRequest& request,
    const TWasmArtifactRow& row);

TString BuildDeleteArtifactChunksQuery(const TString& tablePath);
void SetDeleteArtifactChunksParams(
    Ydb::Table::ExecuteDataQueryRequest& request,
    const TString& id,
    const TString& kind);

TString BuildUpsertArtifactChunkQuery(const TString& tablePath);
void SetUpsertArtifactChunkParams(
    Ydb::Table::ExecuteDataQueryRequest& request,
    const TString& id,
    const TString& kind,
    const TString& blobKind,
    ui64 chunkIdx,
    const TString& data);

TString BuildUpdateCompileStatusQuery(const TString& tablePath);
void SetUpdateCompileStatusParams(
    Ydb::Table::ExecuteDataQueryRequest& request,
    const TString& name,
    const TString& type,
    const TString& status,
    const TString& errorMessage);

bool ExtractQueryResult(
    const Ydb::Table::ExecuteDataQueryResponse& response,
    Ydb::Table::ExecuteQueryResult& result);

} // namespace NKikimr::NUdfStore::NTableQuery
