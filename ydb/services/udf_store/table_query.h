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
    TString Uid;
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

//! Artifacts are keyed by the upload they were built from, so a lookup that
//! finds nothing means this upload has not been compiled here yet, not that
//! the module is unknown.
TString BuildSelectArtifactQuery(const TString& tablePath);
void SetSelectArtifactParams(
    Ydb::Table::ExecuteDataQueryRequest& request,
    const TString& id,
    const TString& kind,
    const TString& uid);
bool ParseArtifactResponse(const Ydb::Table::ExecuteDataQueryResponse& response, TWasmArtifactRow& row);

//! Identity of an artifact without any of its payload. The compile controller
//! only needs to know which `(id, kind, uid)` are already covered on a
//! platform, so it never reads object code.
struct TArtifactKeyRow {
    TString Id;
    TString Kind;
    TString Uid;
};

//! Lists every finished artifact of one platform in a single read. Rows whose
//! object code is not written yet are skipped: a half-published artifact does
//! not close a gap.
TString BuildSelectArtifactKeysQuery(const TString& tablePath);
bool ParseArtifactKeysResponse(
    const Ydb::Table::ExecuteDataQueryResponse& response,
    TVector<TArtifactKeyRow>& rows);

TString BuildSelectArtifactChunksQuery(const TString& tablePath);
void SetSelectArtifactChunksParams(
    Ydb::Table::ExecuteDataQueryRequest& request,
    const TString& id,
    const TString& kind,
    const TString& uid,
    const TString& blobKind);
bool ParseArtifactChunksResponse(const Ydb::Table::ExecuteDataQueryResponse& response, TVector<TString>& chunks);

TString BuildUpsertArtifactQuery(const TString& tablePath);
void SetUpsertArtifactParams(
    Ydb::Table::ExecuteDataQueryRequest& request,
    const TWasmArtifactRow& row);

//! Clears a partial write of this very upload before it is retried. Scoped by
//! uid: at this point the compile has not yet confirmed that its upload is
//! still the current one, so it has no business touching anybody else's rows.
TString BuildDeleteArtifactChunksQuery(const TString& tablePath);
void SetDeleteArtifactChunksParams(
    Ydb::Table::ExecuteDataQueryRequest& request,
    const TString& id,
    const TString& kind,
    const TString& uid);

//! Drops everything compiled for this module from uploads other than `uid`.
//! Gated on `modules` still carrying that same uid in the same statement: a
//! compile that lost the race to a re-upload must not wipe the winner's rows.
TString BuildDeleteStaleArtifactChunksQuery(
    const TString& artifactChunksTablePath,
    const TString& modulesTablePath);
TString BuildDeleteStaleArtifactsQuery(
    const TString& artifactTablePath,
    const TString& modulesTablePath);
void SetDeleteStaleArtifactsParams(
    Ydb::Table::ExecuteDataQueryRequest& request,
    const TString& id,
    const TString& kind,
    const TString& uid,
    const TString& type);

TString BuildUpsertArtifactChunkQuery(const TString& tablePath);
void SetUpsertArtifactChunkParams(
    Ydb::Table::ExecuteDataQueryRequest& request,
    const TString& id,
    const TString& kind,
    const TString& uid,
    const TString& blobKind,
    ui64 chunkIdx,
    const TString& data);

//! Scoped by uid as well as name and type: the compile that started for an
//! earlier upload must not overwrite the status of the one that replaced it.
TString BuildUpdateCompileStatusQuery(const TString& tablePath);
void SetUpdateCompileStatusParams(
    Ydb::Table::ExecuteDataQueryRequest& request,
    const TString& name,
    const TString& type,
    const TString& uid,
    const TString& status,
    const TString& errorMessage);

bool ExtractQueryResult(
    const Ydb::Table::ExecuteDataQueryResponse& response,
    Ydb::Table::ExecuteQueryResult& result);

} // namespace NKikimr::NUdfStore::NTableQuery
