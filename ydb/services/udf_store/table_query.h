#pragma once

#include "metadata_subscription/udf_module.h"

#include <ydb/public/api/protos/ydb_table.pb.h>

#include <util/datetime/base.h>
#include <util/generic/maybe.h>
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
    TString Manifest;
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

//! Storage state exists before any object code has been published. An absent
//! timestamp is distinct from the Unix epoch.
struct TArtifactCompileState {
    ECompileStatus Status = ECompileStatus::Pending;
    TString Error;
    TMaybe<TInstant> StartedAt;
    TMaybe<TInstant> FinishedAt;
};

//! Creates PENDING only when the key is absent; repeated reconciliation must
//! never reset an in-flight, failed or ready artifact. Uses SetSelectArtifactParams.
TString BuildEnsurePendingArtifactQuery(const TString& tablePath);
//! Starts a pending or failed artifact; resets diagnostics from the last attempt.
//! Uses SetSelectArtifactParams. Assignment fencing is the caller's responsibility.
TString BuildMarkArtifactCompilingQuery(const TString& tablePath);
//! Records a terminal failure of a compiling artifact. Retry policy belongs to
//! the controller: do not use this for a failure that is still being retried.
TString BuildMarkArtifactFailedQuery(const TString& tablePath);
void SetMarkArtifactFailedParams(Ydb::Table::ExecuteDataQueryRequest& request,
    const TString& id, const TString& kind, const TString& uid, const TString& error);
TString BuildSelectArtifactCompileStateQuery(const TString& tablePath);
//! Missing row is a successful read with an empty state; malformed results fail.
bool ParseArtifactCompileStateResponse(const Ydb::Table::ExecuteDataQueryResponse& response,
    TMaybe<TArtifactCompileState>& state);

TString BuildSelectModuleByNameQuery(const TString& tablePath);
void SetSelectModuleByNameParams(
    Ydb::Table::ExecuteDataQueryRequest& request,
    const TString& name,
    const TString& type);

bool ParseModuleSourceResponse(const Ydb::Table::ExecuteDataQueryResponse& response, TModuleSourceRow& row);

// Four 8 MiB chunks leave room for result metadata below the 48 MiB query limit.
constexpr ui64 ChunksPerRead = 4;

TString BuildSelectSourceChunksQuery(const TString& tablePath);
void SetSelectSourceChunksParams(Ydb::Table::ExecuteDataQueryRequest& request, const TString& ownerKey, ui64 firstChunk);
//! Append a page, rejecting truncated results, gaps and duplicate indices.
//! The next page starts at chunks.size(); a short page ends the read.
bool AppendSourceChunksResponse(const Ydb::Table::ExecuteDataQueryResponse& response, TVector<TString>& chunks);

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
    const TString& blobKind,
    ui64 firstChunk);
bool AppendArtifactChunksResponse(const Ydb::Table::ExecuteDataQueryResponse& response, TVector<TString>& chunks);

//! Publishes READY and its completion time together with payload metadata,
//! preserving compile_started_at. The caller must have written all chunks first.
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

bool ExtractQueryResult(
    const Ydb::Table::ExecuteDataQueryResponse& response,
    Ydb::Table::ExecuteQueryResult& result);

} // namespace NKikimr::NUdfStore::NTableQuery
