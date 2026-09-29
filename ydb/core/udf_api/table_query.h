#pragma once

#include <ydb/public/api/protos/ydb_table.pb.h>
#include <ydb/services/udf_store/metadata_subscription/udf_module.h>

#include <util/datetime/base.h>
#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NKikimr::NUdfApi::NQuery {

//! One `modules` row as the public API sees it. The API keys modules by name
//! alone, so a row read here is the module, whatever its type says.
struct TModuleRow {
    TString Name;
    TString Uid;
    TString Md5;
    ui64 Size = 0;
    NUdfStore::EUdfType Type = NUdfStore::EUdfType::WASM;
    ui64 Version = 0;
    ui64 ChunkCount = 0;
    TString Manifest;
    TInstant CreatedAt;
};

//! Reads a module by its primary key. WASM modules and libraries share the
//! name space, so the type is a column of the answer rather than part of the
//! lookup.
TString BuildSelectModuleByNameQuery(const TString& modulesTablePath);
void SetSelectModuleByNameParams(Ydb::Table::ExecuteDataQueryRequest& request, const TString& name);
//! Returns false when the query produced no row, i.e. the name is unknown.
bool ParseModuleRowResponse(const Ydb::Table::ExecuteDataQueryResponse& response, TModuleRow& row);

struct TListFilter {
    TMaybe<NUdfStore::EUdfType> Type;
};

//! Lists modules ordered by name so that the offset a page token carries keeps
//! pointing at the same place between two calls.
TString BuildListModulesQuery(const TString& modulesTablePath, const TListFilter& filter);
void SetListModulesParams(
    Ydb::Table::ExecuteDataQueryRequest& request,
    const TListFilter& filter,
    ui64 offset,
    ui64 limit);
bool ParseModuleRowsResponse(const Ydb::Table::ExecuteDataQueryResponse& response, TVector<TModuleRow>& rows);

//! What the `modules` row looked like before a conditional write, read in the
//! same transaction that performed it. `Applied` says whether the write went
//! through; when it did not, the rest of the fields are what the caller has to
//! turn into a status code.
struct TModulePreState {
    bool Existed = false;
    bool Applied = false;
    TString Uid;
    //! The type as stored, kept verbatim so that a row written by something
    //! that knows more types than this build can still be named in an error.
    TString TypeName;
    NUdfStore::EUdfType Type = NUdfStore::EUdfType::WASM;
    bool TypeKnown = false;
};

//! Preconditions of a write, checked against the row inside the writing
//! transaction. An empty `ExpectedUid` means no CAS was asked for.
struct TWriteConditions {
    bool RequireAbsent = false;
    bool RequirePresent = false;
    TString ExpectedUid;
};

//! Publishes an upload: reads the current row, decides whether every
//! precondition holds and upserts the new row, all in one transaction. The
//! preconditions cannot be checked in a transaction of their own — between the
//! read and the write another upload would slip through and neither
//! `write_mode` nor `expected_uid` would mean anything.
//!
//! `created_at` survives a replace because it belongs to the module rather than
//! to the upload.
TString BuildFlipModuleQuery(const TString& modulesTablePath, bool withManifest);
void SetFlipModuleParams(
    Ydb::Table::ExecuteDataQueryRequest& request,
    const TModuleRow& row,
    const TWriteConditions& conditions,
    bool versionGiven,
    bool withManifest);

//! Removes a module and its chunks in one transaction, so no window exists in
//! which the row is still listed while its body is already gone. The uid the
//! caller saw is part of the condition, so a concurrent upload cannot have its
//! row deleted by a delete that was aimed at the upload before it.
TString BuildDeleteModuleQuery(const TString& modulesTablePath, const TString& moduleChunksTablePath);
void SetDeleteModuleParams(
    Ydb::Table::ExecuteDataQueryRequest& request,
    const TString& name,
    const TString& expectedUid,
    TMaybe<NUdfStore::EUdfType> requiredType);

//! Reads the pre-state result set that the conditional writes above return.
bool ParseModulePreStateResponse(
    const Ydb::Table::ExecuteDataQueryResponse& response,
    TModulePreState& preState);

TString BuildDeleteSourceChunksQuery(const TString& moduleChunksTablePath);
void SetDeleteSourceChunksParams(Ydb::Table::ExecuteDataQueryRequest& request, const TString& ownerKey);

TString BuildUpsertSourceChunkQuery(const TString& moduleChunksTablePath);
void SetUpsertSourceChunkParams(
    Ydb::Table::ExecuteDataQueryRequest& request,
    const TString& ownerKey,
    ui64 chunkIdx,
    const TString& data);

struct TArtifactCompileState {
    NUdfStore::ECompileStatus Status = NUdfStore::ECompileStatus::Pending;
    TString Error;
    TMaybe<TInstant> StartedAt;
    TMaybe<TInstant> FinishedAt;
};

//! Reads persisted compile state for this upload on one platform. Absence is
//! reported as PENDING by the caller until reconciliation creates the row.
TString BuildSelectArtifactStateQuery(const TString& artifactTablePath);
void SetSelectArtifactStateParams(
    Ydb::Table::ExecuteDataQueryRequest& request,
    const TString& id,
    const TString& kind,
    const TString& uid);
bool ParseArtifactStateResponse(
    const Ydb::Table::ExecuteDataQueryResponse& response,
    TMaybe<TArtifactCompileState>& state);

//! Drops every artifact of a module, whichever upload built it. Only used on
//! delete, where no upload of that name is left to own them.
TString BuildDeleteArtifactsByIdQuery(const TString& tablePath);
void SetDeleteArtifactsByIdParams(
    Ydb::Table::ExecuteDataQueryRequest& request,
    const TString& id,
    const TString& kind);

//! Kind column of the artifact tables, which spells the module type
//! differently from `modules.type`.
TString ArtifactKindFor(NUdfStore::EUdfType type);

} // namespace NKikimr::NUdfApi::NQuery
