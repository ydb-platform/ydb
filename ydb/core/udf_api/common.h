#pragma once

#include "table_query.h"

#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/public/api/protos/ydb_udf.pb.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actorid.h>

#include <functional>

namespace NKikimr::NUdfApi {

//! The public UdfService only makes sense where the WASM UDF store is turned
//! on: without it nothing ever compiles or loads what an upload writes.
bool IsWasmUdfEnabled();

//! Runs `yql` as `metadata@system` and answers `replyTo` with
//! `NMetadata::NRequest::TEvRequestResult<TDialogYQLRequest>` or
//! `TEvRequestFailed`. Writes need the read-write transaction, so `readOnly`
//! also decides whether the statement commits.
void ExecuteYqlAsSystem(
    const NActors::TActorIdentity& replyTo,
    const TString& yql,
    bool readOnly,
    const std::function<void(Ydb::Table::ExecuteDataQueryRequest&)>& fillParams);

//! Whether the UDF store this node can reach is the one the request names. The
//! store lives in the tables of the tenant the node serves, and every query
//! here goes to that tenant whatever the request says, so a request for another
//! database has to be refused rather than answered from the wrong store.
bool IsDatabaseServedHere(const TString& databaseName, TString& error);

//! Resolves a database in order to read its owner, which is what makes someone
//! its administrator.
TEvTxProxySchemeCache::TEvNavigateKeySet* MakeDatabaseOwnerRequest(const TString& databaseName);
bool ParseDatabaseOwner(const NSchemeCache::TSchemeCacheNavigate& response, TString& owner);

//! Who may change the UDF store of a database. The store is per tenant and its
//! modules only run in queries of that tenant, so its administrator is the one
//! whose right it is; cluster administrators keep their usual reach over
//! everything. The database-admin half is gated on `EnableDatabaseAdmin` the
//! same way it is everywhere else in the tree, so a cluster that has not turned
//! it on keeps the old cluster-admin-only behaviour.
bool IsUdfStoreAdministrator(const NACLib::TUserToken* userToken, const TString& databaseOwner);

//! Whether `IsUdfStoreAdministrator` already has its answer without the
//! database owner, which saves resolving the database whenever the owner cannot
//! change the verdict anyway.
bool CanDecideWithoutDatabaseOwner(const NACLib::TUserToken* userToken);

//! Builds the scheme cache request for the children of the artifacts
//! directory. The set of platforms a cluster compiles for is whatever tables
//! happen to be there, so it cannot be derived from the config of the node
//! serving the request.
TEvTxProxySchemeCache::TEvNavigateKeySet* MakeArtifactDirListingRequest(const TString& databaseName);

//! Collects the `cpu_spec` of every artifact table in the listing, dropping the
//! `_chunks` companions. A missing directory yields an empty list rather than
//! an error: a cluster that has never compiled anything has no directory.
bool ParseArtifactDirListing(
    const NSchemeCache::TSchemeCacheNavigate& response,
    TVector<TString>& cpuSpecs);

inline constexpr TStringBuf NativeUnsupported = "Native UDF modules and libraries are not supported yet";
Ydb::Udf::ModuleType ToProtoType(NUdfStore::EUdfType type);
bool FromProtoType(Ydb::Udf::ModuleType type, NUdfStore::EUdfType& result);
Ydb::StatusIds::StatusCode ValidateKind(Ydb::Udf::ModuleKind kind, TString& error);
Ydb::StatusIds::StatusCode ValidateUpload(const Ydb::Udf::UploadModuleParams& params, TString& error);
Ydb::Udf::CompileStatus ToProtoCompileStatus(NUdfStore::ECompileStatus status);
bool FromProtoCompileStatus(Ydb::Udf::CompileStatus status, NUdfStore::ECompileStatus& result);

void FillModuleInfo(const NQuery::TModuleRow& row, Ydb::Udf::ModuleInfo& info);

} // namespace NKikimr::NUdfApi
