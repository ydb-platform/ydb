#pragma once

#include <ydb/public/api/protos/ydb_udf.pb.h>

#include <ydb/library/actors/core/actor.h>

namespace NKikimr::NUdfApi {

//! Writes one upload into `modules` and `module_chunks`. The body has already
//! been collected by the transport, so the actor only has to name it, verify
//! it and publish it; nothing here starts a compile — the WASM compile
//! controller picks the row up on its own.
NActors::IActor* CreateUploadModuleActor(
    const NActors::TActorId& replyTo,
    const Ydb::Udf::UploadModuleParams& params,
    TString body);

//! Removes a module together with its source chunks, then makes a best-effort
//! pass over the per-platform artifact tables. Artifacts left behind are dead
//! weight rather than a correctness problem, so failing to reach them does not
//! fail the delete.
NActors::IActor* CreateDeleteModuleActor(
    const NActors::TActorId& replyTo,
    const Ydb::Udf::DeleteModuleRequest& request,
    const TString& databaseName);

} // namespace NKikimr::NUdfApi
