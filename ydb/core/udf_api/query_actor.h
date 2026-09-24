#pragma once

#include <ydb/public/api/protos/ydb_udf.pb.h>

#include <ydb/library/actors/core/actor.h>

namespace NKikimr::NUdfApi {

//! Reads a page of the `modules` table. The page token is an offset into the
//! name-ordered listing, so a module added between two calls can shift the
//! page boundary; nothing here promises a snapshot across pages.
NActors::IActor* CreateListModulesActor(
    const NActors::TActorId& replyTo,
    const Ydb::Udf::ListModulesRequest& request);

//! Reads one module plus the persisted compile state of its current upload in
//! the artifact table of every known platform.
NActors::IActor* CreateDescribeModuleActor(
    const NActors::TActorId& replyTo,
    const Ydb::Udf::DescribeModuleRequest& request,
    const TString& databaseName);

} // namespace NKikimr::NUdfApi
