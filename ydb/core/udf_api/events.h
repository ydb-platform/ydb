#pragma once

#include <ydb/public/api/protos/ydb_status_codes.pb.h>
#include <ydb/public/api/protos/ydb_udf.pb.h>

#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/events.h>

namespace NKikimr::NUdfApi {

enum EEv {
    EvUploadModuleResult = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
    EvDeleteModuleResult,
    EvListModulesResult,
    EvDescribeModuleResult,
    EvEnd
};

//! Outcome of one UdfService call carried out by an actor of this library.
//! `Status` is the gRPC-level verdict the caller puts into the operation;
//! `Result` is only meaningful when it is SUCCESS.
template <class TEvSelf, ui32 EventType, class TResultProto>
struct TEvUdfApiResultBase: public NActors::TEventLocal<TEvSelf, EventType> {
    Ydb::StatusIds::StatusCode Status = Ydb::StatusIds::SUCCESS;
    TString ErrorMessage;
    TResultProto Result;

    TEvUdfApiResultBase() = default;

    TEvUdfApiResultBase(Ydb::StatusIds::StatusCode status, TString errorMessage)
        : Status(status)
        , ErrorMessage(std::move(errorMessage))
    {}

    explicit TEvUdfApiResultBase(TResultProto result)
        : Result(std::move(result))
    {}
};

struct TEvUploadModuleResult
    : public TEvUdfApiResultBase<TEvUploadModuleResult, EvUploadModuleResult, Ydb::Udf::UploadModuleResult>
{
    using TEvUdfApiResultBase::TEvUdfApiResultBase;
};

struct TEvDeleteModuleResult
    : public TEvUdfApiResultBase<TEvDeleteModuleResult, EvDeleteModuleResult, Ydb::Udf::DeleteModuleResult>
{
    using TEvUdfApiResultBase::TEvUdfApiResultBase;
};

struct TEvListModulesResult
    : public TEvUdfApiResultBase<TEvListModulesResult, EvListModulesResult, Ydb::Udf::ListModulesResult>
{
    using TEvUdfApiResultBase::TEvUdfApiResultBase;
};

struct TEvDescribeModuleResult
    : public TEvUdfApiResultBase<TEvDescribeModuleResult, EvDescribeModuleResult, Ydb::Udf::DescribeModuleResult>
{
    using TEvUdfApiResultBase::TEvUdfApiResultBase;
};

} // namespace NKikimr::NUdfApi
