#pragma once

#include <ydb/core/kqp/common/kqp.h>
#include <ydb/public/lib/deprecated/kicli/kicli.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>

#include <ydb/library/actors/core/actor.h>


namespace NKikimr::NSQS {

    void RunYqlQuery(
        const TString& query,
        std::optional<NYdb::TParams> params,
        bool readonly,
        TDuration sendAfter,
        const TString& database,
        const TActorContext& ctx
    );

} // namespace NKikimr::NSQS
