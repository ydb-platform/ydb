#pragma once

#include <ydb/core/kqp/common/kqp.h>
#include <ydb/public/lib/deprecated/kicli/kicli.h>

#include <library/cpp/actors/core/actor.h>


namespace NKikimr::NSQS {

    void RunYqlQuery(
        const TString& query,
        std::optional<NKikimr::NClient::TParameters> params,
        bool readonly,
        TDuration sendAfter,
        const TString& database,
        const TActorContext& ctx
    );

} // namespace NKikimr::NSQS
