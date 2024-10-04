#pragma once

#include <ydb/core/tx/tiering/common.h>
#include <ydb/core/tx/tiering/rule/object.h>

#include <library/cpp/threading/future/core/future.h>

namespace NKikimr::NColumnShard {

namespace NTiers {

class TEvListTieringRulesResult: public TEventLocal<TEvListTieringRulesResult, EvListTieringRulesResult> {
private:
    using TTieringRules = THashMap<TString, TTieringRule>;
    TConclusion<TTieringRules> Result;

public:
    TEvListTieringRulesResult(TConclusion<TTieringRules> result)
        : Result(std::move(result)) {
    }

    const TConclusion<TTieringRules>& GetResult() const {
        return Result;
    }
};

}   // namespace NTiers

NThreading::TFuture<TConclusion<THashMap<TString, NTiers::TTieringRule>>> ListTieringRules(const TActorContext& ctx);

}   // namespace NKikimr::NColumnShard
