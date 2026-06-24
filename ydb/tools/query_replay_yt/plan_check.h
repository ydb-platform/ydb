#pragma once

#include "query_replay.h"

#include <library/cpp/json/json_value.h>

#include <functional>
#include <map>

namespace NKikimr::NQueryReplay {

using TTableMetadataLookup = std::function<const NYql::TKikimrTableMetadataPtr*(const TString&)>;

NJson::TJsonValue ParseQueryPlan(const TString& plan);

std::map<std::string, TTableStats> ExtractTableStats(
    const NJson::TJsonValue& plan,
    const TTableMetadataLookup& metadataLookup);

std::pair<TQueryReplayEvents::TCheckQueryPlanStatus, TString> CompareTableStats(
    const TTableStats& oldEngineStats,
    const TTableStats& newEngineStats);

std::pair<TQueryReplayEvents::TCheckQueryPlanStatus, TString> CheckQueryPlans(
    const TString& oldPlanJson,
    const NJson::TJsonValue& newPlan,
    const TTableMetadataLookup& metadataLookup,
    std::map<std::string, TTableStats>* newEngineStats = nullptr);

TString StatusToFailReason(TQueryReplayEvents::TCheckQueryPlanStatus status);

} // namespace NKikimr::NQueryReplay
