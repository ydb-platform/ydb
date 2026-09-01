#pragma once

#include "schema.h"

#include <library/cpp/containers/absl/flat_hash_set.h>

#include <util/generic/ptr.h>
#include <util/generic/string.h>

namespace NACLib {

class TUserToken;

} // namespace NACLib

namespace NKikimrPQ {

class TPQTabletConfig;

} // namespace NKikimrPQ

namespace NKikimr::NPQ::NSchema {

struct TEvCheckDlqTopicsResponse
    : public NActors::TEventLocal<TEvCheckDlqTopicsResponse, EEv::EvCheckDlqTopicsResponse>
{
    TEvCheckDlqTopicsResponse(
        Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS,
        TString&& errorMessage = {}
    )
        : Status(status)
        , ErrorMessage(std::move(errorMessage))
    {
    }

    Ydb::StatusIds::StatusCode Status;
    TString ErrorMessage;
};

struct TCheckDlqTopicsSettings {
    TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
    bool TopicsAreFirstClassCitizen = false;
};

// MOVE + enabled, skip empty and sqs:// paths. Paths are normalized against database.
absl::flat_hash_set<TString> CollectDlqTopicPaths(
    const NKikimrPQ::TPQTabletConfig& config,
    const TString& database,
    bool topicsAreFirstClassCitizen = false
);

// DLQ paths present in newConfig but not in oldConfig.
absl::flat_hash_set<TString> CollectNewDlqTopicPaths(
    const NKikimrPQ::TPQTabletConfig& newConfig,
    const NKikimrPQ::TPQTabletConfig& oldConfig,
    const TString& database,
    bool topicsAreFirstClassCitizen = false
);

// Builds the new-vs-old DLQ path diff. Returns nullptr when there is nothing to check
// (create: pass empty oldConfig).
NActors::IActor* CreateCheckDlqTopicsActorIfNeeded(
    const NActors::TActorId& parent,
    const TString& databasePath,
    const NKikimrPQ::TPQTabletConfig& newConfig,
    const NKikimrPQ::TPQTabletConfig& oldConfig,
    const TCheckDlqTopicsSettings& settings = {}
);

} // namespace NKikimr::NPQ::NSchema
