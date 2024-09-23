#pragma once

#include "kqp_session_actor.h"

#include <ydb/core/docapi/traits.h>
#include <ydb/core/kqp/common/kqp.h>
#include <ydb/core/kqp/provider/yql_kikimr_gateway.h>
#include <ydb/core/kqp/provider/yql_kikimr_provider.h>
#include <ydb/core/kqp/provider/yql_kikimr_settings.h>
#include <ydb/core/protos/kqp.pb.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/log.h>
#include <library/cpp/json/json_reader.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>
#include <util/string/escape.h>

namespace NKikimr::NKqp {

struct TSessionShutdownState {
    TSessionShutdownState(ui32 softTimeout, ui32 hardTimeout)
        : HardTimeout(hardTimeout)
        , SoftTimeout(softTimeout)
    {}

    ui32 Step = 0;
    ui32 HardTimeout;
    ui32 SoftTimeout;

    void MoveToNextState() {
        ++Step;
    }

    ui32 GetNextTickMs() const {
        if (Step == 0) {
            return std::min(HardTimeout, SoftTimeout);
        } else if (Step == 1) {
            return std::max(HardTimeout, SoftTimeout) - std::min(HardTimeout, SoftTimeout) + 1;
        } else {
            return 50;
        }
    }

    bool SoftTimeoutReached() const {
        return Step == 1;
    }

    bool HardTimeoutReached() const {
        return Step == 2;
    }
};

inline bool IsExecuteAction(const NKikimrKqp::EQueryAction& action) {
    switch (action) {
        case NKikimrKqp::QUERY_ACTION_EXECUTE:
        case NKikimrKqp::QUERY_ACTION_EXECUTE_PREPARED:
            return true;

        default:
            return false;
    }
}

inline bool IsQueryAllowedToLog(const TString& text) {
    static const TString user = "user";
    static const TString password = "password";
    auto itUser = std::search(text.begin(), text.end(), user.begin(), user.end(),
        [](const char a, const char b) -> bool { return std::tolower(a) == b; });
    if (itUser == text.end()) {
        return true;
    }
    auto itPassword = std::search(itUser, text.end(), password.begin(), password.end(),
        [](const char a, const char b) -> bool { return std::tolower(a) == b; });
    return itPassword == text.end();
}

inline TIntrusivePtr<NYql::TKikimrConfiguration> CreateConfig(const TKqpSettings::TConstPtr& kqpSettings,
    const TKqpWorkerSettings& workerSettings)
{
    auto cfg = MakeIntrusive<NYql::TKikimrConfiguration>();
    cfg->Init(kqpSettings->DefaultSettings.GetDefaultSettings(), workerSettings.Cluster,
            kqpSettings->Settings, false);

    if (!workerSettings.Database.empty()) {
        cfg->_KqpTablePathPrefix = workerSettings.Database;
    }

    ApplyServiceConfig(*cfg, workerSettings.TableService);

    cfg->FreezeDefaults();
    return cfg;
}

inline ETableReadType ExtractMostHeavyReadType(const TString& queryPlan) {
    ETableReadType maxReadType = ETableReadType::Other;

    if (queryPlan.empty()) {
        return maxReadType;
    }

    NJson::TJsonValue root;
    NJson::ReadJsonTree(queryPlan, &root, false);

    if (root.Has("tables")) {
        for (const auto& table : root["tables"].GetArray()) {
            if (!table.Has("reads")) {
                continue;
            }

            for (const auto& read : table["reads"].GetArray()) {
                Y_ABORT_UNLESS(read.Has("type"));
                const auto& type = read["type"].GetString();

                if (type == "Scan") {
                    maxReadType = Max(maxReadType, ETableReadType::Scan);
                } else if (type == "FullScan") {
                    return ETableReadType::FullScan;
                }
            }
        }
    }

    return maxReadType;
}

bool CanCacheQuery(const NKqpProto::TKqpPhyQuery& query);

void SlowLogQuery(const TActorContext &ctx, const NYql::TKikimrConfiguration* config, const TKqpRequestInfo& requestInfo,
    const TDuration& duration, Ydb::StatusIds::StatusCode status, const TIntrusiveConstPtr<NACLib::TUserToken>& userToken, ui64 parametersSize,
    NKikimrKqp::TEvQueryResponse *record, const std::function<TString()> extractQueryText);

NYql::TKikimrQueryLimits GetQueryLimits(const TKqpWorkerSettings& settings);

inline bool IsDocumentApiRestricted(const TString& requestType) {
    return requestType != NDocApi::RequestType;
}

TMaybe<Ydb::StatusIds::StatusCode> GetYdbStatus(const NYql::TIssue& issue);
Ydb::StatusIds::StatusCode GetYdbStatus(const NYql::NCommon::TOperationResult& queryResult);
Ydb::StatusIds::StatusCode GetYdbStatus(const NYql::TIssues& issues);
void AddQueryIssues(NKikimrKqp::TQueryResponse& response, const NYql::TIssues& issues);
bool HasSchemeOrFatalIssues(const NYql::TIssues& issues);

IActor* CreateKqpWorkerActor(const TActorId& owner, const TString& sessionId,
    const TKqpSettings::TConstPtr& kqpSettings, const TKqpWorkerSettings& workerSettings,
    std::optional<TKqpFederatedQuerySetup> federatedQuerySetup,
    TIntrusivePtr<TModuleResolverState> moduleResolverState,
    TIntrusivePtr<TKqpCounters> counters,
    const NKikimrConfig::TQueryServiceConfig& queryServiceConfig,
    const TGUCSettings::TPtr& gUCSettings
    );

bool IsSameProtoType(const NKikimrMiniKQL::TType& actual, const NKikimrMiniKQL::TType& expected);

} // namespace NKikimr::NKqp
