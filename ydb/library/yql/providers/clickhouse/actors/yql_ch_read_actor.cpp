#include "yql_ch_read_actor.h"

#include <ydb/library/yql/minikql/mkql_string_util.h>
#include <ydb/library/yql/utils/yql_panic.h>
#include <ydb/library/yql/providers/clickhouse/proto/range.pb.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>

#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/hfunc.h>

#include <queue>

namespace NYql::NDq {

using namespace NActors;

namespace {

struct TEvPrivate {
    // Event ids
    enum EEv : ui32 {
        EvBegin = EventSpaceBegin(TEvents::ES_PRIVATE),

        EvReadResult = EvBegin,
        EvReadError,

        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE)");

    // Events
    struct TEvReadResult : public TEventLocal<TEvReadResult, EvReadResult> {
        TEvReadResult(IHTTPGateway::TContent&& result): Result(std::move(result)) {}
        IHTTPGateway::TContent Result;
    };

    struct TEvReadError : public TEventLocal<TEvReadError, EvReadError> {
        TEvReadError(TIssues&& error): Error(std::move(error)) {}
        TIssues Error;
    };
};

} // namespace

class TClickHouseReadActor : public TActorBootstrapped<TClickHouseReadActor>, public IDqComputeActorAsyncInput {
public:
    TClickHouseReadActor(ui64 inputIndex,
        TCollectStatsLevel statsLevel,
        IHTTPGateway::TPtr gateway,
        TString&& url,
        TString&& query,
        const NActors::TActorId& computeActorId
    )   : Gateway(std::move(gateway))
        , InputIndex(inputIndex)
        , ComputeActorId(computeActorId)
        , ActorSystem(TActivationContext::ActorSystem())
        , Url(std::move(url))
        , Query(std::move(query))
    {
        IngressStats.Level = statsLevel;
    }

    void Bootstrap() {
        Become(&TClickHouseReadActor::StateFunc);
        Gateway->Upload(Url, {}, Query, std::bind(&TClickHouseReadActor::OnDownloadFinished, ActorSystem, SelfId(), std::placeholders::_1));
    }

    static constexpr char ActorName[] = "ClickHouse_READ_ACTOR";

private:
    void SaveState(const NDqProto::TCheckpoint&, NDq::TSourceState&) final {}
    void LoadState(const NDq::TSourceState&) final {}
    void CommitState(const NDqProto::TCheckpoint&) final {}

    ui64 GetInputIndex() const final {
        return InputIndex;
    }

    const TDqAsyncStats& GetIngressStats() const final {
        return IngressStats;
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvPrivate::TEvReadResult, Handle);
        hFunc(TEvPrivate::TEvReadError, Handle);
    )

    static void OnDownloadFinished(TActorSystem* actorSystem, TActorId selfId, IHTTPGateway::TResult&& result) {
        if (!result.Issues) {
            actorSystem->Send(new IEventHandle(selfId, TActorId(), new TEvPrivate::TEvReadResult(std::move(result.Content))));
        } else {
            actorSystem->Send(new IEventHandle(selfId, TActorId(), new TEvPrivate::TEvReadError(std::move(result.Issues))));
        }
    }

    i64 GetAsyncInputData(NKikimr::NMiniKQL::TUnboxedValueBatch& buffer, TMaybe<TInstant>&, bool& finished, i64 /*freeSpace*/) final {
        if (Result) {
            const auto size = Result->size();
            buffer.emplace_back(NKikimr::NMiniKQL::MakeString(std::string_view(*Result)));
            // freeSpace -= size;
            finished = true;
            Result.reset();
            return size;
        }

        return 0LL;
    }

    void Handle(TEvPrivate::TEvReadResult::TPtr& result) {
        Result.emplace(std::move(result->Get()->Result));
        Send(ComputeActorId, new TEvNewAsyncInputDataArrived(InputIndex));
    }

    void Handle(TEvPrivate::TEvReadError::TPtr& result) {
        Send(ComputeActorId, new TEvAsyncInputError(InputIndex, result->Get()->Error, NYql::NDqProto::StatusIds::EXTERNAL_ERROR));
    }

    // IActor & IDqComputeActorAsyncInput
    void PassAway() override { // Is called from Compute Actor
        TActorBootstrapped<TClickHouseReadActor>::PassAway();
    }


    const IHTTPGateway::TPtr Gateway;

    const ui64 InputIndex;
    TDqAsyncStats IngressStats;
    const NActors::TActorId ComputeActorId;

    TActorSystem* const ActorSystem;

    const TString Url, Query;
    std::optional<IHTTPGateway::TContent> Result;
};

std::pair<NYql::NDq::IDqComputeActorAsyncInput*, IActor*> CreateClickHouseReadActor(
    IHTTPGateway::TPtr gateway,
    NCH::TSource&& params,
    ui64 inputIndex,
    TCollectStatsLevel statsLevel,
    const THashMap<TString, TString>& secureParams,
    const THashMap<TString, TString>& taskParams,
    const NActors::TActorId& computeActorId,
    ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory)
{
    const auto token = secureParams.Value(params.GetToken(), TString{});
    const auto credentialsProviderFactory = CreateCredentialsProviderFactoryForStructuredToken(credentialsFactory, token);
    const auto authToken = credentialsProviderFactory->CreateProvider()->GetAuthInfo();
    const auto one = token.find('#'), two = token.rfind('#');
    YQL_ENSURE(one != TString::npos && two != TString::npos && one < two, "Bad token format:" << token);

    TStringBuilder part;
    if (const auto taskParamsIt = taskParams.find(ClickHouseProviderName); taskParamsIt != taskParams.cend()) {
        NCH::TRange range;
        TStringInput input(taskParamsIt->second);
        range.Load(&input);
        if (const auto& r = range.GetRange(); !r.empty())
            part << ' ' << r;
    }
    part << ';';

    TStringBuilder url;
    url << params.GetScheme() << token.substr(one + 1u, two - one - 1u) << ':' << token.substr(two + 1u) << '@' << params.GetEndpoint() << "/?default_format=Native";
    const auto actor = new TClickHouseReadActor(inputIndex, statsLevel, std::move(gateway), std::move(url), params.GetQuery() + part, computeActorId);
    return {actor, actor};
}

} // namespace NYql::NDq
