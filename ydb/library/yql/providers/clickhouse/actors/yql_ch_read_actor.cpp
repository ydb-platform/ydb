#include "yql_ch_read_actor.h" 
 
#include <ydb/library/yql/minikql/mkql_string_util.h>
#include <ydb/library/yql/utils/yql_panic.h>
#include <ydb/library/yql/providers/clickhouse/proto/range.pb.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
 
#include <library/cpp/actors/core/actor_bootstrapped.h> 
#include <library/cpp/actors/core/events.h> 
#include <library/cpp/actors/core/event_local.h> 
#include <library/cpp/actors/core/hfunc.h> 
 
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
 
class TClickHouseReadActor : public TActorBootstrapped<TClickHouseReadActor>, public IDqSourceActor { 
public: 
    TClickHouseReadActor(ui64 inputIndex, 
        IHTTPGateway::TPtr gateway, 
        TString&& url, 
        TString&& query, 
        ICallbacks* callbacks 
    )   : Gateway(std::move(gateway)) 
        , InputIndex(inputIndex) 
        , Callbacks(callbacks) 
        , ActorSystem(TActivationContext::ActorSystem()) 
        , Url(std::move(url)) 
        , Query(std::move(query)) 
    {} 
 
    void Bootstrap() { 
        Become(&TClickHouseReadActor::StateFunc); 
        Gateway->Download(Url, {}, 0U, std::bind(&TClickHouseReadActor::OnDownloadFinished, ActorSystem, SelfId(), std::placeholders::_1), Query); 
    } 
 
    static constexpr char ActorName[] = "ClickHouse_READ_ACTOR"; 
 
private: 
    void SaveState(const NDqProto::TCheckpoint&, NDqProto::TSourceState&) final {} 
    void LoadState(const NDqProto::TSourceState&) final {} 
    void CommitState(const NDqProto::TCheckpoint&) final {} 
    ui64 GetInputIndex() const final { return InputIndex; } 
 
    STRICT_STFUNC(StateFunc, 
        hFunc(TEvPrivate::TEvReadResult, Handle); 
        hFunc(TEvPrivate::TEvReadError, Handle); 
    ) 
 
    static void OnDownloadFinished(TActorSystem* actorSystem, TActorId selfId, IHTTPGateway::TResult&& result) { 
        switch (result.index()) { 
        case 0U: 
            actorSystem->Send(new IEventHandle(selfId, TActorId(), new TEvPrivate::TEvReadResult(std::get<IHTTPGateway::TContent>(std::move(result))))); 
            return; 
        case 1U: 
            actorSystem->Send(new IEventHandle(selfId, TActorId(), new TEvPrivate::TEvReadError(std::get<TIssues>(std::move(result))))); 
            return; 
        default: 
            break; 
        } 
    } 
 
    i64 GetSourceData(NKikimr::NMiniKQL::TUnboxedValueVector& buffer, bool& finished, i64 freeSpace) final { 
        if (Result) { 
            const auto size = Result->size(); 
            buffer.emplace_back(NKikimr::NMiniKQL::MakeString(std::string_view(*Result))); 
            freeSpace -= size; 
            finished = true; 
            Result.reset(); 
            return size; 
        } 
 
        return 0LL; 
    } 
 
    void Handle(TEvPrivate::TEvReadResult::TPtr& result) { 
        Result.emplace(std::move(result->Get()->Result)); 
        Callbacks->OnNewSourceDataArrived(InputIndex); 
    } 
 
    void Handle(TEvPrivate::TEvReadError::TPtr& result) { 
        Callbacks->OnSourceError(InputIndex, result->Get()->Error, true); 
    } 
 
    // IActor & IDqSourceActor 
    void PassAway() override { // Is called from Compute Actor 
        TActorBootstrapped<TClickHouseReadActor>::PassAway(); 
    } 
 
 
    const IHTTPGateway::TPtr Gateway; 
 
    const ui64 InputIndex; 
    ICallbacks *const Callbacks; 
 
    TActorSystem* const ActorSystem; 
 
    const TString Url, Query; 
    std::optional<IHTTPGateway::TContent> Result; 
}; 
 
std::pair<NYql::NDq::IDqSourceActor*, IActor*> CreateClickHouseReadActor( 
    IHTTPGateway::TPtr gateway, 
    NCH::TSource&& params, 
    ui64 inputIndex, 
    const THashMap<TString, TString>& secureParams, 
    const THashMap<TString, TString>& taskParams, 
    NYql::NDq::IDqSourceActor::ICallbacks* callback, 
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
    const auto actor = new TClickHouseReadActor(inputIndex, std::move(gateway), std::move(url), params.GetQuery() + part, callback); 
    return {actor, actor}; 
} 
 
} // namespace NYql::NDq 
 
