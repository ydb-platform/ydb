#include "yql_s3_read_actor.h"

#include <ydb/library/yql/minikql/mkql_string_util.h>
#include <ydb/library/yql/utils/yql_panic.h>
#include <ydb/library/yql/providers/s3/proto/range.pb.h>
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
        EvRetry, 

        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE)");

    // Events
    struct TEvReadResult : public TEventLocal<TEvReadResult, EvReadResult> {
        TEvReadResult(IHTTPGateway::TContent&& result): Result(std::move(result)) {}
        IHTTPGateway::TContent Result;
    };

    struct TEvReadError : public TEventLocal<TEvReadError, EvReadError> {
        TEvReadError(TIssues&& error, size_t pathInd): Error(std::move(error)), PathIndex(pathInd) {} 
        TIssues Error;
        size_t PathIndex = 0; 
    };
 
    struct TEvRetryEvent : public NActors::TEventLocal<TEvRetryEvent, EvRetry> { 
        explicit TEvRetryEvent(size_t pathIndex) : PathIndex(pathIndex) {} 
        size_t PathIndex = 0; 
    }; 
};

} // namespace

class TS3ReadActor : public TActorBootstrapped<TS3ReadActor>, public IDqSourceActor {
private: 
    class TRetryParams { 
    public: 
        TRetryParams(const std::shared_ptr<NS3::TRetryConfig>& retryConfig) { 
            if (retryConfig) { 
                DelayMs = retryConfig->GetInitialDelayMs() ? TDuration::MilliSeconds(retryConfig->GetInitialDelayMs()) : DelayMs; 
                Epsilon = retryConfig->GetEpsilon() ? retryConfig->GetEpsilon() : Epsilon; 
                Y_VERIFY(0.0 < Epsilon && Epsilon < 1.0); 
            } 
        } 
 
        TDuration GetNextDelay(ui32 maxRetries) { 
            if (Retries > maxRetries) return TDuration::Zero(); 
            return DelayMs = GenerateNextDelay(); 
        }; 
 
        void IncRetries() { 
            ++Retries; 
        } 
 
    private: 
        TDuration GenerateNextDelay() { 
            double low = 1 - Epsilon; 
            auto jitter = low + std::rand() / (RAND_MAX / (2 * Epsilon)); 
            return DelayMs * jitter; 
        } 
 
        ui32 Retries = 0; 
        TDuration DelayMs = TDuration::MilliSeconds(100); 
        double Epsilon = 0.1; 
    }; 
public:
using TPath = std::tuple<TString, size_t>;
using TPathList = std::vector<TPath>;

    TS3ReadActor(ui64 inputIndex,
        IHTTPGateway::TPtr gateway,
        const TString& url,
        const TString& token,
        TPathList&& paths,
        ICallbacks* callbacks, 
        const std::shared_ptr<NS3::TRetryConfig>& retryConfig 
    )   : Gateway(std::move(gateway))
        , InputIndex(inputIndex)
        , Callbacks(callbacks)
        , ActorSystem(TActivationContext::ActorSystem())
        , Url(url)
        , Headers(MakeHeader(token))
        , Paths(std::move(paths))
        , RetryConfig(retryConfig) 
    { 
        if (RetryConfig) { 
            MaxRetriesPerPath = RetryConfig->GetMaxRetriesPerPath() ? RetryConfig->GetMaxRetriesPerPath() : MaxRetriesPerPath; 
        } 
    } 

    void Bootstrap() {
        Become(&TS3ReadActor::StateFunc);
        RetriesPerPath.resize(Paths.size(), TRetryParams(RetryConfig)); 
        for (size_t pathInd = 0; pathInd < Paths.size(); ++pathInd) { 
            const TPath& path = Paths[pathInd]; 
            Gateway->Download(Url + std::get<TString>(path), 
                Headers, std::get<size_t>(path), 
                std::bind(&TS3ReadActor::OnDownloadFinished, ActorSystem, SelfId(), std::placeholders::_1, pathInd)); 
        }; 
    }
 
    static constexpr char ActorName[] = "S3_READ_ACTOR"; 
 
private:
    void SaveState(const NDqProto::TCheckpoint&, NDqProto::TSourceState&) final {}
    void LoadState(const NDqProto::TSourceState&) final {}
    void CommitState(const NDqProto::TCheckpoint&) final {}
    ui64 GetInputIndex() const final { return InputIndex; }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvPrivate::TEvReadResult, Handle);
        hFunc(TEvPrivate::TEvReadError, Handle);
        hFunc(TEvPrivate::TEvRetryEvent, HandleRetry); 
    )

    static void OnDownloadFinished(TActorSystem* actorSystem, TActorId selfId, IHTTPGateway::TResult&& result, size_t pathInd) { 
        switch (result.index()) {
        case 0U:
            actorSystem->Send(new IEventHandle(selfId, TActorId(), new TEvPrivate::TEvReadResult(std::get<IHTTPGateway::TContent>(std::move(result)))));
            return;
        case 1U:
            actorSystem->Send(new IEventHandle(selfId, TActorId(), new TEvPrivate::TEvReadError(std::get<TIssues>(std::move(result)), pathInd))); 
            return;
        default:
            break;
        }
    }

    i64 GetSourceData(NKikimr::NMiniKQL::TUnboxedValueVector& buffer, bool& finished, i64 freeSpace) final {
        i64 total = 0LL;
        if (!Blocks.empty()) {
            buffer.reserve(buffer.size() + Blocks.size());
            do {
                const auto size = Blocks.front().size();
                buffer.emplace_back(NKikimr::NMiniKQL::MakeString(std::string_view(Blocks.front())));
                Blocks.pop();
                total += size;
                freeSpace -= size;
            } while (!Blocks.empty() && freeSpace > 0LL);
        }

        if (Blocks.empty() && IsDoneCounter == Paths.size()) {
            finished = true;
        }

        return total;
    }


    void Handle(TEvPrivate::TEvReadResult::TPtr& result) {
        ++IsDoneCounter;
        Blocks.emplace(std::move(result->Get()->Result));
        Callbacks->OnNewSourceDataArrived(InputIndex);
    }

    void HandleRetry(TEvPrivate::TEvRetryEvent::TPtr& ev) { 
        const auto pathInd = ev->Get()->PathIndex; 
        RetriesPerPath[pathInd].IncRetries(); 
        Gateway->Download(Url + std::get<TString>(Paths[pathInd]), 
            Headers, std::get<size_t>(Paths[pathInd]), 
            std::bind(&TS3ReadActor::OnDownloadFinished, ActorSystem, SelfId(), std::placeholders::_1, pathInd)); 
    } 
 
    void Handle(TEvPrivate::TEvReadError::TPtr& result) {
        const auto pathInd = result->Get()->PathIndex; 
        Y_VERIFY(pathInd < RetriesPerPath.size()); 
        if (auto nextDelayMs = RetriesPerPath[pathInd].GetNextDelay(MaxRetriesPerPath)) { 
            Schedule(nextDelayMs, new TEvPrivate::TEvRetryEvent(pathInd)); 
            return; 
        } 
        ++IsDoneCounter;
        Callbacks->OnSourceError(InputIndex, result->Get()->Error, true);
    }

    // IActor & IDqSourceActor
    void PassAway() override { // Is called from Compute Actor
        TActorBootstrapped<TS3ReadActor>::PassAway();
    }

    static IHTTPGateway::THeaders MakeHeader(const TString& token) {
        return token.empty() ? IHTTPGateway::THeaders() : IHTTPGateway::THeaders{TString("X-YaCloud-SubjectToken:") += token};
    }

private: 
    size_t IsDoneCounter = 0U;

    const IHTTPGateway::TPtr Gateway;

    const ui64 InputIndex;
    ICallbacks *const Callbacks;

    TActorSystem* const ActorSystem;

    const TString Url;
    const IHTTPGateway::THeaders Headers;
    const TPathList Paths;

    std::queue<IHTTPGateway::TContent> Blocks;
 
    std::vector<TRetryParams> RetriesPerPath; 
    const std::shared_ptr<NS3::TRetryConfig> RetryConfig; 
    ui32 MaxRetriesPerPath = 3; 
};

std::pair<NYql::NDq::IDqSourceActor*, IActor*> CreateS3ReadActor(
    IHTTPGateway::TPtr gateway,
    NS3::TSource&& params,
    ui64 inputIndex,
    const THashMap<TString, TString>& secureParams,
    const THashMap<TString, TString>& taskParams,
    NYql::NDq::IDqSourceActor::ICallbacks* callback,
    ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory, 
    const std::shared_ptr<NS3::TRetryConfig>& retryConfig) 
{
    std::unordered_map<TString, size_t> map(params.GetPath().size());
    for (auto i = 0; i < params.GetPath().size(); ++i)
        map.emplace(params.GetPath().Get(i).GetPath(), params.GetPath().Get(i).GetSize());

    TS3ReadActor::TPathList paths;
    paths.reserve(map.size());
    if (const auto taskParamsIt = taskParams.find(S3ProviderName); taskParamsIt != taskParams.cend()) {
        NS3::TRange range;
        TStringInput input(taskParamsIt->second);
        range.Load(&input);
        for (auto i = 0; i < range.GetPath().size(); ++i) {
            const auto& path = range.GetPath().Get(i);
            paths.emplace_back(path, map[path]);
        }
    } else
        while (auto item = map.extract(map.cbegin()))
            paths.emplace_back(std::move(item.key()), std::move(item.mapped()));

    const auto token = secureParams.Value(params.GetToken(), TString{});
    const auto credentialsProviderFactory = CreateCredentialsProviderFactoryForStructuredToken(credentialsFactory, token);
    const auto authToken = credentialsProviderFactory->CreateProvider()->GetAuthInfo();
    const auto actor = new TS3ReadActor(inputIndex, std::move(gateway), params.GetUrl(), authToken, std::move(paths), callback, retryConfig); 
    return {actor, actor};
}

} // namespace NYql::NDq
