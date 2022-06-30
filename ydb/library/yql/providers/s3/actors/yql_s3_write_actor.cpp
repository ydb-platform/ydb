#include "yql_s3_write_actor.h"

#include <ydb/library/yql/utils/yql_panic.h>

#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/events.h>
#include <library/cpp/actors/core/event_local.h>
#include <library/cpp/actors/core/hfunc.h>

#include <util/generic/size_literals.h>

#include <queue>

#ifdef THROW
#undef THROW
#endif
#include <library/cpp/xml/document/xml-document.h>

namespace NYql::NDq {

using namespace NActors;

namespace {

struct TEvPrivate {
    // Event ids
    enum EEv : ui32 {
        EvBegin = EventSpaceBegin(TEvents::ES_PRIVATE),

        EvUploadError = EvBegin,
        EvUploadStarted,
        EvUploadPartFinished,
        EvUploadFinished,

        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE)");

    // Events
    struct TEvUploadFinished : public TEventLocal<TEvUploadFinished, EvUploadFinished> {};

    struct TEvUploadError : public TEventLocal<TEvUploadError, EvUploadError> {
        explicit TEvUploadError(TIssues&& error) : Error(std::move(error)) {}
        const TIssues Error;
    };

    struct TEvUploadStarted : public TEventLocal<TEvUploadStarted, EvUploadStarted> {
        explicit TEvUploadStarted(TString&& uploadId) : UploadId(std::move(uploadId)) {}
        const TString UploadId;
    };

    struct TEvUploadPartFinished : public TEventLocal<TEvUploadPartFinished, EvUploadPartFinished> {
        explicit TEvUploadPartFinished(size_t size) : Size(size) {}
        const size_t Size;
    };
};

using TPath = std::tuple<TString, size_t>;
using TPathList = std::vector<TPath>;

class TRetryParams {
public:
    TRetryParams(const std::shared_ptr<NS3::TRetryConfig>& retryConfig)
        : MaxRetries(retryConfig && retryConfig->GetMaxRetriesPerPath() ? retryConfig->GetMaxRetriesPerPath() : 3U)
        , InitDelayMs(retryConfig && retryConfig->GetInitialDelayMs() ? TDuration::MilliSeconds(retryConfig->GetInitialDelayMs()) : TDuration::MilliSeconds(100))
        , InitEpsilon(retryConfig && retryConfig->GetEpsilon() ? retryConfig->GetEpsilon() : 0.1)
    {
        Y_VERIFY(0. < InitEpsilon && InitEpsilon < 1.);
        Reset();
    }

    void Reset() {
        Retries = 0U;
        DelayMs = InitDelayMs;
        Epsilon = InitEpsilon;
    }

    TDuration GetNextDelay() {
        if (++Retries > MaxRetries)
            return TDuration::Zero();
        return DelayMs = GenerateNextDelay();
    }
private:
    TDuration GenerateNextDelay() {
        const auto low = 1. - Epsilon;
        const auto jitter = low + std::rand() / (RAND_MAX / (2. * Epsilon));
        return DelayMs * jitter;
    }

    const ui32 MaxRetries;
    const TDuration InitDelayMs;
    const double InitEpsilon;

    ui32 Retries;
    TDuration DelayMs;
    double Epsilon;
};

using namespace NKikimr::NMiniKQL;

class TS3WriteActor : public TActorBootstrapped<TS3WriteActor>, public IDqComputeActorAsyncOutput {
public:
    TS3WriteActor(ui64 outputIndex,
        IHTTPGateway::TPtr gateway,
        const TString& url,
        const TString& token,
        const TString& path,
        IDqComputeActorAsyncOutput::ICallbacks* callbacks,
        const std::shared_ptr<NS3::TRetryConfig>& retryConfig)
        : Gateway(std::move(gateway))
        , OutputIndex(outputIndex)
        , Callbacks(callbacks)
        , ActorSystem(TActivationContext::ActorSystem())
        , Url(url)
        , Headers(MakeHeader(token))
        , Path(path)
        , RetryConfig(retryConfig)
    {}

    void Bootstrap() {
        Become(&TS3WriteActor::InitialStateFunc);
        Gateway->Download(Url + Path + "?uploads", Headers, 0, std::bind(&TS3WriteActor::OnUploadsCreated, ActorSystem, SelfId(), std::placeholders::_1), "", true);
    }

    static constexpr char ActorName[] = "S3_WRITE_ACTOR";
private:
    void CommitState(const NDqProto::TCheckpoint&) final {};
    void LoadState(const NDqProto::TSinkState&) final {};
    ui64 GetOutputIndex() const final { return OutputIndex; }
    i64 GetFreeSpace() const final {
        return 1_GB - InFlight - std::accumulate(Parts.cbegin(), Parts.cend(), 0LL, [](i64 s, const NUdf::TUnboxedValuePod v){ return v ? s + v.AsStringRef().Size() : s; });
    }

    STRICT_STFUNC(InitialStateFunc,
        hFunc(TEvPrivate::TEvUploadError, Handle);
        hFunc(TEvPrivate::TEvUploadStarted, Handle);
    )

    STRICT_STFUNC(WorkingStateFunc,
        hFunc(TEvPrivate::TEvUploadError, Handle);
        hFunc(TEvPrivate::TEvUploadPartFinished, Handle);
    )

    STRICT_STFUNC(FinalStateFunc,
        hFunc(TEvPrivate::TEvUploadError, Handle);
        cFunc(TEvPrivate::EvUploadFinished, HandleFinished);
    )

    static void OnUploadsCreated(TActorSystem* actorSystem, TActorId selfId, IHTTPGateway::TResult&& result) {
        switch (result.index()) {
        case 0U: try {
            const NXml::TDocument xml(std::get<IHTTPGateway::TContent>(std::move(result)).Extract(), NXml::TDocument::String);
            if (const auto& root = xml.Root(); root.Name() == "Error") {
                const auto& code = root.Node("Code", true).Value<TString>();
                const auto& message = root.Node("Message", true).Value<TString>();
                actorSystem->Send(new IEventHandle(selfId, TActorId(), new TEvPrivate::TEvUploadError({TIssue(TStringBuilder() << message << ", error: code: " << code)})));
            } else if (root.Name() != "InitiateMultipartUploadResult")
                actorSystem->Send(new IEventHandle(selfId, TActorId(), new TEvPrivate::TEvUploadError({TIssue(TStringBuilder() << "Unexpected response '" << root.Name() << "' on discovery.")})));
            else
                actorSystem->Send(new IEventHandle(selfId, TActorId(), new TEvPrivate::TEvUploadStarted(root.Node("UploadId", true).Value<TString>())));

            break;
        } catch (const std::exception& ex) {
            actorSystem->Send(new IEventHandle(selfId, TActorId(), new TEvPrivate::TEvUploadError({TIssue(TStringBuilder() << "Error '" << ex.what() << "' on parse discovery response.")})));
            break;
        }
        case 1U:
            actorSystem->Send(new IEventHandle(selfId, TActorId(), new TEvPrivate::TEvUploadError(std::get<TIssues>(std::move(result)))));
            break;
        default:
            actorSystem->Send(new IEventHandle(selfId, TActorId(), new TEvPrivate::TEvUploadError({TIssue(TStringBuilder() << "Unexpected variant index " << result.index() << " on discovery.")})));
            break;
        }
    }

    static void OnPartUploadFinish(TActorSystem* actorSystem, TActorId selfId, size_t size, IHTTPGateway::TResponse&& response) {
        switch (response.index()) {
        case 0U:
            if (const auto code = std::get<long>(std::move(response)); 200L == code)
                actorSystem->Send(new IEventHandle(selfId, TActorId(), new TEvPrivate::TEvUploadPartFinished(size)));
            else
                actorSystem->Send(new IEventHandle(selfId, TActorId(), new TEvPrivate::TEvUploadError({TIssue(TStringBuilder() << "Unexpected response code " << code)})));
        case 1U:
            actorSystem->Send(new IEventHandle(selfId, TActorId(), new TEvPrivate::TEvUploadError(std::get<TIssues>(std::move(response)))));
        }
    }

    static void OnUploadFinish(TActorSystem* actorSystem, TActorId selfId, IHTTPGateway::TResult&& result) {
        switch (result.index()) {
        case 0U: try {
            const NXml::TDocument xml(std::get<IHTTPGateway::TContent>(std::move(result)).Extract(), NXml::TDocument::String);
            if (const auto& root = xml.Root(); root.Name() == "Error") {
                const auto& code = root.Node("Code", true).Value<TString>();
                const auto& message = root.Node("Message", true).Value<TString>();
                actorSystem->Send(new IEventHandle(selfId, TActorId(), new TEvPrivate::TEvUploadError({TIssue(TStringBuilder() << message << ", error: code: " << code)})));
            } else if (root.Name() != "CompleteMultipartUploadResult")
                actorSystem->Send(new IEventHandle(selfId, TActorId(), new TEvPrivate::TEvUploadError({TIssue(TStringBuilder() << "Unexpected response '" << root.Name() << "' on discovery.")})));
            else
                actorSystem->Send(new IEventHandle(selfId, TActorId(), new TEvPrivate::TEvUploadFinished()));

            break;
        } catch (const std::exception& ex) {
            actorSystem->Send(new IEventHandle(selfId, TActorId(), new TEvPrivate::TEvUploadError({TIssue(TStringBuilder() << "Error '" << ex.what() << "' on parse discovery response.")})));
            break;
        }
        case 1U:
            actorSystem->Send(new IEventHandle(selfId, TActorId(), new TEvPrivate::TEvUploadError(std::get<TIssues>(std::move(result)))));
            break;
        default:
            actorSystem->Send(new IEventHandle(selfId, TActorId(), new TEvPrivate::TEvUploadError({TIssue(TStringBuilder() << "Unexpected variant index " << result.index() << " on discovery.")})));
            break;
        }
    }

    void SendData(TUnboxedValueVector&& data, i64 size, const TMaybe<NDqProto::TCheckpoint>&, bool finished) final {
        Cerr << __func__ << '(' << data.size() << ',' << size << ',' << finished << ')' << Endl;
        InputFinished = finished;
        std::move(data.begin(), data.end(), std::back_inserter(Parts));
        if (!UploadId.empty())
            StartUploadParts();
    }

    void Handle(TEvPrivate::TEvUploadError::TPtr& result) {
        Parts.clear();
        Callbacks->OnAsyncOutputError(OutputIndex, result->Get()->Error, true);
        if (!UploadId.empty()) {
            // TODO: Send delete.
        }
    }

    void Handle(TEvPrivate::TEvUploadStarted::TPtr& result) {
        UploadId = result->Get()->UploadId;
        Become(&TS3WriteActor::InitialStateFunc);
        StartUploadParts();
    }

    void Handle(TEvPrivate::TEvUploadPartFinished::TPtr& result) {
        InFlight -= result->Get()->Size;

        if (!InFlight && std::all_of(Parts.cbegin(), Parts.cend(), std::logical_not<NUdf::TUnboxedValuePod>())) {
            Become(&TS3WriteActor::FinalStateFunc);

            TStringBuilder xml;
            xml << "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" << Endl;
            xml << "<CompleteMultipartUpload xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">" << Endl;
            for (auto i = 1U; i <= Parts.size(); ++i)
                xml << "<Part><PartNumber>" << i << "</Part></PartNumber>" << Endl;
            xml << "</CompleteMultipartUpload>" << Endl;
            Gateway->Download(Url + Path + "?uploadId=" + UploadId, Headers, 0, std::bind(&TS3WriteActor::OnUploadFinish, ActorSystem, SelfId(), std::placeholders::_1), xml, true);
        }
    }

    void HandleFinished() {}

    // IActor & IDqComputeActorAsyncInput
    void PassAway() override { // Is called from Compute Actor
        TActorBootstrapped<TS3WriteActor>::PassAway();
    }

    static IHTTPGateway::THeaders MakeHeader(const TString& token) {
        return token.empty() ? IHTTPGateway::THeaders() : IHTTPGateway::THeaders{TString("X-YaCloud-SubjectToken:") += token};
    }

    void StartUploadParts() {
        for (auto i = 0U; i < Parts.size(); ++i) {
            if (auto part = std::move(Parts[i])) {
                const auto size = part.AsStringRef().Size();
                InFlight += size;
                Gateway->Upload(Url + Path + "?partNumber=" + std::to_string(i + 1) + "&uploadId=" + UploadId, Headers, TString(part.AsStringRef()), std::bind(&TS3WriteActor::OnPartUploadFinish, ActorSystem, SelfId(), size, std::placeholders::_1));
            }
        }
    }

    bool InputFinished  = false;
    size_t InFlight = 0ULL;

    const IHTTPGateway::TPtr Gateway;

    const ui64 OutputIndex;
    IDqComputeActorAsyncOutput::ICallbacks *const Callbacks;

    TActorSystem* const ActorSystem;

    const TString Url;
    const IHTTPGateway::THeaders Headers;
    const TString Path;

    TUnboxedValueVector Parts;

    std::vector<TRetryParams> RetriesPerPath;
    const std::shared_ptr<NS3::TRetryConfig> RetryConfig;

    TString UploadId;
};

} // namespace

std::pair<IDqComputeActorAsyncOutput*, NActors::IActor*> CreateS3WriteActor(
    const NKikimr::NMiniKQL::TTypeEnvironment&,
    const NKikimr::NMiniKQL::IFunctionRegistry&,
    IHTTPGateway::TPtr gateway,
    NS3::TSink&& params,
    ui64 outputIndex,
    const THashMap<TString, TString>& secureParams,
    IDqComputeActorAsyncOutput::ICallbacks* callbacks,
    ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory,
    const std::shared_ptr<NYql::NS3::TRetryConfig>& retryConfig)
{
    const auto token = secureParams.Value(params.GetToken(), TString{});
    const auto credentialsProviderFactory = CreateCredentialsProviderFactoryForStructuredToken(credentialsFactory, token);
    const auto authToken = credentialsProviderFactory->CreateProvider()->GetAuthInfo();

    const auto actor = new TS3WriteActor(outputIndex, std::move(gateway), params.GetUrl(), authToken, params.GetPath(), callbacks, retryConfig);
    return {actor, actor};
}

} // namespace NYql::NDq

