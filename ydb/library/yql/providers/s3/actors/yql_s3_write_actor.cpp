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
        TEvUploadPartFinished(size_t size, size_t index, TString&& etag) : Size(size), Index(index), ETag(std::move(etag)) {}
        const size_t Size, Index;
        const TString ETag;
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
        Gateway->Upload(Url + Path + "?uploads", Headers, "", std::bind(&TS3WriteActor::OnUploadsCreated, ActorSystem, SelfId(), std::placeholders::_1), false);
    }

    static constexpr char ActorName[] = "S3_WRITE_ACTOR";
private:
    void CommitState(const NDqProto::TCheckpoint&) final {};
    void LoadState(const NDqProto::TSinkState&) final {};
    ui64 GetOutputIndex() const final { return OutputIndex; }
    i64 GetFreeSpace() const final {
        return 1_GB - InFlight - InQueue;
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
                actorSystem->Send(new IEventHandle(selfId, TActorId(), new TEvPrivate::TEvUploadError({TIssue(TStringBuilder() << "Unexpected response '" << root.Name() << "' on create upload.")})));
            else {
                const NXml::TNamespacesForXPath nss(1U, {"s3", "http://s3.amazonaws.com/doc/2006-03-01/"});
                actorSystem->Send(new IEventHandle(selfId, TActorId(), new TEvPrivate::TEvUploadStarted(root.Node("s3:UploadId", false, nss).Value<TString>())));
            }
            break;
        } catch (const std::exception& ex) {
            actorSystem->Send(new IEventHandle(selfId, TActorId(), new TEvPrivate::TEvUploadError({TIssue(TStringBuilder() << "Error '" << ex.what() << "' on parse create upload response.")})));
            break;
        }
        case 1U:
            actorSystem->Send(new IEventHandle(selfId, TActorId(), new TEvPrivate::TEvUploadError(std::get<TIssues>(std::move(result)))));
            break;
        default:
            actorSystem->Send(new IEventHandle(selfId, TActorId(), new TEvPrivate::TEvUploadError({TIssue(TStringBuilder() << "Unexpected variant index " << result.index() << " on create upload response.")})));
            break;
        }
    }

    static void OnPartUploadFinish(TActorSystem* actorSystem, TActorId selfId, size_t size, size_t index, IHTTPGateway::TResult&& response) {
        switch (response.index()) {
        case 0U: {
            const auto str = std::get<IHTTPGateway::TContent>(std::move(response)).Extract();

            if (const auto p = str.find("etag: \""); p != TString::npos) {
                if (const auto p1 = p + 7, p2 = str.find("\"", p1); p2 != TString::npos) {
                    actorSystem->Send(new IEventHandle(selfId, TActorId(), new TEvPrivate::TEvUploadPartFinished(size, index, str.substr(p1, p2 - p1))));
                    break;
                }
            }
            actorSystem->Send(new IEventHandle(selfId, TActorId(), new TEvPrivate::TEvUploadError({TIssue(TStringBuilder() << "Unexpected response:" << Endl << str)})));
            break;
        }
        case 1U:
            actorSystem->Send(new IEventHandle(selfId, TActorId(), new TEvPrivate::TEvUploadError(std::get<TIssues>(std::move(response)))));
            break;
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
                actorSystem->Send(new IEventHandle(selfId, TActorId(), new TEvPrivate::TEvUploadError({TIssue(TStringBuilder() << "Unexpected response '" << root.Name() << "' on finish upload.")})));
            else
                actorSystem->Send(new IEventHandle(selfId, TActorId(), new TEvPrivate::TEvUploadFinished()));
            break;
        } catch (const std::exception& ex) {
            actorSystem->Send(new IEventHandle(selfId, TActorId(), new TEvPrivate::TEvUploadError({TIssue(TStringBuilder() << "Error '" << ex.what() << "' on parse finish upload response.")})));
            break;
        }
        case 1U:
            actorSystem->Send(new IEventHandle(selfId, TActorId(), new TEvPrivate::TEvUploadError(std::get<TIssues>(std::move(result)))));
            break;
        default:
            actorSystem->Send(new IEventHandle(selfId, TActorId(), new TEvPrivate::TEvUploadError({TIssue(TStringBuilder() << "Unexpected variant index " << result.index() << " on finish upload response.")})));
            break;
        }
    }

    void SendData(TUnboxedValueVector&& data, i64 size, const TMaybe<NDqProto::TCheckpoint>&, bool finished) final {
        InputFinished = finished;
        for (const auto& v : data)
            Parts.emplace(v.AsStringRef());
        data.clear();
        InQueue += size;
        if (!UploadId.empty())
            StartUploadParts();
    }

    void Handle(TEvPrivate::TEvUploadError::TPtr& result) {
        Callbacks->OnAsyncOutputError(OutputIndex, result->Get()->Error, true);
        if (!UploadId.empty()) {
            // TODO: Send delete.
        }
    }

    void Handle(TEvPrivate::TEvUploadStarted::TPtr& result) {
        UploadId = result->Get()->UploadId;
        Become(&TS3WriteActor::WorkingStateFunc);
        StartUploadParts();
    }

    void Handle(TEvPrivate::TEvUploadPartFinished::TPtr& result) {
        InFlight -= result->Get()->Size;
        Tags[result->Get()->Index] = std::move(result->Get()->ETag);

        if (!InFlight && InputFinished && Parts.empty()) {
            Become(&TS3WriteActor::FinalStateFunc);
            TStringBuilder xml;
            xml << "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" << Endl;
            xml << "<CompleteMultipartUpload xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">" << Endl;
            size_t i = 0U;
            for (const auto& tag : Tags)
                xml << "<Part><PartNumber>" << ++i << "</PartNumber><ETag>" << tag << "</ETag></Part>" << Endl;
            xml << "</CompleteMultipartUpload>" << Endl;
            Gateway->Upload(Url + Path + "?uploadId=" + UploadId, Headers, xml, std::bind(&TS3WriteActor::OnUploadFinish, ActorSystem, SelfId(), std::placeholders::_1), false);
        }
    }

    void HandleFinished() {
        return Callbacks->OnAsyncOutputFinished(OutputIndex);
    }

    // IActor & IDqComputeActorAsyncOutput
    void PassAway() override { // Is called from Compute Actor
        TActorBootstrapped<TS3WriteActor>::PassAway();
    }

    static IHTTPGateway::THeaders MakeHeader(const TString& token) {
        return token.empty() ? IHTTPGateway::THeaders() : IHTTPGateway::THeaders{TString("X-YaCloud-SubjectToken:") += token};
    }

    void StartUploadParts() {
        for (InQueue = 0ULL; !Parts.empty(); Parts.pop()) {
            const auto size = Parts.front().size();
            const auto index = Tags.size();
            Tags.emplace_back();
            InFlight += size;
            Gateway->Upload(Url + Path + "?partNumber=" + std::to_string(index + 1) + "&uploadId=" + UploadId, Headers, std::move(Parts.front()), std::bind(&TS3WriteActor::OnPartUploadFinish, ActorSystem, SelfId(), size, index, std::placeholders::_1), true);
        }
    }

    bool InputFinished = false;
    size_t InQueue = 0ULL;
    size_t InFlight = 0ULL;

    const IHTTPGateway::TPtr Gateway;

    const ui64 OutputIndex;
    IDqComputeActorAsyncOutput::ICallbacks *const Callbacks;

    TActorSystem* const ActorSystem;

    const TString Url;
    const IHTTPGateway::THeaders Headers;
    const TString Path;

    std::queue<TString> Parts;
    std::vector<TString> Tags;

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
