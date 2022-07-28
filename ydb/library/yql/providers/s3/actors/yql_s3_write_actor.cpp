#include "yql_s3_write_actor.h"
#include "yql_s3_retry_policy.h"

#include <ydb/library/yql/utils/yql_panic.h>

#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/events.h>
#include <library/cpp/actors/core/event_local.h>
#include <library/cpp/actors/core/hfunc.h>
#include <library/cpp/string_utils/base64/base64.h>
#include <library/cpp/string_utils/quote/quote.h>

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
    struct TEvUploadFinished : public TEventLocal<TEvUploadFinished, EvUploadFinished> {
        explicit TEvUploadFinished(const TString& key) : Key(key) {}
        const TString Key;
    };

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


using namespace NKikimr::NMiniKQL;

class TS3FileWriteActor : public TActorBootstrapped<TS3FileWriteActor> {
public:
    TS3FileWriteActor(
        IHTTPGateway::TPtr gateway,
        NYdb::TCredentialsProviderPtr credProvider,
        const TString& key, const TString& url)
        : Gateway(std::move(gateway))
        , CredProvider(std::move(credProvider))
        , ActorSystem(TActivationContext::ActorSystem())
        , Key(key), Url(url)
    {}

    void Bootstrap(const TActorId& parentId) {
        ParentId = parentId;
        Become(&TS3FileWriteActor::InitialStateFunc);
        Gateway->Upload(Url + "?uploads", MakeHeader(), "", std::bind(&TS3FileWriteActor::OnUploadsCreated, ActorSystem, SelfId(), ParentId, std::placeholders::_1), false, GetS3RetryPolicy());
    }

    static constexpr char ActorName[] = "S3_FILE_WRITE_ACTOR";

    void SendData(TString&& data) {
        InQueue += data.size();
        Parts.emplace(std::move(data));
        if (!UploadId.empty())
            StartUploadParts();
    }

    void Finish() {
        InputFinished = true;
        if (!InFlight && Parts.empty())
            CommitUploadedParts();
    }

    i64 GetMemoryUsed() const {
        return InFlight + InQueue;
    }
private:
    STRICT_STFUNC(InitialStateFunc,
        hFunc(TEvPrivate::TEvUploadStarted, Handle);
    )

    STRICT_STFUNC(WorkingStateFunc,
        hFunc(TEvPrivate::TEvUploadPartFinished, Handle);
    )


    static void OnUploadsCreated(TActorSystem* actorSystem, TActorId selfId, TActorId parentId, IHTTPGateway::TResult&& result) {
        switch (result.index()) {
        case 0U: try {
            const NXml::TDocument xml(std::get<IHTTPGateway::TContent>(std::move(result)).Extract(), NXml::TDocument::String);
            if (const auto& root = xml.Root(); root.Name() == "Error") {
                const auto& code = root.Node("Code", true).Value<TString>();
                const auto& message = root.Node("Message", true).Value<TString>();
                actorSystem->Send(new IEventHandle(parentId, TActorId(), new TEvPrivate::TEvUploadError({TIssue(TStringBuilder() << message << ", error: code: " << code)})));
            } else if (root.Name() != "InitiateMultipartUploadResult")
                actorSystem->Send(new IEventHandle(parentId, TActorId(), new TEvPrivate::TEvUploadError({TIssue(TStringBuilder() << "Unexpected response '" << root.Name() << "' on create upload.")})));
            else {
                const NXml::TNamespacesForXPath nss(1U, {"s3", "http://s3.amazonaws.com/doc/2006-03-01/"});
                actorSystem->Send(new IEventHandle(selfId, TActorId(), new TEvPrivate::TEvUploadStarted(root.Node("s3:UploadId", false, nss).Value<TString>())));
            }
            break;
        } catch (const std::exception& ex) {
            actorSystem->Send(new IEventHandle(parentId, TActorId(), new TEvPrivate::TEvUploadError({TIssue(TStringBuilder() << "Error '" << ex.what() << "' on parse create upload response.")})));
            break;
        }
        case 1U:
            actorSystem->Send(new IEventHandle(parentId, TActorId(), new TEvPrivate::TEvUploadError(std::get<TIssues>(std::move(result)))));
            break;
        default:
            actorSystem->Send(new IEventHandle(parentId, TActorId(), new TEvPrivate::TEvUploadError({TIssue(TStringBuilder() << "Unexpected variant index " << result.index() << " on create upload response.")})));
            break;
        }
    }

    static void OnPartUploadFinish(TActorSystem* actorSystem, TActorId selfId, TActorId parentId, size_t size, size_t index, IHTTPGateway::TResult&& response) {
        switch (response.index()) {
        case 0U: {
            const auto str = std::get<IHTTPGateway::TContent>(std::move(response)).Extract();

            if (const auto p = str.find("etag: \""); p != TString::npos) {
                if (const auto p1 = p + 7, p2 = str.find("\"", p1); p2 != TString::npos) {
                    actorSystem->Send(new IEventHandle(selfId, TActorId(), new TEvPrivate::TEvUploadPartFinished(size, index, str.substr(p1, p2 - p1))));
                    break;
                }
            }
            actorSystem->Send(new IEventHandle(parentId, TActorId(), new TEvPrivate::TEvUploadError({TIssue(TStringBuilder() << "Unexpected response:" << Endl << str)})));
            break;
        }
        case 1U:
            actorSystem->Send(new IEventHandle(parentId, TActorId(), new TEvPrivate::TEvUploadError(std::get<TIssues>(std::move(response)))));
            break;
        }
    }

    static void OnUploadFinish(TActorSystem* actorSystem, TActorId selfId, const TString& key, IHTTPGateway::TResult&& result) {
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
                actorSystem->Send(new IEventHandle(selfId, TActorId(), new TEvPrivate::TEvUploadFinished(key)));
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

    void Handle(TEvPrivate::TEvUploadStarted::TPtr& result) {
        UploadId = result->Get()->UploadId;
        Become(&TS3FileWriteActor::WorkingStateFunc);
        StartUploadParts();
    }

    void Handle(TEvPrivate::TEvUploadPartFinished::TPtr& result) {
        InFlight -= result->Get()->Size;
        Tags[result->Get()->Index] = std::move(result->Get()->ETag);

        if (!InFlight && InputFinished && Parts.empty())
            CommitUploadedParts();
    }

    // IActor & IDqComputeActorAsyncOutput
    void PassAway() override { // Is called from Compute Actor
        TActorBootstrapped<TS3FileWriteActor>::PassAway();
    }

    void StartUploadParts() {
        for (InQueue = 0ULL; !Parts.empty(); Parts.pop()) {
            const auto size = Parts.front().size();
            const auto index = Tags.size();
            Tags.emplace_back();
            InFlight += size;
            Gateway->Upload(Url + "?partNumber=" + std::to_string(index + 1) + "&uploadId=" + UploadId, MakeHeader(), std::move(Parts.front()), std::bind(&TS3FileWriteActor::OnPartUploadFinish, ActorSystem, SelfId(), ParentId, size, index, std::placeholders::_1), true, GetS3RetryPolicy());
        }
    }

    void CommitUploadedParts() {
        Become(nullptr);
        TStringBuilder xml;
        xml << "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" << Endl;
        xml << "<CompleteMultipartUpload xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">" << Endl;
        size_t i = 0U;
        for (const auto& tag : Tags)
            xml << "<Part><PartNumber>" << ++i << "</PartNumber><ETag>" << tag << "</ETag></Part>" << Endl;
        xml << "</CompleteMultipartUpload>" << Endl;
        Gateway->Upload(Url + "?uploadId=" + UploadId, MakeHeader(), xml, std::bind(&TS3FileWriteActor::OnUploadFinish, ActorSystem, ParentId, Key, std::placeholders::_1), false, GetS3RetryPolicy());
    }

    IHTTPGateway::THeaders MakeHeader() const {
        if (const auto& token = CredProvider->GetAuthInfo(); token.empty())
            return IHTTPGateway::THeaders();
        else
            return IHTTPGateway::THeaders{TString("X-YaCloud-SubjectToken:") += token};
    }

    bool InputFinished = false;
    size_t InQueue = 0ULL;
    size_t InFlight = 0ULL;

    const IHTTPGateway::TPtr Gateway;
    const NYdb::TCredentialsProviderPtr CredProvider;

    TActorSystem* const ActorSystem;
    TActorId ParentId;

    const TString Key;
    const TString Url;

    std::queue<TString> Parts;
    std::vector<TString> Tags;

    TString UploadId;
};

class TS3WriteActor : public TActorBootstrapped<TS3WriteActor>, public IDqComputeActorAsyncOutput {
public:
    TS3WriteActor(ui64 outputIndex,
        IHTTPGateway::TPtr gateway,
        NYdb::TCredentialsProviderPtr credProvider,
        IRandomProvider* randomProvider,
        const TString& url,
        const TString& path,
        const std::vector<TString>& keys,
        IDqComputeActorAsyncOutput::ICallbacks* callbacks)
        : Gateway(std::move(gateway))
        , CredProvider(std::move(credProvider))
        , RandomProvider(randomProvider)
        , OutputIndex(outputIndex)
        , Callbacks(callbacks)
        , Url(url)
        , Path(path)
        , Keys(keys)
    {}

    void Bootstrap() {
        Become(&TS3WriteActor::StateFunc);
    }

    static constexpr char ActorName[] = "S3_WRITE_ACTOR";
private:
    void CommitState(const NDqProto::TCheckpoint&) final {};
    void LoadState(const NDqProto::TSinkState&) final {};
    ui64 GetOutputIndex() const final { return OutputIndex; }
    i64 GetFreeSpace() const final {
        return std::accumulate(FileWriteActors.cbegin(), FileWriteActors.cend(), i64(1_GB), [](i64 free, const std::pair<const TString, TS3FileWriteActor*>& item){ return free - item.second->GetMemoryUsed(); });
    }

    TString MakeKey(const NUdf::TUnboxedValuePod v) const {
        if (Keys.empty())
            return {};

        auto elements = v.GetElements();
        TStringBuilder key;
        for (const auto& k : Keys) {
            const std::string_view keyPart = (++elements)->AsStringRef();
            YQL_ENSURE(std::string_view::npos == keyPart.find('/'), "Invalid partition key, contains '/': " << keyPart);
            key << k << '=' << keyPart << '/';
        }
        return UrlEscapeRet(key);
    }

    TString MakeSuffix() const {
        const auto rand = std::make_tuple(RandomProvider->GenUuid4(), RandomProvider->GenRand());
        return Base64EncodeUrl(TStringBuf(reinterpret_cast<const char*>(&rand), sizeof(rand)));
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvPrivate::TEvUploadError, Handle);
        hFunc(TEvPrivate::TEvUploadFinished, Handle);
    )

    void SendData(TUnboxedValueVector&& data, i64, const TMaybe<NDqProto::TCheckpoint>&, bool finished) final {
        for (const auto& v : data) {
            const auto& key = MakeKey(v);
            const auto ins = FileWriteActors.emplace(key, nullptr);
            if (ins.second) {
                auto fileWrite = std::make_unique<TS3FileWriteActor>(Gateway, CredProvider, key, Url + Path + key + MakeSuffix());
                ins.first->second = fileWrite.get();
                RegisterWithSameMailbox(fileWrite.release());
            }

            ins.first->second->SendData(TString((Keys.empty() ? v : *v.GetElements()).AsStringRef()));
        }

        if (finished)
            std::for_each(FileWriteActors.cbegin(), FileWriteActors.cend(), [](const std::pair<const TString, TS3FileWriteActor*>& item){ item.second->Finish(); });
        data.clear();
    }

    void Handle(TEvPrivate::TEvUploadError::TPtr& result) {
        Callbacks->OnAsyncOutputError(OutputIndex, result->Get()->Error, true);
    }

    void Handle(TEvPrivate::TEvUploadFinished::TPtr& result) {
        FileWriteActors.erase(result->Get()->Key);
        if (FileWriteActors.empty())
            Callbacks->OnAsyncOutputFinished(OutputIndex);
    }

    // IActor & IDqComputeActorAsyncOutput
    void PassAway() override { // Is called from Compute Actor
        TActorBootstrapped<TS3WriteActor>::PassAway();
    }

    const IHTTPGateway::TPtr Gateway;
    const NYdb::TCredentialsProviderPtr CredProvider;
    IRandomProvider *const RandomProvider;

    const ui64 OutputIndex;
    IDqComputeActorAsyncOutput::ICallbacks *const Callbacks;

    const TString Url;
    const TString Path;
    const std::vector<TString> Keys;

    std::unordered_map<TString, TS3FileWriteActor*> FileWriteActors;
};

} // namespace

std::pair<IDqComputeActorAsyncOutput*, NActors::IActor*> CreateS3WriteActor(
    const NKikimr::NMiniKQL::TTypeEnvironment&,
    const NKikimr::NMiniKQL::IFunctionRegistry&,
    IRandomProvider* randomProvider,
    IHTTPGateway::TPtr gateway,
    NS3::TSink&& params,
    ui64 outputIndex,
    const THashMap<TString, TString>& secureParams,
    IDqComputeActorAsyncOutput::ICallbacks* callbacks,
    ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory)
{
    const auto token = secureParams.Value(params.GetToken(), TString{});
    const auto credentialsProviderFactory = CreateCredentialsProviderFactoryForStructuredToken(credentialsFactory, token);
    const auto authToken = credentialsProviderFactory->CreateProvider();
    const auto actor = new TS3WriteActor(outputIndex, std::move(gateway), credentialsProviderFactory->CreateProvider(), randomProvider, params.GetUrl(), params.GetPath(), std::vector<TString>(params.GetKeys().cbegin(), params.GetKeys().cend()), callbacks);
    return {actor, actor};
}

} // namespace NYql::NDq
