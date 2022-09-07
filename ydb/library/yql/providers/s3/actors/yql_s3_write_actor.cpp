#include "yql_s3_write_actor.h"
#include "yql_s3_retry_policy.h"

#include <ydb/core/protos/services.pb.h>

#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/s3/compressors/factory.h>
#include <ydb/library/yql/utils/yql_panic.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/events.h>
#include <library/cpp/actors/core/event_local.h>
#include <library/cpp/actors/core/hfunc.h>
#include <library/cpp/actors/core/log.h>
#include <library/cpp/string_utils/base64/base64.h>
#include <library/cpp/string_utils/quote/quote.h>

#include <util/generic/size_literals.h>

#include <queue>

#ifdef THROW
#undef THROW
#endif
#include <library/cpp/xml/document/xml-document.h>


#define LOG_E(name, stream) \
    LOG_ERROR_S(*TlsActivationContext, NKikimrServices::KQP_COMPUTE, name << ": " << this->SelfId() << ", TxId: " << TxId << ". " << stream)
#define LOG_W(name, stream) \
    LOG_WARN_S(*TlsActivationContext, NKikimrServices::KQP_COMPUTE, name << ": " << this->SelfId() << ", TxId: " << TxId << ". " << stream)
#define LOG_I(name, stream) \
    LOG_INFO_S(*TlsActivationContext, NKikimrServices::KQP_COMPUTE, name << ": " << this->SelfId() << ", TxId: " << TxId << ". " << stream)
#define LOG_D(name, stream) \
    LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::KQP_COMPUTE, name << ": " << this->SelfId() << ", TxId: " << TxId << ". " << stream)
#define LOG_T(name, stream) \
    LOG_TRACE_S(*TlsActivationContext, NKikimrServices::KQP_COMPUTE, name << ": " << this->SelfId() << ", TxId: " << TxId << ". " << stream)

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
        TEvUploadFinished(const TString& key, const TString& url) : Key(key), Url(url) {}
        const TString Key, Url;
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

    friend class TS3WriteActor;
    using TActorBootstrapped<TS3FileWriteActor>::PassAway;

public:
    TS3FileWriteActor(
        const TTxId& txId,
        IHTTPGateway::TPtr gateway,
        NYdb::TCredentialsProviderPtr credProvider,
        const TString& key, const TString& url, size_t sizeLimit, const std::string_view& compression)
        : TxId(txId)
        , Gateway(std::move(gateway))
        , CredProvider(std::move(credProvider))
        , ActorSystem(TActivationContext::ActorSystem())
        , Key(key), Url(url), SizeLimit(sizeLimit), Parts(MakeCompressorQueue(compression))
    {
        YQL_ENSURE(Parts, "Compression '" << compression << "' is not supported.");
    }

    void Bootstrap(const TActorId& parentId) {
        ParentId = parentId;
        LOG_D("TS3FileWriteActor", "Bootstrapped by " << ParentId);
        if (Parts->IsSealed() && 1U == Parts->Size()) {
            const auto size = Parts->Volume();
            InFlight += size;
            SentSize += size;
            Gateway->Upload(Url, MakeHeader(), Parts->Pop(), std::bind(&TS3FileWriteActor::OnUploadFinish, ActorSystem, SelfId(), ParentId, Key, Url, std::placeholders::_1), true, GetS3RetryPolicy());
        } else {
            Become(&TS3FileWriteActor::InitialStateFunc);
            Gateway->Upload(Url + "?uploads", MakeHeader(), 0, std::bind(&TS3FileWriteActor::OnUploadsCreated, ActorSystem, SelfId(), ParentId, std::placeholders::_1), false, GetS3RetryPolicy());
        }
    }

    static constexpr char ActorName[] = "S3_FILE_WRITE_ACTOR";

    void PassAway() override {
        if (InFlight || !Parts->Empty()) {
            LOG_W("TS3FileWriteActor", "PassAway but NOT finished, InFlight: " << InFlight << ", Parts: " << Parts->Size());
        } else {
            LOG_D("TS3FileWriteActor", "PassAway");
        }
        TActorBootstrapped<TS3FileWriteActor>::PassAway();
    }

    void SendData(TString&& data) {
        Parts->Push(std::move(data));

        Y_UNUSED(SizeLimit);
//TODO: if (10000U == Tags.size() + Parts->Size() || SizeLimit <= SentSize + Parts->Volume())
            Parts->Seal();

        if (!UploadId.empty())
            StartUploadParts();
    }

    void Finish() {
        Parts->Seal();
        if (!UploadId.empty())
            StartUploadParts();

        if (!InFlight && Parts->Empty())
            CommitUploadedParts();
    }

    bool IsFinishing() const { return Parts->IsSealed(); }

    const TString& GetUrl() const { return Url; }

    i64 GetMemoryUsed() const {
        return InFlight + Parts->Volume();
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
                actorSystem->Send(new IEventHandle(parentId, selfId, new TEvPrivate::TEvUploadError({TIssue(TStringBuilder() << message << ", error: code: " << code)})));
            } else if (root.Name() != "InitiateMultipartUploadResult")
                actorSystem->Send(new IEventHandle(parentId, selfId, new TEvPrivate::TEvUploadError({TIssue(TStringBuilder() << "Unexpected response '" << root.Name() << "' on create upload.")})));
            else {
                const NXml::TNamespacesForXPath nss(1U, {"s3", "http://s3.amazonaws.com/doc/2006-03-01/"});
                actorSystem->Send(new IEventHandle(selfId, selfId, new TEvPrivate::TEvUploadStarted(root.Node("s3:UploadId", false, nss).Value<TString>())));
            }
            break;
        } catch (const std::exception& ex) {
            actorSystem->Send(new IEventHandle(parentId, selfId, new TEvPrivate::TEvUploadError({TIssue(TStringBuilder() << "Error '" << ex.what() << "' on parse create upload response.")})));
            break;
        }
        case 1U:
            actorSystem->Send(new IEventHandle(parentId, selfId, new TEvPrivate::TEvUploadError(std::get<TIssues>(std::move(result)))));
            break;
        default:
            actorSystem->Send(new IEventHandle(parentId, selfId, new TEvPrivate::TEvUploadError({TIssue(TStringBuilder() << "Unexpected variant index " << result.index() << " on create upload response.")})));
            break;
        }
    }

    static void OnPartUploadFinish(TActorSystem* actorSystem, TActorId selfId, TActorId parentId, size_t size, size_t index, IHTTPGateway::TResult&& response) {
        switch (response.index()) {
        case 0U: {
            const auto& str = std::get<IHTTPGateway::TContent>(std::move(response)).Headers;

            if (const auto p = str.find("etag: \""); p != TString::npos) {
                if (const auto p1 = p + 7, p2 = str.find("\"", p1); p2 != TString::npos) {
                    actorSystem->Send(new IEventHandle(selfId, selfId, new TEvPrivate::TEvUploadPartFinished(size, index, str.substr(p1, p2 - p1))));
                    break;
                }
            }
            actorSystem->Send(new IEventHandle(parentId, selfId, new TEvPrivate::TEvUploadError({TIssue(TStringBuilder() << "Unexpected response:" << Endl << str)})));
            break;
        }
        case 1U:
            actorSystem->Send(new IEventHandle(parentId, selfId, new TEvPrivate::TEvUploadError(std::get<TIssues>(std::move(response)))));
            break;
        }
    }

    static void OnMultipartUploadFinish(TActorSystem* actorSystem, TActorId selfId, TActorId parentId, const TString& key, const TString& url, IHTTPGateway::TResult&& result) {
        switch (result.index()) {
        case 0U: try {
            const NXml::TDocument xml(std::get<IHTTPGateway::TContent>(std::move(result)).Extract(), NXml::TDocument::String);
            if (const auto& root = xml.Root(); root.Name() == "Error") {
                const auto& code = root.Node("Code", true).Value<TString>();
                const auto& message = root.Node("Message", true).Value<TString>();
                actorSystem->Send(new IEventHandle(parentId, selfId, new TEvPrivate::TEvUploadError({TIssue(TStringBuilder() << message << ", error: code: " << code)})));
            } else if (root.Name() != "CompleteMultipartUploadResult")
                actorSystem->Send(new IEventHandle(parentId, selfId, new TEvPrivate::TEvUploadError({TIssue(TStringBuilder() << "Unexpected response '" << root.Name() << "' on finish upload.")})));
            else
                actorSystem->Send(new IEventHandle(parentId, selfId, new TEvPrivate::TEvUploadFinished(key, url)));
            break;
        } catch (const std::exception& ex) {
            actorSystem->Send(new IEventHandle(parentId, selfId, new TEvPrivate::TEvUploadError({TIssue(TStringBuilder() << "Error '" << ex.what() << "' on parse finish upload response.")})));
            break;
        }
        case 1U:
            actorSystem->Send(new IEventHandle(parentId, selfId, new TEvPrivate::TEvUploadError(std::get<TIssues>(std::move(result)))));
            break;
        default:
            actorSystem->Send(new IEventHandle(parentId, selfId, new TEvPrivate::TEvUploadError({TIssue(TStringBuilder() << "Unexpected variant index " << result.index() << " on finish upload response.")})));
            break;
        }
    }

    static void OnUploadFinish(TActorSystem* actorSystem, TActorId selfId, TActorId parentId, const TString& key, const TString& url, IHTTPGateway::TResult&& result) {
        switch (result.index()) {
        case 0U:
            {
                auto content = std::get<IHTTPGateway::TContent>(std::move(result));
                if (content.HttpResponseCode >= 300) {
                    actorSystem->Send(new IEventHandle(parentId, selfId, new TEvPrivate::TEvUploadError({TIssue(TStringBuilder() << "HTTP error code: " << content.HttpResponseCode)})));
                } else {
                    actorSystem->Send(new IEventHandle(parentId, selfId, new TEvPrivate::TEvUploadFinished(key, url)));
                }
            }
            break;
        case 1U:
            actorSystem->Send(new IEventHandle(parentId, selfId, new TEvPrivate::TEvUploadError(std::get<TIssues>(std::move(result)))));
            break;
        default:
            actorSystem->Send(new IEventHandle(parentId, selfId, new TEvPrivate::TEvUploadError({TIssue(TStringBuilder() << "Unexpected variant index " << result.index() << " on finish upload response.")})));
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

        if (!InFlight && Parts->IsSealed() && Parts->Empty())
            CommitUploadedParts();
    }

    void StartUploadParts() {
        while (auto part = Parts->Pop()) {
            const auto size = part.size();
            const auto index = Tags.size();
            Tags.emplace_back();
            InFlight += size;
            SentSize += size;
            Gateway->Upload(Url + "?partNumber=" + std::to_string(index + 1) + "&uploadId=" + UploadId, MakeHeader(), std::move(part), std::bind(&TS3FileWriteActor::OnPartUploadFinish, ActorSystem, SelfId(), ParentId, size, index, std::placeholders::_1), true, GetS3RetryPolicy());
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
        Gateway->Upload(Url + "?uploadId=" + UploadId, MakeHeader(), xml, std::bind(&TS3FileWriteActor::OnMultipartUploadFinish, ActorSystem, SelfId(), ParentId, Key, Url, std::placeholders::_1), false, GetS3RetryPolicy());
    }

    IHTTPGateway::THeaders MakeHeader() const {
        if (const auto& token = CredProvider->GetAuthInfo(); token.empty())
            return IHTTPGateway::THeaders();
        else
            return IHTTPGateway::THeaders{TString("X-YaCloud-SubjectToken:") += token};
    }

    size_t InFlight = 0ULL;
    size_t SentSize = 0ULL;

    const TTxId TxId;
    const IHTTPGateway::TPtr Gateway;
    const NYdb::TCredentialsProviderPtr CredProvider;

    TActorSystem* const ActorSystem;
    TActorId ParentId;

    const TString Key;
    const TString Url;
    const size_t SizeLimit;

    IOutputQueue::TPtr Parts;
    std::vector<TString> Tags;

    TString UploadId;
};

class TS3WriteActor : public TActorBootstrapped<TS3WriteActor>, public IDqComputeActorAsyncOutput {
public:
    TS3WriteActor(ui64 outputIndex,
        const TTxId& txId,
        const TString& prefix,
        IHTTPGateway::TPtr gateway,
        NYdb::TCredentialsProviderPtr credProvider,
        IRandomProvider* randomProvider,
        const TString& url,
        const TString& path,
        const std::vector<TString>& keys,
        const size_t memoryLimit,
        const size_t maxFileSize,
        const TString& compression,
        IDqComputeActorAsyncOutput::ICallbacks* callbacks)
        : Gateway(std::move(gateway))
        , CredProvider(std::move(credProvider))
        , RandomProvider(randomProvider)
        , OutputIndex(outputIndex)
        , TxId(txId)
        , Prefix(prefix)
        , Callbacks(callbacks)
        , Url(url)
        , Path(path)
        , Keys(keys)
        , MemoryLimit(memoryLimit)
        , MaxFileSize(maxFileSize)
        , Compression(compression)
    {
        if (!RandomProvider) {
            DefaultRandomProvider = CreateDefaultRandomProvider();
            RandomProvider = DefaultRandomProvider.Get();
        }
    }

    void Bootstrap() {
        LOG_D("TS3WriteActor", "Bootstrapped");
        Become(&TS3WriteActor::StateFunc);
    }

    static constexpr char ActorName[] = "S3_WRITE_ACTOR";
private:
    void CommitState(const NDqProto::TCheckpoint&) final {};
    void LoadState(const NDqProto::TSinkState&) final {};
    ui64 GetOutputIndex() const final { return OutputIndex; }
    i64 GetFreeSpace() const final {
        return std::accumulate(FileWriteActors.cbegin(), FileWriteActors.cend(), i64(MemoryLimit),
            [](i64 free, const std::pair<const TString, std::vector<TS3FileWriteActor*>>& item) {
                return free - std::accumulate(item.second.cbegin(), item.second.cend(), i64(0), [](i64 sum, TS3FileWriteActor* actor) { return sum += actor->GetMemoryUsed(); });
            });
    }

    TString MakePartitionKey(const NUdf::TUnboxedValuePod v) const {
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

    TString MakeOutputName() const {
        const auto rand = std::make_tuple(RandomProvider->GenUuid4(), RandomProvider->GenRand());
        return Prefix + Base64EncodeUrl(TStringBuf(reinterpret_cast<const char*>(&rand), sizeof(rand)));
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvPrivate::TEvUploadError, Handle);
        hFunc(TEvPrivate::TEvUploadFinished, Handle);
    )

    void SendData(TUnboxedValueVector&& data, i64, const TMaybe<NDqProto::TCheckpoint>&, bool finished) final {
        for (const auto& v : data) {
            const auto& key = MakePartitionKey(v);
            const auto ins = FileWriteActors.emplace(key, std::vector<TS3FileWriteActor*>());
            if (ins.second || ins.first->second.empty() || ins.first->second.back()->IsFinishing()) {
                auto fileWrite = std::make_unique<TS3FileWriteActor>(TxId, Gateway, CredProvider, key, Url + Path + key + MakeOutputName(), MaxFileSize, Compression);
                ins.first->second.emplace_back(fileWrite.get());
                RegisterWithSameMailbox(fileWrite.release());
            }

            ins.first->second.back()->SendData(TString((Keys.empty() ? v : *v.GetElements()).AsStringRef()));
        }

        if (finished) {
            std::for_each(FileWriteActors.cbegin(), FileWriteActors.cend(), [](const std::pair<const TString, std::vector<TS3FileWriteActor*>>& item){ item.second.back()->Finish(); });
            Finished = true;
            FinishIfNeeded();
        }
        data.clear();
    }

    void Handle(TEvPrivate::TEvUploadError::TPtr& result) {
        LOG_W("TS3WriteActor", "TEvUploadError " << result->Get()->Error.ToOneLineString());
        Callbacks->OnAsyncOutputError(OutputIndex, result->Get()->Error, NYql::NDqProto::StatusIds::EXTERNAL_ERROR);
    }

    void FinishIfNeeded() {
        if (FileWriteActors.empty() && Finished) {
            LOG_D("TS3WriteActor", "Finished, notify owner");
            Callbacks->OnAsyncOutputFinished(OutputIndex);
        }
    }

    void Handle(TEvPrivate::TEvUploadFinished::TPtr& result) {
        if (const auto it = FileWriteActors.find(result->Get()->Key); FileWriteActors.cend() != it) {
            if (const auto ft = std::find_if(it->second.cbegin(), it->second.cend(), [&](TS3FileWriteActor* actor){ return result->Get()->Url == actor->GetUrl(); }); it->second.cend() != ft) {
                (*ft)->PassAway();
                it->second.erase(ft);
                if (it->second.empty())
                    FileWriteActors.erase(it);
            }
        }
        FinishIfNeeded();
    }

    // IActor & IDqComputeActorAsyncOutput
    void PassAway() override { // Is called from Compute Actor
        ui32 fileWriterCount = 0;
        for (const auto& p : FileWriteActors) {
            for (const auto& fileWriter : p.second) {
                fileWriter->PassAway();
                fileWriterCount++;
            }
        }
        FileWriteActors.clear();

        if (fileWriterCount) {
            LOG_W("TS3WriteActor", "PassAway with " << fileWriterCount << " NOT finished FileWriter(s)");
        } else {
            LOG_D("TS3WriteActor", "PassAway");
        }

        TActorBootstrapped<TS3WriteActor>::PassAway();
    }

    const IHTTPGateway::TPtr Gateway;
    const NYdb::TCredentialsProviderPtr CredProvider;
    IRandomProvider * RandomProvider;
    TIntrusivePtr<IRandomProvider> DefaultRandomProvider;

    const ui64 OutputIndex;
    const TTxId TxId;
    const TString Prefix;
    IDqComputeActorAsyncOutput::ICallbacks *const Callbacks;

    const TString Url;
    const TString Path;
    const std::vector<TString> Keys;

    const size_t MemoryLimit;
    const size_t MaxFileSize;
    const TString Compression;
    bool Finished = false;

    std::unordered_map<TString, std::vector<TS3FileWriteActor*>> FileWriteActors;
};

} // namespace

std::pair<IDqComputeActorAsyncOutput*, NActors::IActor*> CreateS3WriteActor(
    const NKikimr::NMiniKQL::TTypeEnvironment&,
    const NKikimr::NMiniKQL::IFunctionRegistry&,
    IRandomProvider* randomProvider,
    IHTTPGateway::TPtr gateway,
    NS3::TSink&& params,
    ui64 outputIndex,
    const TTxId& txId,
    const TString& prefix,
    const THashMap<TString, TString>& secureParams,
    IDqComputeActorAsyncOutput::ICallbacks* callbacks,
    ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory)
{
    const auto token = secureParams.Value(params.GetToken(), TString{});
    const auto credentialsProviderFactory = CreateCredentialsProviderFactoryForStructuredToken(credentialsFactory, token);
    const auto actor = new TS3WriteActor(
        outputIndex,
        txId,
        prefix,
        std::move(gateway),
        credentialsProviderFactory->CreateProvider(),
        randomProvider, params.GetUrl(),
        params.GetPath(),
        std::vector<TString>(params.GetKeys().cbegin(), params.GetKeys().cend()),
        params.HasMemoryLimit() ? params.GetMemoryLimit() : 1_GB,
        params.HasMaxFileSize() ? params.GetMaxFileSize() : 50_MB,
        params.HasCompression() ? params.GetCompression() : "",
        callbacks);
    return {actor, actor};
}

} // namespace NYql::NDq
