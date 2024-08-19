#include "yql_s3_write_actor.h"
#include "yql_s3_actors_util.h"

#include <ydb/library/services/services.pb.h>

#include <ydb/library/yql/providers/common/http_gateway/yql_http_default_retry_policy.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/s3/common/util.h>
#include <ydb/library/yql/providers/s3/compressors/factory.h>
#include <ydb/library/yql/providers/s3/credentials/credentials.h>
#include <ydb/library/yql/utils/yql_panic.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/http/http.h>
#include <library/cpp/string_utils/base64/base64.h>
#include <library/cpp/string_utils/quote/quote.h>

#include <util/generic/size_literals.h>

#include <queue>

#ifdef THROW
#undef THROW
#endif
#include <library/cpp/string_utils/quote/quote.h>
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
        TEvUploadFinished(const TString& key, const TString& url, ui64 uploadSize)
            : Key(key), Url(url), UploadSize(uploadSize) {
        }
        const TString Key, Url;
        const ui64 UploadSize;
    };

    struct TEvUploadError : public TEventLocal<TEvUploadError, EvUploadError> {

        TEvUploadError(long httpCode, const TString& s3ErrorCode, const TString& message)
        : StatusCode(NYql::NDqProto::StatusIds::UNSPECIFIED), HttpCode(httpCode), S3ErrorCode(s3ErrorCode), Message(message) {
            BuildIssues();
        }

        TEvUploadError(const TString& s3ErrorCode, const TString& message)
        : StatusCode(NYql::NDqProto::StatusIds::UNSPECIFIED), HttpCode(0), S3ErrorCode(s3ErrorCode), Message(message) {
            BuildIssues();
        }

        TEvUploadError(NYql::NDqProto::StatusIds::StatusCode statusCode, const TString& message)
        : StatusCode(statusCode), HttpCode(0), Message(message) {
            BuildIssues();
        }

        TEvUploadError(long httpCode, const TString& message)
        : StatusCode(NYql::NDqProto::StatusIds::UNSPECIFIED), HttpCode(httpCode), Message(message) {
            BuildIssues();
        }

        TEvUploadError(TIssues&& issues)
        : StatusCode(NYql::NDqProto::StatusIds::UNSPECIFIED), HttpCode(0), Issues(issues) {
            // don't build
        }

        void BuildIssues() {
            Issues = ::NYql::NDq::BuildIssues(HttpCode, S3ErrorCode, Message);
        }

        NYql::NDqProto::StatusIds::StatusCode StatusCode;
        long HttpCode;
        TString S3ErrorCode;
        TString Message;
        TIssues Issues;
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

public:
    TS3FileWriteActor(
        const TTxId& txId,
        IHTTPGateway::TPtr gateway,
        TS3Credentials crdentials,
        const TString& key,
        const TString& url,
        const std::string_view& compression,
        const IHTTPGateway::TRetryPolicy::TPtr& retryPolicy,
        bool dirtyWrite,
        const TString& token)
        : TxId(txId)
        , Gateway(std::move(gateway))
        , Credentials(std::move(crdentials))
        , RetryPolicy(retryPolicy)
        , ActorSystem(TActivationContext::ActorSystem())
        , Key(key)
        , Url(url)
        , RequestId(CreateGuidAsString())
        , Parts(MakeCompressorQueue(compression))
        , DirtyWrite(dirtyWrite)
        , Token(token)
    {
        YQL_ENSURE(Parts, "Compression '" << compression << "' is not supported.");
    }

    void Bootstrap(const TActorId& parentId) {
        ParentId = parentId;
        LOG_D("TS3FileWriteActor", "Bootstrap by " << ParentId << " for Key: [" << Key << "], Url: [" << Url << "], request id: [" << RequestId << "]");
        try {
            BeginPartsUpload(Credentials.GetAuthInfo());
        } catch (...) {
            FailOnException();
        }
    }

    void BeginPartsUpload(const TS3Credentials::TAuthInfo& authInfo) {
        if (DirtyWrite && Parts->IsSealed() && Parts->Size() <= 1) {
            Become(&TS3FileWriteActor::StateFuncWrapper<&TS3FileWriteActor::SinglepartWorkingStateFunc>);
            const size_t size = Max<size_t>(Parts->Volume(), 1);
            InFlight += size;
            SentSize += size;
            Gateway->Upload(Url,
                IHTTPGateway::MakeYcHeaders(RequestId, authInfo.GetToken(), {}, authInfo.GetAwsUserPwd(), authInfo.GetAwsSigV4()),
                Parts->Pop(),
                std::bind(&TS3FileWriteActor::OnUploadFinish, ActorSystem, SelfId(), ParentId, Key, Url, RequestId, size, std::placeholders::_1),
                true,
                RetryPolicy);
        } else {
            Become(&TS3FileWriteActor::StateFuncWrapper<&TS3FileWriteActor::MultipartInitialStateFunc>);
            Gateway->Upload(Url + "?uploads",
                IHTTPGateway::MakeYcHeaders(RequestId, authInfo.GetToken(), {}, authInfo.GetAwsUserPwd(), authInfo.GetAwsSigV4()),
                0,
                std::bind(&TS3FileWriteActor::OnUploadsCreated, ActorSystem, SelfId(), ParentId, RequestId, std::placeholders::_1),
                false,
                RetryPolicy);
        }
    }

    static constexpr char ActorName[] = "S3_FILE_WRITE_ACTOR";

    void Handle(TEvPrivate::TEvUploadFinished::TPtr& ev) {
        InFlight -= ev->Get()->UploadSize;
    }

    void PassAway() override {
        if (InFlight || !Parts->Empty()) {
            SafeAbortMultipartUpload();
            LOG_W("TS3FileWriteActor", "PassAway: but NOT finished, InFlight: " << InFlight << ", Parts: " << Parts->Size() << ", Sealed: " << Parts->IsSealed() << ", request id: [" << RequestId << "]");
        } else {
            LOG_D("TS3FileWriteActor", "PassAway: request id: [" << RequestId << "]");
        }
        TActorBootstrapped<TS3FileWriteActor>::PassAway();
    }

    void AddData(TString&& data) {
        Parts->Push(std::move(data));
    }

    void Seal() {
        Parts->Seal();
    }

    void Go() {
        if (!UploadId.empty()) {
            StartUploadParts();
        }
    }

    void Finish() {
        if (IsFinishing()) {
            return;
        }

        Parts->Seal();

        if (!UploadId.empty()) {
            if (!Parts->Empty()) {
                StartUploadParts();
            } else if (!InFlight && Parts->Empty()) {
                FinalizeMultipartCommit();
            }
        }
    }

    bool IsFinishing() const {
        return Parts->IsSealed();
    }

    const TString& GetUrl() const {
        return Url;
    }

    i64 GetMemoryUsed() const {
        return InFlight + Parts->Volume();
    }
private:
    template <void (TS3FileWriteActor::* DelegatedStateFunc)(STFUNC_SIG)>
    STFUNC(StateFuncWrapper) {
        try {
            (this->*DelegatedStateFunc)(ev);
        } catch (...) {
            FailOnException();
        }
    }

    STRICT_STFUNC(MultipartInitialStateFunc,
        hFunc(TEvPrivate::TEvUploadStarted, Handle);
    )

    STRICT_STFUNC(MultipartWorkingStateFunc,
        hFunc(TEvPrivate::TEvUploadPartFinished, Handle);
    )

    STRICT_STFUNC(SinglepartWorkingStateFunc,
        hFunc(TEvPrivate::TEvUploadFinished, Handle);
    )

    static void OnUploadsCreated(TActorSystem* actorSystem, TActorId selfId, TActorId parentId, const TString& requestId, IHTTPGateway::TResult&& result) {
        if (!result.Issues) {
            try {
                TS3Result s3Result(std::move(result.Content.Extract()));
                const auto& root = s3Result.GetRootNode();
                if (s3Result.IsError) {
                    actorSystem->Send(new IEventHandle(parentId, selfId, new TEvPrivate::TEvUploadError(s3Result.S3ErrorCode, TStringBuilder{} << s3Result.ErrorMessage << ", request id: [" << requestId << "]")));
                } else if (root.Name() != "InitiateMultipartUploadResult")
                    actorSystem->Send(new IEventHandle(parentId, selfId, new TEvPrivate::TEvUploadError(NYql::NDqProto::StatusIds::INTERNAL_ERROR, TStringBuilder() << "Unexpected response on create upload: " << root.Name() << ", request id: [" << requestId << "]")));
                else {
                    const NXml::TNamespacesForXPath nss(1U, {"s3", "http://s3.amazonaws.com/doc/2006-03-01/"});
                    actorSystem->Send(new IEventHandle(selfId, selfId, new TEvPrivate::TEvUploadStarted(root.Node("s3:UploadId", false, nss).Value<TString>())));
                }
            } catch (const std::exception& ex) {
                actorSystem->Send(new IEventHandle(parentId, selfId, new TEvPrivate::TEvUploadError(NYql::NDqProto::StatusIds::INTERNAL_ERROR, TStringBuilder() << "Error on parse create upload response: " << ex.what()  << ", request id: [" << requestId << "]")));
            }
        } else {
            auto issues = NS3Util::AddParentIssue(TStringBuilder{} << "Upload error, request id: [" << requestId << "], ", std::move(result.Issues));
            actorSystem->Send(new IEventHandle(parentId, selfId, new TEvPrivate::TEvUploadError(std::move(issues))));
        }
    }

    static void OnPartUploadFinish(TActorSystem* actorSystem, TActorId selfId, TActorId parentId, size_t size, size_t index, const TString& requestId, IHTTPGateway::TResult&& response) {
        if (!response.Issues) {
            const auto& str = response.Content.Headers;
            const auto headerStr = str.substr(str.rfind("HTTP/"));
            if (const NHttp::THeaders headers(headerStr); headers.Has("Etag"))
                actorSystem->Send(new IEventHandle(selfId, selfId, new TEvPrivate::TEvUploadPartFinished(size, index, TString(headers.Get("Etag")))));
            else {
                TS3Result s3Result(std::move(response.Content.Extract()));
                if (s3Result.IsError && s3Result.Parsed) {
                    actorSystem->Send(new IEventHandle(parentId, selfId, new TEvPrivate::TEvUploadError(s3Result.S3ErrorCode, TStringBuilder{} << "Upload failed: " << s3Result.ErrorMessage << ", request id: [" << requestId << "]")));
                } else {
                    constexpr size_t BODY_MAX_SIZE = 1_KB;
                    actorSystem->Send(new IEventHandle(parentId, selfId, new TEvPrivate::TEvUploadError(NYql::NDqProto::StatusIds::INTERNAL_ERROR,
                        TStringBuilder() << "Unexpected response"
                            << ". Headers: " << headerStr
                            << ". Body: \"" << TStringBuf(s3Result.Body).Trunc(BODY_MAX_SIZE)
                            << (s3Result.Body.size() > BODY_MAX_SIZE ? "\"..." : "\"")
                            << ". Request id: [" << requestId << "]")));
                }
            }
        } else {
            auto issues = NS3Util::AddParentIssue(TStringBuilder{} << "PartUpload error, request id: [" << requestId << "], ", std::move(response.Issues));
            actorSystem->Send(new IEventHandle(parentId, selfId, new TEvPrivate::TEvUploadError(std::move(issues))));
        }
    }

    static void OnMultipartUploadFinish(TActorSystem* actorSystem, TActorId selfId, TActorId parentId, const TString& key, const TString& url, const TString& requestId, ui64 sentSize, IHTTPGateway::TResult&& result) {
        if (!result.Issues) {
            try {
                TS3Result s3Result(std::move(result.Content.Extract()));
                const auto& root = s3Result.GetRootNode();
                if (s3Result.IsError) {
                    actorSystem->Send(new IEventHandle(parentId, selfId, new TEvPrivate::TEvUploadError(s3Result.S3ErrorCode, TStringBuilder{} << s3Result.ErrorMessage << ", request id: [" << requestId << "]")));
                } else if (root.Name() != "CompleteMultipartUploadResult")
                    actorSystem->Send(new IEventHandle(parentId, selfId, new TEvPrivate::TEvUploadError(NYql::NDqProto::StatusIds::INTERNAL_ERROR, TStringBuilder() << "Unexpected response on finish upload: " << root.Name() << ", request id: [" << requestId << "]")));
                else
                    actorSystem->Send(new IEventHandle(parentId, selfId, new TEvPrivate::TEvUploadFinished(key, url, sentSize)));
            } catch (const std::exception& ex) {
                actorSystem->Send(new IEventHandle(parentId, selfId, new TEvPrivate::TEvUploadError(NYql::NDqProto::StatusIds::INTERNAL_ERROR, TStringBuilder() << "Error on parse finish upload response: " << ex.what() << ", request id: [" << requestId << "]")));
            }
        } else {
            auto issues = NS3Util::AddParentIssue(TStringBuilder{} << "Multipart error, request id: [" << requestId << "], ", std::move(result.Issues));
            actorSystem->Send(new IEventHandle(parentId, selfId, new TEvPrivate::TEvUploadError(std::move(issues))));
        }
    }

    static void OnMultipartUploadAbort(TActorSystem* actorSystem, TActorId selfId, const TTxId& TxId, const TString& requestId, IHTTPGateway::TResult&& result) {
        if (!result.Issues) {
            LOG_DEBUG_S(*actorSystem, NKikimrServices::KQP_COMPUTE, "TS3FileWriteActor: " << selfId << ", TxId: " << TxId << ". " << "Multipart upload aborted, request id: [" << requestId << "]");
        } else {
            LOG_WARN_S(*actorSystem, NKikimrServices::KQP_COMPUTE, "TS3FileWriteActor: " << selfId << ", TxId: " << TxId << ". " << "Failed to abort multipart upload, request id: [" << requestId << "], issues: " << result.Issues.ToString());
        }
    }

    static void OnUploadFinish(TActorSystem* actorSystem, TActorId selfId, TActorId parentId, const TString& key, const TString& url, const TString& requestId, ui64 sentSize, IHTTPGateway::TResult&& result) {
        if (!result.Issues) {
            if (result.Content.HttpResponseCode >= 300) {
                TString errorText = result.Content.Extract();
                TString errorCode;
                TString message;
                if (ParseS3ErrorResponse(errorText, errorCode, message)) {
                    actorSystem->Send(new IEventHandle(parentId, selfId, new TEvPrivate::TEvUploadError(result.Content.HttpResponseCode, errorCode, TStringBuilder{} << message << ", request id: [" << requestId << "]")));
                } else {
                    actorSystem->Send(new IEventHandle(parentId, selfId, new TEvPrivate::TEvUploadError(result.Content.HttpResponseCode, TStringBuilder{} << errorText << ", request id: [" << requestId << "]")));
                }
            } else {
                actorSystem->Send(new IEventHandle(selfId, selfId, new TEvPrivate::TEvUploadFinished(key, url, sentSize)));
                actorSystem->Send(new IEventHandle(parentId, selfId, new TEvPrivate::TEvUploadFinished(key, url, sentSize)));
            }
        } else {
            auto issues = NS3Util::AddParentIssue(TStringBuilder{} << "UploadFinish error, request id: [" << requestId << "], ", std::move(result.Issues));
            actorSystem->Send(new IEventHandle(parentId, selfId, new TEvPrivate::TEvUploadError(std::move(issues))));
        }
    }

    void Handle(TEvPrivate::TEvUploadStarted::TPtr& result) {
        UploadId = result->Get()->UploadId;
        Become(&TS3FileWriteActor::StateFuncWrapper<&TS3FileWriteActor::MultipartWorkingStateFunc>);
        StartUploadParts();
    }

    void Handle(TEvPrivate::TEvUploadPartFinished::TPtr& result) {
        InFlight -= result->Get()->Size;
        Tags[result->Get()->Index] = std::move(result->Get()->ETag);

        if (!InFlight && Parts->IsSealed() && Parts->Empty()) {
            FinalizeMultipartCommit();
        }
    }

    void StartUploadParts() {
        while (auto part = Parts->Pop()) {
            const auto size = part.size();
            const auto index = Tags.size();
            Tags.emplace_back();
            InFlight += size;
            SentSize += size;
            auto authInfo = Credentials.GetAuthInfo();
            Gateway->Upload(Url + "?partNumber=" + std::to_string(index + 1) + "&uploadId=" + UploadId,
                IHTTPGateway::MakeYcHeaders(RequestId, authInfo.GetToken(), {}, authInfo.GetAwsUserPwd(), authInfo.GetAwsSigV4()),
                std::move(part),
                std::bind(&TS3FileWriteActor::OnPartUploadFinish, ActorSystem, SelfId(), ParentId, size, index, RequestId, std::placeholders::_1),
                true,
                RetryPolicy);
        }
    }

    void FinalizeMultipartCommit() {
        Become(nullptr);
        if (DirtyWrite) {
            CommitUploadedParts();
        } else {
            Send(ParentId, new TEvPrivate::TEvUploadFinished(Key, Url, SentSize));
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
        auto authInfo = Credentials.GetAuthInfo();
        Gateway->Upload(Url + "?uploadId=" + UploadId,
            IHTTPGateway::MakeYcHeaders(RequestId, authInfo.GetToken(), "application/xml", authInfo.GetAwsUserPwd(), authInfo.GetAwsSigV4()),
            xml,
            std::bind(&TS3FileWriteActor::OnMultipartUploadFinish, ActorSystem, SelfId(), ParentId, Key, Url, RequestId, SentSize, std::placeholders::_1),
            false,
            RetryPolicy);
    }

    void SafeAbortMultipartUpload() {
        try {
            AbortMultipartUpload(Credentials.GetAuthInfo());
        } catch (...) {
            LOG_W("TS3FileWriteActor", "Failed to abort multipart upload, error: " << CurrentExceptionMessage());
        }
    }

    void AbortMultipartUpload(const TS3Credentials::TAuthInfo& authInfo) {
        // Try to abort multipart upload in case of unexpected termination.
        // In case of error just logs warning.

        if (!UploadId) {
            return;
        }

        Gateway->Delete(Url + "?uploadId=" + UploadId,
            IHTTPGateway::MakeYcHeaders(RequestId, authInfo.GetToken(), "application/xml", authInfo.GetAwsUserPwd(), authInfo.GetAwsSigV4()),
            std::bind(&TS3FileWriteActor::OnMultipartUploadAbort, ActorSystem, SelfId(), TxId, RequestId, std::placeholders::_1),
            RetryPolicy);
        UploadId.clear();
    }

    void FailOnException() {
        Send(ParentId, new TEvPrivate::TEvUploadError(NYql::NDqProto::StatusIds::BAD_REQUEST, CurrentExceptionMessage()));
        SafeAbortMultipartUpload();
    }

    size_t InFlight = 0ULL;
    size_t SentSize = 0ULL;

    const TTxId TxId;
    const IHTTPGateway::TPtr Gateway;
    const TS3Credentials Credentials;
    const IHTTPGateway::TRetryPolicy::TPtr RetryPolicy;

    TActorSystem* const ActorSystem;
    TActorId ParentId;

    const TString Key;
    const TString Url;
    const TString RequestId;

    IOutputQueue::TPtr Parts;
    std::vector<TString> Tags;

    TString UploadId;
    bool DirtyWrite;
    TString Token;
};

class TS3WriteActor : public TActorBootstrapped<TS3WriteActor>, public IDqComputeActorAsyncOutput {
public:
    TS3WriteActor(ui64 outputIndex,
        TCollectStatsLevel statsLevel,
        const TTxId& txId,
        const TString& prefix,
        IHTTPGateway::TPtr gateway,
        TS3Credentials&& credentials,
        IRandomProvider* randomProvider,
        const TString& url,
        const TString& path,
        const TString& extension,
        const std::vector<TString>& keys,
        const size_t memoryLimit,
        const TString& compression,
        bool multipart,
        IDqComputeActorAsyncOutput::ICallbacks* callbacks,
        const IHTTPGateway::TRetryPolicy::TPtr& retryPolicy,
        bool dirtyWrite,
        const TString& token)
        : Gateway(std::move(gateway))
        , Credentials(std::move(credentials))
        , RandomProvider(randomProvider)
        , RetryPolicy(retryPolicy)
        , OutputIndex(outputIndex)
        , TxId(txId)
        , Prefix(prefix)
        , Callbacks(callbacks)
        , Url(url)
        , Path(path)
        , Extension(extension)
        , Keys(keys)
        , MemoryLimit(memoryLimit)
        , Compression(compression)
        , Multipart(multipart)
        , DirtyWrite(dirtyWrite)
        , Token(token)
    {
        if (!RandomProvider) {
            DefaultRandomProvider = CreateDefaultRandomProvider();
            RandomProvider = DefaultRandomProvider.Get();
        }
        EgressStats.Level = statsLevel;
    }

    void Bootstrap() {
        LOG_D("TS3WriteActor", "Bootstrap");
        Become(&TS3WriteActor::StateFunc);
    }

    static constexpr char ActorName[] = "S3_WRITE_ACTOR";
private:
    void CommitState(const NDqProto::TCheckpoint&) final {};
    void LoadState(const NDqProto::TSinkState&) final {};

    ui64 GetOutputIndex() const final {
        return OutputIndex;
    }

    const TDqAsyncStats& GetEgressStats() const final {
        return EgressStats;
    }

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

    void SendData(TUnboxedValueBatch&& data, i64, const TMaybe<NDqProto::TCheckpoint>&, bool finished) final {
        std::unordered_set<TS3FileWriteActor*> processedActors;
        YQL_ENSURE(!data.IsWide(), "Wide stream is not supported yet");
        EgressStats.Resume();
        data.ForEachRow([&](const auto& row) {
            const auto& key = MakePartitionKey(row);
            const auto [keyIt, insertedNew] = FileWriteActors.emplace(key, std::vector<TS3FileWriteActor*>());
            if (insertedNew || keyIt->second.empty() || keyIt->second.back()->IsFinishing()) {
            auto fileWrite = std::make_unique<TS3FileWriteActor>(
                TxId,
                Gateway,
                Credentials,
                key,
                NS3Util::UrlEscapeRet(Url + Path + key + MakeOutputName() + Extension),
                Compression,
                RetryPolicy, DirtyWrite, Token);
            keyIt->second.emplace_back(fileWrite.get());
                RegisterWithSameMailbox(fileWrite.release());
            }

            const NUdf::TUnboxedValue& value = Keys.empty() ? row : *row.GetElements();
            TS3FileWriteActor* actor = keyIt->second.back();
            if (value) {
                actor->AddData(TString(value.AsStringRef()));
            }
            if (!Multipart || !value) {
                actor->Seal();
            }
            processedActors.insert(actor);
        });

        for (TS3FileWriteActor* actor : processedActors) {
            actor->Go();
        }

        if (finished) {
            std::for_each(
                FileWriteActors.cbegin(),
                FileWriteActors.cend(),
                [](const std::pair<const TString, std::vector<TS3FileWriteActor*>>& item) {
                    item.second.back()->Finish();
                });
            Finished = true;
            FinishIfNeeded();
        }
        data.clear();
    }

    void Handle(TEvPrivate::TEvUploadError::TPtr& result) {
        LOG_W("TS3WriteActor", "TEvUploadError " << result->Get()->Issues.ToOneLineString());

        NDqProto::StatusIds::StatusCode statusCode = result->Get()->StatusCode;
        if (statusCode == NDqProto::StatusIds::UNSPECIFIED) {
            statusCode = StatusFromS3ErrorCode(result->Get()->S3ErrorCode);
        }

        Callbacks->OnAsyncOutputError(OutputIndex, result->Get()->Issues, statusCode);
    }

    void FinishIfNeeded() {
        if (FileWriteActors.empty() && Finished) {
            LOG_D("TS3WriteActor", "Finished, notify owner");
            Callbacks->OnAsyncOutputFinished(OutputIndex);
        }
    }

    void Handle(TEvPrivate::TEvUploadFinished::TPtr& result) {
        if (const auto it = FileWriteActors.find(result->Get()->Key); FileWriteActors.cend() != it) {
            EgressStats.Bytes += result->Get()->UploadSize;
            EgressStats.Chunks++;
            EgressStats.Splits++;
            EgressStats.Resume();
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
            LOG_W("TS3WriteActor", "PassAway: " << " with " << fileWriterCount << " NOT finished FileWriter(s)");
        } else {
            LOG_D("TS3WriteActor", "PassAway");
        }

        TActorBootstrapped<TS3WriteActor>::PassAway();
    }

    const IHTTPGateway::TPtr Gateway;
    const TS3Credentials Credentials;
    IRandomProvider* RandomProvider;
    TIntrusivePtr<IRandomProvider> DefaultRandomProvider;
    const IHTTPGateway::TRetryPolicy::TPtr RetryPolicy;

    const ui64 OutputIndex;
    TDqAsyncStats EgressStats;
    const TTxId TxId;
    const TString Prefix;
    IDqComputeActorAsyncOutput::ICallbacks *const Callbacks;

    const TString Url;
    const TString Path;
    const TString Extension;
    const std::vector<TString> Keys;

    const size_t MemoryLimit;
    const TString Compression;
    const bool Multipart;
    bool Finished = false;

    std::unordered_map<TString, std::vector<TS3FileWriteActor*>> FileWriteActors;
    bool DirtyWrite;
    TString Token;
};

} // namespace

std::pair<IDqComputeActorAsyncOutput*, NActors::IActor*> CreateS3WriteActor(
    const NKikimr::NMiniKQL::TTypeEnvironment&,
    const NKikimr::NMiniKQL::IFunctionRegistry&,
    IRandomProvider* randomProvider,
    IHTTPGateway::TPtr gateway,
    NS3::TSink&& params,
    ui64 outputIndex,
    TCollectStatsLevel statsLevel,
    const TTxId& txId,
    const TString& prefix,
    const THashMap<TString, TString>& secureParams,
    IDqComputeActorAsyncOutput::ICallbacks* callbacks,
    ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory,
    const IHTTPGateway::TRetryPolicy::TPtr& retryPolicy)
{
    const auto token = secureParams.Value(params.GetToken(), TString{});
    const auto actor = new TS3WriteActor(
        outputIndex,
        statsLevel,
        txId,
        prefix,
        std::move(gateway),
        TS3Credentials(credentialsFactory, token),
        randomProvider, params.GetUrl(),
        params.GetPath(),
        params.GetExtension(),
        std::vector<TString>(params.GetKeys().cbegin(), params.GetKeys().cend()),
        params.HasMemoryLimit() ? params.GetMemoryLimit() : 1_GB,
        params.GetCompression(),
        params.GetMultipart(),
        callbacks,
        retryPolicy,
        !params.GetAtomicUploadCommit(),
        token);
    return {actor, actor};
}

} // namespace NYql::NDq
