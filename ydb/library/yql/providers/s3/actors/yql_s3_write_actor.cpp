#include "yql_s3_write_actor.h"
#include "yql_s3_actors_util.h"
#if defined(_linux_) || defined(_darwin_)
#include "yql_arrow_column_converters.h"
#endif

#include <arrow/result.h>
#include <arrow/table.h>
#include <arrow/util/type_fwd.h>
#include <library/cpp/string_utils/base64/base64.h>
#include <parquet/arrow/writer.h>
#include <util/generic/size_literals.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/http/http.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/library/yql/providers/common/http_gateway/yql_http_default_retry_policy.h>
#include <ydb/library/yql/providers/s3/common/util.h>
#include <ydb/library/yql/providers/s3/compressors/factory.h>
#include <ydb/library/yql/providers/s3/credentials/credentials.h>

#include <yql/essentials/minikql/mkql_program_builder.h>
#include <yql/essentials/providers/common/provider/yql_provider_names.h>
#include <yql/essentials/providers/common/schema/mkql/yql_mkql_schema.h>
#include <yql/essentials/utils/yql_panic.h>

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
        EvResumeExecution,

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
        static constexpr size_t BODY_MAX_SIZE = 1_KB;

        TEvUploadError(NDqProto::StatusIds::StatusCode status, TIssues&& issues)
            : Status(status)
            , Issues(std::move(issues))
        {}

        TEvUploadError(const TString& message, const TString& requestId, const TString& responseBody, const IHTTPGateway::TResult& result)
            : Status(NDqProto::StatusIds::INTERNAL_ERROR)
        {
            if (responseBody) {
                if (const TS3Result s3Result(responseBody); s3Result.IsError) {
                    if (s3Result.Parsed) {
                        Status = StatusFromS3ErrorCode(s3Result.S3ErrorCode);
                        Issues.AddIssue(TStringBuilder() << "Error code: " << s3Result.S3ErrorCode);
                        Issues.AddIssue(TStringBuilder() << "Error message: " << s3Result.ErrorMessage);
                    } else {
                        Issues.AddIssue(TStringBuilder() << "Failed to parse s3 response: " << s3Result.ErrorMessage);
                    }
                    Issues = NS3Util::AddParentIssue("S3 issues", TIssues(Issues));
                }
            }
            Issues.AddIssues(NS3Util::AddParentIssue("Http geteway issues", TIssues(result.Issues)));
            if (result.CurlResponseCode != CURLE_OK) {
                Issues.AddIssue(TStringBuilder() << "CURL response code: " << curl_easy_strerror(result.CurlResponseCode));
            }
            if (Status == NDqProto::StatusIds::INTERNAL_ERROR) {
                Issues.AddIssues(NS3Util::AddParentIssue("Http request info", {
                    TIssue(TStringBuilder() << "Response code: " << result.Content.HttpResponseCode),
                    TIssue(TStringBuilder() << "Headers: " << result.Content.Headers),
                    TIssue(TStringBuilder() << "Body: \"" << TStringBuf(responseBody).Trunc(BODY_MAX_SIZE) << (responseBody.size() > BODY_MAX_SIZE ? "\"..." : "\""))
                }));
            }
            Issues = NS3Util::AddParentIssue(TStringBuilder() << message << ", s3 request id: [" << requestId << "]", TIssues(Issues));
        }

        NDqProto::StatusIds::StatusCode Status = NDqProto::StatusIds::UNSPECIFIED;
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

    struct TEvResumeExecution : public TEventLocal<TEvResumeExecution, EvResumeExecution> {
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
            UploadStarted = true;
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
                "",
                std::bind(&TS3FileWriteActor::OnUploadsCreated, ActorSystem, SelfId(), ParentId, Url, RequestId, std::placeholders::_1),
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

    bool IsUploadStarted() const {
        return UploadStarted;
    }

    const TString& GetUrl() const {
        return Url;
    }

    i64 GetMemoryUsed(bool storedOnly) const {
        i64 result = Parts->Volume();
        if (!storedOnly) {
            result += InFlight;
        }
        return result;
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

    static void OnUploadsCreated(TActorSystem* actorSystem, TActorId selfId, TActorId parentId, const TString& url, const TString& requestId, IHTTPGateway::TResult&& result) {
        const TString body = result.Content.Extract();
        if (result.Issues) {
            actorSystem->Send(new IEventHandle(parentId, selfId, new TEvPrivate::TEvUploadError(TStringBuilder() << "Create upload response issues is not empty, url: " << url, requestId, body, result)));
            return;
        }

        try {
            const TS3Result s3Result(body);
            const auto& root = s3Result.GetRootNode();
            if (s3Result.IsError) {
                actorSystem->Send(new IEventHandle(parentId, selfId, new TEvPrivate::TEvUploadError(TStringBuilder() << "Create upload operation failed, url: " << url, requestId, body, result)));
            } else if (root.Name() != "InitiateMultipartUploadResult") {
                actorSystem->Send(new IEventHandle(parentId, selfId, new TEvPrivate::TEvUploadError(TStringBuilder() << "Unexpected response on create upload: " << root.Name() << ", url: " << url, requestId, body, result)));
            } else {
                const NXml::TNamespacesForXPath nss(1U, {"s3", "http://s3.amazonaws.com/doc/2006-03-01/"});
                actorSystem->Send(new IEventHandle(selfId, selfId, new TEvPrivate::TEvUploadStarted(root.Node("s3:UploadId", false, nss).Value<TString>())));
            }
        } catch (const std::exception& ex) {
            actorSystem->Send(new IEventHandle(parentId, selfId, new TEvPrivate::TEvUploadError(TStringBuilder() << "Failed to parse create upload response: " << ex.what() << ", url: " << url, requestId, body, result)));
        }
    }

    static void OnPartUploadFinish(TActorSystem* actorSystem, TActorId selfId, TActorId parentId, size_t size, size_t index, const TString& url, const TString& requestId, IHTTPGateway::TResult&& response) {
        const TString body = response.Content.Extract();
        if (response.Issues) {
            actorSystem->Send(new IEventHandle(parentId, selfId, new TEvPrivate::TEvUploadError(TStringBuilder() << "Part " << index << " upload finish response issues is not empty, url: " << url, requestId, body, response)));
            return;
        }

        const auto& str = response.Content.Headers;
        const auto headerStr = str.substr(str.rfind("HTTP/"));
        if (const NHttp::THeaders headers(headerStr); headers.Has("Etag")) {
            actorSystem->Send(new IEventHandle(selfId, selfId, new TEvPrivate::TEvUploadPartFinished(size, index, TString(headers.Get("Etag")))));
        } else {
            actorSystem->Send(new IEventHandle(parentId, selfId, new TEvPrivate::TEvUploadError(TStringBuilder() << "Part " << index << " upload failed, url: " << url, requestId, body, response)));
        }
    }

    static void OnMultipartUploadFinish(TActorSystem* actorSystem, TActorId selfId, TActorId parentId, const TString& key, const TString& url, const TString& requestId, ui64 sentSize, IHTTPGateway::TResult&& result) {
        const TString body = result.Content.Extract();
        if (result.Issues) {
            actorSystem->Send(new IEventHandle(parentId, selfId, new TEvPrivate::TEvUploadError(TStringBuilder() << "Multipart upload finish response issues is not empty, url: " << url, requestId, body, result)));
            return;
        }

        try {
            const TS3Result s3Result(body);
            const auto& root = s3Result.GetRootNode();
            if (s3Result.IsError) {
                actorSystem->Send(new IEventHandle(parentId, selfId, new TEvPrivate::TEvUploadError(TStringBuilder() << "Multipart upload operation failed, url: " << url, requestId, body, result)));
            } else if (root.Name() != "CompleteMultipartUploadResult") {
                actorSystem->Send(new IEventHandle(parentId, selfId, new TEvPrivate::TEvUploadError(TStringBuilder() << "Unexpected response on finish multipart upload: " << root.Name() << ", url: " << url, requestId, body, result)));
            } else {
                actorSystem->Send(new IEventHandle(parentId, selfId, new TEvPrivate::TEvUploadFinished(key, url, sentSize)));
            }
        } catch (const std::exception& ex) {
            actorSystem->Send(new IEventHandle(parentId, selfId, new TEvPrivate::TEvUploadError(TStringBuilder() << "Error on parse finish multipart upload response: " << ex.what() << ", url: " << url, requestId, body, result)));
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
        const TString body = result.Content.Extract();
        if (result.Issues) {
            actorSystem->Send(new IEventHandle(parentId, selfId, new TEvPrivate::TEvUploadError(TStringBuilder() << "Upload finish response issues is not empty, url: " << url, requestId, body, result)));
            return;
        }

        if (result.Content.HttpResponseCode >= 300) {
            actorSystem->Send(new IEventHandle(parentId, selfId, new TEvPrivate::TEvUploadError(TStringBuilder() << "Upload operation failed, url: " << url, requestId, body, result)));
        } else {
            actorSystem->Send(new IEventHandle(selfId, selfId, new TEvPrivate::TEvUploadFinished(key, url, sentSize)));
            actorSystem->Send(new IEventHandle(parentId, selfId, new TEvPrivate::TEvUploadFinished(key, url, sentSize)));
        }
    }

    void Handle(TEvPrivate::TEvUploadStarted::TPtr& result) {
        UploadId = result->Get()->UploadId;
        UploadStarted = true;
        Become(&TS3FileWriteActor::StateFuncWrapper<&TS3FileWriteActor::MultipartWorkingStateFunc>);
        StartUploadParts();
        Send(ParentId, new TEvPrivate::TEvResumeExecution());
    }

    void Handle(TEvPrivate::TEvUploadPartFinished::TPtr& result) {
        InFlight -= result->Get()->Size;
        Tags[result->Get()->Index] = std::move(result->Get()->ETag);
        Send(ParentId, new TEvPrivate::TEvResumeExecution());

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
                std::bind(&TS3FileWriteActor::OnPartUploadFinish, ActorSystem, SelfId(), ParentId, size, index, Url, RequestId, std::placeholders::_1),
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
        Send(ParentId, new TEvPrivate::TEvUploadError(NDqProto::StatusIds::INTERNAL_ERROR, {TIssue(TStringBuilder() << "Unexpected exception: " << CurrentExceptionMessage())}));
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
    bool UploadStarted = false;
};

class TS3WriteActorBase : public TActorBootstrapped<TS3WriteActorBase>, public IDqComputeActorAsyncOutput {
    using TBase = TActorBootstrapped<TS3WriteActorBase>;

public:
    struct TParams {
        TTxId TxId;
        ui64 OutputIndex;
        TString Prefix;
        TString Url;
        TString Path;
        TString Extension;

        IHTTPGateway::TPtr Gateway;
        TS3Credentials Credentials;
        TString Token;
        IRandomProvider* RandomProvider;
        IDqComputeActorAsyncOutput::ICallbacks* Callbacks;
        IHTTPGateway::TRetryPolicy::TPtr RetryPolicy;

        TCollectStatsLevel StatsLevel;
        size_t MemoryLimit;
        TString Compression;
        bool Multipart;
        bool DirtyWrite;
    };

    explicit TS3WriteActorBase(const TParams& params)
        : TxId(params.TxId)
        , OutputIndex(params.OutputIndex)
        , Prefix(params.Prefix)
        , Url(params.Url)
        , Path(params.Path)
        , Extension(params.Extension)
        , Gateway(params.Gateway)
        , Credentials(params.Credentials)
        , Token(params.Token)
        , Callbacks(params.Callbacks)
        , RetryPolicy(params.RetryPolicy)
        , MemoryLimit(params.MemoryLimit)
        , Compression(params.Compression)
        , Multipart(params.Multipart)
        , DirtyWrite(params.DirtyWrite)
        , RandomProvider(params.RandomProvider)
    {
        if (!RandomProvider) {
            DefaultRandomProvider = CreateDefaultRandomProvider();
            RandomProvider = DefaultRandomProvider.Get();
        }
        EgressStats.Level = params.StatsLevel;
    }

    static constexpr char ActorName[] = "S3_WRITE_ACTOR";

    void Bootstrap() {
        LOG_D("TS3WriteActor", "Bootstrap");
        Become(&TS3WriteActorBase::StateFunc);
    }

protected:
    // Should call AddData or Seal for each data part
    virtual void DoSendData(TUnboxedValueBatch& data, bool finished) = 0;

    // For each partition key at most one file write actor is not sealed (last in FileWriteActors[key]),
    // add data into this actor or create new if all actors are sealed for this key
    void AddData(const TString& key, TString&& data) {
        const auto [keyIt, insertedNew] = FileWriteActors.emplace(key, std::vector<TS3FileWriteActor*>());
        if (insertedNew || keyIt->second.empty() || keyIt->second.back()->IsFinishing()) {
            auto fileWriteActor = std::make_unique<TS3FileWriteActor>(
                TxId, Gateway, Credentials, key,
                NS3Util::UrlEscapeRet(Url + Path + key + MakeOutputName() + Extension),
                Compression, RetryPolicy, DirtyWrite, Token
            );
            keyIt->second.emplace_back(fileWriteActor.get());
            RegisterWithSameMailbox(fileWriteActor.release());
        }

        auto* actor = keyIt->second.back();
        actor->AddData(std::move(data));
        ProcessedActors.insert(actor);
    }

    // Seal last file write actor if it is not already sealed
    void Seal(const TString& key) {
        const auto keyIt = FileWriteActors.find(key);
        if (keyIt == FileWriteActors.end() || keyIt->second.empty() || keyIt->second.back()->IsFinishing()) {
            return;
        }

        TS3FileWriteActor* actor = keyIt->second.back();
        actor->Seal();
        ProcessedActors.insert(actor);
    }

private:
    void CommitState(const NDqProto::TCheckpoint&) final {}
    void LoadState(const TSinkState&) final {}

    ui64 GetOutputIndex() const final {
        return OutputIndex;
    }

    const TDqAsyncStats& GetEgressStats() const final {
        return EgressStats;
    }

    i64 GetUsedSpace(bool storedOnly) const {
        return std::accumulate(FileWriteActors.cbegin(), FileWriteActors.cend(), i64(0), [storedOnly](i64 sum, const std::pair<const TString, std::vector<TS3FileWriteActor*>>& item) {
            return sum + std::accumulate(item.second.cbegin(), item.second.cend(), i64(0), [storedOnly](i64 sum, TS3FileWriteActor* actor) {
                return sum + actor->GetMemoryUsed(storedOnly);
            });
        });
    }

    i64 GetFreeSpace() const final {
        return i64(MemoryLimit) - GetUsedSpace(/* storedOnly */ false);
    }

    TString MakeOutputName() const {
        const auto rand = std::make_tuple(RandomProvider->GenUuid4(), RandomProvider->GenRand());
        return Prefix + Base64EncodeUrl(TStringBuf(reinterpret_cast<const char*>(&rand), sizeof(rand)));
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvPrivate::TEvUploadError, Handle);
        hFunc(TEvPrivate::TEvUploadFinished, Handle);
        sFunc(TEvPrivate::TEvResumeExecution, ResumeExecution);
    )

    void SendData(TUnboxedValueBatch&& data, i64, const TMaybe<NDqProto::TCheckpoint>&, bool finished) final {
        ProcessedActors.clear();
        EgressStats.Resume();

        DoSendData(data, finished);

        for (TS3FileWriteActor* actor : ProcessedActors) {
            actor->Go();
        }
        if (GetFreeSpace() <= 0) {
            CheckDeadlock();
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
        auto status = result->Get()->Status;
        auto issues = std::move(result->Get()->Issues);
        LOG_W("TS3WriteActor", "TEvUploadError, status: " << NDqProto::StatusIds::StatusCode_Name(status) << ", issues: " << issues.ToOneLineString());

        if (status == NDqProto::StatusIds::UNSPECIFIED) {
            status = NDqProto::StatusIds::INTERNAL_ERROR;
            issues.AddIssue("Got upload error with unspecified error code.");
        }

        Callbacks->OnAsyncOutputError(OutputIndex, issues, status);
    }

    void FinishIfNeeded() const {
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
                if (it->second.empty()) {
                    FileWriteActors.erase(it);
                }
            }
        }
        ResumeExecution();
        FinishIfNeeded();
    }

    void ResumeExecution() {
        if (Finished) {
            return;
        }
        if (GetFreeSpace() > 0) {
            LOG_D("TS3WriteActor", "Has free space, notify owner");
            Callbacks->ResumeExecution();
            return;
        }
        CheckDeadlock();
    }

    void CheckDeadlock() const {
        const bool isAllStarted = std::all_of(FileWriteActors.cbegin(), FileWriteActors.cend(), [](const std::pair<const TString, std::vector<TS3FileWriteActor*>>& item) {
            return std::all_of(item.second.cbegin(), item.second.cend(), [](TS3FileWriteActor* actor) {
                return actor->IsUploadStarted();
            });
        });
        if (!isAllStarted) {
            // Wait start notification from all actors
            return;
        }

        const auto usedSpace = GetUsedSpace(/* storedOnly */ false);
        if (usedSpace >= i64(MemoryLimit) && usedSpace == GetUsedSpace(/* storedOnly */ true)) {
            // If all data is not inflight and all uploads running now -- deadlock occurred
            Callbacks->OnAsyncOutputError(OutputIndex, {NYql::TIssue(TStringBuilder() << "Writing deadlock occurred, please increase write actor memory limit (used " << usedSpace << " bytes for " << FileWriteActors.size() << " partitions)")}, NDqProto::StatusIds::INTERNAL_ERROR);
        }
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

        TBase::PassAway();
    }

protected:
    const TTxId TxId;
    const ui64 OutputIndex;
    const TString Prefix;
    const TString Url;
    const TString Path;
    const TString Extension;

    const IHTTPGateway::TPtr Gateway;
    const TS3Credentials Credentials;
    const TString Token;
    IDqComputeActorAsyncOutput::ICallbacks* const Callbacks;
    const IHTTPGateway::TRetryPolicy::TPtr RetryPolicy;

    const size_t MemoryLimit;
    const TString Compression;
    const bool Multipart;
    const bool DirtyWrite;

private:
    IRandomProvider* RandomProvider;
    TIntrusivePtr<IRandomProvider> DefaultRandomProvider;
    TDqAsyncStats EgressStats;

    bool Finished = false;
    std::unordered_set<TS3FileWriteActor*> ProcessedActors;
    std::unordered_map<TString, std::vector<TS3FileWriteActor*>> FileWriteActors;
};

class TS3ScalarWriteActor : public TS3WriteActorBase {
    using TBase = TS3WriteActorBase;

public:
    TS3ScalarWriteActor(const std::vector<TString>& keys, const TBase::TParams& params)
        : TBase(params)
        , Keys(keys)
    {}

private:
    void DoSendData(TUnboxedValueBatch& data, bool finished) final {
        Y_UNUSED(finished);
        YQL_ENSURE(!data.IsWide(), "Wide input is not supported for scalar writer");

        data.ForEachRow([&](const NUdf::TUnboxedValue& row) {
            const auto& key = MakePartitionKey(row);
            const NUdf::TUnboxedValue& value = Keys.empty() ? row : *row.GetElements();
            if (value) {
                AddData(key, TString(value.AsStringRef()));
            }
            if (!value || !Multipart) {
                Seal(key);
            }
        });
    }

    TString MakePartitionKey(const NUdf::TUnboxedValuePod& value) const {
        if (Keys.empty()) {
            return {};
        }

        auto elements = value.GetElements();
        TStringBuilder key;
        for (const auto& k : Keys) {
            const std::string_view keyPart = (++elements)->AsStringRef();
            YQL_ENSURE(std::string_view::npos == keyPart.find('/'), "Invalid partition key, contains '/': " << keyPart);
            key << k << '=' << keyPart << '/';
        }
        return UrlEscapeRet(key);
    }

private:
    const std::vector<TString> Keys;
};

#if defined(_linux_) || defined(_darwin_)
class TS3BlockWriteActor : public TS3WriteActorBase {
    using TBase = TS3WriteActorBase;

    class TBlockWriter : public arrow::io::OutputStream {
    public:
        TBlockWriter(TS3BlockWriteActor& self)
            : Self(self)
            , SerializedOutput(Self.SerializedData)
        {}

        arrow::Status Write(const void* data, int64_t nbytes) override {
            if (Closed) {
                return arrow::Status::IOError("Cannot write to closed stream");
            }
            Position += nbytes;
            SerializedOutput << TStringBuf(reinterpret_cast<const char*>(data), nbytes);
            Self.BatchSize += nbytes;
            Self.FileSize += nbytes;
            return arrow::Status::OK();
        }

        arrow::Status Close() override {
            Self.BatchSize = 0;
            Self.FileSize = 0;
            Closed = true;
            return arrow::Status::OK();
        }

        arrow::Result<int64_t> Tell() const override {
            return Position;
        }

        bool closed() const override {
            return Closed;
        }

    private:
        TS3BlockWriteActor& Self;
        TStringOutput SerializedOutput;
        int64_t Position = 0;
        bool Closed = false;
    };

public:
    struct TBlockSettings {
        std::shared_ptr<arrow::Schema> ArrowSchema;
        std::vector<TColumnConverter> ColumnConverters;
        ui64 MaxFileSize;
        ui64 MaxBlockSize;
    };

    TS3BlockWriteActor(TBlockSettings&& blockSettings, const TBase::TParams& params)
        : TBase(params)
        , ArrowSchema(std::move(blockSettings.ArrowSchema))
        , ColumnConverters(std::move(blockSettings.ColumnConverters))
        , MaxFileSize(blockSettings.MaxFileSize ? blockSettings.MaxFileSize : 50_MB)
        , MaxBlockSize(blockSettings.MaxBlockSize ? blockSettings.MaxBlockSize : 1_MB)
    {}

private:
    void DoSendData(TUnboxedValueBatch& data, bool finished) final {
        YQL_ENSURE(data.IsWide(), "Expected wide input for block writer");

        data.ForEachRowWide([&](NYql::NUdf::TUnboxedValue* values, ui32 width) {
            SerializeValue(values, width);

            const bool finishFile = FileSize > MaxFileSize || finished || !Multipart;
            if (BatchSize > MaxBlockSize || finishFile) {
                FlushWriter(finishFile);
            }
        });

        if (finished && FileSize) {
            FlushWriter(true);
        }
    }

    void SerializeValue(NYql::NUdf::TUnboxedValue* values, ui32 width) {
        YQL_ENSURE(width, "Expected non zero width for block output");

        std::vector<std::shared_ptr<arrow::Array>> columns;
        columns.reserve(width - 1);
        for (ui32 i = 0; i < width - 1; ++i) {
            columns.emplace_back(TArrowBlock::From(values[i]).GetDatum().make_array());
        }
        const auto numRows = TArrowBlock::From(values[width - 1]).GetDatum().scalar_as<arrow::UInt64Scalar>().value;

        auto tableResult = arrow::Table::FromRecordBatches({
            ConvertArrowColumns(arrow::RecordBatch::Make(ArrowSchema, numRows, std::move(columns)), ColumnConverters)
        });
        ARROW_OK(tableResult.status());
        const auto table = std::move(tableResult).ValueOrDie();

        if (!Writer) {
            ARROW_OK(parquet::arrow::FileWriter::Open(
                *ArrowSchema,
                arrow::default_memory_pool(),
                std::make_shared<TBlockWriter>(*this),
                parquet::WriterProperties::Builder().compression(arrow::Compression::SNAPPY)->build(),
                &Writer
            ));
        }
        ARROW_OK(Writer->WriteTable(*table, numRows));
    }

    void FlushWriter(bool close) {
        if (!Writer) {
            return;
        }

        if (close) {
            ARROW_OK(Writer->Close());
            Writer.reset();
            FileSize = 0;
        }

        AddData("", std::move(SerializedData));
        if (close) {
            Seal("");
        }

        SerializedData = TString();
        if (Multipart) {
            SerializedData.reserve(std::min(MaxBlockSize, MaxFileSize));
        }
        BatchSize = 0;
    }

private:
    const std::shared_ptr<arrow::Schema> ArrowSchema;
    std::vector<TColumnConverter> ColumnConverters;
    const ui64 MaxFileSize;
    const ui64 MaxBlockSize;

    ui64 BatchSize = 0;
    ui64 FileSize = 0;
    TString SerializedData;
    std::unique_ptr<parquet::arrow::FileWriter> Writer;
};
#endif

} // namespace

std::pair<IDqComputeActorAsyncOutput*, NActors::IActor*> CreateS3WriteActor(
    const NKikimr::NMiniKQL::TTypeEnvironment& typeEnv,
    const NKikimr::NMiniKQL::IFunctionRegistry& functionRegistry,
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
    const TS3WriteActorBase::TParams s3Params = {
        .TxId = txId,
        .OutputIndex = outputIndex,
        .Prefix = prefix,
        .Url = params.GetUrl(),
        .Path = params.GetPath(),
        .Extension = params.GetExtension(),
        .Gateway = gateway,
        .Credentials = TS3Credentials(credentialsFactory, token),
        .Token = token,
        .RandomProvider = randomProvider,
        .Callbacks = callbacks,
        .RetryPolicy = retryPolicy,
        .StatsLevel = statsLevel,
        .MemoryLimit = params.HasMemoryLimit() ? params.GetMemoryLimit() : 1_GB,
        .Compression = params.GetCompression(),
        .Multipart = params.GetMultipart(),
        .DirtyWrite = !params.GetAtomicUploadCommit(),
    };

    if (!params.HasArrowSettings()) {
        const auto actor = new TS3ScalarWriteActor(std::vector<TString>(params.GetKeys().cbegin(), params.GetKeys().cend()), s3Params);
        return {actor, actor};
    }

#if defined(_linux_) || defined(_darwin_)
    const auto& arrowSettings = params.GetArrowSettings();

    const auto programBuilder = std::make_unique<TProgramBuilder>(typeEnv, functionRegistry);
    const auto outputItemType = NCommon::ParseTypeFromYson(TStringBuf(arrowSettings.GetRowType()), *programBuilder, Cerr);
    YQL_ENSURE(outputItemType->IsStruct(), "Row type is not struct");
    const auto structType = static_cast<TStructType*>(outputItemType);

    arrow::SchemaBuilder builder;
    for (ui32 i = 0; i < structType->GetMembersCount(); ++i) {
        const auto memberType = structType->GetMemberType(i);
        std::shared_ptr<arrow::DataType> dataType;
        YQL_ENSURE(S3ConvertArrowOutputType(memberType, dataType), "Unsupported arrow type");

        const std::string memberName(structType->GetMemberName(i));
        ARROW_OK(builder.AddField(std::make_shared<arrow::Field>(memberName, dataType, memberType->IsOptional())));
    }

    auto schemaResult = builder.Finish();
    ARROW_OK(schemaResult.status());
    auto schema = std::move(schemaResult).ValueOrDie();

    TS3BlockWriteActor::TBlockSettings settings = {
        .ArrowSchema = std::move(schema),
        .MaxFileSize = arrowSettings.GetMaxFileSize(),
        .MaxBlockSize = arrowSettings.GetMaxBlockSize(),
    };
    BuildOutputColumnConverters(structType, settings.ColumnConverters);

    const auto actor = new TS3BlockWriteActor(std::move(settings), s3Params);
    return {actor, actor};
#else
    YQL_ENSURE(false, "Block sink is not supported for this platform");
    return {nullptr, nullptr};
#endif
}

} // namespace NYql::NDq
