#include "yql_s3_actors_util.h"
#include "yql_s3_applicator_actor.h"

#include <ydb/core/fq/libs/events/events.h>
#include <ydb/library/services/services.pb.h>

#include <ydb/library/yql/providers/common/http_gateway/yql_http_default_retry_policy.h>
#include <ydb/library/yql/providers/s3/common/util.h>
#include <ydb/library/yql/providers/s3/credentials/credentials.h>
#include <ydb/library/yql/providers/s3/proto/sink.pb.h>
#include <ydb/library/yql/utils/url_builder.h>
#include <ydb/library/yql/utils/yql_panic.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>

#ifdef THROW
#undef THROW
#endif
#include <library/cpp/xml/document/xml-document.h>

#include <queue>

#define LOG_E(stream) LOG_ERROR_S(*NActors::TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "QueryId: " << QueryId << " TS3ApplicatorActor " << stream)
#define LOG_W(stream) LOG_WARN_S( *NActors::TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "QueryId: " << QueryId << " TS3ApplicatorActor " << stream)
#define LOG_D(stream) LOG_DEBUG_S(*NActors::TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "QueryId: " << QueryId << " TS3ApplicatorActor " << stream)
#define LOG_T(stream) LOG_TRACE_S(*NActors::TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "QueryId: " << QueryId << " TS3ApplicatorActor " << stream)

namespace NYql::NDq {

namespace {

struct TCompleteMultipartUpload {

    using TPtr = std::shared_ptr<TCompleteMultipartUpload>;

    TString RequestId;
    TString Url;
    TString UploadId;
    TString Token;
    std::vector<TString> Tags;

    TCompleteMultipartUpload(const TString& requestId, const TString& url, const TString& uploadId, const TString& token)
        : RequestId(requestId), Url(url), UploadId(uploadId), Token(token) {
    }

    TString BuildUrl() const {
        TUrlBuilder urlBuilder(Url);
        urlBuilder.AddUrlParam("uploadId", UploadId);
        return urlBuilder.Build();
    }

    TString BuildMessage() const {
        TStringBuilder result;
        result << "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" << Endl;
        result << "<CompleteMultipartUpload xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">" << Endl;
        ui32 i = 0;
        for (const auto& tag : Tags)
            result << "<Part><PartNumber>" << ++i << "</PartNumber><ETag>" << tag << "</ETag></Part>" << Endl;
        result << "</CompleteMultipartUpload>" << Endl;
        return result;
    }
};

struct TListMultipartUploads {

    using TPtr = std::shared_ptr<TListMultipartUploads>;

    TString RequestId;
    TString Url;
    TString Prefix;
    TString Token;
    TString KeyMarker;
    TString UploadIdMarker;

    TListMultipartUploads(const TString& requestId, const TString& url, const TString& prefix, const TString& token,
        const TString& keyMarker = "", const TString& uploadIdMarker = "")
        : RequestId(requestId), Url(url), Prefix(prefix), Token(token), KeyMarker(keyMarker), UploadIdMarker(uploadIdMarker) {
    }

    TString BuildUrl() const {
        // We have to sort the cgi parameters for the correct aws signature
        // This requirement will be fixed in the curl library
        // https://github.com/curl/curl/commit/fc76a24c53b08cdf6eec8ba787d8eac64651d56e
        // https://github.com/curl/curl/commit/c87920353883ef9d5aa952e724a8e2589d76add5
        TUrlBuilder urlBuilder(Url);
        if (KeyMarker) {
            urlBuilder.AddUrlParam("key-marker", KeyMarker);
        }
        urlBuilder.AddUrlParam("prefix", Prefix);
        if (UploadIdMarker) {
            urlBuilder.AddUrlParam("upload-id-marker", UploadIdMarker);
        }
        urlBuilder.AddUrlParam("uploads");
        return urlBuilder.Build();
    }
};

struct TAbortMultipartUpload {

    using TPtr = std::shared_ptr<TAbortMultipartUpload>;

    TString RequestId;
    TString Url;
    TString UploadId;
    TString Token;

    TAbortMultipartUpload(const TString& requestId, const TString& url, const TString& uploadId, const TString& token)
        : RequestId(requestId), Url(url), UploadId(uploadId), Token(token) {
    }

    TString BuildUrl() const {
        TUrlBuilder urlBuilder(Url);
        urlBuilder.AddUrlParam("uploadId", UploadId);
        return urlBuilder.Build();
    }
};

struct TListParts {

    using TPtr = std::shared_ptr<TListParts>;

    TString RequestId;
    TString Url;
    TString UploadId;
    TString Token;
    TString PartNumberMarker;
    TCompleteMultipartUpload::TPtr CompleteState;

    TListParts(const TString& requestId, const TString& url, const TString& uploadId, const TString& token, 
        TCompleteMultipartUpload::TPtr completeState)
        : RequestId(requestId), Url(url), UploadId(uploadId), Token(token), CompleteState(completeState) {
    }

    TString BuildUrl() const {
        // We have to sort the cgi parameters for the correct aws signature
        // This requirement will be fixed in the curl library
        // https://github.com/curl/curl/commit/fc76a24c53b08cdf6eec8ba787d8eac64651d56e
        // https://github.com/curl/curl/commit/c87920353883ef9d5aa952e724a8e2589d76add5
        TUrlBuilder urlBuilder(Url);
        if (PartNumberMarker) {
            urlBuilder.AddUrlParam("part-number-marker", PartNumberMarker);
        }
        urlBuilder.AddUrlParam("uploadId", UploadId);
        return urlBuilder.Build();
    }
};

struct TEvPrivate {
    // Event ids
    enum EEv : ui32 {
        EvBegin = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),

        EvCommitMultipartUpload = EvBegin,
        EvListMultipartUploads,
        EvAbortMultipartUpload,
        EvListParts,

        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE)");

    // Events
    struct TEvCommitMultipartUpload : public NActors::TEventLocal<TEvCommitMultipartUpload, EvCommitMultipartUpload> {
        TEvCommitMultipartUpload(TCompleteMultipartUpload::TPtr state, IHTTPGateway::TResult&& result)
            : State(state), Result(std::move(result)) {
        }
        TCompleteMultipartUpload::TPtr State;
        IHTTPGateway::TResult Result;
    };

    struct TEvListMultipartUploads : public NActors::TEventLocal<TEvListMultipartUploads, EvListMultipartUploads> {
        TEvListMultipartUploads(TListMultipartUploads::TPtr state, IHTTPGateway::TResult&& result)
            : State(state), Result(std::move(result)) {
        }
        TListMultipartUploads::TPtr State;
        IHTTPGateway::TResult Result;
    };

    struct TEvAbortMultipartUpload : public NActors::TEventLocal<TEvAbortMultipartUpload, EvAbortMultipartUpload> {
        TEvAbortMultipartUpload(TAbortMultipartUpload::TPtr state, IHTTPGateway::TResult&& result)
            : State(state), Result(std::move(result)) {
        }
        TAbortMultipartUpload::TPtr State;
        IHTTPGateway::TResult Result;
    };

    struct TEvListParts : public NActors::TEventLocal<TEvListParts, EvListParts> {
        TEvListParts(TListParts::TPtr state, IHTTPGateway::TResult&& result)
            : State(state), Result(std::move(result)) {
        }
        TListParts::TPtr State;
        IHTTPGateway::TResult Result;
    };
};

class TS3ApplicatorActor;

using TObjectStorageRequest = std::function<void(TS3ApplicatorActor& actor)>;

class TS3ApplicatorActor : public NActors::TActorBootstrapped<TS3ApplicatorActor> {
    static constexpr ui64 GLOBAL_RETRY_LIMIT = 100;

public:
    using NActors::TActorBootstrapped<TS3ApplicatorActor>::Send;

    static constexpr char ActorName[] = "S3_APPLICATOR_ACTOR";

    TS3ApplicatorActor(
        NActors::TActorId parentId,
        IHTTPGateway::TPtr gateway,
        const TString& queryId,
        const TString& jobId,
        std::optional<ui32> restartNumber,
        bool commit,
        const THashMap<TString, TString>& secureParams,
        ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory,
        const NYql::NDqProto::TExternalEffect& externalEffect)
    : ParentId(parentId)
    , Gateway(gateway) 
    , QueryId(queryId)
    , KeyPrefix(jobId + "_")
    , KeySubPrefix(restartNumber ? ToString(*restartNumber) + "_" : "")
    , Commit(commit)
    , SecureParams(secureParams)
    , CredentialsFactory(credentialsFactory)
    , ExternalEffect(externalEffect)
    , ActorSystem(NActors::TActivationContext::ActorSystem())
    , RetryPolicy(NYql::GetHTTPDefaultRetryPolicy(NYql::THttpRetryPolicyOptions{.MaxRetries = 3, .RetriedCurlCodes = NYql::FqRetriedCurlCodes()}))
    , RetryCount(GLOBAL_RETRY_LIMIT) {
        // ^^^ 3 retries in HTTP GW per operation
        // up to 100 retries at app level for all operations ^^^
    }

    void Bootstrap() {
        Become(&TS3ApplicatorActor::StateFunc);
        LOG_D("Bootstrapped with " << ExternalEffect.GetEffects().size() << " effect(s) to " << (Commit ? "COMMIT" : "ROLLBACK"));
        for (auto& effect : ExternalEffect.GetEffects()) {
            NYql::NS3::TEffect sinkEffect;
            YQL_ENSURE(sinkEffect.ParseFromString(effect.GetData()), "S3Sink Effect is corrupted");
            if (sinkEffect.HasCommit()) {
                if (Commit) {
                    auto& commitEffect = sinkEffect.GetCommit();
                    CommitUploads.insert(commitEffect.GetUploadId());
                    auto state = std::make_shared<TCompleteMultipartUpload>(CreateGuidAsString(),
                        commitEffect.GetUrl(), commitEffect.GetUploadId(), sinkEffect.GetToken());
                    state->Tags.reserve(commitEffect.GetETag().size());
                    for (auto& tag : commitEffect.GetETag()) {
                        state->Tags.push_back(tag);
                    }
                    PushCommitMultipartUpload(state);
                }
            } else if (sinkEffect.HasCleanup()) {
                auto& cleanupEffect = sinkEffect.GetCleanup();
                auto state = std::make_shared<TListMultipartUploads>(CreateGuidAsString(),
                    cleanupEffect.GetUrl(), cleanupEffect.GetPrefix(), sinkEffect.GetToken());
                PushListMultipartUploads(state);
            }
        }
        PopRequests();
        MaybeFinish();
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvPrivate::TEvCommitMultipartUpload, Handle);
        hFunc(TEvPrivate::TEvListMultipartUploads, Handle);
        hFunc(TEvPrivate::TEvAbortMultipartUpload, Handle);
        hFunc(TEvPrivate::TEvListParts, Handle);
    )

    bool RetryOperation(IHTTPGateway::TResult& operationResult, const TString& url, const TString& operationName) {
        const auto curlResponseCode = operationResult.CurlResponseCode;
        const auto httpResponseCode = operationResult.Content.HttpResponseCode;
        const auto result = RetryCount && GetRetryState(operationName)->GetNextRetryDelay(curlResponseCode, httpResponseCode);

        if (const TString errorText = operationResult.Content.Extract()) {
            TString errorCode;
            TString message;
            if (!ParseS3ErrorResponse(errorText, errorCode, message)) {
                message = errorText;
            }
            RetryIssues.AddIssues(NS3Util::AddParentIssue(
                TStringBuilder() << "Retry operation " << operationName << ", curl error: " << curl_easy_strerror(curlResponseCode) << ", url: " << url,
                BuildIssues(httpResponseCode, errorCode, message)
            ));
        } else {
            RetryIssues.AddIssue(TStringBuilder() << "Retry operation " << operationName << ", HTTP code: " << httpResponseCode << ", curl error: " << curl_easy_strerror(curlResponseCode) << ", url: " << url);
        }

        if (result) {
            RetryCount--;
        } else {
            Issues.AddIssues(NS3Util::AddParentIssue(
                RetryCount
                    ? TStringBuilder() << "Number of retries exceeded limit for operation " << operationName
                    : TStringBuilder() << "Number of retries exceeded global limit in " << GLOBAL_RETRY_LIMIT << " retries",
                NYql::TIssues(RetryIssues)
            ));
            RetryIssues.Clear();
            Finish(true);
        }

        return result;
    }

    void PushRequest(TObjectStorageRequest request) {
        RequestQueue.push(request);
    }

    void PushCommitMultipartUpload(TCompleteMultipartUpload::TPtr state) {
        PushRequest([state](TS3ApplicatorActor& actor) {
            actor.CommitMultipartUpload(state);
        });
    }

    void PushListMultipartUploads(TListMultipartUploads::TPtr state) {
        PushRequest([state](TS3ApplicatorActor& actor) {
            actor.ListMultipartUploads(state);
        });
    }

    void PushAbortMultipartUpload(TAbortMultipartUpload::TPtr state) {
        PushRequest([state](TS3ApplicatorActor& actor) {
            actor.AbortMultipartUpload(state);
        });
    }

    void PushListParts(TListParts::TPtr state) {
        PushRequest([state](TS3ApplicatorActor& actor) {
            actor.ListParts(state);
        });
    }

    void PopRequests() {
        while (!RequestQueue.empty() && HttpRequestInflight < 100) {
            try {
                RequestQueue.front()(*this);
                RequestQueue.pop();
                HttpRequestInflight++;
            } catch (const std::exception& e) {
                LOG_E("Exception: " << e.what());
                Finish(true, TStringBuilder{} << "Error while processing HTTP request: " << e.what());
            }
        }
    }

    void RequestFinished() {
        Y_ABORT_UNLESS(HttpRequestInflight > 0);
        HttpRequestInflight--;
        PopRequests();
        MaybeFinish();
    }

    void MaybeFinish() {
        if (HttpRequestInflight == 0) {
            Finish();
        }
    }

    void Finish(bool fatal = false, const TString& message = "") {
        if (ApplicationFinished) {
            return;
        }
        ApplicationFinished = true;

        if (message) {
            Issues.AddIssue(TIssue(message));
        }
        Send(ParentId, new NFq::TEvents::TEvEffectApplicationResult(Issues, fatal));
        PassAway();
    }

    void Handle(TEvPrivate::TEvCommitMultipartUpload::TPtr& ev) {
        Process(ev);
        RequestFinished();
    }

    void Process(TEvPrivate::TEvCommitMultipartUpload::TPtr& ev) {
        auto& result = ev->Get()->Result;
        if (!result.Issues) {
            if (result.Content.HttpResponseCode == 404) {
                LOG_W("CommitMultipartUpload NOT FOUND " << ev->Get()->State->BuildUrl() << " (may be completed already)");
                return;
            }
            if (result.Content.HttpResponseCode >= 200 && result.Content.HttpResponseCode < 300) {
                TS3Result s3Result(result.Content.Extract());
                if (s3Result.IsError) {
                    Finish(true, s3Result.S3ErrorCode + ": " + s3Result.ErrorMessage);
                } else {
                    LOG_D("CommitMultipartUpload SUCCESS " << ev->Get()->State->BuildUrl());
                }
                return;
            }
        }
        const TString& url = ev->Get()->State->BuildUrl();
        LOG_D("CommitMultipartUpload ERROR " << url);
        if (RetryOperation(result, url, "CommitMultipartUpload")) {
            PushCommitMultipartUpload(ev->Get()->State);
        }
    }

    void Handle(TEvPrivate::TEvListMultipartUploads::TPtr& ev) {
        Process(ev);
        RequestFinished();
    }

    void Process(TEvPrivate::TEvListMultipartUploads::TPtr& ev) {
        auto& result = ev->Get()->Result;
        if (!result.Issues && result.Content.HttpResponseCode >= 200 && result.Content.HttpResponseCode < 300) {
            TS3Result s3Result(result.Content.Extract());
            if (s3Result.IsError) {
                Finish(true, s3Result.S3ErrorCode + ": " + s3Result.ErrorMessage);
            } else {
                LOG_D("ListMultipartUploads SUCCESS " << ev->Get()->State->BuildUrl());
                const auto& root = s3Result.GetRootNode();
                if (root.Name() == "ListMultipartUploadsResult") {
                    const NXml::TNamespacesForXPath nss(1U, {"s3", "http://s3.amazonaws.com/doc/2006-03-01/"});
                    if (root.Node("s3:IsTruncated", false, nss).Value<bool>()) {
                        // pagination
                        auto state = std::make_shared<TListMultipartUploads>(CreateGuidAsString(),
                            ev->Get()->State->Url, ev->Get()->State->Prefix, ev->Get()->State->Token,
                            root.Node("s3:NextKeyMarker", false, nss).Value<TString>(),
                            root.Node("s3:NextUploadIdMarker", false, nss).Value<TString>());
                        PushListMultipartUploads(state);
                    }
                    const auto& uploads = root.XPath("s3:Upload", true, nss);
                    for (const auto& upload : uploads) {
                        auto key = upload.Node("s3:Key", false, nss).Value<TString>();
                        auto pos = key.find_last_of('/');
                        if (pos == std::string::npos) {
                            pos = 0;
                        } else {
                            pos++;
                        }
                        auto url = ev->Get()->State->Url + key;
                        if (key.compare(pos, KeyPrefix.size(), KeyPrefix.c_str())) {
                            // unknown upload - skip and report
                            LOG_W("ListMultipartUploads UNKNOWN Upload Url " << url);
                            auto prefix = ev->Get()->State->Url + ev->Get()->State->Prefix;
                            if (!UnknownPrefixes.contains(prefix)) {
                                UnknownPrefixes.insert(prefix);
                                TIssue issue(TStringBuilder() << "Unknown uncommitted upload with prefix: " << prefix);
                                issue.SetCode(NYql::DEFAULT_ERROR, NYql::TSeverityIds::S_INFO);
                                Issues.AddIssue(std::move(issue));
                            }
                        } else {
                            pos += KeyPrefix.size();
                            auto uploadId = upload.Node("s3:UploadId", false, nss).Value<TString>();
                            if (Commit && !key.compare(pos, KeySubPrefix.size(), KeySubPrefix.c_str())) {
                                if (!CommitUploads.contains(uploadId)) {
                                    // have no explicit effect for some reason, list and commit
                                    auto requestId = CreateGuidAsString();
                                    auto state = std::make_shared<TListParts>(requestId,
                                        url, uploadId, ev->Get()->State->Token,
                                        std::make_shared<TCompleteMultipartUpload>(requestId,
                                        url, uploadId, ev->Get()->State->Token));
                                    PushListParts(state);
                                }
                            } else {
                                auto state = std::make_shared<TAbortMultipartUpload>(CreateGuidAsString(),
                                    url, uploadId, ev->Get()->State->Token);
                                PushAbortMultipartUpload(state);
                            }
                        }
                    }
                } else {
                    Finish(true, "ListMultipartUploads reply: " + root.Name());
                }
            }
            return;
        }
        const TString& url = ev->Get()->State->BuildUrl();
        LOG_D("ListMultipartUploads ERROR " << url);
        if (RetryOperation(result, url, "ListMultipartUploads")) {
            PushListMultipartUploads(ev->Get()->State);
        }
    }

    void Handle(TEvPrivate::TEvAbortMultipartUpload::TPtr& ev) {
        Process(ev);
        RequestFinished();
    }

    void Process(TEvPrivate::TEvAbortMultipartUpload::TPtr& ev) {
        auto& result = ev->Get()->Result;
        if (!result.Issues) {
            if (result.Content.HttpResponseCode == 404) {
                LOG_W("AbortMultipartUpload NOT FOUND " << ev->Get()->State->BuildUrl() << " (may be aborted already)");
                return;
            }
            if (result.Content.HttpResponseCode >= 200 && result.Content.HttpResponseCode < 300) {
                LOG_D("AbortMultipartUpload SUCCESS " << ev->Get()->State->BuildUrl());
                return;
            }
        }
        const TString& url = ev->Get()->State->BuildUrl();
        LOG_D("AbortMultipartUpload ERROR " << url);
        if (RetryOperation(result, url, "AbortMultipartUpload")) {
            PushAbortMultipartUpload(ev->Get()->State);
        }
    }

    void Handle(TEvPrivate::TEvListParts::TPtr& ev) {
        Process(ev);
        RequestFinished();
    }

    void Process(TEvPrivate::TEvListParts::TPtr& ev) {
        auto& result = ev->Get()->Result;
        if (!result.Issues) {
            if (result.Content.HttpResponseCode == 404) {
                LOG_W("ListParts NOT FOUND " << ev->Get()->State->BuildUrl() << " (multipart upload may be completed already)");
                return;
            }
            if (result.Content.HttpResponseCode >= 200 && result.Content.HttpResponseCode < 300) {
                TS3Result s3Result(result.Content.Extract());
                if (s3Result.IsError) {
                    Finish(true, s3Result.S3ErrorCode + ": " + s3Result.ErrorMessage);
                } else {
                    LOG_D("ListParts SUCCESS " << ev->Get()->State->BuildUrl());
                    const auto& root = s3Result.GetRootNode();
                    if (root.Name() == "ListPartsResult") {
                        const NXml::TNamespacesForXPath nss(1U, {"s3", "http://s3.amazonaws.com/doc/2006-03-01/"});
                        auto state = ev->Get()->State->CompleteState;
                        state->Tags.reserve(state->Tags.size() + root.Node("s3:MaxParts", false, nss).Value<ui32>());
                        const auto& parts = root.XPath("s3:Part", true, nss);
                        for (const auto& part : parts) {
                            state->Tags.push_back(part.Node("s3:ETag", false, nss).Value<TString>());
                        }
                        if (root.Node("s3:IsTruncated", false, nss).Value<bool>()) {
                            ev->Get()->State->PartNumberMarker = root.Node("s3:NextPartNumberMarker", false, nss).Value<TString>();
                            PushListParts(ev->Get()->State);
                        } else {
                            if (state->Tags.empty()) {
                                LOG_W("ListParts returned empty parts list for " << ev->Get()->State->BuildUrl() << " (multipart upload may be completed already)");
                                return;
                            }
                            PushCommitMultipartUpload(state);
                        }
                    } else {
                        Finish(true, "ListParts reply: " + root.Name());
                    }
                }
                return;
            }
        }
        const TString& url = ev->Get()->State->BuildUrl();
        LOG_D("ListParts ERROR " << url);
        if (RetryOperation(result, url, "ListParts")) {
            PushListParts(ev->Get()->State);
        }
    }

    TS3Credentials::TAuthInfo GetAuthInfo(const TString& token) {
        const auto secureToken = SecureParams.Value(token, TString{});
        return ::NYql::GetAuthInfo(CredentialsFactory, secureToken);
    }

    void CommitMultipartUpload(TCompleteMultipartUpload::TPtr state) {
        LOG_D("CommitMultipartUpload BEGIN " << state->BuildUrl());
        auto authInfo = GetAuthInfo(state->Token);
        Gateway->Upload(state->BuildUrl(),
            IHTTPGateway::MakeYcHeaders(state->RequestId, authInfo.GetToken(), "application/xml", authInfo.GetAwsUserPwd(), authInfo.GetAwsSigV4()),
            state->BuildMessage(),
            std::bind(&TS3ApplicatorActor::OnCommitMultipartUpload, ActorSystem, SelfId(), state, std::placeholders::_1),
            false,
            RetryPolicy);
    }

    static void OnCommitMultipartUpload(NActors::TActorSystem* actorSystem, NActors::TActorId selfId, TCompleteMultipartUpload::TPtr state, IHTTPGateway::TResult&& result) {
        actorSystem->Send(new NActors::IEventHandle(selfId, {}, new TEvPrivate::TEvCommitMultipartUpload(state, std::move(result))));
    }

    void ListMultipartUploads(TListMultipartUploads::TPtr state) {
        LOG_D("ListMultipartUploads BEGIN " << state->BuildUrl());
        auto authInfo = GetAuthInfo(state->Token);
        Gateway->Download(state->BuildUrl(),
            IHTTPGateway::MakeYcHeaders(state->RequestId, authInfo.GetToken(), {}, authInfo.GetAwsUserPwd(), authInfo.GetAwsSigV4()),
            0U,
            0U,
            std::bind(&TS3ApplicatorActor::OnListMultipartUploads, ActorSystem, SelfId(), state, std::placeholders::_1),
            {},
            RetryPolicy);
    }

    static void OnListMultipartUploads(NActors::TActorSystem* actorSystem, NActors::TActorId selfId, TListMultipartUploads::TPtr state, IHTTPGateway::TResult&& result) {
        actorSystem->Send(new NActors::IEventHandle(selfId, {}, new TEvPrivate::TEvListMultipartUploads(state, std::move(result))));
    }

    void AbortMultipartUpload(TAbortMultipartUpload::TPtr state) {
        LOG_D("AbortMultipartUpload BEGIN " << state->BuildUrl());
        auto authInfo = GetAuthInfo(state->Token);
        Gateway->Delete(state->BuildUrl(),
            IHTTPGateway::MakeYcHeaders(state->RequestId, authInfo.GetToken(), "application/xml", authInfo.GetAwsUserPwd(), authInfo.GetAwsSigV4()),
            std::bind(&TS3ApplicatorActor::OnAbortMultipartUpload, ActorSystem, SelfId(), state, std::placeholders::_1),
            RetryPolicy);
    }

    static void OnAbortMultipartUpload(NActors::TActorSystem* actorSystem, NActors::TActorId selfId, TAbortMultipartUpload::TPtr state, IHTTPGateway::TResult&& result) {
        actorSystem->Send(new NActors::IEventHandle(selfId, {}, new TEvPrivate::TEvAbortMultipartUpload(state, std::move(result))));
    }

    void ListParts(TListParts::TPtr state) {
        LOG_D("ListParts BEGIN " << state->BuildUrl());
        auto authInfo = GetAuthInfo(state->Token);
        Gateway->Download(state->BuildUrl(),
            IHTTPGateway::MakeYcHeaders(state->RequestId, authInfo.GetToken(), {}, authInfo.GetAwsUserPwd(), authInfo.GetAwsSigV4()),
            0U,
            0U,
            std::bind(&TS3ApplicatorActor::OnListParts, ActorSystem, SelfId(), state, std::placeholders::_1),
            {},
            RetryPolicy);
    }

    static void OnListParts(NActors::TActorSystem* actorSystem, NActors::TActorId selfId, TListParts::TPtr state, IHTTPGateway::TResult&& result) {
        actorSystem->Send(new NActors::IEventHandle(selfId, {}, new TEvPrivate::TEvListParts(state, std::move(result))));
    }

    IHTTPGateway::TRetryPolicy::IRetryState::TPtr& GetRetryState(const TString& operationName) {
        if (const auto it = RetryStates.find(operationName); it != RetryStates.end()) {
            return it->second;
        }
        return RetryStates.insert({operationName, RetryPolicy->CreateRetryState()}).first->second;
    }

private:
    NActors::TActorId ParentId;
    IHTTPGateway::TPtr Gateway;
    const TString QueryId;
    const TString KeyPrefix;    // job_id ## _
    const TString KeySubPrefix; // run_id ## _
    const bool Commit;
    const THashMap<TString, TString> SecureParams;
    ISecuredServiceAccountCredentialsFactory::TPtr CredentialsFactory;
    NYql::NDqProto::TExternalEffect ExternalEffect;
    NActors::TActorSystem* const ActorSystem;
    const IHTTPGateway::TRetryPolicy::TPtr RetryPolicy;
    std::unordered_map<TString, IHTTPGateway::TRetryPolicy::IRetryState::TPtr> RetryStates;
    ui64 HttpRequestInflight = 0;
    ui64 RetryCount;
    THashSet<TString> UnknownPrefixes;
    THashSet<TString> CommitUploads;
    NYql::TIssues Issues;
    NYql::TIssues RetryIssues;
    std::queue<TObjectStorageRequest> RequestQueue;
    bool ApplicationFinished = false;
};

} // namespace

THolder<NActors::IActor> MakeS3ApplicatorActor(
    NActors::TActorId parentId,
    IHTTPGateway::TPtr gateway,
    const TString& queryId,
    const TString& jobId,
    std::optional<ui32> restartNumber,
    bool commit,
    const THashMap<TString, TString>& secureParams,
    ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory,
    const NYql::NDqProto::TExternalEffect& externalEffect) {

    return MakeHolder<TS3ApplicatorActor>(
        parentId,
        gateway,
        queryId,
        jobId,
        restartNumber,
        commit,
        secureParams,
        credentialsFactory,
        externalEffect
    );
}

} // namespace NYql::NDq