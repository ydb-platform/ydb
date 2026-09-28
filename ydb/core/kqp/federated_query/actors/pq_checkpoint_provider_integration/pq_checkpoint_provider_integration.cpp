#include "pq_checkpoint_provider_integration.h"

#include <ydb/library/aclib/aclib.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/yql/providers/pq/proto/dq_io.pb.h>
#include <ydb/library/yverify_stream/yverify_stream.h>
#include <ydb/public/sdk/cpp/adapters/issue/issue.h>
#include <ydb/services/scheme_secret/resolver.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/threading/future/wait/wait.h>

#include <util/string/builder.h>
#include <util/string/cast.h>

#include <exception>
#include <map>
#include <memory>
#include <unordered_set>
#include <vector>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::KQP_PROXY

namespace NKikimr::NKqp {

namespace {

using namespace NActors;
using namespace NYql;
using namespace NThreading;
using namespace NYdb::NTopic;

class TPqGraphCleanupActor final : public TActorBootstrapped<TPqGraphCleanupActor>, public IActorExceptionHandler {
    struct TEvPrivate {
        enum EEv : ui32 {
            EvBegin = EventSpaceBegin(TEvents::ES_PRIVATE),

            EvListPublications = EvBegin,
            EvCancelPublication,

            EvEnd
        };

        static_assert(EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE)");

        struct TEvListPublications : TEventLocal<TEvListPublications, EvListPublications> {
            explicit TEvListPublications(const TAsyncListPublicationsResult& result)
                : Result(result)
            {}

            TAsyncListPublicationsResult Result;
        };

        struct TEvCancelPublication : TEventLocal<TEvCancelPublication, EvCancelPublication> {
            explicit TEvCancelPublication(const TAsyncCancelPublicationResult& result)
                : Result(result)
            {}

            TAsyncCancelPublicationResult Result;
        };
    };

public:
    TPqGraphCleanupActor(
        IDeferredPublishClient::TPtr client,
        TString database,
        TString writerIdentity,
        std::optional<ui64> generationUpperBound,
        TPromise<TIssues> promise)
        : Client(std::move(client))
        , Database(std::move(database))
        , WriterIdentity(std::move(writerIdentity))
        , GenerationUpperBound(generationUpperBound)
        , Promise(std::move(promise))
    {}

    void Bootstrap() {
        YDB_LOG_DEBUG("[CheckpointCleanup] Starting PQ sink cleanup",
            {"logPrefix", LogPrefix()},
            {"generationUpperBound", GenerationUpperBound});
        Become(&TPqGraphCleanupActor::StateFunc);
        ListPublications();
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvPrivate::TEvListPublications, Handle);
        hFunc(TEvPrivate::TEvCancelPublication, Handle);
    )

private:
    bool OnUnhandledException(const std::exception& e) final {
        YDB_LOG_ERROR("[CheckpointCleanup] Got unexpected exception",
            {"logPrefix", LogPrefix()},
            {"exception", e.what()});
        Finish({TIssue(TStringBuilder() << "PQ checkpoint graph cleanup failed: " << e.what())});
        return true;
    }

    void Handle(TEvPrivate::TEvListPublications::TPtr& ev) {
        const auto& result = ev->Get()->Result.GetValue();
        YDB_LOG_DEBUG("[CheckpointCleanup] Received list publications result",
            {"logPrefix", LogPrefix()},
            {"sender", ev->Sender},
            {"status", result.GetStatus()},
            {"issues", result.GetIssues().ToOneLineString()});

        if (!result.IsSuccess()) {
            Fail("list publications failed", result);
            return;
        }

        Publications.reserve(result.GetPublications().size());
        for (const auto& publication : result.GetPublications()) {
            Y_VALIDATE(publication.WriterIdentity == WriterIdentity, "Unexpected writer identity in checkpoint cleanup publications for writer: " << WriterIdentity << ", got identity: " << publication.WriterIdentity);

            if (GenerationUpperBound) {
                TStringBuf identity(publication.ExtPublicationId);
                TStringBuf generationStr;
                TStringBuf sequenceStr;
                ui64 generation = 0;
                ui64 sequence = 0;
                if (!identity.SkipPrefix(WriterIdentity + ':')
                    || !identity.TrySplit(':', generationStr, sequenceStr)
                    || !TryFromString(generationStr, generation)
                    || !TryFromString(sequenceStr, sequence)
                    || generation >= *GenerationUpperBound) {
                    YDB_LOG_TRACE("[CheckpointCleanup] Skipping publication outside the cleanup generation range",
                        {"logPrefix", LogPrefix()},
                        {"publicationId", publication.IntPublicationId},
                        {"externalPublicationId", publication.ExtPublicationId},
                        {"generationUpperBound", GenerationUpperBound});
                    continue;
                }
            }

            Publications.push_back(publication.IntPublicationId);
        }

        YDB_LOG_DEBUG("[CheckpointCleanup] Selected publications for cancellation",
            {"logPrefix", LogPrefix()},
            {"listedPublicationsCount", result.GetPublications().size()},
            {"publicationsCount", Publications.size()});
        NextPublication();
    }

    void Handle(TEvPrivate::TEvCancelPublication::TPtr& ev) {
        const auto& result = ev->Get()->Result.GetValue();
        YDB_LOG_DEBUG("[CheckpointCleanup] Received cancel publication result",
            {"logPrefix", LogPrefix()},
            {"sender", ev->Sender},
            {"publicationId", Publications[PublicationIndex - 1]},
            {"status", result.GetStatus()},
            {"issues", result.GetIssues().ToOneLineString()});

        if (!result.IsSuccess() && result.GetStatus() != NYdb::EStatus::NOT_FOUND) {
            Fail("cancel publication failed", result);
            return;
        }

        NextPublication();
    }

    void ListPublications() {
        YDB_LOG_DEBUG("[CheckpointCleanup] Listing writer publications",
            {"logPrefix", LogPrefix()});
        Subscribe<TEvPrivate::TEvListPublications>(Client->ListPublications(TListPublicationsSettings().WriterIdentity(WriterIdentity)));
    }

    void NextPublication() {
        if (PublicationIndex == Publications.size()) {
            Finish({});
            return;
        }

        const auto publicationId = Publications[PublicationIndex++];
        YDB_LOG_DEBUG("[CheckpointCleanup] Canceling publication",
            {"logPrefix", LogPrefix()},
            {"publicationId", publicationId},
            {"remainingPublications", Publications.size() - PublicationIndex});
        Subscribe<TEvPrivate::TEvCancelPublication>(Client->CancelPublication(TDeferredPublication(publicationId)));
    }

    void Fail(const TString& message, const NYdb::TStatus& status) {
        TIssue issue(TStringBuilder() << "Failed to clean up publications for writer '" << WriterIdentity << "', " << message << ", status: " << status.GetStatus());
        for (const auto& subIssue : status.GetIssues()) {
            issue.AddSubIssue(MakeIntrusive<TIssue>(NYdb::NAdapters::ToYqlIssue(subIssue)));
        }
        Finish({issue});
    }

    void Finish(TIssues issues) {
        if (issues.Empty()) {
            YDB_LOG_DEBUG("[CheckpointCleanup] PQ sink cleanup succeeded",
                {"logPrefix", LogPrefix()},
                {"publicationsCount", Publications.size()});
        } else {
            YDB_LOG_WARN("[CheckpointCleanup] PQ sink cleanup failed",
                {"logPrefix", LogPrefix()},
                {"publicationsCount", Publications.size()},
                {"cancelRequestsStarted", PublicationIndex},
                {"issues", issues.ToOneLineString()});
        }

        Promise.SetValue(std::move(issues));
        PassAway();
    }

    template <typename TEvent, typename TResult>
    void Subscribe(const TFuture<TResult>& future) const {
        future.Subscribe([actorSystem = TActivationContext::ActorSystem(), selfId = SelfId()](const TFuture<TResult>& result) {
            actorSystem->Send(selfId, new TEvent(result));
        });
    }

    TString LogPrefix() const {
        return TStringBuilder() << "[TPqGraphCleanupActor] ActorId: " << SelfId()
            << " Database: " << Database << " WriterIdentity: " << WriterIdentity << ". ";
    }

    const IDeferredPublishClient::TPtr Client;
    const TString Database;
    const TString WriterIdentity;
    const std::optional<ui64> GenerationUpperBound;
    TPromise<TIssues> Promise;
    TVector<ui64> Publications;
    size_t PublicationIndex = 0;
};

class TPqCheckpointProviderIntegration final : public NFq::ICheckpointProviderIntegration {
    struct TPreparedSink {
        NYql::NPq::NProto::TDqPqTopicSink Settings;
        TCleanupGraphSinkArguments Args;
        TString Token;
        IDeferredPublishClient::TPtr Client;
    };

    struct TCleanupBatch {
        TVector<TPreparedSink> Sinks;
        TString Database;
        TIntrusivePtr<NACLib::TUserToken> UserToken;
        TVector<TString> SecretNames;
    };

public:
    TPqCheckpointProviderIntegration(TActorSystem* actorSystem, IPqStaticGateway::TPtr pqGateway, NYdb::TDriver driver, IStructuredTokenCredentialsFactory::TPtr credentialsFactory)
        : ActorSystem(actorSystem)
        , PqGateway(std::move(pqGateway))
        , Driver(std::move(driver))
        , CredentialsFactory(std::move(credentialsFactory))
    {
        Y_VALIDATE(ActorSystem, "ActorSystem is required");
        Y_VALIDATE(PqGateway, "PqGateway is required");
        Y_VALIDATE(CredentialsFactory, "CredentialsFactory is required");
    }

private:
    TStringBuf GetSinkName() const final {
        return "PqSink";
    }

    TFuture<TIssues> CleanupGraphSinks(TVector<TCleanupGraphSink>&& sinks, std::optional<ui64> generationUpperBound) final try {
        auto batch = std::make_shared<TCleanupBatch>();
        PrepareSinks(std::move(sinks), *batch);

        TFuture<TEvDescribeSecretsResponse::TDescription> resolution;
        if (batch->SecretNames.empty()) {
            resolution = MakeFuture(TEvDescribeSecretsResponse::TDescription(std::vector<TString>{}));
        } else {
            YDB_LOG_DEBUG_CTX(*ActorSystem, "[CheckpointCleanup] Describing unique sink secrets",
                {"database", batch->Database},
                {"secretsCount", batch->SecretNames.size()});
            resolution = NSecret::DescribeSecret(batch->SecretNames, batch->UserToken, batch->Database, ActorSystem);
        }

        return resolution.Apply([self = TIntrusivePtr(this), batch, generationUpperBound](const TFuture<TEvDescribeSecretsResponse::TDescription>& future) {
            const auto& result = future.GetValue();
            YDB_LOG_DEBUG_CTX(*self->ActorSystem, "[CheckpointCleanup] Received describe secrets result",
                {"database", batch->Database},
                {"status", result.Status},
                {"secretsCount", result.SecretValues.size()},
                {"issues", result.Issues.ToOneLineString()});

            if (result.Status != Ydb::StatusIds::SUCCESS) {
                auto issues = result.Issues;
                issues.AddIssue(TIssue(TStringBuilder() << "Failed to resolve secrets for checkpoint graph publication cleanup, status: " << result.Status));
                return MakeFuture(std::move(issues));
            }

            Y_VALIDATE(result.SecretValues.size() == batch->SecretNames.size(), "Unexpected number of resolved secrets");
            std::map<TString, TString> secrets;
            for (size_t i = 0; i < batch->SecretNames.size(); ++i) {
                secrets.emplace(batch->SecretNames[i], result.SecretValues[i]);
            }
            return self->CleanupWriters(*batch, secrets, generationUpperBound);
        }).Apply([actorSystem = ActorSystem](const TFuture<TIssues>& result) {
            TIssues issues;
            try {
                issues = result.GetValue();
            } catch (const std::exception& e) {
                issues.AddIssue(TIssue(TStringBuilder() << "PQ checkpoint graph cleanup failed: " << e.what()));
            }

            if (issues.Empty()) {
                YDB_LOG_DEBUG_CTX(*actorSystem, "[CheckpointCleanup] PQ sink batch cleanup succeeded");
            } else {
                YDB_LOG_WARN_CTX(*actorSystem, "[CheckpointCleanup] PQ sink batch cleanup failed",
                    {"issues", issues.ToOneLineString()});
            }
            return issues;
        });
    } catch (const std::exception& e) {
        YDB_LOG_WARN_CTX(*ActorSystem, "[CheckpointCleanup] Failed to start PQ sink batch cleanup",
            {"exception", e.what()});
        return MakeFuture(TIssues{TIssue(TStringBuilder() << "PQ checkpoint graph cleanup failed: " << e.what())});
    }

    void PrepareSinks(TVector<TCleanupGraphSink>&& sinks, TCleanupBatch& batch) const {
        std::unordered_set<TString> secretNames;
        TString userGroupSids;
        secretNames.reserve(sinks.size());
        batch.Sinks.reserve(sinks.size());
        for (auto& sink : sinks) {
            Y_VALIDATE(sink.Sink.GetType() == GetSinkName(), "Unexpected sink type for PQ checkpoint cleanup: " << sink.Sink.GetType());

            TPreparedSink prepared = {.Args = std::move(sink.Args)};
            Y_ENSURE(sink.Sink.GetSettings().UnpackTo(&prepared.Settings), "Failed to unpack PQ sink settings for checkpoint graph cleanup");
            if (!prepared.Settings.GetDeferredPublicationExtIdPrefix() || prepared.Args.TaskIds.empty()) {
                YDB_LOG_DEBUG_CTX(*ActorSystem, "[CheckpointCleanup] Skipping PQ output without deferred publications or tasks",
                    {"outputIndex", prepared.Args.OutputIndex});
                continue;
            }

            const auto token = prepared.Args.SecureParams.find(prepared.Settings.GetToken().GetName());
            Y_ENSURE(token != prepared.Args.SecureParams.end(), "Missing auth references for checkpoint graph publication cleanup");
            prepared.Token = token->second;

            const auto parser = CreateStructuredTokenParser(prepared.Token);
            TSet<TString> references;
            parser.ListReferences(references);
            if (!prepared.Args.RequestContext.empty() || !references.empty() || parser.HasTransientToken()) {
                const auto& requestContext = prepared.Args.RequestContext;
                const auto database = requestContext.find("Database");
                const auto userSid = requestContext.find("UserSID");
                const auto groupSids = requestContext.find("UserGroupSIDs");
                Y_ENSURE(database != requestContext.end() && userSid != requestContext.end() && groupSids != requestContext.end(), "Missing authorization context for checkpoint graph publication cleanup");

                if (!batch.UserToken) {
                    TVector<NACLib::TSID> groups;
                    NJson::TJsonValue value;
                    NJson::ReadJsonTree(groupSids->second, &value, true);
                    groups.reserve(value.GetArraySafe().size());
                    for (const auto& group : value.GetArraySafe()) {
                        groups.push_back(group.GetStringSafe());
                    }

                    batch.Database = database->second;
                    batch.UserToken = MakeIntrusive<NACLib::TUserToken>(userSid->second, groups);
                    userGroupSids = groupSids->second;
                } else {
                    Y_ENSURE(batch.Database == database->second
                        && batch.UserToken->GetUserSID() == userSid->second
                        && userGroupSids == groupSids->second,
                        "Inconsistent authorization context for checkpoint graph publication cleanup");
                }

                if (parser.HasTransientToken()) {
                    prepared.Token = parser.ToBuilder().SetTransientTokenAuth(batch.UserToken->SerializeAsString()).ToJson();
                }
            }

            secretNames.insert(references.begin(), references.end());
            batch.Sinks.emplace_back(std::move(prepared));
        }

        batch.SecretNames.assign(secretNames.begin(), secretNames.end());
    }

    TFuture<TIssues> CleanupWriters(TCleanupBatch& batch, const std::map<TString, TString>& secrets, std::optional<ui64> generationUpperBound) const {
        for (auto& sink : batch.Sinks) {
            sink.Token = CreateStructuredTokenParser(sink.Token).ToBuilder().ReplaceReferences(secrets).ToJson();

            YDB_LOG_DEBUG_CTX(*ActorSystem, "[CheckpointCleanup] Creating client for PQ stage output",
                {"database", sink.Settings.GetDatabase()},
                {"endpoint", sink.Settings.GetEndpoint()},
                {"outputIndex", sink.Args.OutputIndex},
                {"tasksCount", sink.Args.TaskIds.size()});
            sink.Client = PqGateway->GetDeferredPublishClient(Driver, NYdb::TCommonClientSettings()
                .Database(sink.Settings.GetDatabase())
                .DiscoveryEndpoint(sink.Settings.GetEndpoint())
                .SslCredentials(NYdb::TSslCredentials(sink.Settings.GetUseSsl()))
                .CredentialsProviderFactory(CredentialsFactory->Create(sink.Token, sink.Settings.GetAddBearerToToken())));
            sink.Token.clear();
        }

        TVector<TFuture<TIssues>> cleanups;
        cleanups.reserve(batch.Sinks.size());
        for (const auto& sink : batch.Sinks) {
            for (const auto taskId : sink.Args.TaskIds) {
                auto promise = NewPromise<TIssues>();
                cleanups.push_back(promise.GetFuture());

                const TString writerIdentity = TStringBuilder() << sink.Settings.GetDeferredPublicationExtIdPrefix() << ':' << taskId << ':' << sink.Args.OutputIndex;
                ActorSystem->Register(new TPqGraphCleanupActor(sink.Client, sink.Settings.GetDatabase(), writerIdentity, generationUpperBound, promise));
            }
        }

        return WaitAll(cleanups).Apply([cleanups = std::move(cleanups)](const TFuture<void>&) {
            TIssues issues;
            for (const auto& cleanup : cleanups) {
                issues.AddIssues(cleanup.GetValue());
            }
            return issues;
        });
    }

    TActorSystem* const ActorSystem;
    const IPqStaticGateway::TPtr PqGateway;
    const NYdb::TDriver Driver;
    const IStructuredTokenCredentialsFactory::TPtr CredentialsFactory;
};

} // anonymous namespace

NFq::ICheckpointProviderIntegration::TPtr CreatePqCheckpointProviderIntegration(
    NActors::TActorSystem* actorSystem,
    NYql::IPqStaticGateway::TPtr pqGateway,
    NYdb::TDriver driver,
    NYql::IStructuredTokenCredentialsFactory::TPtr credentialsFactory)
{
    return MakeIntrusive<TPqCheckpointProviderIntegration>(actorSystem, std::move(pqGateway), std::move(driver), std::move(credentialsFactory));
}

} // namespace NKikimr::NKqp
