#include "pq_checkpoint_provider_integration.h"

#include <ydb/library/aclib/aclib.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/yql/providers/pq/proto/dq_io.pb.h>
#include <ydb/library/yql/providers/pq/proto/dq_io_state.pb.h>
#include <ydb/library/yql/providers/pq/task_meta/task_meta.h>
#include <ydb/library/yverify_stream/yverify_stream.h>
#include <ydb/public/sdk/cpp/adapters/issue/issue.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/federated_topic/federated_topic.h>
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

template <typename TDerived>
class TPqCheckpointActorBase : public TActorBootstrapped<TDerived>, public IActorExceptionHandler {
protected:
    explicit TPqCheckpointActorBase(TPromise<TIssues> promise)
        : Promise(std::move(promise))
    {}

    void Finish(TIssues issues) {
        Promise.SetValue(std::move(issues));
        this->PassAway();
    }

    template <typename TEvent, typename TResult, typename... TArgs>
    void Subscribe(const TFuture<TResult>& future, TArgs... args) const {
        future.Subscribe([actorSystem = TActivationContext::ActorSystem(), selfId = this->SelfId(), args...](const TFuture<TResult>& result) {
            actorSystem->Send(selfId, new TEvent(args..., result));
        });
    }

private:
    TPromise<TIssues> Promise;
};

class TPqGraphCleanupActor final : public TPqCheckpointActorBase<TPqGraphCleanupActor> {
    using TBase = TPqCheckpointActorBase<TPqGraphCleanupActor>;

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
        : TBase(std::move(promise))
        , Client(std::move(client))
        , Database(std::move(database))
        , WriterIdentity(std::move(writerIdentity))
        , GenerationUpperBound(generationUpperBound)
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

        TBase::Finish(std::move(issues));
    }

    TString LogPrefix() const {
        return TStringBuilder() << "[TPqGraphCleanupActor] ActorId: " << SelfId()
            << " Database: " << Database << " WriterIdentity: " << WriterIdentity << ". ";
    }

    const IDeferredPublishClient::TPtr Client;
    const TString Database;
    const TString WriterIdentity;
    const std::optional<ui64> GenerationUpperBound;
    TVector<ui64> Publications;
    size_t PublicationIndex = 0;
};

struct TSourceRecoveryPartition {
    ui64 Id = 0;
    std::optional<ui64> Offset;
    ui64 TimestampMs = 0;
    ui64 StartOffset = 0;
    ui64 EndOffset = 0;
    bool Done = false;
    std::shared_ptr<NFq::IMessageStreamReadSession> Session;
};

class TPqSourceRecoveryActor final : public TPqCheckpointActorBase<TPqSourceRecoveryActor> {
    using TBase = TPqCheckpointActorBase<TPqSourceRecoveryActor>;
    static constexpr ui64 READ_SESSION_MEMORY = 1_MB; // SDK minimum for read sessions.
    static constexpr TDuration PREPARATION_TIMEOUT = TDuration::Seconds(30);

    struct TEvPrivate {
        enum EEv : ui32 {
            EvBegin = EventSpaceBegin(TEvents::ES_PRIVATE),
            EvConsumer = EvBegin,
            EvPartition,
            EvRewind,
            EvReadReady,
            EvEnd
        };

        static_assert(EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE));

        struct TEvConsumer : TEventLocal<TEvConsumer, EvConsumer> {
            explicit TEvConsumer(NThreading::TFuture<NFq::TMessageStreamResult<NFq::TMessageStreamConsumerDescription>> result)
                : Result(std::move(result))
            {}

            NThreading::TFuture<NFq::TMessageStreamResult<NFq::TMessageStreamConsumerDescription>> Result;
        };

        struct TEvPartition : TEventLocal<TEvPartition, EvPartition> {
            TEvPartition(const size_t index, NThreading::TFuture<NFq::TMessageStreamResult<NFq::TMessageStreamPartitionDescription>> result)
                : Index(index)
                , Result(std::move(result))
            {}

            const size_t Index = 0;
            NThreading::TFuture<NFq::TMessageStreamResult<NFq::TMessageStreamPartitionDescription>> Result;
        };

        struct TEvRewind : TEventLocal<TEvRewind, EvRewind> {
            TEvRewind(const size_t index, NThreading::TFuture<NFq::TMessageStreamResult<NFq::TMessageStreamConsumerPosition>> result)
                : Index(index)
                , Result(std::move(result))
            {}

            const size_t Index;
            NThreading::TFuture<NFq::TMessageStreamResult<NFq::TMessageStreamConsumerPosition>> Result;
        };

        struct TEvReadReady : TEventLocal<TEvReadReady, EvReadReady> {
            TEvReadReady(const size_t index, TFuture<void> result)
                : Index(index)
                , Result(std::move(result))
            {}

            const size_t Index;
            TFuture<void> Result;
        };
    };

public:
    TPqSourceRecoveryActor(std::shared_ptr<NFq::IMessageStreamClient> client, TString topic, TString consumer, TVector<TSourceRecoveryPartition> partitions, TPromise<TIssues> promise)
        : TBase(std::move(promise))
        , Client(std::move(client))
        , Topic(std::move(topic))
        , Consumer(std::move(consumer))
        , Partitions(std::move(partitions))
    {}

    void Bootstrap() {
        Become(&TThis::StateWork);
        Schedule(PREPARATION_TIMEOUT, new TEvents::TEvWakeup());

        if (Partitions.empty()) {
            Finish({});
        } else if (Consumer.empty()) {
            for (size_t index = 0; index < Partitions.size(); ++index) {
                Subscribe<TEvPrivate::TEvPartition>(Client->DescribePartition(NFq::TMessageStreamPartitionId{Partitions[index].Id}), index);
            }
        } else {
            Subscribe<TEvPrivate::TEvConsumer>(Client->DescribeConsumer(Consumer, {.IncludeStats = true}));
        }
    }

    STRICT_STFUNC(StateWork,
        hFunc(TEvPrivate::TEvConsumer, Handle);
        hFunc(TEvPrivate::TEvPartition, Handle);
        hFunc(TEvPrivate::TEvRewind, Handle);
        hFunc(TEvPrivate::TEvReadReady, Handle);
        cFunc(TEvents::TSystem::Wakeup, HandleTimeout);
    )

private:
    bool OnUnhandledException(const std::exception& e) final {
        Finish({TIssue(TStringBuilder() << "Cannot prepare topic source recovery for " << Topic << ": " << e.what())});
        return true;
    }

    void Handle(TEvPrivate::TEvConsumer::TPtr& ev) {
        const auto& result = ev->Get()->Result.GetValue();
        Y_ENSURE(result.IsSuccess(), "Cannot describe consumer: " << result.Issues.ToOneLineString());
        const auto& consumerPartitions = result.Value.Partitions;

        THashMap<ui64, const NFq::TMessageStreamConsumerPartition*> partitions;
        partitions.reserve(consumerPartitions.size());
        for (const auto& partition : consumerPartitions) {
            partitions.emplace(partition.PartitionId.Value, &partition);
        }

        for (size_t index = 0; index < Partitions.size(); ++index) {
            const auto it = partitions.find(Partitions[index].Id);
            Y_ENSURE(it != partitions.end(), "Missing topic partition " << Partitions[index].Id);
            Y_ENSURE(it->second->StartOffset && it->second->EndOffset, "Topic partition statistics are unavailable");
            PreparePartition(index, *it->second->StartOffset, *it->second->EndOffset, it->second->CommittedOffset);
        }
    }

    void Handle(TEvPrivate::TEvPartition::TPtr& ev) {
        const auto& result = ev->Get()->Result.GetValue();
        Y_ENSURE(result.IsSuccess(), "Cannot describe partition: " << result.Issues.ToOneLineString());
        Y_ENSURE(result.Value.StartOffset && result.Value.EndOffset, "Topic partition statistics are unavailable");
        PreparePartition(ev->Get()->Index, *result.Value.StartOffset, *result.Value.EndOffset, std::nullopt);
    }

    void Handle(TEvPrivate::TEvRewind::TPtr& ev) {
        const auto& result = ev->Get()->Result.GetValue();
        Y_ENSURE(result.IsSuccess(), "Cannot rewind consumer: " << result.Issues.ToOneLineString());
        CheckHistory(ev->Get()->Index, /* rewound */ true);
    }

    void Handle(TEvPrivate::TEvReadReady::TPtr& ev) {
        const auto index = ev->Get()->Index;
        auto& partition = Partitions[index];

        for (auto& event : partition.Session->GetEvents({.Block = false})) {
            if (auto* const start = std::get_if<NFq::TMessageStreamPartitionStartRequestedEvent>(&event)) {
                start->PartitionControl->ConfirmStart(partition.StartOffset, std::nullopt);
            } else if (auto* data = std::get_if<NFq::TMessageStreamDataEvent>(&event)) {
                if (data->Records.empty()) {
                    continue;
                }

                const auto& first = data->Records.front();
                if (partition.Offset) {
                    Y_ENSURE(first.Id.Offset <= *partition.Offset,
                        "Required history has expired for partition " << partition.Id
                            << ": requested checkpoint offset " << *partition.Offset
                            << " precedes first retained message offset " << first.Id.Offset);
                } else {
                    const auto timestamp = TInstant::MilliSeconds(partition.TimestampMs);
                    if (!first.WriteTime) {
                        ythrow NFq::TMessageStreamException(NFq::EMessageStreamStatus::Unsupported)
                            << "Timestamp recovery requires backend message write time";
                    }
                    Y_ENSURE(*first.WriteTime <= timestamp,
                        "Required history has expired for partition " << partition.Id
                            << ": requested recovery timestamp " << timestamp
                            << " precedes first retained message write time " << *first.WriteTime
                            << " (offset " << first.Id.Offset << ")");
                }

                Complete(index);
                return;
            } else if (auto* stop = std::get_if<NFq::TMessageStreamPartitionStopRequestedEvent>(&event)) {
                stop->PartitionControl->ConfirmStop();
            } else if (auto* closed = std::get_if<NFq::TMessageStreamSessionClosedEvent>(&event)) {
                ythrow yexception() << "Cannot check topic history: " << closed->Issues.ToOneLineString();
            } else if (std::holds_alternative<NFq::TMessageStreamPartitionExhaustedEvent>(event) || std::holds_alternative<NFq::TMessageStreamPartitionClosedEvent>(event)) {
                ythrow yexception() << "Partition session ended before the recovery boundary was validated for partition " << partition.Id;
            }
        }

        WaitForRead(index);
    }

    void HandleTimeout() {
        Finish({TIssue(TStringBuilder() << "Cannot prepare topic source recovery for " << Topic << ": timed out after " << PREPARATION_TIMEOUT)});
    }

    void PreparePartition(size_t index, ui64 startOffset, ui64 endOffset, std::optional<ui64> committedOffset) {
        auto& partition = Partitions[index];
        partition.StartOffset = startOffset;
        partition.EndOffset = endOffset;

        YDB_LOG_DEBUG("Preparing PQ source partition recovery",
            {"topic", Topic},
            {"consumer", Consumer},
            {"partition", partition.Id},
            {"offset", partition.Offset},
            {"timestampMs", partition.TimestampMs},
            {"startOffset", partition.StartOffset},
            {"endOffset", partition.EndOffset});

        if (partition.Offset) {
            Y_ENSURE(*partition.Offset >= partition.StartOffset && *partition.Offset <= partition.EndOffset,
                "Required checkpoint offset is unavailable for partition " << partition.Id);
        }

        if (partition.StartOffset == partition.EndOffset) {
            Y_ENSURE(partition.Offset || !partition.StartOffset,
                "Required history has expired for partition " << partition.Id
                    << ": no retained messages at recovery timestamp " << TInstant::MilliSeconds(partition.TimestampMs));
            Complete(index);
            return;
        }

        if (!Consumer.empty()) {
            Y_ENSURE(committedOffset, "Consumer partition statistics are unavailable");

            const auto committed = *committedOffset;
            const bool rewind = partition.Offset ? *partition.Offset < committed : TInstant::MilliSeconds(partition.TimestampMs) < TInstant::Now();
            if (rewind && partition.StartOffset < committed) {
                YDB_LOG_INFO("Rewinding PQ consumer for source recovery",
                    {"topic", Topic},
                    {"consumer", Consumer},
                    {"partition", partition.Id},
                    {"committedOffset", committed},
                    {"recoveryOffset", partition.Offset},
                    {"recoveryTimestampMs", partition.TimestampMs},
                    {"startOffset", partition.StartOffset});
                Subscribe<TEvPrivate::TEvRewind>(Client->CommitPosition(NFq::TMessageStreamPartitionId{partition.Id}, Consumer, partition.StartOffset), index);
                return;
            }
        }

        CheckHistory(index, /* rewound */ false);
    }

    void CheckHistory(size_t index, bool rewound) {
        auto& partition = Partitions[index];
        if (!rewound && (partition.Offset || !partition.StartOffset)) {
            Complete(index);
            return;
        }

        NFq::TMessageStreamReadSessionSettings settings;
        settings.PartitionIds = {NFq::TMessageStreamPartitionId{partition.Id}};
        settings.MaxMemoryUsageBytes = READ_SESSION_MEMORY;
        settings.RequireWriteTime = !partition.Offset.has_value();
        if (!Consumer.empty()) {
            settings.Consumer = Consumer;
        }

        partition.Session = Client->CreateReadSession(settings);
        WaitForRead(index);
    }

    void WaitForRead(size_t index) {
        Subscribe<TEvPrivate::TEvReadReady>(Partitions[index].Session->WaitEvent(), index);
    }

    void Complete(size_t index) {
        auto& partition = Partitions[index];
        Y_VALIDATE(!partition.Done, "Partition recovery completed twice");
        partition.Done = true;

        if (partition.Session) {
            partition.Session->Close();
            partition.Session.reset();
        }

        if (++Completed == Partitions.size()) {
            Finish({});
        }
    }

    void PassAway() override {
        for (auto& partition : Partitions) {
            if (partition.Session) {
                partition.Session->Close();
            }
        }
        TBase::PassAway();
    }

    const std::shared_ptr<NFq::IMessageStreamClient> Client;
    const TString Topic;
    const TString Consumer;
    TVector<TSourceRecoveryPartition> Partitions;
    size_t Completed = 0;
};

class TPqCheckpointProviderIntegration final : public NFq::ICheckpointProviderIntegration {
    struct TAuthorizationContext {
        TString Database;
        TIntrusivePtr<NACLib::TUserToken> UserToken;
        TString UserGroupSids;
        TVector<TString> SecretNames;
        std::unordered_set<TString> UniqueSecretNames;
    };

    struct TPreparedSink {
        NYql::NPq::NProto::TDqPqTopicSink Settings;
        TCleanupGraphSinkArguments Args;
        TString Token;
        IDeferredPublishClient::TPtr Client;
    };

    struct TCleanupBatch : TAuthorizationContext {
        TVector<TPreparedSink> Sinks;
    };

    struct TPreparedSource {
        NYql::NPq::NProto::TDqPqTopicSource Settings;
        TVector<TPrepareSource::TTask> Tasks;
        TString Token;
    };

    struct TRecoveryBatch : TAuthorizationContext {
        TVector<TPreparedSource> Sources;
        TIssues Issues;
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
    TStringBuf GetSourceName() const final {
        return "PqSource";
    }

    TFuture<TIssues> PrepareSourceRecovery(TVector<TPrepareSource>&& sources) final try {
        auto batch = std::make_shared<TRecoveryBatch>();

        for (auto& source : sources) {
            try {
                Y_VALIDATE(source.Source.GetType() == GetSourceName(), "Unexpected source type for PQ recovery");
                TPreparedSource prepared;
                Y_ENSURE(source.Source.GetSettings().UnpackTo(&prepared.Settings), "Invalid PQ source settings for recovery");
                prepared.Tasks = std::move(source.Tasks);
                prepared.Token = PrepareToken(prepared.Settings.GetToken().GetName(), source.SecureParams, source.RequestContext, *batch);
                batch->Sources.emplace_back(std::move(prepared));
            } catch (const std::exception& e) {
                batch->Issues.AddIssue(TIssue(TStringBuilder() << "PQ source recovery preparation failed: " << e.what()));
            }
        }

        return ResolveSecrets(*batch).Apply([self = TIntrusivePtr(this), batch](const TFuture<std::map<TString, TString>>& future) {
            return self->PrepareReaders(*batch, future.GetValue());
        }).Apply([batch](const TFuture<TIssues>& result) {
            TIssues issues = batch->Issues;
            try {
                issues.AddIssues(result.GetValue());
            } catch (const std::exception& e) {
                issues.AddIssue(TIssue(TStringBuilder() << "PQ source recovery preparation failed: " << e.what()));
            }
            return issues;
        });
    } catch (const std::exception& e) {
        return MakeFuture(TIssues{TIssue(TStringBuilder() << "PQ source recovery preparation failed: " << e.what())});
    }

    TStringBuf GetSinkName() const final {
        return "PqSink";
    }

    TFuture<TIssues> CleanupGraphSinks(TVector<TCleanupGraphSink>&& sinks, std::optional<ui64> generationUpperBound) final try {
        auto batch = std::make_shared<TCleanupBatch>();
        PrepareSinks(std::move(sinks), *batch);

        return ResolveSecrets(*batch).Apply([self = TIntrusivePtr(this), batch, generationUpperBound](const TFuture<std::map<TString, TString>>& future) {
            return self->CleanupWriters(*batch, future.GetValue(), generationUpperBound);
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

    static TString PrepareToken(const TString& tokenName, const THashMap<TString, TString>& secureParams, const THashMap<TString, TString>& requestContext, TAuthorizationContext& batch) {
        const auto token = secureParams.find(tokenName);
        Y_ENSURE(token != secureParams.end(), "Missing auth references for checkpoint provider operation");
        TString tokenValue = token->second;

        const auto parser = CreateStructuredTokenParser(tokenValue);
        TSet<TString> references;
        parser.ListReferences(references);
        if (!requestContext.empty() || !references.empty() || parser.HasTransientToken()) {
            const auto database = requestContext.find("Database");
            const auto userSid = requestContext.find("UserSID");
            const auto groupSids = requestContext.find("UserGroupSIDs");
            Y_ENSURE(database != requestContext.end() && userSid != requestContext.end() && groupSids != requestContext.end(), "Missing authorization context for checkpoint provider operation");

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
                batch.UserGroupSids = groupSids->second;
            } else {
                Y_ENSURE(batch.Database == database->second
                    && batch.UserToken->GetUserSID() == userSid->second
                    && batch.UserGroupSids == groupSids->second,
                    "Inconsistent authorization context for checkpoint provider operation");
            }

            if (parser.HasTransientToken()) {
                tokenValue = parser.ToBuilder().SetTransientTokenAuth(batch.UserToken->SerializeAsString()).ToJson();
            }
        }

        for (const auto& name : references) {
            if (batch.UniqueSecretNames.insert(name).second) {
                batch.SecretNames.push_back(name);
            }
        }

        return tokenValue;
    }

    TFuture<std::map<TString, TString>> ResolveSecrets(const TAuthorizationContext& context) const {
        const auto resolution = context.SecretNames.empty()
            ? MakeFuture(TEvDescribeSecretsResponse::TDescription(std::vector<TString>{}))
            : NSecret::DescribeSecret(context.SecretNames, context.UserToken, context.Database, ActorSystem);

        return resolution.Apply([names = context.SecretNames](const TFuture<TEvDescribeSecretsResponse::TDescription>& future) {
            const auto& result = future.GetValue();
            Y_ENSURE(result.Status == Ydb::StatusIds::SUCCESS, "Failed to resolve secrets for checkpoint provider operation: " << result.Issues.ToOneLineString());
            Y_VALIDATE(result.SecretValues.size() == names.size(), "Unexpected number of resolved secrets");

            std::map<TString, TString> secrets;
            for (size_t i = 0; i < names.size(); ++i) {
                secrets.emplace(names[i], result.SecretValues[i]);
            }

            return secrets;
        });
    }

    TFuture<TIssues> PrepareReaders(TRecoveryBatch& batch, const std::map<TString, TString>& secrets) const {
        struct TCluster {
            NYql::NPq::NProto::TDqPqTopicSource Settings;
            ui64 PartitionsCount = 0;
            THashMap<ui64, TSourceRecoveryPartition> Partitions;
        };

        const auto nowMs = TInstant::Now().MilliSeconds();
        TVector<TFuture<TIssues>> recoveries;
        recoveries.reserve(batch.Sources.size());
        const auto prepareSource = [&](TPreparedSource& source) {
            source.Token = CreateStructuredTokenParser(source.Token).ToBuilder().ReplaceReferences(secrets).ToJson();

            THashMap<TString, TCluster> clusters;
            if (source.Settings.GetFederatedClusters().empty()) {
                clusters[TString{}].Settings = source.Settings;
            } else {
                for (const auto& cluster : source.Settings.GetFederatedClusters()) {
                    auto [it, inserted] = clusters.try_emplace(cluster.GetName());
                    Y_VALIDATE(inserted, "Duplicate federated cluster in source recovery");

                    it->second.PartitionsCount = cluster.GetPartitionsCount();
                    auto& settings = it->second.Settings;
                    settings = source.Settings;
                    settings.ClearFederatedClusters();

                    if (!cluster.GetName().empty()) {
                        settings.SetEndpoint(cluster.GetEndpoint());
                        settings.SetDatabase(cluster.GetDatabase());
                    }

                    std::string path = settings.GetTopicPath();
                    const NYdb::NFederatedTopic::TFederatedTopicClient::TClusterInfo info{
                        .Name = cluster.GetName(),
                        .Endpoint = cluster.GetEndpoint(),
                        .Path = cluster.GetDatabase(),
                    };
                    info.AdjustTopicPath(path);
                    settings.SetTopicPath(TString(path));
                }
            }

            for (const auto& task : source.Tasks) {
                std::optional<ui64> timestamp;
                THashMap<std::pair<TString, ui64>, ui64> offsets;
                for (const auto& data : task.State.Data) {
                    NYql::NPq::NProto::TDqPqTopicSourceState state;
                    Y_ENSURE(data.Version == 1 && state.ParseFromString(data.Blob), "Invalid PQ source checkpoint for recovery");

                    timestamp = std::min(timestamp.value_or(state.GetStartingMessageTimestampMs()), state.GetStartingMessageTimestampMs());

                    for (const auto& partition : state.GetPartitions()) {
                        auto [it, inserted] = offsets.emplace(std::make_pair(partition.GetCluster(), partition.GetPartition()), partition.GetOffset());
                        if (!inserted) {
                            it->second = std::min(it->second, partition.GetOffset());
                        }
                    }
                }

                Y_ENSURE(timestamp, "Missing PQ source checkpoint data");

                NYql::NDqProto::TDqTask metadata;
                *metadata.MutableMeta() = task.Meta;
                metadata.MutableReadRanges()->Assign(task.ReadRanges.begin(), task.ReadRanges.end());

                const auto sets = NYql::NPq::GetTopicPartitionsSets(metadata);
                Y_VALIDATE(!sets.empty(), "Missing topic partition mapping for source recovery");
                for (auto& [name, cluster] : clusters) {
                    for (const auto& set : sets) {
                        Y_VALIDATE(set.DqPartitionsCount, "Invalid topic partition mapping for source recovery");
                        const auto count = cluster.PartitionsCount ? cluster.PartitionsCount : set.TopicPartitionsCount;

                        for (ui64 id = set.EachTopicPartitionGroupId; id < count; id += set.DqPartitionsCount) {
                            const auto* offset = offsets.FindPtr(std::make_pair(name, id));
                            if (!offset && *timestamp > nowMs) {
                                continue;
                            }

                            Y_ENSURE(cluster.Partitions.emplace(id, TSourceRecoveryPartition{
                                .Id = id,
                                .Offset = offset ? std::optional(*offset) : std::nullopt,
                                .TimestampMs = *timestamp,
                            }).second, "Duplicate partition in source recovery");
                        }
                    }
                }
            }

            for (auto& [_, cluster] : clusters) {
                if (cluster.Partitions.empty()) {
                    continue;
                }

                auto settings = PqGateway->GetTopicClientSettings();
                settings.Database(cluster.Settings.GetDatabase())
                    .DiscoveryEndpoint(cluster.Settings.GetEndpoint())
                    .SslCredentials(NYdb::TSslCredentials(cluster.Settings.GetUseSsl()))
                    .CredentialsProviderFactory(CredentialsFactory->Create(source.Token, cluster.Settings.GetAddBearerToToken()));
                auto client = PqGateway->GetTopicClient(cluster.Settings.GetTopicPath(), Driver, settings);
                Y_ENSURE(client, "Topic client is unavailable for source recovery");

                TVector<TSourceRecoveryPartition> partitions;
                partitions.reserve(cluster.Partitions.size());
                for (auto& [_, partition] : cluster.Partitions) {
                    partitions.emplace_back(std::move(partition));
                }

                auto promise = NewPromise<TIssues>();
                recoveries.emplace_back(promise.GetFuture());
                ActorSystem->Register(new TPqSourceRecoveryActor(std::move(client), cluster.Settings.GetTopicPath(), cluster.Settings.GetConsumerName(), std::move(partitions), promise));
            }

        };

        for (auto& source : batch.Sources) {
            try {
                prepareSource(source);
            } catch (const std::exception& e) {
                recoveries.emplace_back(MakeFuture(TIssues{TIssue(TStringBuilder() << "Cannot prepare topic source recovery for " << source.Settings.GetTopicPath() << ": " << e.what())}));
            }
            source.Token.clear();
        }

        return WaitAll(recoveries).Apply([recoveries](const TFuture<void>&) {
            TIssues issues;
            for (const auto& recovery : recoveries) {
                try {
                    issues.AddIssues(recovery.GetValue());
                } catch (const std::exception& e) {
                    issues.AddIssue(TIssue(e.what()));
                }
            }
            return issues;
        });
    }

    void PrepareSinks(TVector<TCleanupGraphSink>&& sinks, TCleanupBatch& batch) const {
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

            prepared.Token = PrepareToken(prepared.Settings.GetToken().GetName(), prepared.Args.SecureParams, prepared.Args.RequestContext, batch);
            batch.Sinks.emplace_back(std::move(prepared));
        }
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
