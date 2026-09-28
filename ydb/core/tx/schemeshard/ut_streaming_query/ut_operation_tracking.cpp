#include <ydb/core/base/metadata.h>
#include <ydb/core/base/tablet.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/tx/scheme_board/events_schemeshard.h>
#include <ydb/core/tx/schemeshard/schemeshard_private.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/event_pb.h>

#include <util/generic/hash.h>
#include <util/string/builder.h>

using namespace NSchemeShardUT_Private;

namespace {

using NMetadata::NProvider::TEvTrackOperationCompletion;

class TMetadataServiceMock : public TActor<TMetadataServiceMock> {
public:
    explicit TMetadataServiceMock(TActorId edge)
        : TActor(&TThis::StateWork)
        , Edge(edge)
    {}

private:
    void Handle(TEvTrackOperationCompletion::TPtr& ev) {
        ++Requests;
        Forward(ev, Edge);
    }

    void Handle(TEvents::TEvPing::TPtr& ev) {
        // A mailbox barrier lets tests also check that no tracker was started.
        Send(ev->Sender, new TEvents::TEvPong, 0, Requests);
    }

    STRICT_STFUNC(StateWork,
        hFunc(TEvTrackOperationCompletion, Handle);
        hFunc(TEvents::TEvPing, Handle);
    )

    const TActorId Edge;
    ui64 Requests = 0;
};

struct TFixture {
    enum class EDatabase {
        Root,
        Subdomain,
        Serverless,
    };

    const TString Database;
    const TString WorkingDir;
    const TString QueryPath;
    TString DatabaseId;
    ui64 SchemeShardId = TTestTxConfig::SchemeShard;
    TTestBasicRuntime Runtime;
    THashMap<ui64, ui32> Generations;
    TTestActorRuntime::TEventObserverHolder GenerationObserver;
    THolder<TTestEnv> Env;
    TActorId Edge;
    TActorId Owner;
    TActorId MetadataService;
    ui64 TxId = 100;

    explicit TFixture(EDatabase database = EDatabase::Root)
        : Database(database == EDatabase::Serverless ? "/MyRoot/ServerLessDB"
            : database == EDatabase::Subdomain ? "/MyRoot/Database" : "/MyRoot")
        , WorkingDir(Database + "/DirStreamingQuery")
        , QueryPath(WorkingDir + "/Query")
        , DatabaseId(Database)
    {
        GenerationObserver = Runtime.AddObserver<TEvTablet::TEvRestored>([this](auto& ev) {
            Generations[ev->Get()->TabletID] = ev->Get()->Generation;
        });
        Env = MakeHolder<TTestEnv>(Runtime);
        Edge = Runtime.AllocateEdgeActor();
        Owner = Runtime.AllocateEdgeActor();
        MetadataService = NMetadata::NProvider::MakeServiceId(Runtime.GetNodeId());
        Runtime.RegisterService(MetadataService, Runtime.Register(new TMetadataServiceMock(Edge)));

        if (database == EDatabase::Serverless) {
            TestCreateServerLessDb(Runtime, *Env, TxId, SchemeShardId);
            const auto description = DescribePath(Runtime, Database);
            const auto& domainKey = description.GetPathDescription().GetDomainDescription().GetDomainKey();
            DatabaseId = TStringBuilder() << domainKey.GetSchemeShard() << ":" << domainKey.GetPathId() << ":" << Database;
        } else if (database == EDatabase::Subdomain) {
            TestCreateSubDomain(Runtime, ++TxId, "/MyRoot", R"(
                Name: "Database"
                PlanResolution: 50
                Coordinators: 1
                Mediators: 1
                TimeCastBucketsPerMediator: 2
            )");
            Env->TestWaitNotification(Runtime, TxId);
        }
        TestMkDir(Runtime, SchemeShardId, ++TxId, Database, "DirStreamingQuery");
        Env->TestWaitNotification(Runtime, TxId, SchemeShardId);
    }

    THolder<TEvTx> MakeRequest(bool alter, TActorId owner = {}, std::optional<ui64> version = std::nullopt,
        bool replace = false)
    {
        THolder<TEvTx> request(CreateStreamingQueryRequest(SchemeShardId, ++TxId, WorkingDir, R"(
            Name: "Query"
            Properties { Properties { key: "run" value: "true" } }
        )"));
        auto& tx = *request->Record.MutableTransaction(0);
        tx.SetOperationType(alter && !replace
            ? NKikimrSchemeOp::ESchemeOpAlterStreamingQuery
            : NKikimrSchemeOp::ESchemeOpCreateStreamingQuery);
        tx.SetReplaceIfExists(replace);
        if (owner) {
            ActorIdToProto(owner, tx.MutableCreateStreamingQuery()->MutableOperationOwnerActorId());
        }
        if (version) {
            const auto description = DescribePath(Runtime, SchemeShardId, QueryPath);
            auto* condition = tx.AddApplyIf();
            condition->SetPathId(description.GetPathDescription().GetSelf().GetPathId());
            condition->SetPathVersion(*version);
            condition->SetCheckEntityVersion(true);
        }
        return request;
    }

    void Propose(THolder<TEvTx> request, TExpectedResult expected = NKikimrScheme::StatusAccepted) {
        const auto txId = request->Record.GetTxId();
        AsyncSend(Runtime, SchemeShardId, request.Release());
        TestModificationResults(Runtime, txId, {expected});
        if (expected.Status == NKikimrScheme::StatusAccepted) {
            Env->TestWaitNotification(Runtime, txId, SchemeShardId);
        }
    }

    TEvTrackOperationCompletion::TPtr GrabTracking() {
        auto ev = Runtime.GrabEdgeEventRethrow<TEvTrackOperationCompletion>(Edge, TDuration::Seconds(5));
        UNIT_ASSERT_C(ev, "Metadata service did not receive an operation tracker request");
        return ev;
    }

    TPathId CheckTracking(ui64 version, TActorId owner, const std::optional<TString>& token = std::nullopt,
        const TString& objectId = "DirStreamingQuery/Query")
    {
        auto ev = GrabTracking();
        const auto& request = *ev->Get();
        UNIT_ASSERT_VALUES_EQUAL(request.GetDatabase(), Database);
        UNIT_ASSERT_VALUES_EQUAL(request.GetDatabaseId(), DatabaseId);
        UNIT_ASSERT_VALUES_EQUAL(request.GetTypeId(), "STREAMING_QUERY");
        UNIT_ASSERT_VALUES_EQUAL(request.GetObjectId(), objectId);
        const auto description = DescribePath(Runtime, SchemeShardId, Database + "/" + objectId);
        TestDescribeResult(description, {NLs::PathExist});
        const auto& path = description.GetPathDescription().GetSelf();
        UNIT_ASSERT_VALUES_EQUAL(request.GetPathId().OwnerId, path.GetSchemeshardId());
        UNIT_ASSERT_VALUES_EQUAL(request.GetPathId().LocalPathId, path.GetPathId());
        UNIT_ASSERT(Generations.at(SchemeShardId) > 0);
        UNIT_ASSERT_VALUES_EQUAL(request.GetRequestGeneration(), Generations.at(SchemeShardId));
        UNIT_ASSERT_VALUES_EQUAL(request.GetObjectGeneration(), version);
        UNIT_ASSERT_VALUES_EQUAL(request.GetOperationOwner(), owner);
        const auto it = request.GetProperties().find("__operation_owner_user_token");
        UNIT_ASSERT_VALUES_EQUAL(it != request.GetProperties().end(), token.has_value());
        if (token) {
            UNIT_ASSERT_VALUES_EQUAL(it->second, *token);
        }
        UNIT_ASSERT(request.GetSchemeTxId());
        return request.GetPathId();
    }

    void CheckRequests(ui64 count) {
        Runtime.Send(new IEventHandle(MetadataService, Edge, new TEvents::TEvPing));
        auto ev = Runtime.GrabEdgeEventRethrow<TEvents::TEvPong>(Edge, TDuration::Seconds(5));
        UNIT_ASSERT(ev);
        UNIT_ASSERT_VALUES_EQUAL(ev->Cookie, count);
    }

    void CheckQuery(ui64 version, const TString& run = "true") {
        const auto description = DescribePath(Runtime, SchemeShardId, QueryPath);
        TestDescribeResult(description, {NLs::Finished, NLs::IsStreamingQuery});
        const auto& path = description.GetPathDescription();
        UNIT_ASSERT_VALUES_EQUAL(path.GetSelf().GetVersion().GetStreamingQueryVersion(), version);
        const auto& properties = path.GetStreamingQueryDescription().GetProperties().GetProperties();
        UNIT_ASSERT_VALUES_EQUAL(properties.size(), properties.contains("__operation_owner_user_token") ? 2 : 1);
        UNIT_ASSERT_VALUES_EQUAL(properties.at("run"), run);
    }

    void Reboot() {
        const auto previousGeneration = Generations.at(SchemeShardId);
        RebootTablet(Runtime, SchemeShardId, Runtime.AllocateEdgeActor());
        TestDescribeResult(DescribePath(Runtime, SchemeShardId, WorkingDir), {NLs::Finished});
        UNIT_ASSERT_C(Generations.at(SchemeShardId) > previousGeneration, "SchemeShard did not restart");
    }

    void CheckOwner(TActorId owner) {
        const auto description = DescribePath(Runtime, SchemeShardId, QueryPath);
        TestDescribeResult(description, {NLs::Finished, NLs::IsStreamingQuery});
        UNIT_ASSERT_VALUES_EQUAL(ActorIdFromProto(description.GetPathDescription().GetStreamingQueryDescription().GetOperationOwnerActorId()), owner);
    }
};

std::optional<TString> MakeUserToken(bool enabled) {
    if (enabled) {
        return "opaque-user-token";
    }
    return std::nullopt;
}

void SetUserToken(TEvTx& request, const std::optional<TString>& token) {
    if (token) {
        (*request.Record.MutableTransaction(0)->MutableCreateStreamingQuery()->MutableProperties()->MutableProperties())["__operation_owner_user_token"] = *token;
    }
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(TStreamingQueryOperationTrackingTest) {
    Y_UNIT_TEST_FLAGS(PendingOperationResumesAfterReboot, Alter, WithUserToken) {
        TFixture f;
        if (Alter) {
            f.Propose(f.MakeRequest(false));
        }
        const auto token = MakeUserToken(WithUserToken);
        auto request = f.MakeRequest(Alter, f.Owner);
        SetUserToken(*request, token);
        f.Propose(std::move(request));
        f.CheckRequests(0);
        for (ui64 requests = 1; requests <= 2; ++requests) {
            f.Reboot();
            f.CheckQuery(Alter ? 2 : 1);
            f.CheckTracking(Alter ? 2 : 1, f.Owner, token);
            f.CheckRequests(requests);
        }
    }

    Y_UNIT_TEST_FLAG(TrackerUsesQueryDatabase, Alter) {
        for (const auto database : {TFixture::EDatabase::Subdomain, TFixture::EDatabase::Serverless}) {
            TFixture f(database);
            if (Alter) {
                f.Propose(f.MakeRequest(false));
            }
            f.Propose(f.MakeRequest(Alter, f.Owner));
            f.Reboot();
            f.CheckTracking(Alter ? 2 : 1, f.Owner);
            f.CheckRequests(1);
        }
    }

    Y_UNIT_TEST_FLAG(OperationResumesWhenRebootedBeforePlan, Alter) {
        TFixture f;
        if (Alter) {
            f.Propose(f.MakeRequest(false));
        }
        auto request = f.MakeRequest(Alter, f.Owner);
        const ui64 txId = request->Record.GetTxId();
        TBlockEvents<TEvTxProcessing::TEvPlanStep> blockedPlan(f.Runtime, [txId](const auto& ev) {
            for (const auto& tx : ev->Get()->Record.GetTransactions()) {
                if (tx.GetTxId() == txId) {
                    return true;
                }
            }
            return false;
        });
        AsyncSend(f.Runtime, f.SchemeShardId, request.Release());
        TestModificationResult(f.Runtime, txId);
        f.Runtime.WaitFor("streaming query plan blocked", [&] { return !blockedPlan.empty(); });
        f.CheckRequests(0);
        f.Reboot();
        const auto tracking = f.GrabTracking();
        UNIT_ASSERT_VALUES_EQUAL(tracking->Get()->GetSchemeTxId(), txId);
        UNIT_ASSERT_VALUES_EQUAL(tracking->Get()->GetObjectGeneration(), Alter ? 2 : 1);
        UNIT_ASSERT_VALUES_EQUAL(tracking->Get()->GetOperationOwner(), f.Owner);
        f.CheckRequests(1);
        blockedPlan.Unblock().Stop();
        f.Env->TestWaitNotification(f.Runtime, txId);
        f.CheckQuery(Alter ? 2 : 1);
        f.CheckRequests(1);
    }

    Y_UNIT_TEST_FLAG(OperationResumesBeforePublicationAcknowledgement, Alter) {
        TFixture f;
        if (Alter) {
            f.Propose(f.MakeRequest(false));
        }
        auto request = f.MakeRequest(Alter, f.Owner);
        const ui64 txId = request->Record.GetTxId();
        TBlockEvents<NSchemeBoard::NSchemeshardEvents::TEvUpdateAck> acknowledgements(f.Runtime, [txId](const auto& ev) {
            return ev->Cookie == txId;
        });
        AsyncSend(f.Runtime, f.SchemeShardId, request.Release());
        TestModificationResult(f.Runtime, txId);
        f.Runtime.WaitFor("publication acknowledgement blocked", [&] { return !acknowledgements.empty(); });
        f.CheckRequests(0);
        f.Reboot();
        const auto tracking = f.GrabTracking();
        UNIT_ASSERT_VALUES_EQUAL(tracking->Get()->GetSchemeTxId(), txId);
        acknowledgements.Unblock().Stop();
        f.Env->TestWaitNotification(f.Runtime, txId, f.SchemeShardId);
        f.CheckQuery(Alter ? 2 : 1);
        f.CheckRequests(1);
    }

    Y_UNIT_TEST_FLAG(PendingOperationRejectsAnotherOwner, Replace) {
        TFixture f;
        f.Propose(f.MakeRequest(false, f.Owner));
        f.Propose(f.MakeRequest(true, f.Runtime.AllocateEdgeActor(), 1, Replace),
            {NKikimrScheme::StatusPreconditionFailed, "Streaming query already under operation"});
        f.Reboot();
        f.CheckTracking(1, f.Owner);
        f.Propose(f.MakeRequest(true, f.Runtime.AllocateEdgeActor(), 1, Replace),
            {NKikimrScheme::StatusPreconditionFailed, "Streaming query already under operation"});
        f.CheckRequests(1);
    }

    Y_UNIT_TEST_FLAGS(RejectsEmptyOperationOwner, Alter, Replace) {
        TFixture f;
        if (Alter) {
            f.Propose(f.MakeRequest(false));
        }
        auto request = f.MakeRequest(Alter, {}, std::nullopt, Replace);
        request->Record.MutableTransaction(0)->MutableCreateStreamingQuery()->MutableOperationOwnerActorId();
        f.Propose(std::move(request), {NKikimrScheme::StatusInvalidParameter, "Operation owner actor id must not be empty"});
        f.Reboot();
        f.CheckRequests(0);
    }

    Y_UNIT_TEST(CompletingOperationChecksVersionAndClearsPersistedOwner) {
        TFixture f;
        f.Propose(f.MakeRequest(false, f.Owner));
        f.Propose(f.MakeRequest(true, {}, 0), {NKikimrScheme::StatusPreconditionFailed, "ApplyIf"});
        f.CheckOwner(f.Owner);
        f.Propose(f.MakeRequest(true, {}, 1));
        f.Reboot();
        f.CheckOwner({});
        f.CheckQuery(2);
        f.CheckRequests(0);
        const auto nextOwner = f.Runtime.AllocateEdgeActor();
        f.Propose(f.MakeRequest(true, nextOwner, 2));
        f.Reboot();
        f.CheckTracking(3, nextOwner);
    }

    Y_UNIT_TEST(DroppedQueryDoesNotResumeOperation) {
        TFixture f;
        f.Propose(f.MakeRequest(false, f.Owner));
        TestDropStreamingQuery(f.Runtime, ++f.TxId, f.WorkingDir, "Query");
        f.Env->TestWaitNotification(f.Runtime, f.TxId);
        f.Reboot();
        f.CheckRequests(0);
        f.Propose(f.MakeRequest(false));
        f.Reboot();
        f.CheckRequests(0);
    }
}
