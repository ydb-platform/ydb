#include <ydb/core/base/tablet.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/tx/scheme_board/events_schemeshard.h>
#include <ydb/core/tx/schemeshard/schemeshard_private.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/event_pb.h>
#include <ydb/services/metadata/abstract/events.h>
#include <ydb/services/metadata/abstract/service.h>

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

    TPathId CheckTracking(ui64 version, TActorId owner, const std::optional<NACLib::TUserToken>& token = std::nullopt,
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
        UNIT_ASSERT_VALUES_EQUAL(request.GetUserToken().has_value(), token.has_value());
        if (token) {
            UNIT_ASSERT_VALUES_EQUAL(request.GetUserToken()->GetUserSID(), token->GetUserSID());
            const auto groups = token->GetGroupSIDs();
            UNIT_ASSERT_VALUES_EQUAL(request.GetUserToken()->GetGroupSIDs().size(), groups.size());
            for (const auto& group : groups) {
                UNIT_ASSERT(request.GetUserToken()->IsExist(group));
            }
        }
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
        UNIT_ASSERT_VALUES_EQUAL(properties.size(), 1);
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

std::optional<NACLib::TUserToken> MakeUserToken(bool enabled) {
    if (enabled) {
        return NACLib::TUserToken("streaming-user", TVector<TString>{"streaming-group", "all-users"});
    }
    return std::nullopt;
}

void SetUserToken(TEvTx& request, const std::optional<NACLib::TUserToken>& token) {
    if (token) {
        request.Record.SetUserToken(token->SerializeAsString());
    }
}

void StopQuery(TEvTx& request) {
    auto* description = request.Record.MutableTransaction(0)->MutableCreateStreamingQuery();
    (*description->MutableProperties()->MutableProperties())["run"] = "false";
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(TStreamingQueryOperationTrackingTest) {
    Y_UNIT_TEST_FLAG(CreateStartsOperationTracker, WithUserToken) {
        TFixture f;
        const auto token = MakeUserToken(WithUserToken);
        auto request = f.MakeRequest(false, f.Owner);
        SetUserToken(*request, token);
        f.Propose(std::move(request));

        f.CheckQuery(1);
        f.CheckOwner(f.Owner);
        f.CheckTracking(1, f.Owner, token);
        f.CheckRequests(1);
    }

    Y_UNIT_TEST_FLAGS(AlterStartsOperationTracker, Replace, WithUserToken) {
        TFixture f;
        f.Propose(f.MakeRequest(false));
        f.CheckQuery(1);
        f.CheckRequests(0);

        const auto token = MakeUserToken(WithUserToken);
        auto request = f.MakeRequest(true, f.Owner, 1, Replace);
        SetUserToken(*request, token);
        StopQuery(*request);
        f.Propose(std::move(request));

        f.CheckQuery(2, "false");
        f.CheckOwner(f.Owner);
        f.CheckTracking(2, f.Owner, token);
        f.CheckRequests(1);
    }

    Y_UNIT_TEST(CreateChecksAlterVersionBeforeStartingOperation) {
        TFixture f;
        f.Propose(f.MakeRequest(false));
        f.Propose(f.MakeRequest(true));
        f.CheckQuery(2);

        // A new path can depend on the entity version of an existing query.
        for (const ui64 version : {1, 3, 2}) {
            auto request = f.MakeRequest(false, f.Owner, version);
            request->Record.MutableTransaction(0)->MutableCreateStreamingQuery()->SetName("NewQuery");
            if (version != 2) {
                f.Propose(std::move(request), {NKikimrScheme::StatusPreconditionFailed, "ApplyIf"});
                TestDescribeResult(DescribePath(f.Runtime, "/MyRoot/DirStreamingQuery/NewQuery"), {NLs::PathNotExist});
                f.CheckRequests(0);
            } else {
                f.Propose(std::move(request));
                const auto description = DescribePath(f.Runtime, "/MyRoot/DirStreamingQuery/NewQuery");
                TestDescribeResult(description, {NLs::Finished, NLs::IsStreamingQuery});
                UNIT_ASSERT_VALUES_EQUAL(description.GetPathDescription().GetSelf().GetVersion().GetStreamingQueryVersion(), 1);
                f.CheckTracking(1, f.Owner, std::nullopt, "DirStreamingQuery/NewQuery");
                f.CheckRequests(1);
            }
            f.CheckQuery(2);
        }
    }

    Y_UNIT_TEST_FLAG(AlterChecksAlterVersionBeforeStartingOperation, Replace) {
        TFixture f;
        f.Propose(f.MakeRequest(false));
        f.Propose(f.MakeRequest(true));
        f.CheckQuery(2);

        for (const ui64 version : {1, 3}) {
            auto request = f.MakeRequest(true, f.Owner, version, Replace);
            StopQuery(*request);
            f.Propose(std::move(request), {NKikimrScheme::StatusPreconditionFailed, "ApplyIf"});
            f.CheckQuery(2);
            f.CheckRequests(0);
        }

        auto request = f.MakeRequest(true, f.Owner, 2, Replace);
        StopQuery(*request);
        f.Propose(std::move(request));
        f.CheckQuery(3, "false");
        f.CheckTracking(3, f.Owner);
        f.CheckRequests(1);
    }

    Y_UNIT_TEST_FLAG(PendingOperationRejectsAnotherOwner, Replace) {
        TFixture f;
        f.Propose(f.MakeRequest(false, f.Owner));
        f.CheckTracking(1, f.Owner);

        // Both a duplicate owner and a different owner must respect the lock.
        for (const auto owner : {f.Owner, f.Runtime.AllocateEdgeActor()}) {
            auto request = f.MakeRequest(true, owner, 1, Replace);
            StopQuery(*request);
            f.Propose(std::move(request), {NKikimrScheme::StatusPreconditionFailed, "Streaming query already under operation"});
            f.CheckQuery(1);
            f.CheckRequests(1);
        }

        f.Reboot();
        f.CheckQuery(1);
        f.CheckTracking(1, f.Owner);
        f.CheckRequests(2);
        f.Propose(f.MakeRequest(true, f.Runtime.AllocateEdgeActor(), 1, Replace),
            {NKikimrScheme::StatusPreconditionFailed, "Streaming query already under operation"});
        f.CheckQuery(1);
        f.CheckRequests(2);
    }

    Y_UNIT_TEST_FLAGS(PendingOperationResumesAfterReboot, Alter, WithUserToken) {
        TFixture f;
        if (Alter) {
            f.Propose(f.MakeRequest(false));
        }
        const auto token = MakeUserToken(WithUserToken);
        auto request = f.MakeRequest(Alter, f.Owner);
        SetUserToken(*request, token);
        f.Propose(std::move(request));
        f.GrabTracking();

        const ui64 version = Alter ? 2 : 1;
        for (ui64 requests = 2; requests <= 3; ++requests) {
            f.Reboot();
            f.CheckQuery(version);
            f.CheckTracking(version, f.Owner, token);
            f.CheckRequests(requests);
        }
    }

    Y_UNIT_TEST_FLAGS(TrackerUsesQueryDatabase, Alter, Reboot) {
        for (const auto database : {TFixture::EDatabase::Subdomain, TFixture::EDatabase::Serverless}) {
            TFixture f(database);
            if (Alter) {
                f.Propose(f.MakeRequest(false));
            }
            f.Propose(f.MakeRequest(Alter, f.Owner));
            if (Reboot) {
                f.CheckTracking(Alter ? 2 : 1, f.Owner);
                f.Reboot();
            }
            f.CheckQuery(Alter ? 2 : 1);
            f.CheckTracking(Alter ? 2 : 1, f.Owner);
            f.CheckRequests(Reboot ? 2 : 1);
        }
    }

    Y_UNIT_TEST_FLAG(OperationResumesWhenRebootedBeforePlan, Alter) {
        TFixture f;
        if (Alter) {
            f.Propose(f.MakeRequest(false));
        }
        const auto token = MakeUserToken(true);
        auto request = f.MakeRequest(Alter, f.Owner);
        SetUserToken(*request, token);
        const ui64 txId = request->Record.GetTxId();
        TBlockEvents<TEvTxProcessing::TEvPlanStep> blockedPlan(f.Runtime, [txId](const auto& ev) {
            for (const auto& tx : ev->Get()->Record.GetTransactions()) {
                if (tx.GetTxId() == txId) {
                    return true;
                }
            }
            return false;
        });

        AsyncSend(f.Runtime, TTestTxConfig::SchemeShard, request.Release());
        TestModificationResult(f.Runtime, txId);
        f.Runtime.WaitFor("streaming query plan blocked", [&] { return !blockedPlan.empty(); });
        f.CheckRequests(0);

        // Ownership is already durable while the schema transaction awaits its plan.
        f.Reboot();
        const ui64 version = Alter ? 2 : 1;
        f.CheckRequests(0);
        blockedPlan.Unblock().Stop();
        f.Env->TestWaitNotification(f.Runtime, txId);
        f.CheckQuery(version);
        f.CheckTracking(version, f.Owner, token);
        f.CheckRequests(1);
    }

    Y_UNIT_TEST_FLAGS(TrackerWaitsForPublicationAcknowledgement, Alter, Reboot) {
        TFixture f;
        if (Alter) {
            f.Propose(f.MakeRequest(false));
        }
        const auto token = MakeUserToken(true);
        auto request = f.MakeRequest(Alter, f.Owner);
        SetUserToken(*request, token);
        const ui64 txId = request->Record.GetTxId();
        TBlockEvents<NSchemeBoard::NSchemeshardEvents::TEvUpdateAck> acknowledgements(f.Runtime, [txId](const auto& ev) {
            return ev->Cookie == txId;
        });

        AsyncSend(f.Runtime, f.SchemeShardId, request.Release());
        TestModificationResult(f.Runtime, txId);
        f.Runtime.WaitFor("publication acknowledgement blocked", [&] { return !acknowledgements.empty(); });
        const auto checkWaiting = [&] {
            f.Runtime.SimulateSleep(TDuration::Seconds(1));
            f.CheckRequests(0);
            TestDescribeResult(DescribePath(f.Runtime, f.SchemeShardId, f.QueryPath),
                {NLs::PathExist, NLs::CheckPathState(Alter ? NKikimrSchemeOp::EPathStateAlter : NKikimrSchemeOp::EPathStateCreate)});
        };
        checkWaiting();
        if (Reboot) {
            f.Reboot();
            checkWaiting();
        }

        acknowledgements.Unblock().Stop();
        f.Env->TestWaitNotification(f.Runtime, txId, f.SchemeShardId);
        TestDescribeResult(DescribePath(f.Runtime, f.SchemeShardId, f.QueryPath), {NLs::CheckPathState()});
        f.CheckQuery(Alter ? 2 : 1);
        f.CheckOwner(f.Owner);
        f.CheckTracking(Alter ? 2 : 1, f.Owner, token);
        f.CheckRequests(1);
    }

    Y_UNIT_TEST_FLAG(TrackerWaitsForOperationCommit, Alter) {
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
        AsyncSend(f.Runtime, TTestTxConfig::SchemeShard, request.Release());
        TestModificationResult(f.Runtime, txId);
        f.Runtime.WaitFor("streaming query plan blocked", [&] { return !blockedPlan.empty(); });

        TBlockEvents<NSchemeShard::TEvPrivate::TEvProgressOperation> blockedDone(f.Runtime, [txId](const auto& ev) {
            return ev->Get()->TxId == txId;
        });
        blockedPlan.Unblock().Stop();
        f.Runtime.WaitFor("streaming query done state blocked", [&] { return !blockedDone.empty(); });

        TBlockEvents<TEvTablet::TEvCommit> blockedCommit(f.Runtime, [](const auto& ev) {
            return ev->Get()->TabletID == TTestTxConfig::SchemeShard;
        });
        blockedDone.Unblock().Stop();
        f.Runtime.WaitFor("streaming query completion commit blocked", [&] { return !blockedCommit.empty(); });
        f.CheckRequests(0);

        blockedCommit.Unblock().Stop();
        f.Env->TestWaitNotification(f.Runtime, txId);
        f.CheckQuery(Alter ? 2 : 1);
        f.CheckTracking(Alter ? 2 : 1, f.Owner);
        f.CheckRequests(1);
    }

    Y_UNIT_TEST_FLAG(CreateRejectsEmptyOperationOwner, Replace) {
        TFixture f;
        auto request = f.MakeRequest(false, {}, std::nullopt, Replace);
        request->Record.MutableTransaction(0)->MutableCreateStreamingQuery()->MutableOperationOwnerActorId();
        f.Propose(std::move(request), {NKikimrScheme::StatusInvalidParameter, "Operation owner actor id must not be empty"});
        TestDescribeResult(DescribePath(f.Runtime, "/MyRoot/DirStreamingQuery/Query"), {NLs::PathNotExist});
        f.CheckRequests(0);

        f.Propose(f.MakeRequest(false, f.Owner, std::nullopt, Replace));
        f.CheckQuery(1);
        f.CheckTracking(1, f.Owner);
    }

    Y_UNIT_TEST_FLAG(AlterRejectsEmptyOperationOwner, Replace) {
        TFixture f;
        f.Propose(f.MakeRequest(false));
        auto request = f.MakeRequest(true, {}, 1, Replace);
        request->Record.MutableTransaction(0)->MutableCreateStreamingQuery()->MutableOperationOwnerActorId();
        StopQuery(*request);
        f.Propose(std::move(request), {NKikimrScheme::StatusInvalidParameter, "Operation owner actor id must not be empty"});
        f.CheckQuery(1);
        f.CheckRequests(0);

        f.Propose(f.MakeRequest(true, f.Owner, 1, Replace));
        f.CheckQuery(2);
        f.CheckTracking(2, f.Owner);
    }

    Y_UNIT_TEST(CompletingOperationChecksVersionAndClearsPersistedOwner) {
        TFixture f;
        auto request = f.MakeRequest(false, f.Owner);
        SetUserToken(*request, MakeUserToken(true));
        f.Propose(std::move(request));
        f.GrabTracking();

        // A stale completion must neither update properties nor release ownership.
        request = f.MakeRequest(true, {}, 0);
        StopQuery(*request);
        f.Propose(std::move(request), {NKikimrScheme::StatusPreconditionFailed, "ApplyIf"});
        f.CheckQuery(1);
        f.CheckRequests(1);
        f.Propose(f.MakeRequest(true, f.Owner, 1),
            {NKikimrScheme::StatusPreconditionFailed, "Streaming query already under operation"});

        // The ownerless alter completes the operation using its current version.
        request = f.MakeRequest(true, {}, 1);
        StopQuery(*request);
        f.Propose(std::move(request));
        f.CheckQuery(2, "false");
        f.CheckRequests(1);
        f.CheckOwner({});
        f.Reboot();
        f.CheckQuery(2, "false");
        f.CheckOwner({});
        f.CheckRequests(1);

        const auto nextOwner = f.Runtime.AllocateEdgeActor();
        f.Propose(f.MakeRequest(true, nextOwner, 2));
        f.CheckQuery(3);
        f.CheckTracking(3, nextOwner);
        f.Reboot();
        f.CheckTracking(3, nextOwner);
        f.CheckRequests(3);

        // A delayed completion of the first operation cannot clear the new owner.
        f.Propose(f.MakeRequest(true, {}, 1), {NKikimrScheme::StatusPreconditionFailed, "ApplyIf"});
        f.Propose(f.MakeRequest(true, f.Owner, 3),
            {NKikimrScheme::StatusPreconditionFailed, "Streaming query already under operation"});
        f.CheckQuery(3);
        f.CheckRequests(3);
    }

    Y_UNIT_TEST(RecreatedQueryTracksNewPathId) {
        TFixture f;
        f.Propose(f.MakeRequest(false, f.Owner));
        const auto originalPathId = f.CheckTracking(1, f.Owner);

        TestDropStreamingQuery(f.Runtime, ++f.TxId, f.WorkingDir, "Query");
        f.Env->TestWaitNotification(f.Runtime, f.TxId);

        f.Propose(f.MakeRequest(false, f.Owner));
        const auto newPathId = f.CheckTracking(1, f.Owner);
        UNIT_ASSERT_VALUES_EQUAL(newPathId.OwnerId, originalPathId.OwnerId);
        UNIT_ASSERT_VALUES_UNEQUAL(newPathId.LocalPathId, originalPathId.LocalPathId);

        f.Reboot();
        UNIT_ASSERT_VALUES_EQUAL(f.CheckTracking(1, f.Owner), newPathId);
        f.CheckRequests(3);
    }

    Y_UNIT_TEST(DroppedQueryDoesNotResumeOperation) {
        TFixture f;
        f.Propose(f.MakeRequest(false, f.Owner));
        f.CheckTracking(1, f.Owner);

        TestDropStreamingQuery(f.Runtime, ++f.TxId, "/MyRoot/DirStreamingQuery", "Query");
        f.Env->TestWaitNotification(f.Runtime, f.TxId);
        f.Reboot();
        TestDescribeResult(DescribePath(f.Runtime, "/MyRoot/DirStreamingQuery/Query"), {NLs::PathNotExist});
        f.CheckRequests(1);

        f.Propose(f.MakeRequest(false));
        f.Reboot();
        f.CheckQuery(1);
        f.CheckRequests(1);
    }
}
