#include <ydb/core/persqueue/writer/partition_chooser.h>
#include <ydb/core/persqueue/writer/partition_chooser_impl__abstract_chooser_actor.h>
#include <ydb/core/persqueue/writer/partition_chooser_impl__old_chooser_actor.h>
#include <ydb/core/persqueue/writer/partition_chooser_impl__sm_chooser_actor.h>
#include <ydb/core/persqueue/writer/pipe_utils.h>

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/persqueue/events/internal.h>
#include <ydb/core/persqueue/public/utils.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/library/persqueue/topic_parser/topic_parser.h>
#include <ydb/core/persqueue/public/nameresolver/nameresolver.h>
#include <ydb/services/metadata/service.h>

#include <library/cpp/testing/unittest/registar.h>

#include <google/protobuf/text_format.h>

#include <stdexcept>

using namespace NActors;
using namespace NKikimr;
using namespace NKikimr::NPQ;
using namespace NKikimr::NPQ::NPartitionChooser;
using namespace NKikimr::NTabletPipe::NTest;
using namespace Ydb::PersQueue::ErrorCode;

namespace {

void AddPartition(
    NKikimrSchemeOp::TPersQueueGroupDescription& conf,
    ui32 id,
    std::optional<TString> boundaryFrom = std::nullopt,
    std::optional<TString> boundaryTo = std::nullopt,
    std::vector<ui32> children = {},
    NKikimrPQ::ETopicPartitionStatus status = NKikimrPQ::ETopicPartitionStatus::Active)
{
    auto* p = conf.AddPartitions();
    p->SetPartitionId(id);
    p->SetTabletId(1000 + id);
    p->SetStatus(status);
    if (boundaryFrom) {
        p->MutableKeyRange()->SetFromBound(*boundaryFrom);
    }
    if (boundaryTo) {
        p->MutableKeyRange()->SetToBound(*boundaryTo);
    }
    for (ui32 c : children) {
        p->AddChildPartitionIds(c);
    }
}

NKikimrSchemeOp::TPersQueueGroupDescription MakeSmConfig() {
    NKikimrSchemeOp::TPersQueueGroupDescription result;
    result.SetBalancerTabletID(999);
    auto* config = result.MutablePQTabletConfig();
    config->SetTopicName("topic-1");
    config->SetTopicPath("/Root/topic-1");
    auto* strategy = config->MutablePartitionStrategy();
    strategy->SetMinPartitionCount(1);
    strategy->SetMaxPartitionCount(10);
    strategy->SetPartitionStrategyType(
        NKikimrPQ::TPQTabletConfig_TPartitionStrategyType_CAN_SPLIT);
    return result;
}

NKikimrSchemeOp::TPersQueueGroupDescription MakeOldConfig() {
    auto result = MakeSmConfig();
    result.MutablePQTabletConfig()->MutablePartitionStrategy()->SetPartitionStrategyType(
        NKikimrPQ::TPQTabletConfig_TPartitionStrategyType_DISABLED);
    return result;
}

NKikimr::NPQ::NNameResolver::TTopicNamesPtr MakeConverter(const NKikimrSchemeOp::TPersQueueGroupDescription& config) {
    return NKikimr::NPQ::NNameResolver::MakeTopicNamesPtr(
        NKikimr::NPQ::NNameResolver::NamesFromFirstClassConfig(config.GetPQTabletConfig()));
}

Ydb::ResultSet MakeSelectResultSet(std::optional<ui32> partition, std::optional<ui64> seqNo) {
    Ydb::ResultSet rs;
    auto addCol = [&](const char* name, Ydb::Type::PrimitiveTypeId typeId) {
        auto& column = *rs.add_columns();
        column.set_name(name);
        column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(typeId);
    };
    addCol("Partition", Ydb::Type::UINT32);
    addCol("CreateTime", Ydb::Type::UINT64);
    addCol("AccessTime", Ydb::Type::UINT64);
    addCol("SeqNo", Ydb::Type::UINT64);

    if (partition) {
        auto& row = *rs.add_rows();
        row.add_items()->set_uint32_value(*partition);
        row.add_items()->set_uint64_value(1);
        row.add_items()->set_uint64_value(2);
        row.add_items()->set_uint64_value(seqNo.value_or(0));
    }
    return rs;
}

class TTabletMock: public TActor<TTabletMock> {
public:
    TTabletMock(
        NKikimrPQ::ETopicPartitionStatus status = NKikimrPQ::ETopicPartitionStatus::Active,
        std::optional<ui64> checkSeqNo = std::nullopt,
        std::optional<ui64> maxSeqNo = std::nullopt,
        bool maxSeqNoPartitionActive = false,
        bool emptySourceIdInfo = false,
        bool noMaxSeqNoResult = false,
        NMsgBusProxy::EResponseStatus responseStatus = NMsgBusProxy::MSTATUS_OK)
        : TActor(&TThis::StateWork)
        , Status(status)
        , CheckSeqNo(checkSeqNo)
        , MaxSeqNo(maxSeqNo)
        , MaxSeqNoPartitionActive(maxSeqNoPartitionActive)
        , EmptySourceIdInfo(emptySourceIdInfo)
        , NoMaxSeqNoResult(noMaxSeqNoResult)
        , ResponseStatus(responseStatus)
    {
    }

private:
    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvPQ::TEvCheckPartitionStatusRequest, Handle);
            HFunc(TEvPersQueue::TEvRequest, Handle);
            HFunc(TEvPersQueue::TEvGetPartitionIdForWrite, Handle);
            default:
                break;
        }
    }

    void Handle(TEvPQ::TEvCheckPartitionStatusRequest::TPtr& ev, const TActorContext& ctx) {
        auto response = MakeHolder<TEvPQ::TEvCheckPartitionStatusResponse>();
        response->Record.SetStatus(Status);
        if (CheckSeqNo) {
            response->Record.SetSeqNo(*CheckSeqNo);
        }
        ctx.Send(ev->Sender, response.Release());
    }

    void Handle(TEvPersQueue::TEvRequest::TPtr& ev, const TActorContext& ctx) {
        auto response = MakeHolder<TEvPersQueue::TEvResponse>();
        response->Record.SetStatus(ResponseStatus);
        response->Record.SetErrorCode(NPersQueue::NErrorCode::OK);
        response->Record.MutablePartitionResponse();

        if (!NoMaxSeqNoResult) {
            auto* result = response->Record.MutablePartitionResponse()->MutableCmdGetMaxSeqNoResult();
            result->SetIsPartitionActive(MaxSeqNoPartitionActive);
            if (!EmptySourceIdInfo) {
                auto* sn = result->AddSourceIdInfo();
                sn->SetSeqNo(MaxSeqNo.value_or(0));
                sn->SetState(MaxSeqNo
                    ? NKikimrPQ::TMessageGroupInfo::STATE_REGISTERED
                    : NKikimrPQ::TMessageGroupInfo::STATE_PENDING_REGISTRATION);
            }
        }
        ctx.Send(ev->Sender, response.Release());
    }

    void Handle(TEvPersQueue::TEvGetPartitionIdForWrite::TPtr& ev, const TActorContext& ctx) {
        auto response = MakeHolder<TEvPersQueue::TEvGetPartitionIdForWriteResponse>();
        response->Record.SetPartitionId(PqrPartitionId);
        ctx.Send(ev->Sender, response.Release());
    }

public:
    ui32 PqrPartitionId = 0;

private:
    NKikimrPQ::ETopicPartitionStatus Status;
    std::optional<ui64> CheckSeqNo;
    std::optional<ui64> MaxSeqNo;
    bool MaxSeqNoPartitionActive;
    bool EmptySourceIdInfo;
    bool NoMaxSeqNoResult;
    NMsgBusProxy::EResponseStatus ResponseStatus;
};

class TMetaMock: public TActor<TMetaMock> {
public:
    TMetaMock()
        : TActor(&TThis::StateWork)
    {
    }

private:
    STRICT_STFUNC(StateWork,
        HFunc(NMetadata::NProvider::TEvPrepareManager, Handle);
    )

    void Handle(NMetadata::NProvider::TEvPrepareManager::TPtr& ev, const TActorContext& ctx) {
        ctx.Send(ev->Sender, new NMetadata::NProvider::TEvManagerPrepared(ev->Get()->GetManager()));
    }
};

struct TKqpSettings {
    Ydb::StatusIds::StatusCode CreateSessionStatus = Ydb::StatusIds::SUCCESS;
    Ydb::StatusIds::StatusCode SelectStatus = Ydb::StatusIds::SUCCESS;
    Ydb::StatusIds::StatusCode UpdateStatus = Ydb::StatusIds::SUCCESS;
    ui32 UpdateAbortsBeforeSuccess = 0;
    std::optional<ui32> SelectPartition;
    std::optional<ui64> SelectSeqNo;
    bool HoldCreateSession = false;
};

class TKqpMock: public TActor<TKqpMock> {
public:
    explicit TKqpMock(TKqpSettings settings)
        : TActor(&TThis::StateWork)
        , Settings(std::move(settings))
    {
    }

    ui32 CreateSessionRequests = 0;
    ui32 SelectRequests = 0;
    ui32 UpdateRequests = 0;
    TActorId PendingCreateSessionSender;
    ui64 PendingCreateSessionCookie = 0;

    void ReplyPendingCreateSession(const TActorContext& ctx) {
        UNIT_ASSERT(PendingCreateSessionSender);
        auto response = MakeHolder<NKqp::TEvKqp::TEvCreateSessionResponse>();
        response->Record.SetYdbStatus(Settings.CreateSessionStatus);
        if (Settings.CreateSessionStatus == Ydb::StatusIds::SUCCESS) {
            response->Record.MutableResponse()->SetSessionId("session-1");
        }
        ctx.Send(PendingCreateSessionSender, response.Release(), 0, PendingCreateSessionCookie);
        PendingCreateSessionSender = {};
    }

private:
    STRICT_STFUNC(StateWork,
        HFunc(NKqp::TEvKqp::TEvCreateSessionRequest, Handle);
        HFunc(NKqp::TEvKqp::TEvQueryRequest, Handle);
        IgnoreFunc(NKqp::TEvKqp::TEvCloseSessionRequest);
    )

    void Handle(NKqp::TEvKqp::TEvCreateSessionRequest::TPtr& ev, const TActorContext& ctx) {
        ++CreateSessionRequests;
        if (Settings.HoldCreateSession) {
            PendingCreateSessionSender = ev->Sender;
            PendingCreateSessionCookie = ev->Cookie;
            return;
        }
        auto response = MakeHolder<NKqp::TEvKqp::TEvCreateSessionResponse>();
        response->Record.SetYdbStatus(Settings.CreateSessionStatus);
        if (Settings.CreateSessionStatus == Ydb::StatusIds::SUCCESS) {
            response->Record.MutableResponse()->SetSessionId("session-1");
        }
        ctx.Send(ev->Sender, response.Release(), 0, ev->Cookie);
    }

    void Handle(NKqp::TEvKqp::TEvQueryRequest::TPtr& ev, const TActorContext& ctx) {
        const bool isUpdate = ev->Get()->Record.GetRequest().GetTxControl().commit_tx();
        auto response = MakeHolder<NKqp::TEvKqp::TEvQueryResponse>();

        if (isUpdate) {
            ++UpdateRequests;
            auto status = Settings.UpdateStatus;
            if (Settings.UpdateAbortsBeforeSuccess > 0) {
                --Settings.UpdateAbortsBeforeSuccess;
                status = Ydb::StatusIds::ABORTED;
            }
            response->Record.SetYdbStatus(status);
        } else {
            ++SelectRequests;
            response->Record.SetYdbStatus(Settings.SelectStatus);
            if (Settings.SelectStatus == Ydb::StatusIds::SUCCESS) {
                response->Record.MutableResponse()->MutableTxMeta()->set_id("tx-1");
                *response->Record.MutableResponse()->AddYdbResults() =
                    MakeSelectResultSet(Settings.SelectPartition, Settings.SelectSeqNo);
            }
        }
        ctx.Send(ev->Sender, response.Release(), 0, ev->Cookie);
    }

    TKqpSettings Settings;
};

class TPipeFailActor: public TActorBootstrapped<TPipeFailActor> {
public:
    TPipeFailActor(TActorId owner, ui64 tabletId)
        : Owner(owner)
        , TabletId(tabletId)
    {
    }

    void Bootstrap(const TActorContext& ctx) {
        ctx.Send(Owner, new TEvTabletPipe::TEvClientConnected(
            TabletId, NKikimrProto::ERROR, SelfId(), SelfId(), true, false, 0));
        PassAway();
    }

private:
    TActorId Owner;
    ui64 TabletId;
};

struct TPipeFailCreator {
    static IActor* CreateClient(const TActorId& owner, ui64 tabletId, const NTabletPipe::TClientConfig& = {}) {
        return new TPipeFailActor(owner, tabletId);
    }
};

class TPipeDestroyActor: public TActorBootstrapped<TPipeDestroyActor> {
public:
    TPipeDestroyActor(TActorId owner, ui64 tabletId)
        : Owner(owner)
        , TabletId(tabletId)
    {
    }

    void Bootstrap(const TActorContext& ctx) {
        ctx.Send(Owner, new TEvTabletPipe::TEvClientConnected(
            TabletId, NKikimrProto::OK, SelfId(), SelfId(), true, false, 1));
        ctx.Send(Owner, new TEvTabletPipe::TEvClientDestroyed(TabletId, SelfId(), SelfId()));
        PassAway();
    }

private:
    TActorId Owner;
    ui64 TabletId;
};

struct TPipeDestroyCreator {
    static IActor* CreateClient(const TActorId& owner, ui64 tabletId, const NTabletPipe::TClientConfig& = {}) {
        return new TPipeDestroyActor(owner, tabletId);
    }
};

class TMapChooser: public IPartitionChooser {
public:
    void Add(ui32 partitionId, ui64 tabletId) {
        Partitions[partitionId] = TPartitionInfo(partitionId, tabletId);
    }

    void SetSourcePartition(const TString& sourceId, ui32 partitionId) {
        BySource[sourceId] = partitionId;
    }

    void SetRandomPartition(ui32 partitionId) {
        RandomPartition = partitionId;
    }

    const TPartitionInfo* GetPartition(const TString& sourceId) const override {
        auto it = BySource.find(sourceId);
        if (it == BySource.end()) {
            return nullptr;
        }
        return GetPartition(it->second);
    }

    const TPartitionInfo* GetPartition(ui32 partitionId) const override {
        auto it = Partitions.find(partitionId);
        return it == Partitions.end() ? nullptr : &it->second;
    }

    const TPartitionInfo* GetRandomPartition() const override {
        return RandomPartition ? GetPartition(*RandomPartition) : nullptr;
    }

private:
    THashMap<ui32, TPartitionInfo> Partitions;
    THashMap<TString, ui32> BySource;
    std::optional<ui32> RandomPartition;
};

class TThrowInOnSelectedActor
    : public TAbstractPartitionChooserActor<TThrowInOnSelectedActor, TPipeMock>
{
public:
    using TParent = TAbstractPartitionChooserActor<TThrowInOnSelectedActor, TPipeMock>;

    TThrowInOnSelectedActor(
        TActorId parentId,
        const std::shared_ptr<IPartitionChooser>& chooser,
        NKikimr::NPQ::NNameResolver::TTopicNamesPtr& fullConverter)
        : TParent(parentId, chooser, fullConverter, "source", std::nullopt, {})
    {
    }

    void Bootstrap(const TActorContext& ctx) {
        if (!Initialize(ctx)) {
            return;
        }
        // Go to KQP path with NeedTable=false → OnSelected throws.
        InitTable(ctx);
    }

    void OnSelected(const TActorContext&) override {
        throw std::runtime_error("handler-boom");
    }
};

struct TEnv {
    TTestBasicRuntime Runtime;
    TActorId Edge;
    TKqpMock* Kqp = nullptr;
    TActorId KqpId;
    NKikimr::NPQ::NNameResolver::TTopicNamesPtr Converter;
    NKikimrSchemeOp::TPersQueueGroupDescription Config;
    std::shared_ptr<IPartitionChooser> Chooser;
    std::shared_ptr<TPartitionGraph> Graph;

    explicit TEnv(
        bool firstClassCitizen = true,
        bool useSrcIdMapping = true,
        TKqpSettings kqpSettings = {})
        : Runtime(1, false)
    {
        TPipeMock::Clear();

        TAppPrepare app;
        app.FeatureFlags.SetEnableTabletRestartOnUnhandledExceptions(true);
        app.PQConfig.SetTopicsAreFirstClassCitizen(firstClassCitizen);
        app.PQConfig.SetUseSrcIdMetaMappingInFirstClass(useSrcIdMapping);
        app.PQConfig.SetSourceIdTablePath("/Root/.metadata/TopicPartitionsMapping");
        Runtime.Initialize(app.Unwrap());
        Runtime.GetAppData().TenantName = "/Root";

        Edge = Runtime.AllocateEdgeActor();

        auto* meta = new TMetaMock();
        auto metaId = Runtime.Register(meta);
        Runtime.EnableScheduleForActor(metaId);
        Runtime.RegisterService(NMetadata::NProvider::MakeServiceId(Runtime.GetNodeId(0)), metaId);

        Kqp = new TKqpMock(std::move(kqpSettings));
        KqpId = Runtime.Register(Kqp);
        Runtime.EnableScheduleForActor(KqpId);
        Runtime.RegisterService(NKqp::MakeKqpProxyID(Runtime.GetNodeId(0)), KqpId);
    }

    TActorId RegisterTablet(ui32 partitionId, TTabletMock* mock) {
        auto id = Runtime.Register(mock);
        Runtime.EnableScheduleForActor(id);
        TPipeMock::Register(1000 + partitionId, id);
        return id;
    }

    TActorId RegisterBalancer(TTabletMock* mock) {
        auto id = Runtime.Register(mock);
        Runtime.EnableScheduleForActor(id);
        TPipeMock::Register(Config.GetBalancerTabletID(), id);
        return id;
    }

    void SetupSm(NKikimrSchemeOp::TPersQueueGroupDescription config) {
        Config = std::move(config);
        Converter = MakeConverter(Config);
        Chooser = CreatePartitionChooser(Config, true);
        Graph = MakeSharedPartitionGraph(Config);
    }

    void SetupOld(NKikimrSchemeOp::TPersQueueGroupDescription config) {
        Config = std::move(config);
        Converter = MakeConverter(Config);
        Chooser = CreatePartitionChooser(Config, true);
        Graph = MakeSharedPartitionGraph(Config);
    }

    TActorId StartSm(
        const TString& sourceId,
        std::optional<ui32> prefered = std::nullopt)
    {
        auto actorId = Runtime.Register(CreatePartitionChooserActor<TPipeMock>(
            Edge, Config, Chooser, Graph, Converter, sourceId, prefered, {}));
        Runtime.EnableScheduleForActor(actorId);
        Runtime.DispatchEvents();
        return actorId;
    }

    template <typename TPipeCreator>
    TActorId StartSmWithPipe(
        const TString& sourceId,
        std::optional<ui32> prefered = std::nullopt)
    {
        auto actorId = Runtime.Register(new TSMPartitionChooserActor<TPipeCreator>(
            Edge, Config, Chooser, Graph, Converter, sourceId, prefered, {}));
        Runtime.EnableScheduleForActor(actorId);
        Runtime.DispatchEvents();
        return actorId;
    }

    TActorId StartOld(
        const TString& sourceId,
        std::optional<ui32> prefered = std::nullopt)
    {
        auto actorId = Runtime.Register(CreatePartitionChooserActor<TPipeMock>(
            Edge, Config, Chooser, Graph, Converter, sourceId, prefered, {}));
        Runtime.EnableScheduleForActor(actorId);
        Runtime.DispatchEvents();
        return actorId;
    }

    THolder<TEvPartitionChooser::TEvChooseResult> WaitResult(TDuration timeout = TDuration::Seconds(2)) {
        auto ev = Runtime.GrabEdgeEvent<TEvPartitionChooser::TEvChooseResult>(Edge, timeout);
        if (!ev) {
            return {};
        }
        return THolder<TEvPartitionChooser::TEvChooseResult>(ev->Release().Release());
    }

    THolder<TEvPartitionChooser::TEvChooseError> WaitError(TDuration timeout = TDuration::Seconds(2)) {
        auto ev = Runtime.GrabEdgeEvent<TEvPartitionChooser::TEvChooseError>(Edge, timeout);
        if (!ev) {
            return {};
        }
        return THolder<TEvPartitionChooser::TEvChooseError>(ev->Release().Release());
    }
};

} // namespace

Y_UNIT_TEST_SUITE(TPartitionChooserActorScenarios) {

Y_UNIT_TEST(FastPath_ActiveWithSeqNo) {
    TEnv env;
    auto config = MakeSmConfig();
    AddPartition(config, 0, {}, {});
    env.SetupSm(std::move(config));
    env.RegisterTablet(0, new TTabletMock(NKikimrPQ::ETopicPartitionStatus::Active, /*checkSeqNo=*/42));

    env.StartSm("A_Source");
    auto result = env.WaitResult();
    UNIT_ASSERT(result);
    UNIT_ASSERT_VALUES_EQUAL(result->PartitionId, 0);
    UNIT_ASSERT(result->SeqNo);
    UNIT_ASSERT_VALUES_EQUAL(*result->SeqNo, 42);
    UNIT_ASSERT_VALUES_EQUAL(env.Kqp->SelectRequests, 0);
    UNIT_ASSERT(env.Kqp->UpdateRequests >= 1);
}

Y_UNIT_TEST(EmptySourceId_RandomPartition) {
    TEnv env(/*fcc=*/true, /*mapping=*/false);
    auto config = MakeSmConfig();
    AddPartition(config, 0, {}, {});
    AddPartition(config, 1, {}, {});
    env.SetupSm(std::move(config));

    auto chooser = std::make_shared<TMapChooser>();
    chooser->Add(0, 1000);
    chooser->Add(1, 1001);
    chooser->SetRandomPartition(1);
    env.Chooser = chooser;

    env.RegisterTablet(1, new TTabletMock());
    env.StartSm("");
    auto result = env.WaitResult();
    UNIT_ASSERT(result);
    UNIT_ASSERT_VALUES_EQUAL(result->PartitionId, 1);
}

Y_UNIT_TEST(PipeConnectFail_OwnershipFast) {
    TEnv env;
    auto config = MakeSmConfig();
    AddPartition(config, 0, {}, {});
    env.SetupSm(std::move(config));

    env.StartSmWithPipe<TPipeFailCreator>("A_Source");
    auto error = env.WaitError();
    UNIT_ASSERT(error);
    UNIT_ASSERT_EQUAL(error->Code, ErrorCode::INITIALIZING);
    UNIT_ASSERT(error->ErrorMessage.Contains("Pipe closed"));
}

Y_UNIT_TEST(PipeDestroyed_OwnershipFast) {
    TEnv env;
    auto config = MakeSmConfig();
    AddPartition(config, 0, {}, {});
    env.SetupSm(std::move(config));

    env.StartSmWithPipe<TPipeDestroyCreator>("A_Source");
    auto error = env.WaitError();
    UNIT_ASSERT(error);
    UNIT_ASSERT_EQUAL(error->Code, ErrorCode::INITIALIZING);
    UNIT_ASSERT(error->ErrorMessage.Contains("Pipe closed"));
}

Y_UNIT_TEST(KqpCreateSessionError) {
    TKqpSettings kqp;
    kqp.CreateSessionStatus = Ydb::StatusIds::UNAVAILABLE;
    TEnv env(/*fcc=*/true, /*mapping=*/true, kqp);

    auto config = MakeSmConfig();
    AddPartition(config, 0, {}, {});
    env.SetupSm(std::move(config));
    env.RegisterTablet(0, new TTabletMock());

    env.StartSm("A_Source");
    auto error = env.WaitError();
    UNIT_ASSERT(error);
    UNIT_ASSERT_EQUAL(error->Code, ErrorCode::INITIALIZING);
    UNIT_ASSERT(error->ErrorMessage.Contains("PQ53"));
}

Y_UNIT_TEST(KqpSelectError) {
    TKqpSettings kqp;
    kqp.SelectStatus = Ydb::StatusIds::GENERIC_ERROR;
    TEnv env(/*fcc=*/true, /*mapping=*/true, kqp);

    auto config = MakeSmConfig();
    AddPartition(config, 0, {}, {});
    env.SetupSm(std::move(config));
    env.RegisterTablet(0, new TTabletMock());

    env.StartSm("A_Source");
    auto error = env.WaitError();
    UNIT_ASSERT(error);
    UNIT_ASSERT_EQUAL(error->Code, ErrorCode::INITIALIZING);
    UNIT_ASSERT(error->ErrorMessage.Contains("PQ50"));
}

Y_UNIT_TEST(KqpUpdateError) {
    TKqpSettings kqp;
    kqp.UpdateStatus = Ydb::StatusIds::GENERIC_ERROR;
    TEnv env(/*fcc=*/true, /*mapping=*/true, kqp);

    auto config = MakeSmConfig();
    AddPartition(config, 0, {}, {});
    env.SetupSm(std::move(config));
    env.RegisterTablet(0, new TTabletMock());

    env.StartSm("A_Source");
    auto error = env.WaitError();
    UNIT_ASSERT(error);
    UNIT_ASSERT_EQUAL(error->Code, ErrorCode::INITIALIZING);
    UNIT_ASSERT(error->ErrorMessage.Contains("PQ51"));
}

Y_UNIT_TEST(KqpUpdateAbortedThenSuccess) {
    TKqpSettings kqp;
    kqp.UpdateAbortsBeforeSuccess = 1;
    TEnv env(/*fcc=*/true, /*mapping=*/true, kqp);

    auto config = MakeSmConfig();
    AddPartition(config, 0, {}, {});
    env.SetupSm(std::move(config));
    env.RegisterTablet(0, new TTabletMock());

    env.StartSm("A_Source");
    auto result = env.WaitResult();
    UNIT_ASSERT(result);
    UNIT_ASSERT_VALUES_EQUAL(result->PartitionId, 0);
    UNIT_ASSERT(env.Kqp->CreateSessionRequests >= 2);
    UNIT_ASSERT(env.Kqp->UpdateRequests >= 2);
}

Y_UNIT_TEST(NeedTableFalse_SkipsKqpSelect) {
    TEnv env(/*fcc=*/true, /*mapping=*/false);
    auto config = MakeSmConfig();
    AddPartition(config, 0, {}, {});
    env.SetupSm(std::move(config));
    env.RegisterTablet(0, new TTabletMock());

    env.StartSm("A_Source");
    auto result = env.WaitResult();
    UNIT_ASSERT(result);
    UNIT_ASSERT_VALUES_EQUAL(result->PartitionId, 0);
    UNIT_ASSERT_VALUES_EQUAL(env.Kqp->SelectRequests, 0);
    UNIT_ASSERT_VALUES_EQUAL(env.Kqp->UpdateRequests, 0);
}

Y_UNIT_TEST(PartitionInactiveAfterChoose) {
    TEnv env(/*fcc=*/true, /*mapping=*/false);
    auto config = MakeSmConfig();
    AddPartition(config, 0, {}, {});
    env.SetupSm(std::move(config));
    env.RegisterTablet(0, new TTabletMock(NKikimrPQ::ETopicPartitionStatus::Inactive));

    env.StartSm("A_Source");
    auto error = env.WaitError();
    UNIT_ASSERT(error);
    UNIT_ASSERT_EQUAL(error->Code, ErrorCode::INITIALIZING);
    UNIT_ASSERT(error->ErrorMessage.Contains("active"));
}

Y_UNIT_TEST(PreferedPartitionMissing) {
    TEnv env(/*fcc=*/true, /*mapping=*/false);
    auto config = MakeSmConfig();
    AddPartition(config, 0, {}, {});
    env.SetupSm(std::move(config));

    auto chooser = std::make_shared<TMapChooser>();
    chooser->Add(0, 1000);
    env.Chooser = chooser;

    env.StartSm("", /*prefered=*/5);
    auto error = env.WaitError();
    UNIT_ASSERT(error);
    UNIT_ASSERT_EQUAL(error->Code, ErrorCode::WRITE_ERROR_PARTITION_INACTIVE);
}

Y_UNIT_TEST(PreferedConflictsWithBoundSourceId) {
    TKqpSettings kqp;
    kqp.SelectPartition = 0;
    kqp.SelectSeqNo = 7;
    TEnv env(/*fcc=*/true, /*mapping=*/true, kqp);

    auto config = MakeSmConfig();
    AddPartition(config, 0, {}, "F");
    AddPartition(config, 1, "F", {});
    env.SetupSm(std::move(config));
    env.RegisterTablet(0, new TTabletMock());
    env.RegisterTablet(1, new TTabletMock());

    env.StartSm("A_Source", /*prefered=*/1);
    auto error = env.WaitError();
    UNIT_ASSERT(error);
    UNIT_ASSERT_EQUAL(error->Code, ErrorCode::BAD_REQUEST);
    UNIT_ASSERT(error->ErrorMessage.Contains("already bound"));
}

Y_UNIT_TEST(BadSourceId_ErrorCode) {
    TEnv env;
    auto config = MakeSmConfig();
    AddPartition(config, 0, {}, {});
    env.SetupSm(std::move(config));

    env.StartSm("base64:a***");
    auto error = env.WaitError();
    UNIT_ASSERT(error);
    UNIT_ASSERT_EQUAL(error->Code, ErrorCode::BAD_REQUEST);
}

Y_UNIT_TEST(PC02_ChooserMissesActiveChild) {
    TKqpSettings kqp;
    kqp.SelectPartition = 0;
    kqp.SelectSeqNo = 3;
    TEnv env(/*fcc=*/true, /*mapping=*/true, kqp);

    auto config = MakeSmConfig();
    // 0 is old inactive parent; leaf 1 is inactive in config → not in boundary chooser;
    // active partition 2 exists for boundary of new writes.
    AddPartition(config, 0, {}, "F", {1}, NKikimrPQ::ETopicPartitionStatus::Inactive);
    AddPartition(config, 1, {}, "F", {}, NKikimrPQ::ETopicPartitionStatus::Inactive);
    AddPartition(config, 2, "F", {});
    env.SetupSm(std::move(config));
    env.RegisterTablet(0, new TTabletMock(NKikimrPQ::ETopicPartitionStatus::Inactive));
    env.RegisterTablet(2, new TTabletMock());

    // SourceId that maps outside child-1 hierarchy boundary path: use AsIs "Y..." → partition 2.
    // Table says old writes were to partition 0; activeChildren={1}; chooser has no 1 → PC02.
    env.StartSm("Y_Source");
    auto error = env.WaitError();
    UNIT_ASSERT(error);
    UNIT_ASSERT_EQUAL(error->Code, ErrorCode::ERROR);
    UNIT_ASSERT(error->ErrorMessage.Contains("PC02"));
}

Y_UNIT_TEST(GetMaxSeqNo_BasicCheckFail) {
    TKqpSettings kqp;
    kqp.SelectPartition = 0;
    kqp.SelectSeqNo = 3;
    TEnv env(/*fcc=*/true, /*mapping=*/true, kqp);

    auto config = MakeSmConfig();
    AddPartition(config, 0, {}, {}, {1}, NKikimrPQ::ETopicPartitionStatus::Inactive);
    AddPartition(config, 1, {}, {});
    env.SetupSm(std::move(config));
    env.RegisterTablet(0, new TTabletMock(
        NKikimrPQ::ETopicPartitionStatus::Inactive,
        /*checkSeqNo=*/std::nullopt,
        /*maxSeqNo=*/std::nullopt,
        /*maxSeqNoPartitionActive=*/false,
        /*emptySourceIdInfo=*/false,
        /*noMaxSeqNoResult=*/false,
        NMsgBusProxy::MSTATUS_ERROR));
    env.RegisterTablet(1, new TTabletMock());

    env.StartSm("A_Source");
    auto error = env.WaitError();
    UNIT_ASSERT(error);
    UNIT_ASSERT_EQUAL(error->Code, ErrorCode::INITIALIZING);
}

Y_UNIT_TEST(GetMaxSeqNo_AbsentResult) {
    TKqpSettings kqp;
    kqp.SelectPartition = 0;
    kqp.SelectSeqNo = 3;
    TEnv env(/*fcc=*/true, /*mapping=*/true, kqp);

    auto config = MakeSmConfig();
    AddPartition(config, 0, {}, {}, {1}, NKikimrPQ::ETopicPartitionStatus::Inactive);
    AddPartition(config, 1, {}, {});
    env.SetupSm(std::move(config));
    env.RegisterTablet(0, new TTabletMock(
        NKikimrPQ::ETopicPartitionStatus::Inactive,
        std::nullopt, std::nullopt, false, false, /*noMaxSeqNoResult=*/true));
    env.RegisterTablet(1, new TTabletMock());

    env.StartSm("A_Source");
    auto error = env.WaitError();
    UNIT_ASSERT(error);
    UNIT_ASSERT_EQUAL(error->Code, ErrorCode::INITIALIZING);
    UNIT_ASSERT(error->ErrorMessage.Contains("Absent MaxSeqNo"));
}

Y_UNIT_TEST(GetMaxSeqNo_StaleLeader) {
    TKqpSettings kqp;
    kqp.SelectPartition = 0;
    kqp.SelectSeqNo = 3;
    TEnv env(/*fcc=*/true, /*mapping=*/true, kqp);

    auto config = MakeSmConfig();
    AddPartition(config, 0, {}, {}, {1}, NKikimrPQ::ETopicPartitionStatus::Inactive);
    AddPartition(config, 1, {}, {});
    env.SetupSm(std::move(config));
    env.RegisterTablet(0, new TTabletMock(
        NKikimrPQ::ETopicPartitionStatus::Inactive,
        std::nullopt, 10, /*maxSeqNoPartitionActive=*/true));
    env.RegisterTablet(1, new TTabletMock());

    env.StartSm("A_Source");
    auto error = env.WaitError();
    UNIT_ASSERT(error);
    UNIT_ASSERT_EQUAL(error->Code, ErrorCode::INITIALIZING);
    UNIT_ASSERT(error->ErrorMessage.Contains("stale leader"));
}

Y_UNIT_TEST(GetMaxSeqNo_EmptySourceIdInfo) {
    TKqpSettings kqp;
    kqp.SelectPartition = 0;
    kqp.SelectSeqNo = 3;
    TEnv env(/*fcc=*/true, /*mapping=*/true, kqp);

    auto config = MakeSmConfig();
    AddPartition(config, 0, {}, {}, {1}, NKikimrPQ::ETopicPartitionStatus::Inactive);
    AddPartition(config, 1, {}, {});
    env.SetupSm(std::move(config));
    env.RegisterTablet(0, new TTabletMock(
        NKikimrPQ::ETopicPartitionStatus::Inactive,
        std::nullopt, std::nullopt, false, /*emptySourceIdInfo=*/true));
    env.RegisterTablet(1, new TTabletMock());

    env.StartSm("A_Source");
    auto error = env.WaitError();
    UNIT_ASSERT(error);
    UNIT_ASSERT_EQUAL(error->Code, ErrorCode::INITIALIZING);
    UNIT_ASSERT(error->ErrorMessage.Contains("Empty source id info"));
}

Y_UNIT_TEST(OldActor_PqrRoundRobin) {
    TEnv env(/*fcc=*/false, /*mapping=*/false);
    auto config = MakeOldConfig();
    AddPartition(config, 0);
    AddPartition(config, 1);
    env.SetupOld(std::move(config));

    auto* balancer = new TTabletMock();
    balancer->PqrPartitionId = 1;
    env.RegisterBalancer(balancer);

    env.StartOld("");
    auto result = env.WaitResult();
    UNIT_ASSERT(result);
    UNIT_ASSERT_VALUES_EQUAL(result->PartitionId, 1);
}

Y_UNIT_TEST(OldActor_PqrPipeFail) {
    TEnv env(/*fcc=*/false, /*mapping=*/false);
    auto config = MakeOldConfig();
    AddPartition(config, 0);
    env.SetupOld(std::move(config));
    // Balancer tablet is not registered → pipe connect fails for TPipeMock ERROR path.
    // Use fail creator via direct old actor construction.
    auto actorId = env.Runtime.Register(new TPartitionChooserActor<TPipeFailCreator>(
        env.Edge, env.Config, env.Chooser, env.Converter, "", std::nullopt, {}));
    env.Runtime.EnableScheduleForActor(actorId);
    env.Runtime.DispatchEvents();

    auto error = env.WaitError();
    UNIT_ASSERT(error);
    UNIT_ASSERT_EQUAL(error->Code, ErrorCode::INITIALIZING);
    UNIT_ASSERT(error->ErrorMessage.Contains("Pipe"));
}

Y_UNIT_TEST(OldActor_PreferedConflict) {
    TKqpSettings kqp;
    kqp.SelectPartition = 0;
    kqp.SelectSeqNo = 5;
    TEnv env(/*fcc=*/true, /*mapping=*/true, kqp);

    auto config = MakeOldConfig();
    AddPartition(config, 0);
    AddPartition(config, 1);
    env.SetupOld(std::move(config));

    env.StartOld("A_Source", /*prefered=*/1);
    auto error = env.WaitError();
    UNIT_ASSERT(error);
    UNIT_ASSERT_EQUAL(error->Code, ErrorCode::BAD_REQUEST);
}

Y_UNIT_TEST(OldActor_PreferedMissing) {
    TEnv env(/*fcc=*/true, /*mapping=*/false);
    auto config = MakeOldConfig();
    AddPartition(config, 0);
    env.SetupOld(std::move(config));

    auto chooser = std::make_shared<TMapChooser>();
    chooser->Add(0, 1000);
    env.Chooser = chooser;

    env.StartOld("A_Source", /*prefered=*/7);
    auto error = env.WaitError();
    UNIT_ASSERT(error);
    UNIT_ASSERT_EQUAL(error->Code, ErrorCode::BAD_REQUEST);
}

Y_UNIT_TEST(RefreshRequest_UpdatesAccessTimeWithoutSecondResult) {
    TKqpSettings kqp;
    TEnv env(/*fcc=*/true, /*mapping=*/true, kqp);

    auto config = MakeSmConfig();
    AddPartition(config, 0, {}, {});
    env.SetupSm(std::move(config));
    env.RegisterTablet(0, new TTabletMock(NKikimrPQ::ETopicPartitionStatus::Active, 11));

    auto actorId = env.StartSm("A_Source");
    auto result = env.WaitResult();
    UNIT_ASSERT(result);

    const ui32 updatesAfterResult = env.Kqp->UpdateRequests;
    UNIT_ASSERT(updatesAfterResult >= 1);

    ui32 extraResults = 0;
    env.Runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
        if (ev->Recipient == env.Edge && ev->CastAsLocal<TEvPartitionChooser::TEvChooseResult>()) {
            ++extraResults;
        }
        return TTestActorRuntimeBase::EEventAction::PROCESS;
    });

    env.Runtime.Send(new IEventHandle(actorId, env.Edge, new TEvPartitionChooser::TEvRefreshRequest()));

    TDispatchOptions options;
    options.CustomFinalCondition = [&]() {
        return env.Kqp->UpdateRequests > updatesAfterResult;
    };
    env.Runtime.DispatchEvents(options, TDuration::Seconds(1));

    UNIT_ASSERT_C(env.Kqp->UpdateRequests > updatesAfterResult,
        "updatesAfterResult=" << updatesAfterResult << " now=" << env.Kqp->UpdateRequests);
    UNIT_ASSERT_VALUES_EQUAL(extraResults, 0);
}

Y_UNIT_TEST(ExceptionInOnSelected_RepliesError) {
    TEnv env(/*fcc=*/true, /*mapping=*/false);
    auto config = MakeSmConfig();
    AddPartition(config, 0, {}, {});
    env.Converter = MakeConverter(config);
    env.Chooser = CreatePartitionChooser(config, true);

    auto actorId = env.Runtime.Register(new TThrowInOnSelectedActor(env.Edge, env.Chooser, env.Converter));
    env.Runtime.EnableScheduleForActor(actorId);
    env.Runtime.DispatchEvents();

    auto error = env.WaitError();
    UNIT_ASSERT(error);
    UNIT_ASSERT_EQUAL(error->Code, ErrorCode::ERROR);
    UNIT_ASSERT(error->ErrorMessage.Contains("Unhandled exception"));
    UNIT_ASSERT(error->ErrorMessage.Contains("handler-boom"));
}

Y_UNIT_TEST(RegisteredSourceId_PersistsSeqNo) {
    TKqpSettings kqp;
    kqp.SelectPartition = 0;
    kqp.SelectSeqNo = 17;
    TEnv env(/*fcc=*/true, /*mapping=*/true, kqp);

    auto config = MakeSmConfig();
    AddPartition(config, 0, {}, {});
    env.SetupSm(std::move(config));
    env.RegisterTablet(0, new TTabletMock());

    env.StartSm("A_Source");
    auto result = env.WaitResult();
    UNIT_ASSERT(result);
    UNIT_ASSERT_VALUES_EQUAL(result->PartitionId, 0);
    UNIT_ASSERT(result->SeqNo);
    UNIT_ASSERT_VALUES_EQUAL(*result->SeqNo, 17);
}

} // Y_UNIT_TEST_SUITE(TPartitionChooserActorScenarios)
