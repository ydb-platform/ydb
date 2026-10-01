#include <ydb/core/base/metadata.h>
#include <ydb/core/base/path.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_proxy/mon.h>
#include <ydb/library/testlib/helpers.h>
#include <ydb/services/metadata/scheme_transaction/interface.h>

#include <library/cpp/testing/unittest/registar.h>

#include <stdexcept>

namespace NKikimr::NKqp {
namespace {

namespace TEvSchemeShard = NSchemeShard::TEvSchemeShard;
using NMetadata::ISchemeTransactionFactory;
using NMetadata::NProvider::MakeServiceId;
using NMetadata::NProvider::TEvTrackOperationCompletion;
using TNavigate = NSchemeCache::TSchemeCacheNavigate;
using EStatus = TNavigate::EStatus;
using TStatus = TEvTxUserProxy::TResultStatus;

struct TInvokeExceptionHandler final : TActorRunnableItem::TImpl<TInvokeExceptionHandler> {
    void DoRun(IActor* actor) noexcept {
        auto* handler = dynamic_cast<IActorExceptionHandler*>(actor);
        Y_ABORT_UNLESS(handler && handler->OnUnhandledException(std::runtime_error("injected failure")));
        delete this;
    }
};

// Keep the execution actor real and control its external services. In particular,
// recording pipe traffic lets us verify that no proposal is queued before connect
// and that a submitted proposal is never replayed onto a replacement pipe.
struct TFixture {
    static constexpr ui64 SchemeShard = 42;
    static constexpr ui64 TxId = 100;
    static constexpr ui64 Generation = 7;

    TTestBasicRuntime Runtime{2, false};
    TActorId Client;
    TActorId Cache;
    TActorId Proxy;
    TActorId Tablet;
    TActorId Metadata;
    TActorId RemoteMetadata;
    TActorId Actor;
    TVector<TActorId> Pipes;
    TVector<TActorId> UsedPipes;
    ui32 Proposals = 0;
    ui32 Subscriptions = 0;
    ui32 Trackers = 0;
    ui32 Unsubscriptions = 0;
    ui64 Cookie = 0;
    TTestActorRuntimeBase::TEventObserverHolder Observer;

    TFixture() {
        Runtime.Initialize(TAppPrepare().Unwrap());
        Client = Runtime.AllocateEdgeActor();
        Cache = Runtime.AllocateEdgeActor();
        Proxy = Runtime.AllocateEdgeActor();
        Tablet = Runtime.AllocateEdgeActor();
        Metadata = Runtime.AllocateEdgeActor();
        RemoteMetadata = Runtime.AllocateEdgeActor(1);
        Runtime.RegisterService(MakeSchemeCacheID(), Cache);
        Runtime.RegisterService(MakeTxProxyID(), Proxy);
        Runtime.RegisterService(MakeServiceId(Runtime.GetNodeId()), Metadata);
        Runtime.RegisterService(MakeServiceId(Runtime.GetNodeId(1)), RemoteMetadata, 1);
        Runtime.SetRegistrationObserverFunc([&](auto&, const TActorId& parent, const TActorId& child) {
            if (Actor && parent == Actor) {
                Pipes.push_back(child);
            }
        });
        Runtime.SetEventFilter([&](auto&, auto& ev) {
            if (ev->GetTypeRewrite() == TEvTrackOperationCompletion::EventType) {
                ++Trackers;
            }
            return false;
        });
        Observer = Runtime.AddObserver([&](auto& ev) {
            if (ev->Sender == Actor && ev->GetTypeRewrite() == TEvents::TEvUnsubscribe::EventType) {
                ++Unsubscriptions;
            }
            if (Find(Pipes, ev->Recipient) == Pipes.end()) {
                return;
            }
            if (ev->GetTypeRewrite() == TEvTabletPipe::EvSend) {
                UsedPipes.push_back(ev->Recipient);
                Proposals += ev->Type == TEvSchemeShard::TEvModifySchemeTransaction::EventType;
                Subscriptions += ev->Type == TEvSchemeShard::TEvNotifyTxCompletion::EventType;
                Runtime.Send(new IEventHandle(Tablet, ev->Sender, ev->ReleaseBase().Release(), 0, ev->Cookie));
            }
            // Suppress the actual pipe, including its bootstrap and shutdown.
            ev.Reset();
        });
    }

    THolder<TEvTxUserProxy::TEvProposeTransaction> Request(bool create = false, bool replace = false) {
        auto request = MakeHolder<TEvTxUserProxy::TEvProposeTransaction>();
        request->Record.SetDatabaseName("/Root");
        request->Record.SetPeerName("peer");
        request->Record.SetUserToken(NACLib::TUserToken("user", TVector<NACLib::TSID>{"group"}).SerializeAsString());
        auto& tx = *request->Record.MutableTransaction()->MutableModifyScheme();
        tx.SetWorkingDir("/Root");
        tx.SetOperationType(create ? NKikimrSchemeOp::ESchemeOpCreateStreamingQuery : NKikimrSchemeOp::ESchemeOpAlterStreamingQuery);
        tx.SetReplaceIfExists(replace);
        tx.MutableCreateStreamingQuery()->SetName("Query");
        ActorIdToProto(Client, tx.MutableCreateStreamingQuery()->MutableOperationOwnerActorId());
        return request;
    }

    void Start(THolder<TEvTxUserProxy::TEvProposeTransaction> request = {}, ui64 cookie = 0) {
        Cookie = cookie;
        if (!request) {
            request = Request();
        }
        TEvTxUserProxy::TEvProposeTransaction::TPtr event = reinterpret_cast<TEventHandle<TEvTxUserProxy::TEvProposeTransaction>*>(
            new IEventHandle(MakeServiceId(Runtime.GetNodeId()), Client, request.Release(), 0, Cookie));
        const auto operation = event->Get()->Record.GetTransaction().GetModifyScheme().GetOperationType();
        const THolder<ISchemeTransactionFactory> factory(ISchemeTransactionFactory::TFactory::Construct(
            operation == NKikimrSchemeOp::ESchemeOpAlterStreamingQuery ? operation : NKikimrSchemeOp::ESchemeOpCreateStreamingQuery));
        UNIT_ASSERT(factory);
        Actor = Runtime.Register(factory->CreateActor(std::move(event)));
        Runtime.EnableScheduleForActor(Actor);
    }

    auto Navigation() {
        auto ev = Runtime.GrabEdgeEvent<TEvTxProxySchemeCache::TEvNavigateKeySet>(Cache);
        UNIT_ASSERT_VALUES_EQUAL(ev->Sender, Actor);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Request->DatabaseName, "/Root");
        return ev;
    }

    void ReplyNavigation(TEvTxProxySchemeCache::TEvNavigateKeySet::TPtr ev) {
        Runtime.Send(new IEventHandle(Actor, Cache,
            new TEvTxProxySchemeCache::TEvNavigateKeySetResult(ev->Get()->Request.Release())));
    }

    static void Allow(TNavigate::TEntry& entry, ui32 access = NACLib::CreateTable | NACLib::AlterSchema) {
        entry.Status = EStatus::Ok;
        entry.DomainInfo = MakeIntrusive<NSchemeCache::TDomainInfo>(TPathId(SchemeShard, 1), TPathId(SchemeShard, 1));
        entry.SecurityObject = MakeIntrusive<TSecurityObject>("owner", "", true);
        entry.SecurityObject->AddAccess(NACLib::EAccessType::Allow, access, "group");
    }

    void ResolveAccess() {
        auto nav = Navigation();
        UNIT_ASSERT_VALUES_EQUAL(nav->Get()->Request->ResultSet.size(), 1);
        auto& entry = nav->Get()->Request->ResultSet.front();
        UNIT_ASSERT_VALUES_EQUAL(CanonizePath(entry.Path), "/Root/Query");
        UNIT_ASSERT(entry.SyncVersion);
        UNIT_ASSERT(entry.ShowPrivatePath);
        Allow(entry);
        ReplyNavigation(std::move(nav));
    }

    TActorId Pipe(size_t index = 0) {
        Runtime.WaitFor("pipe created", [&] { return Pipes.size() > index; }, TDuration::Seconds(5));
        return Pipes[index];
    }

    void Connect(ui32 node = 0, size_t index = 0, ui64 generation = Generation,
        NKikimrProto::EReplyStatus status = NKikimrProto::OK, bool leader = true) {
        Runtime.Send(new IEventHandle(Actor, Pipe(index), new TEvTabletPipe::TEvClientConnected(
            SchemeShard, status, Pipe(index), node ? RemoteMetadata : Tablet, leader, false, generation)));
    }

    void Allocate() {
        Runtime.GrabEdgeEvent<TEvTxUserProxy::TEvAllocateTxId>(Proxy);
        Runtime.Send(new IEventHandle(Actor, Proxy, new TEvTxUserProxy::TEvAllocateTxIdResult(TxId, {}, {})));
    }

    auto Proposal() {
        return Runtime.GrabEdgeEvent<TEvSchemeShard::TEvModifySchemeTransaction>(Tablet);
    }

    void Submit(ui64 cookie = 0) {
        Start({}, cookie);
        ResolveAccess();
        Connect();
        Allocate();
        Proposal();
    }

    void Accept(NKikimrScheme::EStatus status = NKikimrScheme::StatusAccepted) {
        auto response = MakeHolder<TEvSchemeShard::TEvModifySchemeTransactionResult>(status, TxId, SchemeShard);
        response->Record.SetPathId(10);
        Runtime.Send(new IEventHandle(Actor, Tablet, response.Release()));
        auto notify = Runtime.GrabEdgeEvent<TEvSchemeShard::TEvNotifyTxCompletion>(Tablet);
        UNIT_ASSERT_VALUES_EQUAL(notify->Get()->Record.GetTxId(), TxId);
        UNIT_ASSERT_VALUES_EQUAL(Trackers, 0);
    }

    void Complete() {
        Runtime.Send(new IEventHandle(Actor, Tablet, new TEvSchemeShard::TEvNotifyTxCompletionResult(TxId)));
    }

    void FillQuery(TNavigate::TEntry& entry, TActorId owner = {}) {
        Allow(entry);
        auto self = MakeIntrusive<TNavigate::TDirEntryInfo>();
        self->Info.SetSchemeshardId(SchemeShard);
        self->Info.SetPathId(10);
        self->Info.MutableVersion()->SetStreamingQueryVersion(3);
        entry.Self = self;
        auto query = MakeIntrusive<TNavigate::TStreamingQueryInfo>();
        ActorIdToProto(owner ? owner : Client, query->Description.MutableOperationOwnerActorId());
        (*query->Description.MutableProperties()->MutableProperties())["opaque"] = "value";
        entry.StreamingQueryInfo = query;
    }

    void ResolveQuery() {
        auto nav = Navigation();
        UNIT_ASSERT_VALUES_EQUAL(CanonizePath(nav->Get()->Request->ResultSet.front().Path), "/Root/Query");
        FillQuery(nav->Get()->Request->ResultSet.front());
        ReplyNavigation(std::move(nav));
    }

    void ResolveDatabase(const TString& path = "/Root") {
        auto nav = Navigation();
        auto& entry = nav->Get()->Request->ResultSet.front();
        UNIT_ASSERT(entry.RequestType == TNavigate::TEntry::ERequestType::ByTableId);
        UNIT_ASSERT_VALUES_EQUAL(entry.TableId.PathId, TPathId(SchemeShard, 1));
        UNIT_ASSERT(!entry.RedirectRequired);
        Allow(entry);
        entry.Path = SplitPath(path);
        ReplyNavigation(std::move(nav));
    }

    auto Response(TStatus::EStatus status, NKikimrScheme::EStatus schemeStatus) {
        auto ev = Runtime.GrabEdgeEvent<TEvTxUserProxy::TEvProposeTransactionStatus>(Client);
        UNIT_ASSERT_VALUES_EQUAL(ev->Cookie, Cookie);
        UNIT_ASSERT_VALUES_EQUAL_C(ev->Get()->Status(), status, ev->Get()->Record.DebugString());
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetSchemeShardStatus(), static_cast<ui32>(schemeStatus));
        UNIT_ASSERT(!Runtime.FindActor(Actor));
        return ev;
    }
};

} // anonymous namespace

Y_UNIT_TEST_SUITE(StreamingQuerySchemeTransaction) {
    Y_UNIT_TEST_TWIN(UnhandledExceptionRepliesAndClosesResources, Remote) {
        TFixture f;
        f.Start();
        f.ResolveAccess();
        f.Connect(Remote ? 1 : 0);
        if constexpr (Remote) {
            f.Runtime.GrabEdgeEvent<TEvTxUserProxy::TEvProposeTransaction>(f.RemoteMetadata);
        } else {
            f.Allocate();
            f.Proposal();
        }
        f.Runtime.Send(new IEventHandle(f.Actor, f.Client,
            new TEvents::TEvResumeRunnable(new TInvokeExceptionHandler()), TEvents::TEvResumeRunnable::EventFlags));
        const auto response = f.Response(TStatus::ExecError, NKikimrScheme::StatusSchemeError);
        UNIT_ASSERT_STRING_CONTAINS(response->Get()->Record.GetSchemeShardReason(), "Unhandled exception: injected failure");
        UNIT_ASSERT_VALUES_EQUAL(f.Trackers, 0);
        if constexpr (Remote) {
            f.Runtime.WaitFor("session unsubscribed", [&] { return f.Unsubscriptions == 1; }, TDuration::Seconds(5));
        }
    }

    Y_UNIT_TEST(RejectsRequestsThatDoNotStartOneStreamingOperation) {
        for (ui32 variant = 0; variant < 4; ++variant) {
            TFixture f;
            auto request = f.Request();
            auto* tx = request->Record.MutableTransaction()->MutableModifyScheme();
            if (variant == 0) {
                tx->SetOperationType(NKikimrSchemeOp::ESchemeOpMkDir);
            } else if (variant == 1) {
                tx->MutableCreateStreamingQuery()->ClearOperationOwnerActorId();
            } else if (variant == 2) {
                *request->Record.MutableTransaction()->AddTransactionalModification() = *tx;
            } else {
                tx->MutableCreateStreamingQuery()->MutableOperationOwnerActorId()->Clear();
            }
            f.Start(std::move(request));
            f.Response(TStatus::ExecError, NKikimrScheme::StatusInvalidParameter);
            UNIT_ASSERT(f.Pipes.empty());
            UNIT_ASSERT_VALUES_EQUAL(f.Trackers, 0);
        }
    }

    Y_UNIT_TEST(CreateResolvesDeepestExistingDirectoryAndPreservesRequest) {
        TFixture f;
        auto request = f.Request(true);
        request->Record.MutableTransaction()->MutableModifyScheme()->MutableCreateStreamingQuery()->SetName("Folder/Missing/Query");
        const auto token = request->Record.GetUserToken();
        f.Start(std::move(request));
        auto nav = f.Navigation();
        auto& entries = nav->Get()->Request->ResultSet;
        UNIT_ASSERT_VALUES_EQUAL(entries.size(), 3);
        for (auto& entry : entries) {
            UNIT_ASSERT(!entry.RedirectRequired);
            TFixture::Allow(entry);
        }
        entries.back().Status = EStatus::PathErrorUnknown;
        f.ReplyNavigation(std::move(nav));
        nav = f.Navigation();
        UNIT_ASSERT_VALUES_EQUAL(CanonizePath(nav->Get()->Request->ResultSet.front().Path), "/Root/Folder");
        TFixture::Allow(nav->Get()->Request->ResultSet.front(), NACLib::CreateTable);
        f.ReplyNavigation(std::move(nav));
        f.Connect();
        f.Allocate();
        const auto proposal = f.Proposal();
        const auto& record = proposal->Get()->Record;
        UNIT_ASSERT_VALUES_EQUAL(record.GetTxId(), TFixture::TxId);
        UNIT_ASSERT_VALUES_EQUAL(record.GetUserToken(), token);
        UNIT_ASSERT_VALUES_EQUAL(record.GetOwner(), "user");
        UNIT_ASSERT_VALUES_EQUAL(record.GetPeerName(), "peer");
        UNIT_ASSERT_VALUES_EQUAL(record.GetTransaction(0).GetWorkingDir(), "/Root/Folder");
        UNIT_ASSERT_VALUES_EQUAL(record.GetTransaction(0).GetCreateStreamingQuery().GetName(), "Missing/Query");
        UNIT_ASSERT_VALUES_EQUAL(ActorIdFromProto(record.GetTransaction(0).GetCreateStreamingQuery().GetOperationOwnerActorId()), f.Client);
    }

    Y_UNIT_TEST(WorkingDirectoryResolutionErrors) {
        for (const auto status : {EStatus::PathErrorUnknown, EStatus::LookupError, EStatus::RedirectLookupError}) {
            TFixture f;
            f.Start(f.Request(true));
            auto nav = f.Navigation();
            nav->Get()->Request->ResultSet.front().Status = status;
            f.ReplyNavigation(std::move(nav));
            if (status == EStatus::PathErrorUnknown) {
                f.Response(TStatus::ResolveError, NKikimrScheme::StatusPathDoesNotExist);
            } else {
                f.Response(TStatus::ProxyShardNotAvailable, NKikimrScheme::StatusNotAvailable);
            }
            UNIT_ASSERT(f.Pipes.empty());
        }
    }

    Y_UNIT_TEST(AccessResolutionErrors) {
        for (const auto status : {EStatus::AccessDenied, EStatus::PathErrorUnknown, EStatus::LookupError,
            EStatus::RedirectLookupError, EStatus::TableCreationNotComplete}) {
            TFixture f;
            f.Start();
            auto nav = f.Navigation();
            nav->Get()->Request->ResultSet.front().Status = status;
            f.ReplyNavigation(std::move(nav));
            if (status == EStatus::AccessDenied) {
                f.Response(TStatus::AccessDenied, NKikimrScheme::StatusAccessDenied);
            } else if (status == EStatus::PathErrorUnknown) {
                f.Response(TStatus::ResolveError, NKikimrScheme::StatusPathDoesNotExist);
            } else {
                f.Response(TStatus::ProxyShardNotAvailable, NKikimrScheme::StatusNotAvailable);
            }
            UNIT_ASSERT(f.Pipes.empty());
        }
    }

    Y_UNIT_TEST(IncompleteNavigationFailsBeforeSubmission) {
        for (ui32 variant = 0; variant < 3; ++variant) {
            TFixture f;
            f.Start();
            auto nav = f.Navigation();
            auto& entries = nav->Get()->Request->ResultSet;
            TFixture::Allow(entries.front());
            if (variant == 0) {
                entries.clear();
            } else if (variant == 1) {
                entries.emplace_back();
            } else {
                entries.front().DomainInfo.Reset();
            }
            f.ReplyNavigation(std::move(nav));
            f.Response(TStatus::ProxyShardNotAvailable, NKikimrScheme::StatusNotAvailable);
            UNIT_ASSERT(f.Pipes.empty());
        }
    }

    Y_UNIT_TEST_TWIN(SelectsDomainOrTenantSchemeShard, Redirect) {
        TFixture f;
        f.Start();
        auto nav = f.Navigation();
        auto& entry = nav->Get()->Request->ResultSet.front();
        TFixture::Allow(entry);
        entry.DomainInfo->Params.SetSchemeShard(TFixture::SchemeShard + 1);
        entry.RedirectRequired = Redirect;
        f.ReplyNavigation(std::move(nav));
        f.Connect();
        f.Allocate();
        const auto proposal = f.Proposal();
        UNIT_ASSERT_VALUES_EQUAL(proposal->Get()->Record.GetTabletId(), TFixture::SchemeShard + Redirect);
    }

    Y_UNIT_TEST_TWIN(PreservesSystemOwnerPolicy, SystemOwner) {
        TFixture f;
        f.Runtime.GetAppData().AlwaysSetSystemOwner = SystemOwner;
        f.Start();
        f.ResolveAccess();
        f.Connect();
        f.Allocate();
        const auto proposal = f.Proposal();
        UNIT_ASSERT_VALUES_EQUAL(proposal->Get()->Record.GetOwner(), SystemOwner ? BUILTIN_ACL_BASIC_OWNER : "user");
    }

    Y_UNIT_TEST(ChecksCreateAlterReplaceAndAttributePermissions) {
        // Replace requires both CREATE on the parent and ALTER on an existing target.
        for (ui32 variant = 0; variant < 6; ++variant) {
            TFixture f;
            const bool create = variant != 1 && variant != 2;
            const bool replace = variant >= 3;
            auto request = f.Request(create, replace);
            if (variant == 2) {
                request->Record.MutableTransaction()->MutableModifyScheme()->MutableAlterUserAttributes();
            }
            f.Start(std::move(request));
            if (create) {
                auto nav = f.Navigation();
                TFixture::Allow(nav->Get()->Request->ResultSet.front());
                f.ReplyNavigation(std::move(nav));
            }
            auto nav = f.Navigation();
            auto& entries = nav->Get()->Request->ResultSet;
            UNIT_ASSERT_VALUES_EQUAL(entries.size(), replace ? 2 : 1);
            for (auto& entry : entries) {
                TFixture::Allow(entry);
            }
            if (variant < 2) {
                entries.front().SecurityObject->ClearAccess();
            } else if (variant == 3) {
                entries.back().SecurityObject->ClearAccess();
            } else if (variant == 4) {
                entries.back().Status = EStatus::PathErrorUnknown;
            }
            f.ReplyNavigation(std::move(nav));
            if (variant < 4) {
                f.Response(TStatus::AccessDenied, NKikimrScheme::StatusAccessDenied);
                UNIT_ASSERT(f.Pipes.empty());
            } else {
                f.Pipe();
            }
            UNIT_ASSERT_VALUES_EQUAL(f.Trackers, 0);
        }
    }

    Y_UNIT_TEST_TWIN(ConnectionFailureDoesNotSubmit, Follower) {
        TFixture f;
        f.Start();
        f.ResolveAccess();
        f.Connect(0, 0, TFixture::Generation, Follower ? NKikimrProto::OK : NKikimrProto::ERROR, !Follower);
        f.Response(TStatus::ProxyShardNotAvailable, NKikimrScheme::StatusNotAvailable);
        UNIT_ASSERT_VALUES_EQUAL(f.Proposals, 0);
    }

    Y_UNIT_TEST(ReconnectBeforeSubmissionUsesOnlyNewConnectedPipe) {
        TFixture f;
        f.Start();
        f.ResolveAccess();
        f.Connect();
        f.Runtime.GrabEdgeEvent<TEvTxUserProxy::TEvAllocateTxId>(f.Proxy);
        const auto oldPipe = f.Pipe();
        f.Runtime.Send(new IEventHandle(f.Actor, oldPipe,
            new TEvTabletPipe::TEvClientDestroyed(TFixture::SchemeShard, oldPipe, f.Tablet)));
        const auto newPipe = f.Pipe(1);
        f.Runtime.Send(new IEventHandle(f.Actor, f.Proxy, new TEvTxUserProxy::TEvAllocateTxIdResult(TFixture::TxId, {}, {})));
        // Late notifications from the old pipe must not reconnect or fail the actor.
        f.Connect();
        f.Runtime.Send(new IEventHandle(f.Actor, oldPipe,
            new TEvTabletPipe::TEvClientDestroyed(TFixture::SchemeShard, oldPipe, f.Tablet)));
        f.Runtime.SimulateSleep(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL(f.Proposals, 0);
        f.Connect(0, 1, TFixture::Generation + 1);
        f.Proposal();
        f.Accept();
        f.Complete();
        f.ResolveQuery();
        f.ResolveDatabase();
        const auto tracker = f.Runtime.GrabEdgeEvent<TEvTrackOperationCompletion>(f.Metadata);
        UNIT_ASSERT_VALUES_EQUAL(tracker->Get()->GetRequestGeneration(), TFixture::Generation + 1);
        f.Response(TStatus::ExecComplete, NKikimrScheme::StatusSuccess);
        UNIT_ASSERT_VALUES_EQUAL(f.Proposals, 1);
        for (auto pipe : f.UsedPipes) {
            UNIT_ASSERT_VALUES_EQUAL(pipe, newPipe);
        }
    }

    Y_UNIT_TEST_TWIN(SubmittedTransactionIsNeverResubmittedAfterDisconnect, Accepted) {
        TFixture f;
        f.Submit();
        if constexpr (Accepted) {
            f.Accept();
        }
        f.Runtime.Send(new IEventHandle(f.Actor, f.Pipe(),
            new TEvTabletPipe::TEvClientDestroyed(TFixture::SchemeShard, f.Pipe(), f.Tablet)));
        f.Response(TStatus::ProxyShardNotAvailable, NKikimrScheme::StatusNotAvailable);
        UNIT_ASSERT_VALUES_EQUAL(f.Pipes.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(f.Proposals, 1);
        UNIT_ASSERT_VALUES_EQUAL(f.Trackers, 0);
    }

    Y_UNIT_TEST(ForwardsToSchemeShardNodeAndRelaysResponse) {
        TFixture f;
        auto request = f.Request();
        const auto original = request->Record.SerializeAsString();
        f.Start(std::move(request), 2);
        f.ResolveAccess();
        f.Connect(1);
        const auto forwarded = f.Runtime.GrabEdgeEvent<TEvTxUserProxy::TEvProposeTransaction>(f.RemoteMetadata);
        UNIT_ASSERT_VALUES_EQUAL(forwarded->Get()->Record.SerializeAsString(), original);
        UNIT_ASSERT_VALUES_EQUAL(forwarded->Cookie, 3);
        UNIT_ASSERT(forwarded->Flags & IEventHandle::FlagTrackDelivery);
        auto response = MakeHolder<TEvTxUserProxy::TEvProposeTransactionStatus>(TStatus::ExecComplete);
        response->Record.SetSchemeShardStatus(NKikimrScheme::StatusSuccess);
        response->Record.SetTxId(TFixture::TxId);
        const auto expected = response->Record.SerializeAsString();
        f.Runtime.Send(new IEventHandle(f.Actor, f.RemoteMetadata, response.Release()), 1);
        const auto actual = f.Response(TStatus::ExecComplete, NKikimrScheme::StatusSuccess);
        UNIT_ASSERT_VALUES_EQUAL(actual->Get()->Record.SerializeAsString(), expected);
        f.Runtime.WaitFor("session unsubscribed", [&] { return f.Unsubscriptions == 1; }, TDuration::Seconds(5));
        UNIT_ASSERT_VALUES_EQUAL(f.Proposals, 0);
        UNIT_ASSERT_VALUES_EQUAL(f.Trackers, 0);
    }

    Y_UNIT_TEST(RejectsExcessiveMetadataRedirects) {
        TFixture f;
        f.Start({}, 3);
        f.ResolveAccess();
        f.Connect(1);
        const auto response = f.Response(TStatus::ProxyShardNotAvailable, NKikimrScheme::StatusNotAvailable);
        UNIT_ASSERT_STRING_CONTAINS(response->Get()->Record.GetSchemeShardReason(), "Too many SchemeShard redirects");
        UNIT_ASSERT_VALUES_EQUAL(f.Proposals, 0);
        UNIT_ASSERT_VALUES_EQUAL(f.Trackers, 0);
        UNIT_ASSERT_VALUES_EQUAL(f.Unsubscriptions, 0);
    }

    Y_UNIT_TEST_TWIN(RemoteServiceFailureReturnsUnavailable, Disconnect) {
        TFixture f;
        f.Start();
        f.ResolveAccess();
        f.Connect(1);
        f.Runtime.GrabEdgeEvent<TEvTxUserProxy::TEvProposeTransaction>(f.RemoteMetadata);
        if constexpr (Disconnect) {
            f.Runtime.DisconnectNodes(0, 1);
        } else {
            f.Runtime.Send(new IEventHandle(f.Actor, f.RemoteMetadata,
                new TEvents::TEvUndelivered(TEvTxUserProxy::TEvProposeTransaction::EventType, TEvents::TEvUndelivered::ReasonActorUnknown)));
        }
        f.Response(TStatus::ProxyShardNotAvailable, NKikimrScheme::StatusNotAvailable);
        f.Runtime.WaitFor("session unsubscribed", [&] { return f.Unsubscriptions == 1; }, TDuration::Seconds(5));
        UNIT_ASSERT_VALUES_EQUAL(f.Proposals, 0);
        UNIT_ASSERT_VALUES_EQUAL(f.Trackers, 0);
    }

    Y_UNIT_TEST(RejectedProposalsPreserveStatusAndDoNotSubscribeOrTrack) {
        for (const auto status : {NKikimrScheme::StatusAlreadyExists, NKikimrScheme::StatusInvalidParameter,
            NKikimrScheme::StatusPreconditionFailed, NKikimrScheme::StatusMultipleModifications}) {
            TFixture f;
            f.Submit();
            auto response = MakeHolder<TEvSchemeShard::TEvModifySchemeTransactionResult>(status, TFixture::TxId, TFixture::SchemeShard, "reason");
            response->Record.SetPathId(10);
            response->Record.AddIssues()->set_message("issue");
            f.Runtime.Send(new IEventHandle(f.Actor, f.Tablet, response.Release()));
            const auto result = f.Response(status == NKikimrScheme::StatusAlreadyExists ? TStatus::ExecComplete : TStatus::ExecError, status);
            UNIT_ASSERT_VALUES_EQUAL(result->Get()->Record.GetTxId(), TFixture::TxId);
            UNIT_ASSERT_VALUES_EQUAL(result->Get()->Record.GetSchemeShardTabletId(), TFixture::SchemeShard);
            UNIT_ASSERT_VALUES_EQUAL(result->Get()->Record.GetPathId(), 10);
            UNIT_ASSERT_VALUES_EQUAL(result->Get()->Record.GetSchemeShardReason(), "reason");
            UNIT_ASSERT_VALUES_EQUAL(result->Get()->Record.GetIssues(0).message(), "issue");
            UNIT_ASSERT_VALUES_EQUAL(f.Subscriptions, 0);
            UNIT_ASSERT_VALUES_EQUAL(f.Trackers, 0);
        }
    }

    Y_UNIT_TEST_TWIN(TracksOnlyAfterCompletionAndPublishedDescription, ImmediateSuccess) {
        TFixture f;
        f.Submit(3); // Reaching the redirect limit still allows execution on the local SchemeShard.
        UNIT_ASSERT_VALUES_EQUAL(f.Subscriptions, 0);
        f.Accept(ImmediateSuccess ? NKikimrScheme::StatusSuccess : NKikimrScheme::StatusAccepted);
        f.Runtime.Send(new IEventHandle(f.Actor, f.Tablet, new TEvSchemeShard::TEvNotifyTxCompletionResult(TFixture::TxId + 1)));
        f.Runtime.SimulateSleep(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL(f.Trackers, 0);
        f.Complete();
        f.Complete(); // duplicate completion must not launch another navigation/tracker
        f.ResolveQuery();
        UNIT_ASSERT_VALUES_EQUAL(f.Trackers, 0);
        f.ResolveDatabase();
        const auto tracker = f.Runtime.GrabEdgeEvent<TEvTrackOperationCompletion>(f.Metadata);
        UNIT_ASSERT_VALUES_EQUAL(tracker->Sender.NodeId(), tracker->Recipient.NodeId());
        UNIT_ASSERT_VALUES_EQUAL(tracker->Get()->GetDatabase(), "/Root");
        UNIT_ASSERT_VALUES_EQUAL(tracker->Get()->GetDatabaseId(), "/Root");
        UNIT_ASSERT_VALUES_EQUAL(tracker->Get()->GetObjectId(), "Query");
        UNIT_ASSERT_VALUES_EQUAL(tracker->Get()->GetTypeId(), "STREAMING_QUERY");
        UNIT_ASSERT_VALUES_EQUAL(tracker->Get()->GetPathId(), TPathId(TFixture::SchemeShard, 10));
        UNIT_ASSERT_VALUES_EQUAL(tracker->Get()->GetObjectGeneration(), 3);
        UNIT_ASSERT_VALUES_EQUAL(tracker->Get()->GetRequestGeneration(), TFixture::Generation);
        UNIT_ASSERT_VALUES_EQUAL(tracker->Get()->GetOperationOwner(), f.Client);
        UNIT_ASSERT_VALUES_EQUAL(tracker->Get()->GetProperties().at("opaque"), "value");
        UNIT_ASSERT_VALUES_EQUAL(tracker->Get()->GetSchemeTxId(), 0);
        f.Response(TStatus::ExecComplete, NKikimrScheme::StatusSuccess);
        UNIT_ASSERT_VALUES_EQUAL(f.Trackers, 1);
        UNIT_ASSERT_VALUES_EQUAL(f.Subscriptions, 1);
        for (auto pipe : f.UsedPipes) {
            UNIT_ASSERT_VALUES_EQUAL(pipe, f.Pipe());
        }
    }

    Y_UNIT_TEST_TWIN(RetriesTransientPublicationLookups, Database) {
        TFixture f;
        f.Submit();
        f.Accept();
        f.Complete();
        if constexpr (Database) {
            f.ResolveQuery();
        }
        for (const auto status : {EStatus::LookupError, EStatus::RedirectLookupError, EStatus::TableCreationNotComplete}) {
            auto nav = f.Navigation();
            nav->Get()->Request->ResultSet.front().Status = status;
            f.ReplyNavigation(std::move(nav));
            f.Runtime.SimulateSleep(TDuration::Seconds(1));
            UNIT_ASSERT_VALUES_EQUAL(f.Trackers, 0);
        }
        if constexpr (!Database) {
            f.ResolveQuery();
        }
        f.ResolveDatabase();
        f.Runtime.GrabEdgeEvent<TEvTrackOperationCompletion>(f.Metadata);
        f.Response(TStatus::ExecComplete, NKikimrScheme::StatusSuccess);
        UNIT_ASSERT_VALUES_EQUAL(f.Proposals, 1);
        UNIT_ASSERT_VALUES_EQUAL(f.Trackers, 1);
    }

    Y_UNIT_TEST(DoesNotTrackDroppedOrSupersededObject) {
        for (ui32 variant = 0; variant < 5; ++variant) {
            TFixture f;
            f.Submit();
            f.Accept();
            f.Complete();
            auto nav = f.Navigation();
            auto& entry = nav->Get()->Request->ResultSet.front();
            f.FillQuery(entry, variant == 0 ? f.Tablet : f.Client);
            switch (variant) {
            case 1: entry.Status = EStatus::PathErrorUnknown; break;
            case 2: entry.Status = EStatus::RootUnknown; break;
            case 3: {
                auto self = MakeIntrusive<TNavigate::TDirEntryInfo>(*entry.Self);
                self->Info.SetPathId(11);
                entry.Self = self;
                entry.StreamingQueryInfo.Reset();
                break;
            }
            case 4: {
                auto query = MakeIntrusive<TNavigate::TStreamingQueryInfo>(*entry.StreamingQueryInfo);
                query->Description.ClearOperationOwnerActorId();
                entry.StreamingQueryInfo = query;
                break;
            }
            }
            f.ReplyNavigation(std::move(nav));
            f.Response(TStatus::ExecComplete, NKikimrScheme::StatusSuccess);
            UNIT_ASSERT_VALUES_EQUAL(f.Trackers, 0);
        }
    }

    Y_UNIT_TEST_TWIN(PublicationAccessDeniedReturnsError, Database) {
        TFixture f;
        f.Submit();
        f.Accept();
        f.Complete();
        if constexpr (Database) {
            f.ResolveQuery();
        }
        auto nav = f.Navigation();
        nav->Get()->Request->ResultSet.front().Status = EStatus::AccessDenied;
        f.ReplyNavigation(std::move(nav));
        f.Response(TStatus::AccessDenied, NKikimrScheme::StatusAccessDenied);
        UNIT_ASSERT_VALUES_EQUAL(f.Trackers, 0);
    }

    Y_UNIT_TEST(DoesNotTrackDroppedDatabase) {
        TFixture f;
        f.Submit();
        f.Accept();
        f.Complete();
        f.ResolveQuery();
        auto nav = f.Navigation();
        nav->Get()->Request->ResultSet.front().Status = EStatus::PathErrorUnknown;
        f.ReplyNavigation(std::move(nav));
        f.Response(TStatus::ExecComplete, NKikimrScheme::StatusSuccess);
        UNIT_ASSERT_VALUES_EQUAL(f.Trackers, 0);
    }
}

} // namespace NKikimr::NKqp
