#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/services/metadata/ds_table/accessor_snapshot_base.h>
#include <ydb/services/metadata/ds_table/service.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NMetadata::NProvider {

class TTestBehaviour : public IClassBehaviour {
public:
    TString GetInternalStorageTablePath() const override { return "my_table"; }
    TString GetTypeId() const override { return "test_type"; }
    std::shared_ptr<NInitializer::IInitializationBehaviour> ConstructInitializer() const override { return nullptr; }
    std::shared_ptr<NModifications::IOperationsManager> GetOperationsManager() const override { return nullptr; }
};

class TTestSnapshot : public NFetcher::ISnapshot {
protected:
    bool DoDeserializeFromResultSet(const Ydb::Table::ExecuteQueryResult&) override {
        // Accept any data, parse nothing
        return true; 
    }
    TString DoSerializeToString() const override {
        return "test_snapshot";
    }
public:
    using NFetcher::ISnapshot::ISnapshot;
};

class TMockFetcher : public NFetcher::ISnapshotsFetcher {
    std::vector<IClassBehaviour::TPtr> Managers;
public:
    TMockFetcher(std::vector<IClassBehaviour::TPtr> managers) 
        : Managers(std::move(managers)) 
    {}

protected:
    std::vector<IClassBehaviour::TPtr> DoGetManagers() const override {
        return Managers;
    }

    NFetcher::ISnapshot::TPtr CreateSnapshot(const TInstant actuality) const override {
        return std::make_shared<TTestSnapshot>(actuality); 
    }

public:
};

class TTestAccessor : public TDSAccessorBase {
    ISnapshotConstructorController::TPtr OutputController;
public:
    TTestAccessor(const NRequest::TConfig& cfg,
                  ISnapshotConstructorController::TPtr out,
                  NFetcher::ISnapshotsFetcher::TPtr fetcher)
        : TDSAccessorBase(cfg, fetcher), OutputController(out) {}

    void OnBootstrap() override {
        Become(&TDSAccessorBase::StateMain);
        // Populates ManagersByPath and ExistenceChecks
        StartSnapshotsFetching();
    }
    void OnNewEnrichedSnapshot(NFetcher::ISnapshot::TPtr s) override {
        OutputController->OnSnapshotConstructionResult(s);
        PassAway();
    }
    void OnConstructSnapshotError(const TString& msg) override {
        TDSAccessorBase::OnConstructSnapshotError(msg);
        PassAway();
    }

    using TDSAccessorBase::IsPathPinned;
};

void SetServicePath(const TString& path) {
    NKikimrConfig::TMetadataProviderConfig proto1;
    proto1.SetPath(path);
    NProvider::TConfig config1;
    config1.DeserializeFromProto(proto1);

    // Change path in a TServiceOperator via TService
    NProvider::TService service1(config1);
}

std::unique_ptr<NRequest::TEvRequestResult<NRequest::TDialogYQLRequest>> 
CreateYQLResponse(ui32 resultSetsCount = 1) {
    typename NRequest::TDialogYQLRequest::TResponse response;
    
    Ydb::Table::ExecuteQueryResult qResult;
    for (ui32 i = 0; i < resultSetsCount; ++i) {
        qResult.add_result_sets();
    }

    auto* op = response.mutable_operation();
    op->set_ready(true);
    op->set_status(Ydb::StatusIds::SUCCESS);
    op->mutable_result()->PackFrom(qResult);
    
    return std::make_unique<NRequest::TEvRequestResult<NRequest::TDialogYQLRequest>>(std::move(response));
}

template <typename TExtractor>
auto RunInContext(TTestBasicRuntime& runtime, TExtractor&& extractor) {
    return runtime.RunCall([&]() {
        return extractor();
    });
}

Y_UNIT_TEST_SUITE(TDSAccessorPathDrift) {
    ///
    /// The test simulates a race condition where the IBehavior table path
    /// prefix changes. Mutation can happen via GetStorageTablePath while
    /// a YQL query is already in flight. It ensures that
    /// TDSAccessorBase "pins" the path used for the YQL request and
    /// uses this pinned path for subsequent result mapping, preventing
    /// lookup failures in CurrentExistence maps and ABORTs.
    ///
    Y_UNIT_TEST(TestPathConsistencyDuringMutation) {
        TTestBasicRuntime runtime(1, false);
        runtime.Initialize(TAppPrepare().Unwrap());
        runtime.GetAppData().TenantName = "test_db";
        runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvTxProxySchemeCache::TEvNavigateKeySet::EventType) {
                Cerr <<"Sender:"<< ev->Sender << Endl;
                Cerr <<"Recipient:"<< ev->Recipient << Endl;

                auto nav = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
                auto& entry = nav->ResultSet.emplace_back();
                entry.Status = NSchemeCache::TSchemeCacheNavigate::EStatus::Ok;
                entry.Kind   = NSchemeCache::TSchemeCacheNavigate::EKind::KindTable;
                runtime.Send(new IEventHandle(
                    ev->Sender, ev->Recipient,
                    new TEvTxProxySchemeCache::TEvNavigateKeySetResult(nav.Release())));

                return TTestActorRuntimeBase::EEventAction::DROP;
            }

            return TTestActorRuntimeBase::EEventAction::PROCESS;
        });

        // Set init path
        SetServicePath("v1_path");
        
        auto manager = std::make_shared<TTestBehaviour>();
        auto fetcher = std::make_shared<TMockFetcher>(std::vector<IClassBehaviour::TPtr>{manager});

        // AppData exists only in a runtime context
        const TString path1 = RunInContext(runtime, [&] { 
            return manager->GetStorageTablePath(); 
        });

        UNIT_ASSERT_EQUAL(path1, "/test_db/v1_path/my_table");

        // Messages sent to a non-existent service (e.g., MakeSchemeCacheID()) 
        // are dropped by the runtime and won't reach the ObserverFunc.
        // We must register the service ID to ensure TEvTxProxySchemeCache events 
        // are intercepted correctly. Jut use dummy actor.
        TActorId s = runtime.AllocateEdgeActor();
        runtime.RegisterService(MakeSchemeCacheID(), s);

        TActorId sender = runtime.AllocateEdgeActor();
        auto outputController = std::make_shared<TDSAccessorSimple::TEvController>(TActorIdentity(sender));

        NRequest::TConfig regConfig;
        auto actor = new TTestAccessor(regConfig, outputController, fetcher);
        TActorId accessorId = runtime.Register(actor);

        // Wait until the manager's storage path is pinned and sent to YQL
        TDispatchOptions options2;
        options2.FinalEvents.emplace_back([](IEventHandle&) { return false; });
        options2.CustomFinalCondition = [&]() -> bool {
            return actor->IsPathPinned("/test_db/v1_path/my_table");
        };
        runtime.DispatchEvents(options2);

        // Mutate path
        SetServicePath("v2_new_path");
        const TString path2 = RunInContext(runtime, [&] { 
            return manager->GetStorageTablePath(); 
        });

        UNIT_ASSERT_EQUAL(path2, "/test_db/v2_new_path/my_table");

        // As YQL is missing in the runtime and TDSAccessorBase does not receive answer
        // make it manually. Check that TDSAccessorBase works after path mutation.
        runtime.Send(new IEventHandle(accessorId, sender, CreateYQLResponse(1).release()));

        auto res = runtime.GrabEdgeEvent<TDSAccessorSimple::TEvController::TEvResult>(sender);
        UNIT_ASSERT(res);
        UNIT_ASSERT(res->Get()->GetResult());
    }
}

}
