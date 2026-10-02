#include <ydb/core/cms/console/configs_dispatcher.h>
#include <ydb/core/testlib/cs_helper.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/wrappers/ut_helpers/s3_mock.h>
#include <ydb/core/wrappers/s3_wrapper.h>
#include <ydb/core/wrappers/fake_storage.h>
#include <ydb/library/accessor/accessor.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/services/metadata/abstract/common.h>
#include <ydb/services/metadata/initializer/initializer.h>
#include <ydb/services/metadata/manager/alter.h>
#include <ydb/services/metadata/manager/common.h>
#include <ydb/services/metadata/manager/table_record.h>
#include <ydb/services/metadata/manager/ydb_value_operator.h>
#include <ydb/services/metadata/secret/manager.h>
#include <ydb/services/metadata/secret/fetcher.h>
#include <ydb/services/metadata/secret/snapshot.h>
#include <ydb/services/metadata/service.h>

#include <ydb/library/actors/core/av_bootstrapped.h>
#include <library/cpp/protobuf/json/proto2json.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/system/hostname.h>
#include <util/system/type_name.h>

namespace NKikimr {

namespace {

class TTestInitializer : public NMetadata::NInitializer::IInitializationBehaviour {
public:
    bool IsObjectLeaked() const {
        return !LifetimeTracker.expired();
    }

protected:
    void DoPrepare(NMetadata::NInitializer::IInitializerInput::TPtr controller) const override {
        LifetimeTracker = controller;

        TVector<NMetadata::NInitializer::ITableModifier::TPtr> result;
        constexpr char tablePath[] = "/Root/.metadata/test";
        {
            Ydb::Table::CreateTableRequest request;
            request.set_session_id("");
            request.set_path(tablePath);
            request.add_primary_key("test");
            {
                auto& column = *request.add_columns();
                column.set_name("test");
                column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::STRING);
            }
            result.emplace_back(new NMetadata::NInitializer::TGenericTableModifier<NMetadata::NRequest::TDialogCreateTable>(request, "create"));
        }
        result.emplace_back(NMetadata::NInitializer::TACLModifierConstructor::GetReadOnlyModifier(tablePath, "acl"));
        controller->OnPreparationFinished(result);
    }

private:
    mutable std::weak_ptr<NMetadata::NInitializer::IInitializerInput> LifetimeTracker;
};

/**
 * A mock implementation of ITableModifier for testing asynchronous error handling.
 * 
 * Main goals:
 * 1. Test Retry Path: Specifically triggers the 'OnModificationFailed' branch 
 *    in TDSAccessorInitialized to verify the correctness of the 1-second 
 *    delayed retry (ScheduleInvokeActivity).
 * 
 * 2. Self-Correction: Successfully completes on the second attempt (after failure) 
 *    to ensure the initialization chain can finish and drop all references.
 * 
 * 3. Leak Verification: Used by the test to check if capturing shared_ptr in 
 *    retry lambdas leads to memory leaks after the final execution.
 */
class TMockModifier : public NMetadata::NInitializer::ITableModifier {
public:
    explicit TMockModifier(const TString& id)
        : ITableModifier(id)
    {}

protected:
    bool DoExecute(NMetadata::NInitializer::IModifierExternalController::TPtr controller, const NMetadata::NRequest::TConfig&) const override {
        if (!FailedOnce) {
            FailedOnce = true;
            controller->OnModificationFailed(Ydb::StatusIds::BAD_REQUEST, "Simulated Modification Error", GetModificationId());
        } else {
            controller->OnModificationFinished(GetModificationId());
        }
        return true;
    }

private:
    mutable bool FailedOnce = false;
};

/**
 * A mock implementation of IInitializationBehaviour designed to check TDSAccessorInitialized.
 * 
 * Main goals:
 * 1. Test failure resilience: Simulates failures during different initialization phases 
 *    (Preparation, Modification) to trigger asynchronous retry paths 
 *    (e.g., ScheduleInvokeActivity).
 * 
 * 2. Lifecycle tracking: Captures a weak_ptr to the TDSAccessorInitialized instance. 
 *    This allows verifying that the object is properly destroyed after the 
 *    initialization chain completes, ensuring no circular dependencies (leaks) 
 *    exist due to SelfPtr or lambda captures.
 */
class TMockInitializer : public NMetadata::NInitializer::IInitializationBehaviour {
public:
    bool IsObjectLeaked() const {
        return !LifetimeTracker.expired();
    }

protected:
    virtual void DoPrepare(NMetadata::NInitializer::IInitializerInput::TPtr controller) const override {
        LifetimeTracker = controller;
        
        if (!FailedOnce) {
            FailedOnce = true;
            controller->OnPreparationProblem("Simulated preparation failure");
        } else {
            TVector<NMetadata::NInitializer::ITableModifier::TPtr> result;

            result.push_back(std::make_shared<TMockModifier>("test_mod_1"));
            result.push_back(std::make_shared<TMockModifier>("test_mod_2"));

            controller->OnPreparationFinished(result);
        }
    }

private:
    mutable std::weak_ptr<NMetadata::NInitializer::IInitializerInput> LifetimeTracker;
    mutable bool FailedOnce = false;
};

template<typename TInitializer>
class TInitBehaviourTest : public NMetadata::IClassBehaviour {
public:
    explicit TInitBehaviourTest(std::shared_ptr<TInitializer> init)
        : Initializer(std::move(init))
    {}

protected:
    TString GetInternalStorageTablePath() const override {
        return "test";
    }
    
    std::shared_ptr<NMetadata::NInitializer::IInitializationBehaviour> ConstructInitializer() const override {
        return Initializer;
    }
    
    std::shared_ptr<NMetadata::NModifications::IOperationsManager> GetOperationsManager() const override {
        return nullptr;
    }

public:
    TString GetTypeId() const override {
        return TypeName<TInitBehaviourTest<TInitializer>>();
    }

private:
    std::shared_ptr<TInitializer> Initializer;
};

class TInitUserEmulator : public NActors::TActorBootstrapped<TInitUserEmulator> {
private:
    using TBase = NActors::TActorBootstrapped<TInitUserEmulator>;
    using TThis = TInitUserEmulator;

    YDB_READONLY_FLAG(Initialized, false);
    NMetadata::IClassBehaviour::TPtr ClassBehavior; 

public:
    explicit TInitUserEmulator(NMetadata::IClassBehaviour::TPtr cb)
        : ClassBehavior(std::move(cb))
    {}

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NMetadata::NProvider::TEvManagerPrepared, Handle);
            default:
                Y_ABORT_UNLESS(false);
        }
    }

    void Handle(NMetadata::NProvider::TEvManagerPrepared::TPtr& /*ev*/) {
        InitializedFlag = true;
    }

    void Bootstrap() {
        TBase::Become(&TThis::StateWork);
        this->template Sender<NMetadata::NProvider::TEvPrepareManager>(ClassBehavior)
            .SendTo(NMetadata::NProvider::MakeServiceId(this->SelfId().NodeId()));
    }
};

class TMetaServiceFixture : public NUnitTest::TBaseFixture {
public:
    TMetaServiceFixture()
        : Server(GetServer(PortManager))
        , Runtime(*Server->GetRuntime())
        , Helper(*Server)
    {}

    static Tests::TServer::TPtr GetServer(TPortManager& pm) {
        ui32 grpcPort = pm.GetPort();
        ui32 msgbPort = pm.GetPort();

        NKikimrProto::TAuthConfig authConfig;
        authConfig.SetUseBuiltinDomain(true);
        Tests::TServerSettings serverSettings(msgbPort, authConfig);
        serverSettings.Port = msgbPort;
        serverSettings.GrpcPort = grpcPort;
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetEnableMetadataProvider(true)
            .SetEnableOlapSchemaOperations(true);

        auto server = MakeIntrusive<Tests::TServer>(serverSettings);
        server->EnableGRpc(grpcPort);
        
        auto sender = server->GetRuntime()->AllocateEdgeActor();
        server->SetupRootStoragePools(sender);
        return server;
    }

protected:
    // Should be first to hold ports
    TPortManager PortManager;
    Tests::TServer::TPtr Server;
    TTestActorRuntime& Runtime;
    Tests::NCS::THelper Helper;
};

} // anonymous namespace

Y_UNIT_TEST_SUITE(Initializer) {
    Y_UNIT_TEST_F(TestRunWithoutErrors, TMetaServiceFixture) {
        Helper.StartDataRequest("SELECT * FROM `/Root/.metadata/test`", false);
        
        auto tracker = std::make_shared<TTestInitializer>();
        auto classBehavior = std::make_shared<TInitBehaviourTest<TTestInitializer>>(tracker);

        auto emulator = new TInitUserEmulator(classBehavior);
        Runtime.Register(emulator);

        TDispatchOptions options;
        options.CustomFinalCondition = [&] { return emulator->IsInitialized(); };
        Runtime.DispatchEvents(options);

        UNIT_ASSERT(emulator->IsInitialized());
        Cerr << "Initialization finished" << Endl;

        Helper.StartDataRequest("SELECT * FROM `/Root/.metadata/test`");
        Helper.StartSchemaRequest("DROP TABLE `/Root/.metadata/test`", false);
        Helper.StartDataRequest("SELECT * FROM `/Root/.metadata/initialization/migrations`");
        Helper.StartSchemaRequest("DELETE FROM `/Root/.metadata/initialization/migrations`", false);
        Helper.StartSchemaRequest("DROP TABLE `/Root/.metadata/initialization/migrations`", false);

        UNIT_ASSERT_C(!tracker->IsObjectLeaked(), 
            "Memory Leak detected: TDSAccessorInitialized instance is still alive!");
    }

    Y_UNIT_TEST_F(TestRunWithErrors, TMetaServiceFixture) {
        auto tracker = std::make_shared<TMockInitializer>();
        auto classBehavior = std::make_shared<TInitBehaviourTest<TMockInitializer>>(tracker);
        auto emulator = new TInitUserEmulator(classBehavior); 

        Runtime.Register(emulator);
        
        TDispatchOptions options;
        options.CustomFinalCondition = [&] { return emulator->IsInitialized(); };
        Runtime.DispatchEvents(options);

        UNIT_ASSERT(emulator->IsInitialized());
        Cerr << "Initialization finished" << Endl;

        UNIT_ASSERT_C(!tracker->IsObjectLeaked(), 
            "Memory Leak detected: TDSAccessorInitialized instance is still alive!");
    }
}

} // NKikimr
