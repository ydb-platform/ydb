#include <ydb/core/cms/console/configs_dispatcher.h>
#include <ydb/core/testlib/cs_helper.h>
#include <ydb/core/tx/tiering/external_data.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/wrappers/ut_helpers/s3_mock.h>
#include <ydb/core/wrappers/s3_wrapper.h>
#include <ydb/core/wrappers/fake_storage.h>
#include <ydb/library/accessor/accessor.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
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

using namespace NColumnShard;

Y_UNIT_TEST_SUITE(Initializer) {

    class TTestInitializer: public NMetadata::NInitializer::IInitializationBehaviour {
    protected:
        virtual void DoPrepare(NMetadata::NInitializer::IInitializerInput::TPtr controller) const override {
            TVector<NMetadata::NInitializer::ITableModifier::TPtr> result;
            const TString tablePath = "/Root/.metadata/test";
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
    public:
    };

    class TInitBehaviourTest: public NMetadata::IClassBehaviour {
    protected:
        virtual TString GetInternalStorageTablePath() const override {
            return "test";
        }
        virtual std::shared_ptr<NMetadata::NInitializer::IInitializationBehaviour> ConstructInitializer() const override {
            return std::make_shared<TTestInitializer>();
        }
        virtual std::shared_ptr<NMetadata::NModifications::IOperationsManager> GetOperationsManager() const override {
            return nullptr;
        }
    public:
        virtual TString GetTypeId() const override {
            return TypeName<TInitBehaviourTest>();
        }

        static IClassBehaviour::TPtr GetInstance() {
            static std::shared_ptr<TInitBehaviourTest> result = std::make_shared<TInitBehaviourTest>();
            return result;
        }
    };

    class TInitUserEmulator: public NActors::TActorBootstrapped<TInitUserEmulator> {
    private:
        using TBase = NActors::TActorBootstrapped<TInitUserEmulator>;
        YDB_READONLY_FLAG(Initialized, false);
    public:

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
            Become(&TThis::StateWork);
            Sender<NMetadata::NProvider::TEvPrepareManager>(TInitBehaviourTest::GetInstance()).
                SendTo(NMetadata::NProvider::MakeServiceId(SelfId().NodeId()));
        }
    };

    Y_UNIT_TEST(Simple) {
        TPortManager pm;

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
        ;

        Tests::TServer::TPtr server = new Tests::TServer(serverSettings);
        server->EnableGRpc(grpcPort);

        Tests::TClient client(serverSettings);

        auto& runtime = *server->GetRuntime();

        auto sender = runtime.AllocateEdgeActor();
        server->SetupRootStoragePools(sender);

        Tests::NCS::THelper lHelper(*server);
        lHelper.StartDataRequest("SELECT * FROM `/Root/.metadata/test`", false);

        TInitUserEmulator* emulator = new TInitUserEmulator;
        runtime.Register(emulator);

        const TInstant start = Now();
        while (Now() - start < TDuration::Seconds(35) && !emulator->IsInitialized()) {
            runtime.SimulateSleep(TDuration::Seconds(1));
        }
        UNIT_ASSERT(emulator->IsInitialized());
        Cerr << "Initialization finished" << Endl;

        lHelper.StartDataRequest("SELECT * FROM `/Root/.metadata/test`");
        lHelper.StartSchemaRequest("DROP TABLE `/Root/.metadata/test`", false);
        lHelper.StartDataRequest("SELECT * FROM `/Root/.metadata/initialization/migrations`");
        lHelper.StartSchemaRequest("DELETE FROM `/Root/.metadata/initialization/migrations`", false);
        lHelper.StartSchemaRequest("DROP TABLE `/Root/.metadata/initialization/migrations`", false);
    }
}
}
