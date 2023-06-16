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
#include <ydb/services/metadata/manager/alter.h>
#include <ydb/services/metadata/manager/common.h>
#include <ydb/services/metadata/manager/table_record.h>
#include <ydb/services/metadata/manager/ydb_value_operator.h>
#include <ydb/services/metadata/secret/manager.h>
#include <ydb/services/metadata/secret/fetcher.h>
#include <ydb/services/metadata/secret/snapshot.h>
#include <ydb/services/metadata/service.h>

#include <library/cpp/actors/core/av_bootstrapped.h>
#include <library/cpp/protobuf/json/proto2json.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/system/hostname.h>

namespace NKikimr {

using namespace NColumnShard;

Y_UNIT_TEST_SUITE(Secret) {

    class TJsonChecker {
    private:
        YDB_ACCESSOR_DEF(TString, Path);
        YDB_ACCESSOR_DEF(TString, Expectation);
    public:
        TJsonChecker(const TString& path, const TString& expectation)
            : Path(path)
            , Expectation(expectation) {

        }
        bool Check(const NJson::TJsonValue& jsonInfo) const {
            auto* jsonPathValue = jsonInfo.GetValueByPath(Path);
            if (!jsonPathValue) {
                return Expectation == "__NULL";
            }
            return jsonPathValue->GetStringRobust() == Expectation;
        }

        TString GetDebugString() const {
            TStringBuilder sb;
            sb << "path=" << Path << ";"
                << "expectation=" << Expectation << ";";
            return sb;
        }
    };

    class TSecretUserEmulator: public NActors::TActorBootstrapped<TSecretUserEmulator> {
    private:
        using TBase = NActors::TActorBootstrapped<TSecretUserEmulator>;
        std::shared_ptr<NMetadata::NSecret::TSnapshotsFetcher> Manager = std::make_shared<NMetadata::NSecret::TSnapshotsFetcher>();
        YDB_READONLY_FLAG(Found, false);
        YDB_READONLY(TInstant, Start, Now());
        YDB_ACCESSOR(ui32, ExpectedSecretsCount, 1);
        YDB_ACCESSOR(ui32, ExpectedAccessCount, 1);
        using TKeyCheckers = TMap<NMetadata::NSecret::TSecretId, TJsonChecker>;
        YDB_ACCESSOR_DEF(TKeyCheckers, Checkers);
    public:

        void ResetConditions() {
            FoundFlag = false;
            Checkers.clear();
        }

        STATEFN(StateInit) {
            switch (ev->GetTypeRewrite()) {
                hFunc(NMetadata::NProvider::TEvRefreshSubscriberData, Handle);
                default:
                    Y_VERIFY(false);
            }
        }

        void CheckRuntime(TTestActorRuntime& runtime) {
            const auto pred = [this](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event)->TTestActorRuntimeBase::EEventAction {
                if (event->HasBuffer() && !event->HasEvent()) {
                } else if (!event->HasEvent()) {
                } else {
                    auto ptr = event->CastAsLocal<NMetadata::NProvider::TEvRefreshSubscriberData>();
                    if (ptr) {
                        CheckFound(ptr);
                    }
                }
                return TTestActorRuntimeBase::EEventAction::PROCESS;
            };

            runtime.SetObserverFunc(pred);

            for (const TInstant start = Now(); !IsFound() && Now() - start < TDuration::Seconds(10); ) {
                runtime.SimulateSleep(TDuration::Seconds(1));
            }
            runtime.SetObserverFunc(TTestActorRuntime::DefaultObserverFunc);
            Y_VERIFY(IsFound());
        }

        void CheckFound(NMetadata::NProvider::TEvRefreshSubscriberData* event) {
            auto snapshot = event->GetSnapshotAs<NMetadata::NSecret::TSnapshot>();
            Y_VERIFY(!!snapshot);
            if (ExpectedSecretsCount) {
                if (snapshot->GetSecrets().size() != ExpectedSecretsCount) {
                    Cerr << "snapshot->GetSecrets().size() incorrect: " << snapshot->SerializeToString() << Endl;
                    return;
                }
            } else if (snapshot->GetSecrets().size()) {
                Cerr << "snapshot->GetSecrets().size() incorrect (zero expects): " << snapshot->SerializeToString() << Endl;
                return;
            }
            if (ExpectedAccessCount) {
                if (snapshot->GetAccess().size() != ExpectedAccessCount) {
                    Cerr << "snapshot->GetAccess().size() incorrect: " << snapshot->SerializeToString() << Endl;
                    return;
                }
            } else if (snapshot->GetAccess().size()) {
                Cerr << "snapshot->GetAccess().size() incorrect (zero expects): " << snapshot->SerializeToString() << Endl;
                return;
            }
            FoundFlag = true;
        }

        void Handle(NMetadata::NProvider::TEvRefreshSubscriberData::TPtr& ev) {
            CheckFound(ev->Get());
        }

        void Bootstrap() {
            auto manager = std::make_shared<NMetadata::NSecret::TSnapshotsFetcher>();
            Become(&TThis::StateInit);
            Y_VERIFY(NMetadata::NProvider::TServiceOperator::IsEnabled());
            Sender<NMetadata::NProvider::TEvSubscribeExternal>(manager).SendTo(NMetadata::NProvider::MakeServiceId(SelfId().NodeId()));
            Start = Now();
        }
    };

    Y_UNIT_TEST(Simple) {
        TPortManager pm;

        ui32 grpcPort = pm.GetPort();
        ui32 msgbPort = pm.GetPort();

        Tests::TServerSettings serverSettings(msgbPort);
        serverSettings.Port = msgbPort;
        serverSettings.GrpcPort = grpcPort;
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetEnableMetadataProvider(true)
            .SetEnableOlapSchemaOperations(true);
        ;

        Tests::TServer::TPtr server = new Tests::TServer(serverSettings);
        server->EnableGRpc(grpcPort);
        //        server->SetupDefaultProfiles();

        Tests::TClient client(serverSettings);

        auto& runtime = *server->GetRuntime();

        auto sender = runtime.AllocateEdgeActor();
        server->SetupRootStoragePools(sender);

        TSecretUserEmulator* emulator = new TSecretUserEmulator;
        runtime.Register(emulator);
        {
            runtime.SimulateSleep(TDuration::Seconds(10));
            Cerr << "Initialization finished" << Endl;

            Tests::NCS::THelper lHelper(*server);
            lHelper.StartSchemaRequest("CREATE OBJECT secret1 (TYPE SECRET) WITH value = `100`");
            {
                TString resultData;
                lHelper.StartDataRequest("SELECT COUNT(*) FROM `/Root/.metadata/initialization/migrations`", true, &resultData);
                UNIT_ASSERT_EQUAL_C(resultData, "[6u]", resultData);
            }

            emulator->SetExpectedSecretsCount(1).SetExpectedAccessCount(0);
            {
                const TInstant start = Now();
                while (!emulator->IsFound() && Now() - start < TDuration::Seconds(20)) {
                    runtime.SimulateSleep(TDuration::Seconds(1));
                }
                Y_VERIFY(emulator->IsFound());
            }

            lHelper.StartSchemaRequest("ALTER OBJECT secret1 (TYPE SECRET) SET value = `abcde`");
            lHelper.StartSchemaRequest("CREATE OBJECT `secret1:test@test1` (TYPE SECRET_ACCESS)");
            {
                TString resultData;
                lHelper.StartDataRequest("SELECT COUNT(*) FROM `/Root/.metadata/initialization/migrations`", true, &resultData);
                UNIT_ASSERT_EQUAL_C(resultData, "[10u]", resultData);
            }

            emulator->SetExpectedSecretsCount(1).SetExpectedAccessCount(1);
            {
                const TInstant start = Now();
                while (!emulator->IsFound() && Now() - start < TDuration::Seconds(20)) {
                    runtime.SimulateSleep(TDuration::Seconds(1));
                }
                Y_VERIFY(emulator->IsFound());
            }

            lHelper.StartSchemaRequest("DROP OBJECT `secret1:test@test1` (TYPE SECRET_ACCESS)");
            lHelper.StartSchemaRequest("DROP OBJECT `secret1` (TYPE SECRET)");
            lHelper.StartDataRequest("SELECT * FROM `/Root/.metadata/initialization/migrations`");

            emulator->SetExpectedSecretsCount(0).SetExpectedAccessCount(0);
            {
                const TInstant start = Now();
                while (!emulator->IsFound() && Now() - start < TDuration::Seconds(20)) {
                    runtime.SimulateSleep(TDuration::Seconds(1));
                }
                Y_VERIFY(emulator->IsFound());
            }
        }
    }

    Y_UNIT_TEST(Validation) {
        TPortManager pm;

        ui32 grpcPort = pm.GetPort();
        ui32 msgbPort = pm.GetPort();

        Tests::TServerSettings serverSettings(msgbPort);
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

        {
            runtime.SimulateSleep(TDuration::Seconds(10));
            Cerr << "Initialization finished" << Endl;

            Tests::NCS::THelper lHelper(*server);
            lHelper.StartSchemaRequest("CREATE OBJECT secret-1 (TYPE SECRET) WITH value = `100`", false);
            lHelper.StartSchemaRequest("ALTER OBJECT secret1 (TYPE SECRET) SET value = `abcde`", false);
            lHelper.StartSchemaRequest("CREATE OBJECT secret1 (TYPE SECRET) WITH value = `100`");
            lHelper.StartSchemaRequest("ALTER OBJECT secret1 (TYPE SECRET) SET value = `abcde`");
            lHelper.StartSchemaRequest("CREATE OBJECT `secret1:test@test1` (TYPE SECRET_ACCESS)");
            lHelper.StartSchemaRequest("CREATE OBJECT `secret2:test@test1` (TYPE SECRET_ACCESS)", false);
            lHelper.StartSchemaRequest("DROP OBJECT `secret1` (TYPE SECRET)", false);
            lHelper.StartDataRequest("SELECT * FROM `/Root/.metadata/secrets/values`", false);
            {
                TString resultData;
                lHelper.StartDataRequest("SELECT COUNT(*) FROM `/Root/.metadata/initialization/migrations`", true, &resultData);
                UNIT_ASSERT_EQUAL_C(resultData, "[10u]", resultData);
            }
        }
    }

    Y_UNIT_TEST(Deactivated) {
        TPortManager pm;

        ui32 grpcPort = pm.GetPort();
        ui32 msgbPort = pm.GetPort();

        Tests::TServerSettings serverSettings(msgbPort);
        serverSettings.Port = msgbPort;
        serverSettings.GrpcPort = grpcPort;
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetEnableMetadataProvider(false)
            .SetEnableOlapSchemaOperations(true);
        ;

        Tests::TServer::TPtr server = new Tests::TServer(serverSettings);
        server->EnableGRpc(grpcPort);

        Tests::TClient client(serverSettings);

        auto& runtime = *server->GetRuntime();

        auto sender = runtime.AllocateEdgeActor();
        server->SetupRootStoragePools(sender);

        {
            runtime.SimulateSleep(TDuration::Seconds(10));
            Cerr << "Initialization finished" << Endl;

            Tests::NCS::THelper lHelper(*server);
            lHelper.StartSchemaRequest("CREATE OBJECT secret1 (TYPE SECRET) WITH value = `100`", false);
        }
    }
}
}
