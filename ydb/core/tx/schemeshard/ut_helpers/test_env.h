#pragma once


#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/testlib/fake_coordinator.h>

#include <ydb/core/base/blobstorage.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/schemeshard/schemeshard_export.h>
#include <ydb/core/tx/schemeshard/schemeshard_identificators.h>
#include <ydb/core/tx/schemeshard/schemeshard_import.h>
#include <ydb/library/ydb_issue/proto/issue_id.pb.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>
#include <ydb/core/protos/follower_group.pb.h>
#include <ydb/core/protos/msgbus_kv.pb.h>

#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>

#include <functional>

namespace NSchemeShardUT_Private {
    using namespace NKikimr;
    using namespace NSchemeShard;

    void TestWaitNotification(NActors::TTestActorRuntime &runtime, TSet<ui64> txIds, TActorId subscriberActorId);
    NActors::TActorId CreateNotificationSubscriber(NActors::TTestActorRuntime &runtime, ui64 schemeshardId);
    NActors::TActorId CreateFakeMetering(NActors::TTestActorRuntime &runtime);

    struct TTestEnvOptions {
        using TSelf = TTestEnvOptions;

        #define OPTION(type, name, defaultValue) \
            TSelf& name(type value) { \
                name##_ = value; \
                return *this; \
            } \
            type name##_ = defaultValue

        OPTION(ui32, NChannels, 4);
        OPTION(bool, EnablePipeRetries, true);
        OPTION(bool, RunFakeConfigDispatcher, false);
        OPTION(bool, InitYdbDriver, false);
        OPTION(std::optional<bool>, EnableSystemViews, std::nullopt);
        OPTION(std::optional<bool>, EnablePersistentQueryStats, std::nullopt);
        OPTION(std::optional<bool>, EnablePersistentPartitionStats, std::nullopt);
        OPTION(std::optional<bool>, AllowUpdateChannelsBindingOfSolomonPartitions, std::nullopt);
        OPTION(std::optional<bool>, EnableNotNullColumns, std::nullopt);
        OPTION(std::optional<bool>, EnableProtoSourceIdInfo, std::nullopt);
        OPTION(std::optional<bool>, EnablePqBilling, std::nullopt);
        OPTION(std::optional<bool>, EnableBackgroundCompaction, std::nullopt);
        OPTION(std::optional<bool>, EnableBorrowedSplitCompaction, std::nullopt);
        OPTION(std::optional<bool>, DisableStatsBatching, std::nullopt);
        OPTION(THashSet<TString>, SystemBackupSIDs, {});
        OPTION(std::optional<bool>, EnableMoveIndex, std::nullopt);
        OPTION(std::optional<bool>, EnableChangefeedInitialScan, std::nullopt);
        OPTION(std::optional<bool>, EnableNotNullDataColumns, std::nullopt);
        OPTION(std::optional<bool>, EnableAlterDatabaseCreateHiveFirst, std::nullopt);
        OPTION(std::optional<bool>, EnableTopicDiskSubDomainQuota, std::nullopt);
        OPTION(std::optional<bool>, EnablePQConfigTransactionsAtSchemeShard, std::nullopt);
        OPTION(std::optional<bool>, EnableTopicSplitMerge, std::nullopt);
        OPTION(std::optional<bool>, EnableChangefeedDynamoDBStreamsFormat, std::nullopt);
        OPTION(std::optional<bool>, EnableChangefeedDebeziumJsonFormat, std::nullopt);
        OPTION(std::optional<bool>, EnableTablePgTypes, std::nullopt);
        OPTION(std::optional<bool>, EnableServerlessExclusiveDynamicNodes, std::nullopt);
        OPTION(std::optional<bool>, EnableAddColumsWithDefaults, std::nullopt);
        OPTION(std::optional<bool>, EnableChangeNotNullConstraint, std::nullopt);
        OPTION(std::optional<bool>, EnableReplaceIfExistsForExternalEntities, std::nullopt);
        OPTION(std::optional<TString>, GraphBackendType, std::nullopt);
        OPTION(std::optional<bool>, EnableChangefeedsOnIndexTables, std::nullopt);

        #undef OPTION
    };

    class TTestEnv {
    public:
        using TSchemeShardFactory = std::function<IActor* (const TActorId &, TTabletStorageInfo *)>;

    private:
        TSchemeShardFactory SchemeShardFactory;
        TFakeHiveState::TPtr HiveState;
        TFakeCoordinator::TState::TPtr CoordinatorState;
        TAutoPtr<ITabletScheduledEventsGuard> EnableSchemeshardPipeRetriesGuard;
        TMap<ui64, TActorId> TxNotificationSubscribers;
        TActorId TxReliablePropose;
        ui32 ChannelsCount;
        TActorId MeteringFake;
        THolder<NYdb::TDriver> YdbDriver;

    public:
        static bool ENABLE_SCHEMESHARD_LOG;

        TTestEnv(TTestActorRuntime& runtime, ui32 nchannels = 4, bool enablePipeRetries = true,
            TSchemeShardFactory ssFactory = &CreateFlatTxSchemeShard, bool enableSystemViews = false);
        TTestEnv(TTestActorRuntime& runtime, const TTestEnvOptions& opts,
            TSchemeShardFactory ssFactory = &CreateFlatTxSchemeShard, std::shared_ptr<NKikimr::NDataShard::IExportFactory> dsExportFactory = {});

        TFakeHiveState::TPtr GetHiveState() const;
        TAutoPtr<ITabletScheduledEventsGuard> EnableSchemeshardPipeRetries(TTestActorRuntime& runtime);
        ui32 ReliablePropose(TTestActorRuntime& runtime, TEvSchemeShard::TEvModifySchemeTransaction* evTx, const TVector<TEvSchemeShard::EStatus>& expectedResults = {NKikimrScheme::StatusAccepted});
        ui32 ReliablePropose(TTestActorRuntime& runtime, TEvSchemeShard::TEvCancelTx* evTx, const TVector<TEvSchemeShard::EStatus>& expectedResults = {NKikimrScheme::StatusAccepted});
        ui32 ReliablePropose(TTestActorRuntime& runtime, TEvExport::TEvCancelExportRequest* ev, const TVector<Ydb::StatusIds::StatusCode>& expectedStatuses = {Ydb::StatusIds::SUCCESS});
        ui32 ReliablePropose(TTestActorRuntime& runtime, TEvExport::TEvForgetExportRequest* ev, const TVector<Ydb::StatusIds::StatusCode>& expectedStatuses = {Ydb::StatusIds::SUCCESS});
        ui32 ReliablePropose(TTestActorRuntime& runtime, TEvImport::TEvCancelImportRequest* ev, const TVector<Ydb::StatusIds::StatusCode>& expectedStatuses = {Ydb::StatusIds::SUCCESS});
        template <class TContainer>
        void TestWaitNotification(TTestActorRuntime& runtime, TContainer txs, ui64 schemeshardId = TTestTxConfig::SchemeShard) {
            TSet<ui64> set(txs.begin(), txs.end());
            TestWaitNotification(runtime, std::move(set), schemeshardId);
        }
        void TestWaitNotification(TTestActorRuntime& runtime, TSet<ui64> txIds, ui64 schemeshardId = TTestTxConfig::SchemeShard);
        void TestWaitNotification(TTestActorRuntime& runtime, int txId, ui64 schemeshardId = TTestTxConfig::SchemeShard);
        void TestWaitNotification(TTestActorRuntime& runtime, ui64 txId, ui64 schemeshardId = TTestTxConfig::SchemeShard);

        template <class TContainer>
        void TestWaitTabletDeletion(TTestActorRuntime& runtime, const TContainer& tabletIds, ui64 hive = TTestTxConfig::Hive) {
            TSet<ui64> set(tabletIds.begin(), tabletIds.end());
            TestWaitTabletDeletion(runtime, std::move(set), hive);
        }
        void TestWaitTabletDeletion(TTestActorRuntime& runtime, TSet<ui64> tabletIds, ui64 hive = TTestTxConfig::Hive);
        void TestWaitTabletDeletion(TTestActorRuntime& runtime, ui64 tabletId, ui64 hive = TTestTxConfig::Hive);

        void TestWaitShardDeletion(TTestActorRuntime& runtime, TSet<ui64> localIds);
        void TestWaitShardDeletion(TTestActorRuntime& runtime, ui64 schemeShard, TSet<ui64> localIds);
        void TestWaitShardDeletion(TTestActorRuntime& runtime, ui64 schemeShard, TSet<TShardIdx> shardIds);

        void SimulateSleep(TTestActorRuntime& runtime, TDuration duration);

        void TestServerlessComputeResourcesModeInHive(TTestActorRuntime& runtime, const TString& path,
                                                      NKikimrSubDomains::EServerlessComputeResourcesMode serverlessComputeResourcesMode,
                                                      ui64 hive = TTestTxConfig::Hive);

        TEvSchemeShard::TEvInitRootShardResult::EStatus InitRoot(TTestActorRuntime& runtime, ui64 schemeRoot, const TActorId& sender, const TString& domainName, const TDomainsInfo::TDomain::TStoragePoolKinds& StoragePoolTypes = {}, const TString& owner = {});
        void InitRootStoragePools(TTestActorRuntime& runtime, ui64 schemeRoot, const TActorId& sender, ui64 domainUid);

        void SetupLogging(TTestActorRuntime& runtime);

    private:
        static std::function<IActor*(const TActorId&, TTabletStorageInfo*)> GetTabletCreationFunc(ui32 type);
        void AddDomain(TTestActorRuntime& runtime, TAppPrepare& app, ui32 domainUid, ui64 hive, ui64 schemeRoot);

        void BootSchemeShard(TTestActorRuntime& runtime, ui64 schemeRoot);
        void BootTxAllocator(TTestActorRuntime& runtime, ui64 tabletId);
    };

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // A wrapper to run test scenarios with reboots of schemeshard, hive and coordinator
    class TTestWithReboots {
    protected:
        struct TDatashardLogBatchingSwitch {
            explicit TDatashardLogBatchingSwitch(bool newVal);
            ~TDatashardLogBatchingSwitch();
        private:
            bool PrevVal;
        };

    public:
        TVector<ui64> TabletIds;
        THolder<TTestActorRuntime> Runtime;
        TTestEnvOptions EnvOpts;
        THolder<TTestEnv> TestEnv;
        TTestEnv::TSchemeShardFactory SchemeShardFactory;
        ui64 TxId;
        const ui64 HiveTabletId;
        const ui64 SchemeShardTabletId;
        const ui64 CoordinatorTabletId;
        const ui64 TxAllocatorId;
        const bool KillOnCommit;

        explicit TTestWithReboots(bool killOnCommit = false, TTestEnv::TSchemeShardFactory ssFactory = &CreateFlatTxSchemeShard);
        virtual ~TTestWithReboots() = default;

        void Run(std::function<void(TTestActorRuntime& runtime, bool& activeZone)> testScenario);
        void Run(std::function<void(TTestActorRuntime& runtime, bool& activeZone)> testScenario, bool allowLogBatching);
        void RunWithTabletReboots(std::function<void(TTestActorRuntime& runtime, bool& activeZone)> testScenario);
        void RunWithPipeResets(std::function<void(TTestActorRuntime& runtime, bool& activeZone)> testScenario);
        void RunWithDelays(std::function<void(TTestActorRuntime& runtime, bool& activeZone)> testScenario);
        void RestoreLogging();

        static TTestEnvOptions GetDefaultTestEnvOptions();

        TTestEnvOptions& GetTestEnvOptions();
        const TTestEnvOptions& GetTestEnvOptions() const;

    private:
        void Prepare(const TString& dispatchName, std::function<void(TTestActorRuntime&)> setup, bool& outActiveZone);
        void EnableTabletResolverScheduling(ui32 nodeIdx = 0);
        void Finalize();
    private:
        virtual TTestEnv* CreateTestEnv();
        // Make sure that user requests are not dropped
        static bool PassUserRequests(TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event);

    private:
        struct TFinalizer;
    };

    struct TInactiveZone {
        bool& ActiveZone;

        explicit TInactiveZone(bool& activeZone)
            : ActiveZone(activeZone)
        {
            ActiveZone = false;
        }

        ~TInactiveZone() {
            ActiveZone = true;
        }
    };
}
