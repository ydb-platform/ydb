#include "appdata.h"

#include <ydb/library/yql/minikql/invoke_builtins/mkql_builtins.h>
#include <ydb/library/yql/minikql/mkql_function_registry.h>

#include <ydb/core/protos/netclassifier.pb.h>
#include <ydb/core/protos/stream.pb.h>

namespace NKikimr {

    TAppPrepare::TMine::~TMine()
    {
    }

    TAppPrepare::TAppPrepare(std::shared_ptr<NKikimr::NDataShard::IExportFactory> ef)
    {
        Mine = new TMine;
        Mine->Types = new NScheme::TKikimrTypeRegistry;
        Mine->Types->CalculateMetadataEtag();
        Mine->DataShardExportFactory = ef;
        Mine->IoContext = std::make_shared<NPDisk::TIoContextFactoryOSS>();

        Domains = new TDomainsInfo;
    }

    NActors::TTestActorRuntime::TEgg TAppPrepare::Unwrap() noexcept
    {
        if (Mine->Funcs == nullptr) {
            Mine->Funcs = NMiniKQL::CreateFunctionRegistry(
                            NMiniKQL::CreateBuiltinRegistry());
        }

        if (Mine->Formats == nullptr) {
            Mine->Formats = new TFormatFactory;
        }

        auto *app = new TAppData(0, 0, 0, 0, { }, Mine->Types.Get(), Mine->Funcs.Get(), Mine->Formats.Get(), nullptr);
        app->DataShardExportFactory = Mine->DataShardExportFactory.get();
        app->IoContextFactory = Mine->IoContext.get();

        app->DomainsInfo = std::move(Domains);
        app->ChannelProfiles = Channels ? Channels : new TChannelProfiles;
        app->StreamingConfig.SetEnableOutputStreams(true);
        app->PQConfig.MergeFrom(PQConfig);
        app->PQConfig.SetACLRetryTimeoutSec(1);
        app->PQConfig.SetBalancerMetadataRetryTimeoutSec(1);
        app->PQConfig.SetClustersUpdateTimeoutSec(1);
        app->PQConfig.SetCheckACL(true);
        if (NetDataSourceUrl) {
            auto& updaterConfig = *app->NetClassifierConfig.MutableUpdaterConfig();
            updaterConfig.SetNetDataSourceUrl(NetDataSourceUrl);
            updaterConfig.SetRetryIntervalSeconds(1);
            updaterConfig.SetNetDataUpdateIntervalSeconds(1);
        }
        app->EnableKqpSpilling = EnableKqpSpilling;
        app->CompactionConfig = CompactionConfig;
        app->HiveConfig = HiveConfig;
        app->DataShardConfig = DataShardConfig;
        app->ColumnShardConfig = ColumnShardConfig;
        app->SchemeShardConfig = SchemeShardConfig;
        app->MeteringConfig = MeteringConfig;
        app->AwsCompatibilityConfig = AwsCompatibilityConfig;
        app->S3ProxyResolverConfig = S3ProxyResolverConfig;
        app->GraphConfig = GraphConfig;
        app->FeatureFlags = FeatureFlags;

        // This is a special setting active in test runtime only
        app->EnableMvccSnapshotWithLegacyDomainRoot = true;

        auto tempKeys = std::move(Keys);
        auto keyGenerator = [tempKeys] (ui32 node) {
            return tempKeys.contains(node) ?
                        tempKeys.at(node) :
                        NKikimrProto::TKeyConfig();
        };

        return { app, Mine.Release(), keyGenerator, std::move(Icb) };
    }

    void TAppPrepare::AddDomain(TDomainsInfo::TDomain* domain)
    {
        Domains->AddDomain(domain);
    }

    void TAppPrepare::AddHive(ui64 hive)
    {
        Domains->AddHive(hive);
    }

    void TAppPrepare::ClearDomainsAndHive()
    {
        Domains->ClearDomainsAndHive();
    }

    void TAppPrepare::SetChannels(TIntrusivePtr<TChannelProfiles> channels)
    {
        Channels = std::move(channels);
    }

    void TAppPrepare::SetBSConf(NKikimrBlobStorage::TNodeWardenServiceSet config)
    {
        BSConf = std::move(config);
    }

    void TAppPrepare::SetFnRegistry(TFnReg func)
    {
        Mine->Funcs = func(*Mine->Types);
    }

    void TAppPrepare::SetFormatsFactory(TIntrusivePtr<TFormatFactory> formats)
    {
        Mine->Formats = formats;
    }

    void TAppPrepare::SetKeyForNode(const TString& path, ui32 node)
    {
        auto& nodeConfig = Keys[node];
        nodeConfig.Clear();
        auto key = nodeConfig.AddKeys();
        key->SetContainerPath(path);
        key->SetPin(path);
        key->SetId(path);
        key->SetVersion(1);
    }

    void TAppPrepare::SetEnableKqpSpilling(bool value)
    {
        EnableKqpSpilling = value;
    }

    void TAppPrepare::SetNetDataSourceUrl(const TString& value)
    {
        NetDataSourceUrl = value;
    }

    void TAppPrepare::SetKeepSnapshotTimeout(TDuration value)
    {
        if (value) {
            DataShardConfig.SetKeepSnapshotTimeout(value.MilliSeconds());
        } else {
            DataShardConfig.ClearKeepSnapshotTimeout();
        }
    }

    void TAppPrepare::SetChangesQueueItemsLimit(ui64 value)
    {
        if (value) {
            DataShardConfig.SetChangesQueueItemsLimit(value);
        } else {
            DataShardConfig.ClearChangesQueueItemsLimit();
        }
    }

    void TAppPrepare::SetChangesQueueBytesLimit(ui64 value)
    {
        if (value) {
            DataShardConfig.SetChangesQueueBytesLimit(value);
        } else {
            DataShardConfig.ClearChangesQueueBytesLimit();
        }
    }

    void TAppPrepare::SetMinRequestSequenceSize(ui64 value)
    {
        HiveConfig.SetMinRequestSequenceSize(value);
    }

    void TAppPrepare::SetRequestSequenceSize(ui64 value)
    {
        HiveConfig.SetRequestSequenceSize(value);
    }

    void TAppPrepare::SetHiveStoragePoolFreshPeriod(ui64 value)
    {
        HiveConfig.SetStoragePoolFreshPeriod(value);
    }

    void TAppPrepare::AddSystemBackupSID(const TString& sid)
    {
        MeteringConfig.AddSystemBackupSIDs(sid);
    }

    void TAppPrepare::SetEnableProtoSourceIdInfo(std::optional<bool> value)
    {
        if (value) {
            PQConfig.SetEnableProtoSourceIdInfo(*value);
        }
    }

    void TAppPrepare::SetEnablePqBilling(std::optional<bool> value)
    {
        if (value) {
            PQConfig.MutableBillingMeteringConfig()->SetEnabled(*value);
        }
    }

    void TAppPrepare::SetEnableDbCounters(bool value)
    {
        FeatureFlags.SetEnableDbCounters(value);
    }

    void TAppPrepare::SetAwsRegion(const TString& value)
    {
        AwsCompatibilityConfig.SetAwsRegion(value);
    }

    void TAppPrepare::InitIcb(ui32 numNodes)
    {
        for (ui32 i = 0; i < numNodes; ++i) {
            Icb.emplace_back(new TControlBoard);
        }
    }
}
