#pragma once

#include "feature_flags.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/channel_profiles.h>
#include <ydb/core/base/domain.h>
#include <ydb/core/formats/factory.h>
#include <ydb/core/scheme/scheme_type_registry.h>
#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/core/tx/datashard/export_iface.h>
#include <ydb/core/tx/datashard/export_s3.h>
#include <ydb/core/protos/blobstorage.pb.h>
#include <ydb/core/protos/datashard_config.pb.h>
#include <ydb/core/protos/kqp.pb.h>
#include <ydb/core/protos/table_service_config.pb.h>
#include <ydb/core/protos/pqconfig.pb.h>

namespace NKikimr {

    // FIXME
    // Split this factory
    class TDataShardExportFactory : public NDataShard::IExportFactory {
        using IExport = NDataShard::IExport;

    public:
        IExport* CreateExportToYt(
                const IExport::TTask& task, const IExport::TTableColumns& columns) const override
        {
            Y_UNUSED(task);
            Y_UNUSED(columns);
            return nullptr;
        }

        IExport* CreateExportToS3(
                const IExport::TTask& task, const IExport::TTableColumns& columns) const override
        {
        #ifndef KIKIMR_DISABLE_S3_OPS
            return new NDataShard::TS3Export(task, columns);
        #else
            Y_UNUSED(task);
            Y_UNUSED(columns);
            return nullptr;
        #endif
        }

        void Shutdown() override {
        }
    };

    struct TAppPrepare : public TTestFeatureFlagsHolder<TAppPrepare> {
        struct TMine : public NActors::IDestructable {
            TIntrusivePtr<NScheme::TTypeRegistry> Types;
            TIntrusivePtr<NMiniKQL::IFunctionRegistry> Funcs;
            TIntrusivePtr<TFormatFactory> Formats;
            std::shared_ptr<NDataShard::IExportFactory> DataShardExportFactory;
            std::shared_ptr<NPDisk::IIoContextFactory> IoContext;

            ~TMine();
        };

        using TFnReg = std::function<NMiniKQL::IFunctionRegistry*(const NScheme::TTypeRegistry&)>;

        TAppPrepare(std::shared_ptr<NKikimr::NDataShard::IExportFactory> ef = {});

        NActors::TTestActorRuntime::TEgg Unwrap() noexcept;

        void AddDomain(TDomainsInfo::TDomain* domain);
        void AddHive(ui64 hive);
        inline void AddHive(ui32, ui64 hive) { AddHive(hive); }
        void ClearDomainsAndHive();
        void SetChannels(TIntrusivePtr<TChannelProfiles> channels);
        void SetBSConf(NKikimrBlobStorage::TNodeWardenServiceSet config);
        void SetFnRegistry(TFnReg func);
        void SetFormatsFactory(TIntrusivePtr<TFormatFactory> formats);
        void SetKeyForNode(const TString& path, ui32 node);
        void SetEnableKqpSpilling(bool value);
        void SetNetDataSourceUrl(const TString& value);
        void SetKeepSnapshotTimeout(TDuration value);
        void SetChangesQueueItemsLimit(ui64 value);
        void SetChangesQueueBytesLimit(ui64 value);
        void SetMinRequestSequenceSize(ui64 value);
        void SetRequestSequenceSize(ui64 value);
        void SetHiveStoragePoolFreshPeriod(ui64 value);
        void AddSystemBackupSID(const TString& sid);
        void SetEnableProtoSourceIdInfo(std::optional<bool> value);
        void SetEnablePqBilling(std::optional<bool> value);
        void SetEnableDbCounters(bool value);
        void SetAwsRegion(const TString& value);
        void InitIcb(ui32 numNodes);

        TIntrusivePtr<TChannelProfiles> Channels;
        NKikimrBlobStorage::TNodeWardenServiceSet BSConf;
        TIntrusivePtr<TDomainsInfo> Domains;
        TMap<ui32, NKikimrProto::TKeyConfig> Keys;
        bool EnableKqpSpilling = false;
        NKikimrConfig::TCompactionConfig CompactionConfig;
        TString NetDataSourceUrl;
        NKikimrConfig::THiveConfig HiveConfig;
        NKikimrConfig::TDataShardConfig DataShardConfig;
        NKikimrConfig::TColumnShardConfig ColumnShardConfig;
        NKikimrConfig::TSchemeShardConfig SchemeShardConfig;
        NKikimrConfig::TMeteringConfig MeteringConfig;
        NKikimrPQ::TPQConfig PQConfig;
        NKikimrConfig::TAwsCompatibilityConfig AwsCompatibilityConfig;
        NKikimrConfig::TS3ProxyResolverConfig S3ProxyResolverConfig;
        NKikimrConfig::TGraphConfig GraphConfig;
        NKikimrConfig::TImmediateControlsConfig ImmediateControlsConfig;
        std::vector<TIntrusivePtr<NKikimr::TControlBoard>> Icb;

    private:
        TAutoPtr<TMine> Mine;
    };

}
