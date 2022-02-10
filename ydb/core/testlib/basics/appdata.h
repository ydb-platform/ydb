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

namespace NKikimr {

    // FIXME
    // Split this factory
    class TDataShardExportFactory : public NKikimr::NDataShard::IExportFactory {
    public:
        NKikimr::NDataShard::IExport* CreateExportToYt(bool useTypeV3) const override {
            Y_UNUSED(useTypeV3);
            return nullptr;
        }

        NKikimr::NDataShard::IExport* CreateExportToS3() const override {
        #ifndef KIKIMR_DISABLE_S3_OPS
            return new NKikimr::NDataShard::TS3Export();
        #else
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
        void AddHive(ui32 hiveUid, ui64 hive);
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
        void AddSystemBackupSID(const TString& sid);
        void SetEnableProtoSourceIdInfo(std::optional<bool> value);

        TIntrusivePtr<TChannelProfiles> Channels;
        NKikimrBlobStorage::TNodeWardenServiceSet BSConf;
        TIntrusivePtr<TDomainsInfo> Domains;
        TMap<ui32, NKikimrProto::TKeyConfig> Keys;
        bool EnableKqpSpilling = false;
        NKikimrConfig::TCompactionConfig CompactionConfig;
        TString NetDataSourceUrl;
        NKikimrConfig::THiveConfig HiveConfig;
        NKikimrConfig::TDataShardConfig DataShardConfig; 
        NKikimrConfig::TMeteringConfig MeteringConfig;
        NKikimrPQ::TPQConfig PQConfig;

    private:
        TAutoPtr<TMine> Mine;
    };

}
