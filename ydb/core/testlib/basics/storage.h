#pragma once

#include "helpers.h"
#include <ydb/core/blobstorage/crypto/default.h>
#include <ydb/core/blobstorage/nodewarden/node_warden.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_tools.h>
#include <ydb/core/testlib/actors/test_runtime.h>
#include <google/protobuf/text_format.h>

#include <util/folder/dirut.h>
#include <util/string/printf.h>
#include <util/string/subst.h>

namespace NKikimr {

    struct TTestStorageFactory {
        using TRuntime = TTestActorRuntime;

        static constexpr ui64 MEM_DISK_SIZE = ui64(32) << 30;
        static constexpr ui64 DISK_SIZE = ui64(16000) << 20;
        static constexpr ui64 SECTOR_SIZE = ui64(4) << 10;
        static constexpr ui64 MEM_CHUNK_SIZE = 32000000;
        static constexpr ui64 CHUNK_SIZE = 80000000;
        static constexpr bool STRAND_PDISK = true;
        static constexpr bool USE_SYNC_PDISK = false;
        static constexpr bool USE_MEM_SYNC_PDISK = true;

        TTestStorageFactory(TRuntime &runtime, NFake::TStorage conf, bool mock)
            : DomainsNum(TBlobStorageGroupType(BootGroupErasure).BlobSubgroupSize())
            , Mock(mock)
            , Conf(FixConf(conf))
            , Runtime(runtime)
        {
            /* Do not use real backend for storage mock, it is useless */

            if (!Conf.UseDisk && !Mock) {
                static TIntrusivePtr<NPDisk::TSectorMap> unsafeSectorMap;
                static TMutex unsafeBufferLock;
                with_lock (unsafeBufferLock) {
                    unsafeSectorMap.Reset(new NPDisk::TSectorMap());
                    unsafeSectorMap->ForceSize(Conf.DiskSize);
                    SectorMap = unsafeSectorMap;
                }
            }

            const bool strandedPDisk = STRAND_PDISK && !Runtime.IsRealThreads();
            if (strandedPDisk) {
                Factory = new TStrandedPDiskServiceFactory(Runtime);
            } else {
                Factory = new TRealPDiskServiceFactory();
            }

            NPDisk::TKey mainKey = NPDisk::YdbDefaultPDiskSequence;

            static ui64 keySalt = 0;
            ui64 salt = ++keySalt;
            TString baseDir = Runtime.GetTempDir();

            if (Conf.UseDisk) {
                MakeDirIfNotExist(baseDir.c_str());
            }

            PDiskPath = TStringBuilder() << baseDir << "pdisk_1.dat";

            if (!Mock) {
                FormatPDisk(PDiskPath,
                    Conf.DiskSize, Conf.SectorSize, Conf.ChunkSize, PDiskGuid,
                    0x123 + salt, 0x456 + salt, 0x789 + salt, mainKey,
                    "", false, false, SectorMap, false);
            }
        }

        static NFake::TStorage FixConf(NFake::TStorage conf) noexcept
        {
            if (conf.DiskSize == 0) {
                conf.DiskSize = conf.UseDisk ? DISK_SIZE : MEM_DISK_SIZE;
                conf.SectorSize = SECTOR_SIZE;
                conf.ChunkSize = conf.UseDisk ? CHUNK_SIZE : MEM_CHUNK_SIZE;
            }

            return conf;
        }

        TIntrusivePtr<TNodeWardenConfig> MakeWardenConf(const TDomainsInfo &domains, const NKikimrProto::TKeyConfig& keyConfig) const
        {
            TIntrusivePtr<TNodeWardenConfig> conf(new TNodeWardenConfig(Factory));

            {
                auto text = MakeTextConf(domains);

                google::protobuf::TextFormat::ParseFromString(text, conf->BlobStorageConfig.MutableServiceSet());
            }

            conf->BlobStorageConfig.MutableServiceSet()->SetEnableProxyMock(Mock);
            conf->PDiskConfigOverlay.SetGetDriveDataSwitch(NKikimrBlobStorage::TPDiskConfig::DoNotTouch);
            conf->PDiskConfigOverlay.SetWriteCacheSwitch(NKikimrBlobStorage::TPDiskConfig::DoNotTouch);

            if (SectorMap) {
                conf->SectorMaps[PDiskPath] = SectorMap;
            }

            auto baseInfo = TVDiskConfig::TBaseInfo::SampleForTests();
            TIntrusivePtr<TVDiskConfig> vDisk = conf->AllVDiskKinds->MakeVDiskConfig(baseInfo);
            vDisk->AdvanceEntryPointTimeout = TDuration::Seconds(5);
            vDisk->RecoveryLogCutterFirstDuration = TDuration::Seconds(3);
            vDisk->RecoveryLogCutterRegularDuration = TDuration::Seconds(5);
            vDisk->FreshCompaction = false;
            vDisk->LevelCompaction = true;
            vDisk->MaxLogoBlobDataSize = Conf.UseDisk ? CHUNK_SIZE / 3 : MEM_CHUNK_SIZE / 3;

            ObtainTenantKey(&conf->TenantKey, keyConfig);
            ObtainStaticKey(&conf->StaticKey);

            return conf;
        }

        TString MakeTextConf(const TDomainsInfo &domains) const
        {
            TString escapedPdiskPath = PDiskPath;
            SubstGlobal(escapedPdiskPath, "\\", "\\\\");
            TStringStream str;

            if (const auto& domain = domains.Domain) {
                str << "AvailabilityDomains: " << domain->DomainUid << Endl;
            }
            str << "PDisks { NodeID: " << Runtime.GetNodeId(0) << " PDiskID: 1 PDiskGuid: " << PDiskGuid
                << " Path: \"" << escapedPdiskPath << "\"}" << Endl;
            str << "" << Endl;

            for (const ui32 ringIdx : xrange(1)) {
                for (const ui32 domainIdx : xrange(DomainsNum)) {
                    for (const ui32 vDiskIdx : xrange(DisksInDomain)) {
                        ui32 slotId = vDiskIdx + domainIdx * DisksInDomain + ringIdx * DomainsNum * DisksInDomain;

                        str << "VDisks {" << Endl;
                        str << "    VDiskID { GroupID: 0 GroupGeneration: 1 Ring: " << ringIdx
                            << " Domain: " << domainIdx << " VDisk: " << vDiskIdx << " }" << Endl;
                        str << "    VDiskLocation { NodeID: " << Runtime.GetNodeId(0) << " PDiskID: 1 PDiskGuid: " << PDiskGuid
                            << " VDiskSlotID: " << slotId << " }" << Endl;
                        str << "}" << Endl;
                    }
                }
            }

            str << "" << Endl;
            str << "Groups {" << Endl;
            str << "    GroupID: 0" << Endl;
            str << "    GroupGeneration: 1 " << Endl;
            str << "    ErasureSpecies: " << (ui32)BootGroupErasure << Endl;

            for (const ui32 ringIdx : xrange(1)) {
                str << "    Rings {" << Endl;
                for (const ui32 domainIdx : xrange(DomainsNum)) {
                    str << "        FailDomains {" << Endl;
                    for (const ui32 vDiskIdx : xrange(DisksInDomain)) {
                        ui32 slotId = vDiskIdx + domainIdx * DisksInDomain + ringIdx * DomainsNum * DisksInDomain;
                        str << "            VDiskLocations { NodeID: " << Runtime.GetNodeId(0) << " PDiskID: 1 VDiskSlotID: " << slotId
                            << " PDiskGuid: " << PDiskGuid << " }" << Endl;
                    }
                    str << "        }" << Endl;
                }
                str << "    }" << Endl;
            }
            str << "}";

            return str.Str();
        }

        const ui32 DisksInDomain = 1;
        const ui32 DomainsNum = 0;
        const ui64 PDiskGuid = 123;
        const bool Mock = false;
        const NFake::TStorage Conf;

    private:
        TTestActorRuntime &Runtime;
        TIntrusivePtr<IPDiskServiceFactory> Factory;
        TString PDiskPath;
        TIntrusivePtr<NPDisk::TSectorMap> SectorMap;
    };

}
