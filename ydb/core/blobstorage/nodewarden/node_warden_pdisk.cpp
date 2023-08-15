#include "node_warden_impl.h"

#include <ydb/core/blobstorage/crypto/default.h>
#include <ydb/library/pdisk_io/file_params.h>
#include <ydb/library/pdisk_io/wcache.h>

#include <util/string/split.h>

namespace NKikimr::NStorage {

    static const std::unordered_map<NPDisk::EDeviceType, ui64> DefaultSpeedLimit{
        {NPDisk::DEVICE_TYPE_ROT, 100000000},
        {NPDisk::DEVICE_TYPE_SSD, 200000000},
        {NPDisk::DEVICE_TYPE_NVME, 300000000},
    };

    TIntrusivePtr<TPDiskConfig> TNodeWarden::CreatePDiskConfig(const NKikimrBlobStorage::TNodeWardenServiceSet::TPDisk& pdisk)  {
        const TString& path = pdisk.GetPath();
        const ui64 pdiskGuid = pdisk.GetPDiskGuid();
        const ui32 pdiskID = pdisk.GetPDiskID();
        const ui64 pdiskCategory = pdisk.GetPDiskCategory();
        const ui64 inMemoryForTestsBufferBytes = pdisk.GetInMemoryForTestsBufferBytes();
        Y_VERIFY_S(!inMemoryForTestsBufferBytes, "InMemory PDisk is deprecated, use SectorMap instead");

        TIntrusivePtr<TPDiskConfig> pdiskConfig = new TPDiskConfig(path, pdiskGuid, pdiskID, pdiskCategory);
        pdiskConfig->StartOwnerRound = NextLocalPDiskInitOwnerRound();
        pdiskConfig->FeatureFlags = Cfg->FeatureFlags;
        if (pdisk.HasManagementStage()) {
            pdiskConfig->SerialManagementStage = pdisk.GetManagementStage();
        }
        if (pdisk.HasSpaceColorBorder()) {
            pdiskConfig->SpaceColorBorder = pdisk.GetSpaceColorBorder();
        }
        if (pdisk.HasPDiskConfig()) {
            pdiskConfig->Apply(&pdisk.GetPDiskConfig());
        }
        pdiskConfig->Apply(&Cfg->PDiskConfigOverlay);
        if (pdisk.HasExpectedSerial()) {
            pdiskConfig->ExpectedSerial = pdisk.GetExpectedSerial();
        }

        // Path scheme: "SectorMap:unique_name[:3000]"
        // where '3000' is device size of in GiB.
        if (path.Contains(":")) {
            TVector<TString> splitted;
            size_t tokenCount = Split(path, ":", splitted);

            if (splitted[0] == "SectorMap") {
                Y_VERIFY(tokenCount >= 2);
                ui64 size = (ui64)100 << 30; // 100GB is default
                if (splitted.size() >= 3) {
                    ui64 minSize = (ui64)100 << 30;
                    if (pdiskConfig->FeatureFlags.GetEnableSmallDiskOptimization()) {
                        minSize = (32ull << 20) * 256; // at least needed 256 chunks
                    }
                    size = Max(minSize, FromStringWithDefault<ui64>(splitted[2], size) << 30);
                }

                auto diskMode = NPDisk::NSectorMap::DM_NONE;
                if (splitted.size() >= 4) {
                    diskMode = NPDisk::NSectorMap::DiskModeFromString(splitted[3]);
                }

                auto& maps = Cfg->SectorMaps;
                if (auto it = maps.find(path); it == maps.end()) {
                    maps[path] = new NPDisk::TSectorMap(size, diskMode);
                    maps[path]->ZeroInit(1000); // Format PDisk
                }

                const auto& map = maps[path];
                bool found = false;
                for (const auto& drive : MockDevicesConfig.GetDevices()) {
                    if (drive.GetPath() == path) {
                        map->Serial = drive.GetSerialNumber();
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    auto *p = MockDevicesConfig.AddDevices();
                    p->SetPath(path);
                    p->SetSerialNumber(map->Serial);
                    p->SetFirmwareRevision("rev.1");
                    p->SetModelNumber("SectorMap");
                    p->SetDeviceType(NKikimrBlobStorage::NVME);
                    p->SetSize(map->DeviceSize);
                    p->SetIsMock(true);

                    TString data;
                    google::protobuf::TextFormat::PrintToString(MockDevicesConfig, &data);
                    try {
                        TFile f(MockDevicesPath, CreateAlways | WrOnly);
                        f.Write(data.Data(), data.Size());
                        f.Flush();
                    } catch (TFileError ex) {
                        STLOG(PRI_WARN, BS_NODE, NW89, "Can't write new MockDevicesConfig to file", (Path, MockDevicesPath));
                    }
                }
            } else if (splitted[0] == "Serial") {
                ;
            } else if (splitted[0] == "PCIe") {
                ;
            } else {
                STLOG(PRI_ERROR, BS_NODE, NW27, "unknown pdisk path scheme", (Path, path));
            }
        }

        // testlib uses SectorMap with fs-style path like "/place/vartmp/tmpAMjsJ0/pdisk_1.dat"
        if (auto it = Cfg->SectorMaps.find(path); it != Cfg->SectorMaps.end()) {
            pdiskConfig->SectorMap = it->second;
            pdiskConfig->EnableSectorEncryption = !pdiskConfig->SectorMap;
        }

        NPDisk::TMainKey pdiskKey = Cfg->CreatePDiskKey();
        TString keyPrintSalt = "@N2#_lW19)2-31!iifI@n1178349617";
        pdiskConfig->HashedMainKey.resize(pdiskKey.size());
        for (ui32 i = 0; i < pdiskKey.size(); ++i) {
            THashCalculator hasher;
            hasher.Hash(keyPrintSalt.Detach(), keyPrintSalt.Size());
            hasher.Hash(&pdiskKey[i], sizeof(pdiskKey[i]));
            pdiskConfig->HashedMainKey[i] = TStringBuilder() << Hex(hasher.GetHashResult(), HF_ADDX);
        }

        pdiskConfig->Initialize();

        return pdiskConfig;
    }

    void TNodeWarden::StartLocalPDisk(const NKikimrBlobStorage::TNodeWardenServiceSet::TPDisk& pdisk) {
        const TPDiskKey key(pdisk.GetNodeID(), pdisk.GetPDiskID());
        auto [it, inserted] = LocalPDisks.try_emplace(key, pdisk);
        TPDiskRecord& record = it->second;
        if (!inserted) {
            Y_VERIFY(record.Record.GetPDiskGuid() == pdisk.GetPDiskGuid());
            return;
        }

        TPDiskCategory category(record.Record.GetPDiskCategory());
        std::optional<ui64> readBytesPerSecond, writeBytesPerSecond;
        for (const auto& item : Cfg->BlobStorageConfig.GetServiceSet().GetReplBrokerConfig().GetMediaTypeQuota()) {
            if (PDiskTypeToPDiskType(item.GetType()) == category.Type()) {
                if (item.HasReadBytesPerSecond()) {
                    readBytesPerSecond.emplace(item.GetReadBytesPerSecond());
                }
                if (item.HasWriteBytesPerSecond()) {
                    writeBytesPerSecond.emplace(item.GetWriteBytesPerSecond());
                }
            }
        }
        std::optional<ui64> def;
        if (const auto it = DefaultSpeedLimit.find(category.Type()); it != DefaultSpeedLimit.end()) {
            def = it->second;
        }
        readBytesPerSecond = readBytesPerSecond ? readBytesPerSecond : def;
        writeBytesPerSecond = writeBytesPerSecond ? writeBytesPerSecond : def;
        if (readBytesPerSecond) {
            record.ReplPDiskReadQuoter = std::make_shared<TReplQuoter>(*readBytesPerSecond);
        }
        if (writeBytesPerSecond) {
            record.ReplPDiskWriteQuoter = std::make_shared<TReplQuoter>(*writeBytesPerSecond);
        }

        STLOG(PRI_DEBUG, BS_NODE, NW04, "StartLocalPDisk", (NodeId, key.NodeId), (PDiskId, key.PDiskId),
            (Path, TString(TStringBuilder() << '"' << pdisk.GetPath() << '"')),
            (PDiskCategory, TPDiskCategory(record.Record.GetPDiskCategory())));

        auto pdiskConfig = CreatePDiskConfig(pdisk);

        const ui32 pdiskID = pdisk.GetPDiskID();
        const TString& path = pdisk.GetPath();
        const ui64 pdiskGuid = pdisk.GetPDiskGuid();
        const ui64 pdiskCategory = pdisk.GetPDiskCategory();
        Cfg->PDiskServiceFactory->Create(ActorContext(), pdiskID, pdiskConfig,
            Cfg->CreatePDiskKey(), AppData()->SystemPoolId, LocalNodeId);
        Send(WhiteboardId, new NNodeWhiteboard::TEvWhiteboard::TEvPDiskStateUpdate(pdiskID, path, pdiskGuid, pdiskCategory));
        Send(WhiteboardId, new NNodeWhiteboard::TEvWhiteboard::TEvSystemStateAddRole("Storage"));
    }

    void TNodeWarden::DestroyLocalPDisk(ui32 pdiskId) {
        STLOG(PRI_INFO, BS_NODE, NW36, "DestroyLocalPDisk", (PDiskId, pdiskId));
        if (auto it = LocalPDisks.find({LocalNodeId, pdiskId}); it != LocalPDisks.end()) {
            const TActorId actorId = MakeBlobStoragePDiskID(LocalNodeId, pdiskId);
            TActivationContext::Send(new IEventHandle(TEvents::TSystem::Poison, 0, actorId, {}, nullptr, 0));
            Send(WhiteboardId, new NNodeWhiteboard::TEvWhiteboard::TEvPDiskStateDelete(pdiskId));
            LocalPDisks.erase(it);

            // mark vdisks still living over this PDisk as destroyed ones
            for (auto it = LocalVDisks.lower_bound({LocalNodeId, pdiskId, 0}); it != LocalVDisks.end() &&
                    it->first.NodeId == LocalNodeId && it->first.PDiskId == pdiskId; ++it) {
                it->second.UnderlyingPDiskDestroyed = true;
            }
        }
    }

    void TNodeWarden::RestartLocalPDiskStart(ui32 pdiskId, TIntrusivePtr<TPDiskConfig> pdiskConfig) {
        auto it = LocalPDisks.find(TPDiskKey(LocalNodeId, pdiskId));
        if (it == LocalPDisks.end()) {
            STLOG(PRI_WARN, BS_NODE, NW66, "Cannot restart local pdisk since there is no such pdisk",
                    (NodeId, LocalNodeId), (PDiskId, pdiskId));
            return;
        }

        bool inserted = InFlightRestartedPDisks.emplace(LocalNodeId, pdiskId).second;
        if (!inserted) {
            STLOG(PRI_WARN, BS_NODE, NW67, "Cannot restart local pdisk since it already in the process of restart",
                    (NodeId, LocalNodeId), (PDiskId, pdiskId));
            return;
        }

        const TActorId actorId = MakeBlobStoragePDiskID(LocalNodeId, pdiskId);
        Send(actorId, new TEvBlobStorage::TEvRestartPDisk(pdiskId, Cfg->CreatePDiskKey(), pdiskConfig));
        STLOG(PRI_NOTICE, BS_NODE, NW69, "RestartLocalPDisk is started", (PDiskId, pdiskId));
    }

    void TNodeWarden::RestartLocalPDiskFinish(ui32 pdiskId, NKikimrProto::EReplyStatus status) {
        const TPDiskKey pdiskKey(LocalNodeId, pdiskId);

        size_t erasedCount = InFlightRestartedPDisks.erase(pdiskKey);
        Y_VERIFY_S(erasedCount == 1, "PDiskId# " << pdiskId << " restarted, but wasn't in the process of removal");

        const TVSlotId from(pdiskKey.NodeId, pdiskKey.PDiskId, 0);
        const TVSlotId to(pdiskKey.NodeId, pdiskKey.PDiskId, Max<ui32>());

        if (status == NKikimrProto::EReplyStatus::OK) {
            TStringStream vdisks;
            bool first = true;
            vdisks << "{";
            for (auto it = LocalVDisks.lower_bound(from); it != LocalVDisks.end() && it->first <= to; ++it) {
                auto& [key, value] = *it;

                PoisonLocalVDisk(value);
                vdisks << (std::exchange(first, false) ? "" : ", ") << value.GetVDiskId().ToString();
                if (const auto it = SlayInFlight.find(key); it != SlayInFlight.end()) {
                    const ui64 round = NextLocalPDiskInitOwnerRound();
                    Send(MakeBlobStoragePDiskID(key.NodeId, key.PDiskId), new NPDisk::TEvSlay(value.GetVDiskId(), round,
                        key.PDiskId, key.VDiskSlotId));
                    it->second = round;
                } else {
                    StartLocalVDiskActor(value, TDuration::Zero());
                }
            }
            SendDiskMetrics(false);

            vdisks << "}";
            STLOG(PRI_NOTICE, BS_NODE, NW74, "RestartLocalPDisk has finished",
                    (PDiskId, pdiskId), (VDiskIds, vdisks.Str()));
        } else {
            for (auto it = LocalVDisks.lower_bound(from); it != LocalVDisks.end() && it->first <= to; ++it) {
                auto& [key, value] = *it;
                if (!value.RuntimeData && !SlayInFlight.contains(key)) {
                    StartLocalVDiskActor(value, TDuration::Zero());
                }
            }
        }
    }

    void TNodeWarden::ApplyServiceSetPDisks(const NKikimrBlobStorage::TNodeWardenServiceSet& serviceSet) {
        for (const auto& pdisk : serviceSet.GetPDisks()) {
            if (!pdisk.HasNodeID() || !pdisk.HasPDiskID() || !pdisk.HasPath() || !pdisk.HasPDiskGuid() ||
                    pdisk.GetNodeID() != LocalNodeId) {
                continue;
            }

            auto entityStatus = pdisk.HasEntityStatus()
                ? pdisk.GetEntityStatus()
                : NKikimrBlobStorage::INITIAL;

            switch (entityStatus) {
                case NKikimrBlobStorage::INITIAL:
                case NKikimrBlobStorage::CREATE:
                    StartLocalPDisk(pdisk);
                    break;

                case NKikimrBlobStorage::DESTROY:
                    DestroyLocalPDisk(pdisk.GetPDiskID());
                    break;

                case NKikimrBlobStorage::RESTART:
                    if (auto it = LocalPDisks.find({pdisk.GetNodeID(), pdisk.GetPDiskID()}); it != LocalPDisks.end()) {
                        it->second.Record = pdisk;
                    }
                    RestartLocalPDiskStart(pdisk.GetPDiskID(), CreatePDiskConfig(pdisk));
                    break;
            }
        }
    }

} // NKikimr::NStorage
