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
                Y_ABORT_UNLESS(tokenCount >= 2);
                ui32 defaultSizeGb = 100;
                ui64 size = (ui64)defaultSizeGb << 30;
                if (splitted.size() >= 3) {
                    ui64 minSize = (ui64)100 << 30;
                    if (pdiskConfig->FeatureFlags.GetEnableSmallDiskOptimization()) {
                        minSize = (32ull << 20) * 256; // we need at least 256 chunks
                    }
                    size = Max(minSize, (ui64)FromStringWithDefault<ui32>(splitted[2], defaultSizeGb) << 30);
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

        const NPDisk::TMainKey& pdiskKey = Cfg->PDiskKey;
        TString keyPrintSalt = "@N2#_lW19)2-31!iifI@n1178349617";
        pdiskConfig->HashedMainKey.resize(pdiskKey.Keys.size());
        for (ui32 i = 0; i < pdiskKey.Keys.size(); ++i) {
            THashCalculator hasher;
            hasher.Hash(keyPrintSalt.Detach(), keyPrintSalt.Size());
            hasher.Hash(&pdiskKey.Keys[i], sizeof(pdiskKey.Keys[i]));
            pdiskConfig->HashedMainKey[i] = TStringBuilder() << Hex(hasher.GetHashResult(), HF_ADDX);
        }

        pdiskConfig->Initialize();

        return pdiskConfig;
    }

    void TNodeWarden::StartLocalPDisk(const NKikimrBlobStorage::TNodeWardenServiceSet::TPDisk& pdisk, bool temporary) {
        const TString& path = pdisk.GetPath();
        if (const auto it = PDiskByPath.find(path); it != PDiskByPath.end()) {
            const auto jt = LocalPDisks.find(it->second.RunningPDiskId);
            Y_ABORT_UNLESS(jt != LocalPDisks.end());
            TPDiskRecord& record = jt->second;
            if (record.Temporary) { // this is temporary PDisk spinning, have to wait for it to finish
                Y_ABORT_UNLESS(!temporary);
                PDisksWaitingToStart.insert(pdisk.GetPDiskID());
                it->second.Pending = pdisk;
            } else { // incorrect configuration: we are trying to start two different PDisks with the same path
                STLOG(PRI_ERROR, BS_NODE, NW48, "starting two PDisks with the same path", (Path, path),
                    (ExistingPDiskId, jt->first.PDiskId), (NewPDiskId, pdisk.GetPDiskID()));
            }
            return;
        }

        const TPDiskKey key(pdisk.GetNodeID(), pdisk.GetPDiskID());
        auto [it, inserted] = LocalPDisks.try_emplace(key, pdisk);
        TPDiskRecord& record = it->second;
        if (inserted) {
            const auto [_, inserted] = PDiskByPath.emplace(path, TPDiskByPathInfo{key, std::nullopt});
            Y_DEBUG_ABORT_UNLESS(inserted);
            record.Temporary = temporary;
        } else {
            Y_ABORT_UNLESS(record.Record.GetPDiskGuid() == pdisk.GetPDiskGuid());
            Y_ABORT_UNLESS(!record.Temporary);
            Y_ABORT_UNLESS(!temporary);
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
            (PDiskCategory, TPDiskCategory(record.Record.GetPDiskCategory())),
            (Temporary, temporary));

        auto pdiskConfig = CreatePDiskConfig(pdisk);
        if (temporary) {
            pdiskConfig->MetadataOnly = true;
        }

        const ui32 pdiskID = pdisk.GetPDiskID();
        const ui64 pdiskGuid = pdisk.GetPDiskGuid();
        const ui64 pdiskCategory = pdisk.GetPDiskCategory();
        Cfg->PDiskKey.Initialize();
        Cfg->PDiskServiceFactory->Create(ActorContext(), pdiskID, pdiskConfig, Cfg->PDiskKey, AppData()->SystemPoolId, LocalNodeId);
        if (!temporary) {
            Send(WhiteboardId, new NNodeWhiteboard::TEvWhiteboard::TEvPDiskStateUpdate(pdiskID, path, pdiskGuid, pdiskCategory));
            Send(WhiteboardId, new NNodeWhiteboard::TEvWhiteboard::TEvSystemStateAddRole("Storage"));
        }
    }

    void TNodeWarden::DestroyLocalPDisk(ui32 pdiskId) {
        std::optional<NKikimrBlobStorage::TNodeWardenServiceSet::TPDisk> pending;

        STLOG(PRI_INFO, BS_NODE, NW36, "DestroyLocalPDisk", (PDiskId, pdiskId));

        if (auto it = LocalPDisks.find({LocalNodeId, pdiskId}); it != LocalPDisks.end()) {
            const TActorId actorId = MakeBlobStoragePDiskID(LocalNodeId, pdiskId);
            TActivationContext::Send(new IEventHandle(TEvents::TSystem::Poison, 0, actorId, {}, nullptr, 0));
            Send(WhiteboardId, new NNodeWhiteboard::TEvWhiteboard::TEvPDiskStateDelete(pdiskId));
            if (const auto jt = PDiskByPath.find(it->second.Record.GetPath()); jt != PDiskByPath.end() &&
                    jt->second.RunningPDiskId == it->first) {
                pending = std::move(jt->second.Pending);
                PDiskByPath.erase(jt);
                PDisksWaitingToStart.erase(pending->GetPDiskID());
            } else {
                Y_DEBUG_ABORT("missing entry in PDiskByPath");
            }
            LocalPDisks.erase(it);
            PDiskRestartInFlight.erase(pdiskId);

            // mark vdisks still living over this PDisk as destroyed ones
            for (auto it = LocalVDisks.lower_bound({LocalNodeId, pdiskId, 0}); it != LocalVDisks.end() &&
                    it->first.NodeId == LocalNodeId && it->first.PDiskId == pdiskId; ++it) {
                it->second.UnderlyingPDiskDestroyed = true;
            }
        }

        if (pending) {
            StartLocalPDisk(*pending, false);

            // start VDisks over this one waiting for their turn
            const ui32 actualPDiskId = pending->GetPDiskID();
            const TVSlotId from(LocalNodeId, actualPDiskId, 0);
            const TVSlotId to(LocalNodeId, actualPDiskId, Max<ui32>());
            for (auto it = LocalVDisks.lower_bound(from); it != LocalVDisks.end() && it->first <= to; ++it) {
                auto& [key, value] = *it;
                Y_ABORT_UNLESS(!value.RuntimeData); // they can't be working
                StartLocalVDiskActor(value);
            }
        }
    }

    void TNodeWarden::SendPDiskReport(ui32 pdiskId, NKikimrBlobStorage::TEvControllerNodeReport::EPDiskPhase phase) {
        STLOG(PRI_DEBUG, BS_NODE, NW41, "SendPDiskReport", (PDiskId, pdiskId), (Phase, phase));

        auto report = std::make_unique<TEvBlobStorage::TEvControllerNodeReport>(LocalNodeId);
        auto *pReport = report->Record.AddPDiskReports();
        pReport->SetPDiskId(pdiskId);
        pReport->SetPhase(phase);

        SendToController(std::move(report));
    }

    void TNodeWarden::AskBSCToRestartPDisk(ui32 pdiskId, ui64 requestCookie) {
        auto ev = std::make_unique<TEvBlobStorage::TEvControllerConfigRequest>();

        NKikimrBlobStorage::TRestartPDisk* cmd = ev->Record.MutableRequest()->AddCommand()->MutableRestartPDisk();

        auto targetPDiskId = cmd->MutableTargetPDiskId();
        targetPDiskId->SetNodeId(LocalNodeId);
        targetPDiskId->SetPDiskId(pdiskId);

        const ui64 cookie = NextConfigCookie++;
        SendToController(std::move(ev), cookie);
        ConfigInFlight.emplace(cookie, [=](TEvBlobStorage::TEvControllerConfigResponse *ev) {
            if (auto node = PDiskRestartRequests.extract(requestCookie)) {
                if (!ev || !ev->Record.GetResponse().GetSuccess()) {
                    OnUnableToRestartPDisk(node.mapped(), ev ? ev->Record.GetResponse().GetErrorDescription() : "BSC disconnected");
                }
            }
        });
    }

    void TNodeWarden::OnPDiskRestartFinished(ui32 pdiskId, NKikimrProto::EReplyStatus status) {
        if (PDiskRestartInFlight.erase(pdiskId) == 0) {
            // There was no restart in progress.
            return;
        }

        const TPDiskKey pdiskKey(LocalNodeId, pdiskId);

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
                    StartLocalVDiskActor(value);
                }
            }

            vdisks << "}";
            STLOG(PRI_NOTICE, BS_NODE, NW74, "RestartLocalPDisk has finished",
                    (PDiskId, pdiskId), (VDiskIds, vdisks.Str()));
        } else {
            for (auto it = LocalVDisks.lower_bound(from); it != LocalVDisks.end() && it->first <= to; ++it) {
                auto& [key, value] = *it;
                if (!value.RuntimeData) {
                    StartLocalVDiskActor(value);
                }
            }
        }

        SendPDiskReport(pdiskId, NKikimrBlobStorage::TEvControllerNodeReport::PD_RESTARTED);
    }

    void TNodeWarden::DoRestartLocalPDisk(const NKikimrBlobStorage::TNodeWardenServiceSet::TPDisk& pdisk) {
        ui32 pdiskId = pdisk.GetPDiskID();

        STLOG(PRI_NOTICE, BS_NODE, NW75, "DoRestartLocalPDisk", (PDiskId, pdiskId));

        const auto [_, inserted] = PDiskRestartInFlight.emplace(pdiskId);

        if (!inserted) {
            STLOG(PRI_NOTICE, BS_NODE, NW76, "Restart already in progress", (PDiskId, pdiskId));
            // Restart is already in progress.
            return;
        }

        auto it = LocalPDisks.find(TPDiskKey(LocalNodeId, pdiskId));
        if (it == LocalPDisks.end()) {
            PDiskRestartInFlight.erase(pdiskId);

            STLOG(PRI_NOTICE, BS_NODE, NW77, "Restart state carried from previous start, just starting", (PDiskId, pdiskId));

            // This can happen if warden didn't handle pdisk's restart before node's restart.
            // In this case, PDisk has EntityStatus::RESTART instead of EntityStatus::INITIAL.
            StartLocalPDisk(pdisk, false);
            SendPDiskReport(pdiskId, NKikimrBlobStorage::TEvControllerNodeReport::PD_RESTARTED);
            return;
        }

        const TActorId actorId = MakeBlobStoragePDiskID(LocalNodeId, pdiskId);

        TIntrusivePtr<TPDiskConfig> pdiskConfig = CreatePDiskConfig(it->second.Record);

        Cfg->PDiskKey.Initialize();
        Send(actorId, new TEvBlobStorage::TEvAskWardenRestartPDiskResult(pdiskId, Cfg->PDiskKey, true, pdiskConfig));
    }

    void TNodeWarden::OnUnableToRestartPDisk(ui32 pdiskId, TString error) {
        const TActorId actorId = MakeBlobStoragePDiskID(LocalNodeId, pdiskId);

        Cfg->PDiskKey.Initialize();
        Send(actorId, new TEvBlobStorage::TEvAskWardenRestartPDiskResult(pdiskId, Cfg->PDiskKey, false, nullptr, error));
    }

    void TNodeWarden::MergeServiceSetPDisks(NProtoBuf::RepeatedPtrField<TServiceSetPDisk> *to,
            const NProtoBuf::RepeatedPtrField<TServiceSetPDisk>& from) {
        THashMap<TPDiskKey, TServiceSetPDisk*> pdiskMap;
        for (int i = 0; i < to->size(); ++i) {
            TServiceSetPDisk *pdisk = to->Mutable(i);
            const auto [it, inserted] = pdiskMap.try_emplace(TPDiskKey(*pdisk), pdisk);
            Y_DEBUG_ABORT_UNLESS(inserted); // entries must be unique
        }

        for (const TServiceSetPDisk& pdisk : from) {
            if (!pdisk.HasNodeID() || !pdisk.HasPDiskID() || !pdisk.HasPath() || !pdisk.HasPDiskGuid() ||
                    pdisk.GetNodeID() != LocalNodeId) {
                continue;
            }

            const NKikimrBlobStorage::EEntityStatus entityStatus = pdisk.HasEntityStatus()
                ? pdisk.GetEntityStatus()
                : NKikimrBlobStorage::INITIAL;

            const TPDiskKey key(pdisk);

            switch (entityStatus) {
                case NKikimrBlobStorage::RESTART:
                    if (auto it = LocalPDisks.find({pdisk.GetNodeID(), pdisk.GetPDiskID()}); it != LocalPDisks.end()) {
                        it->second.Record = pdisk;
                    }
                    DoRestartLocalPDisk(pdisk);
                    [[fallthrough]];
                case NKikimrBlobStorage::INITIAL:
                case NKikimrBlobStorage::CREATE: {
                    const auto [it, inserted] = pdiskMap.try_emplace(key, nullptr);
                    if (inserted) {
                        it->second = to->Add();
                    }
                    it->second->CopyFrom(pdisk);
                    it->second->ClearEntityStatus();
                    break;
                }
                case NKikimrBlobStorage::DESTROY:
                    pdiskMap.erase(key);
                    break;
            }
        }

        for (int i = 0; i < to->size(); ++i) {
            if (const TServiceSetPDisk& pdisk = to->Get(i); !pdiskMap.contains(pdisk)) {
                to->SwapElements(i, to->size() - 1);
                to->RemoveLast();
                --i;
            }
        }
    }

    void TNodeWarden::ApplyServiceSetPDisks() {
        THashSet<TPDiskKey> pdiskToDelete;
        THashSet<TString> pathsToResetPending;
        for (const auto& [key, value] : LocalPDisks) {
            if (!value.Temporary) { // ignore temporary disks, they are destroyed automatically
                pdiskToDelete.insert(key);
            }
        }
        for (const auto& [path, record] : PDiskByPath) {
            if (record.Pending) {
                pathsToResetPending.insert(path);
            }
        }

        auto processDisk = [&](const TServiceSetPDisk& pdisk) {
            const TPDiskKey key(pdisk);
            if (!LocalPDisks.contains(key)) {
                StartLocalPDisk(pdisk, false);
            }
            pdiskToDelete.erase(key);
            pathsToResetPending.erase(pdisk.GetPath());
        };

        for (const auto& pdisk : StaticServices.GetPDisks()) {
            processDisk(pdisk);
        }
        for (const auto& pdisk : DynamicServices.GetPDisks()) {
            processDisk(pdisk);
        }

        for (const auto& [key, value] : LocalVDisks) {
            pdiskToDelete.erase({key.NodeId, key.PDiskId});
        }

        for (const TPDiskKey& key : pdiskToDelete) {
            DestroyLocalPDisk(key.PDiskId);
        }
        for (const TString& path : pathsToResetPending) {
            if (const auto it = PDiskByPath.find(path); it != PDiskByPath.end()) {
                PDisksWaitingToStart.erase(it->second.Pending->GetPDiskID());
                it->second.Pending.reset();
            }
        }
    }

    class TNodeWarden::TPDiskMetadataInteractionActor : public TActorBootstrapped<TPDiskMetadataInteractionActor> {
        const TPDiskKey PDiskKey;
        std::unique_ptr<IEventHandle> OriginalEv;
        std::unique_ptr<IEventBase> ConvertedEv;
        TActorId ParentId;
        const char* const EventType;

    public:
        TPDiskMetadataInteractionActor(TPDiskKey pdiskKey, TAutoPtr<IEventHandle> originalEv,
                std::unique_ptr<IEventBase> convertedEv, const char *eventType)
            : PDiskKey(pdiskKey)
            , OriginalEv(originalEv.Release())
            , ConvertedEv(std::move(convertedEv))
            , EventType(eventType)
        {}

        void Bootstrap(TActorId parentId) {
            ParentId = parentId;
            Y_ABORT_UNLESS(PDiskKey.NodeId == SelfId().NodeId());
            Send(MakeBlobStoragePDiskID(PDiskKey.NodeId, PDiskKey.PDiskId), ConvertedEv.release(),
                IEventHandle::FlagTrackDelivery);
            Become(&TThis::StateFunc, TDuration::Seconds(10), new TEvents::TEvWakeup);
        }

        void Handle(TEvents::TEvUndelivered::TPtr /*ev*/) {
            // send this event again, this may be a race with PDisk destruction
            TActivationContext::Send(OriginalEv.release());
            PassAway();
        }

        void Handle(NPDisk::TEvReadMetadataResult::TPtr ev) {
            auto *msg = ev->Get();
            NKikimrBlobStorage::TPDiskMetadataRecord record;
            TRope rope(std::move(msg->Metadata));
            TRopeStream stream(rope.begin(), rope.size());
            STLOG(PRI_DEBUG, BS_NODE, NW59, "TEvReadMetadataResult", (PDiskId, PDiskKey.PDiskId),
                (Outcome, msg->Outcome), (PDiskGuid, msg->PDiskGuid), (Metadata.size, rope.size()));
            if (msg->Outcome == NPDisk::EPDiskMetadataOutcome::OK && !record.ParseFromZeroCopyStream(&stream)) {
                STLOG(PRI_CRIT, BS_NODE, NW44, "ParseFromString failed for TPDiskMetadataRecord",
                    (PDiskId, PDiskKey.PDiskId));
                msg->Outcome = NPDisk::EPDiskMetadataOutcome::ERROR;
            }
            Send(OriginalEv->Sender, new TEvNodeWardenReadMetadataResult(msg->PDiskGuid, msg->Outcome, std::move(record)),
                0, OriginalEv->Cookie);
            PassAway();
        }

        void Handle(NPDisk::TEvWriteMetadataResult::TPtr ev) {
            auto *msg = ev->Get();
            STLOG(PRI_DEBUG, BS_NODE, NW60, "TEvWriteMetadataResult", (PDiskId, PDiskKey.PDiskId),
                (Outcome, msg->Outcome), (PDiskGuid, msg->PDiskGuid));
            Send(OriginalEv->Sender, new TEvNodeWardenWriteMetadataResult(msg->PDiskGuid, msg->Outcome), 0,
                OriginalEv->Cookie);
            PassAway();
        }

        void PassAway() override {
            Send(ParentId, new TEvPrivate::TEvDereferencePDisk(PDiskKey));
            TActorBootstrapped::PassAway();
        }

        void HandleWakeup() {
            Y_DEBUG_ABORT("Event# %s took too long to process", EventType);
            STLOG(PRI_CRIT, BS_NODE, NW61, "TPDiskMetadataInteractionActor::Wakeup", (EventType, EventType));
        }

        STRICT_STFUNC(StateFunc,
            hFunc(TEvents::TEvUndelivered, Handle);
            hFunc(NPDisk::TEvReadMetadataResult, Handle);
            hFunc(NPDisk::TEvWriteMetadataResult, Handle);
            cFunc(TEvents::TSystem::Wakeup, HandleWakeup);
        )
    };

    void TNodeWarden::Handle(TEvNodeWardenReadMetadata::TPtr ev) {
        const TString& path = ev->Get()->Path;
        STLOG(PRI_DEBUG, BS_NODE, NW56, "TEvNodeWardenReadMetadata", (Path, path));
        Register(new TPDiskMetadataInteractionActor(GetPDiskForMetadata(path), ev.Release(),
            std::make_unique<NPDisk::TEvReadMetadata>(), "TEvNodeWardenReadMetadata"));
    }

    void TNodeWarden::Handle(TEvNodeWardenWriteMetadata::TPtr ev) {
        auto *msg = ev->Get();
        TString data;
        const bool success = msg->Record.SerializeToString(&data);
        Y_ABORT_UNLESS(success);
        const TString& path = msg->Path;
        STLOG(PRI_DEBUG, BS_NODE, NW57, "TEvNodeWardenWriteMetadata", (Path, path), (Metadata.size, data.size()));
        Register(new TPDiskMetadataInteractionActor(GetPDiskForMetadata(path), ev.Release(),
            std::make_unique<NPDisk::TEvWriteMetadata>(TRcBuf(std::move(data))), "TEvNodeWardenWriteMetadata"));
    }

    TPDiskKey TNodeWarden::GetPDiskForMetadata(const TString& path) {
        TPDiskKey key(LocalNodeId, Max<ui32>());
        if (const auto it = PDiskByPath.find(path); it != PDiskByPath.end()) {
            key = it->second.RunningPDiskId;
        } else {
            while (LocalPDisks.contains(key)) {
                --key.PDiskId;
            }

            NKikimrBlobStorage::TNodeWardenServiceSet::TPDisk pdisk;
            pdisk.SetPath(path);
            pdisk.SetNodeID(key.NodeId);
            pdisk.SetPDiskID(key.PDiskId);
            StartLocalPDisk(pdisk, true);
        }

        const auto it = LocalPDisks.find(key);
        Y_ABORT_UNLESS(it != LocalPDisks.end());
        TPDiskRecord& pdisk = it->second;
        ++pdisk.RefCount;

        return key;
    }

    void TNodeWarden::Handle(TEvPrivate::TEvDereferencePDisk::TPtr ev) {
        STLOG(PRI_DEBUG, BS_NODE, NW58, "TEvDereferencePDisk", (PDiskId, ev->Get()->PDiskKey.PDiskId));
        const auto it = LocalPDisks.find(ev->Get()->PDiskKey);
        Y_ABORT_UNLESS(it != LocalPDisks.end());
        TPDiskRecord& pdisk = it->second;
        --pdisk.RefCount;
        if (!pdisk.RefCount && pdisk.Temporary) {
            DestroyLocalPDisk(it->first.PDiskId);
        }
    }

} // NKikimr::NStorage
