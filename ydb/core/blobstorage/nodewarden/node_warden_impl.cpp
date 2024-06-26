#include "node_warden_impl.h"

#include <ydb/core/blobstorage/crypto/secured_block.h>
#include <ydb/core/blobstorage/pdisk/drivedata_serializer.h>
#include <ydb/library/pdisk_io/file_params.h>
#include <ydb/core/base/nameservice.h>

#include <ydb/core/protos/key.pb.h>

using namespace NKikimr;
using namespace NStorage;

void TNodeWarden::RemoveDrivesWithBadSerialsAndReport(TVector<NPDisk::TDriveData>& drives, TStringStream& details) {
    // Serial number's size definitely won't exceed this number of bytes.
    size_t maxSerialSizeInBytes = 100;

    auto isValidSerial = [maxSerialSizeInBytes](TString& serial) {
        if (serial.Size() > maxSerialSizeInBytes) {
            // Not sensible size.
            return false;
        }

        // Check if serial number contains only ASCII characters.
        for (size_t i = 0; i < serial.Size(); ++i) {
            i8 c = serial[i];

            if (c <= 0) {
                // Encountered null terminator earlier than expected or non-ASCII character.
                return false;
            }

            if (!isprint(c)) {
                // Encountered non-printable character.
                return false;
            }
        }

        return true;
    };

    std::unordered_set<TString> drivePaths;

    for (const auto& drive : drives) {
        drivePaths.insert(drive.Path);
    }

    // Remove counters for drives that are no longer present.
    for (auto countersIt = ByPathDriveCounters.begin(); countersIt != ByPathDriveCounters.end();) {
        if (drivePaths.find(countersIt->first) == drivePaths.end()) {
            countersIt = ByPathDriveCounters.erase(countersIt);
        } else {
            countersIt++;
        }
    }

    // Prepare removal of drives with invalid serials.
    auto toRemove = std::remove_if(drives.begin(), drives.end(), [&isValidSerial](auto& driveData) {
        TString& serial = driveData.SerialNumber;

        return !isValidSerial(serial);
    });

    // Add counters for every drive path.
    for (auto it = drives.begin(); it != toRemove; ++it) {
        TString& path = it->Path;
        ByPathDriveCounters.try_emplace(path, AppData()->Counters, path);
    }

    // And for drives with invalid serials log serial and report to the monitoring.
    for (auto it = toRemove; it != drives.end(); ++it) {
        TString& serial = it->SerialNumber;
        TString& path = it->Path;

        auto [mapIt, _] = ByPathDriveCounters.try_emplace(path, AppData()->Counters, path);

        // Cut string in case it exceeds max size.
        size_t size = std::min(serial.Size(), maxSerialSizeInBytes);

        // Encode in case it contains weird symbols.
        TString encoded = Base64Encode(serial.substr(0, size));

        // Output bad serial number in base64 encoding.
        STLOG(PRI_WARN, BS_NODE, NW03, "Bad serial number", (Path, path), (SerialBase64, encoded.Quote()), (Details, details.Str()));

        mapIt->second.BadSerialsRead->Inc();
    }

    // Remove drives with invalid serials.
    drives.erase(toRemove, drives.end());
}

TVector<NPDisk::TDriveData> TNodeWarden::ListLocalDrives() {
    TStringStream details;
    TVector<NPDisk::TDriveData> drives = ListDevicesWithPartlabel(details);

    try {
        TString raw = TFileInput(MockDevicesPath).ReadAll();
        if (google::protobuf::TextFormat::ParseFromString(raw, &MockDevicesConfig)) {
            for (const auto& device : MockDevicesConfig.GetDevices()) {
                NPDisk::TDriveData data;
                DriveDataToDriveData(device, data);
                drives.push_back(data);
            }
        } else {
            STLOG(PRI_WARN, BS_NODE, NW01, "Error parsing mock devices protobuf from file", (Path, MockDevicesPath));
        }
    } catch (...) {
        STLOG(PRI_INFO, BS_NODE, NW90, "Unable to find mock devices file", (Path, MockDevicesPath));
    }

    std::sort(drives.begin(), drives.end(), [] (const auto& lhs, const auto& rhs) {
        return lhs.Path < rhs.Path;
    });

    RemoveDrivesWithBadSerialsAndReport(drives, details);

    return drives;
}

void TNodeWarden::StartInvalidGroupProxy() {
    const ui32 groupId = Max<ui32>();
    STLOG(PRI_DEBUG, BS_NODE, NW11, "StartInvalidGroupProxy", (GroupId, groupId));
    TlsActivationContext->ExecutorThread.ActorSystem->RegisterLocalService(MakeBlobStorageProxyID(groupId), Register(
        CreateBlobStorageGroupEjectedProxy(groupId, DsProxyNodeMon), TMailboxType::ReadAsFilled, AppData()->SystemPoolId));
}

void TNodeWarden::StopInvalidGroupProxy() {
    ui32 groupId = Max<ui32>();
    STLOG(PRI_DEBUG, BS_NODE, NW15, "StopInvalidGroupProxy", (GroupId, groupId));
    TActivationContext::Send(new IEventHandle(TEvents::TSystem::Poison, 0, MakeBlobStorageProxyID(groupId), {}, nullptr, 0));
}

void TNodeWarden::PassAway() {
    STLOG(PRI_DEBUG, BS_NODE, NW25, "PassAway");
    NTabletPipe::CloseClient(SelfId(), PipeClientId);
    StopInvalidGroupProxy();
    TActivationContext::Send(new IEventHandle(TEvents::TSystem::Poison, 0, DsProxyNodeMonActor, {}, nullptr, 0));
    return TActorBootstrapped::PassAway();
}

void TNodeWarden::Bootstrap() {
    STLOG(PRI_DEBUG, BS_NODE, NW26, "Bootstrap");

    LocalNodeId = SelfId().NodeId();
    WhiteboardId = NNodeWhiteboard::MakeNodeWhiteboardServiceId(LocalNodeId);

    Become(&TThis::StateOnline, TDuration::Seconds(10), new TEvPrivate::TEvSendDiskMetrics());

    const auto& dyn = AppData()->DynamicNameserviceConfig;
    ui32 maxStaticNodeId = dyn ? dyn->MaxStaticNodeId : Max<ui32>();
    bool checkNodeDrives = (LocalNodeId <= maxStaticNodeId);
    if (checkNodeDrives) {
        Schedule(TDuration::Seconds(10), new TEvPrivate::TEvUpdateNodeDrives());
    }

    NLwTraceMonPage::ProbeRegistry().AddProbesList(LWTRACE_GET_PROBES(BLOBSTORAGE_PROVIDER));

    TActorSystem *actorSystem = TlsActivationContext->ExecutorThread.ActorSystem;
    if (auto mon = AppData()->Mon) {

        TString name = "NodeWarden";
        TString path = ::to_lower(name);
        NMonitoring::TIndexMonPage *actorsMonPage = mon->RegisterIndexPage("actors", "Actors");

        mon->RegisterActorPage(actorsMonPage, path, name, false, actorSystem, SelfId());
    }

    DsProxyNodeMon = new TDsProxyNodeMon(AppData()->Counters, true);
    DsProxyNodeMonActor = Register(CreateDsProxyNodeMon(DsProxyNodeMon));
    DsProxyPerPoolCounters = new TDsProxyPerPoolCounters(AppData()->Counters);

    if (actorSystem && actorSystem->AppData<TAppData>() && actorSystem->AppData<TAppData>()->Icb) {
        const TIntrusivePtr<NKikimr::TControlBoard>& icb = actorSystem->AppData<TAppData>()->Icb;

        icb->RegisterLocalControl(EnablePutBatching, "BlobStorage_EnablePutBatching");
        icb->RegisterLocalControl(EnableVPatch, "BlobStorage_EnableVPatch");
        icb->RegisterSharedControl(EnableLocalSyncLogDataCutting, "VDiskControls.EnableLocalSyncLogDataCutting");
        icb->RegisterSharedControl(EnableSyncLogChunkCompressionHDD, "VDiskControls.EnableSyncLogChunkCompressionHDD");
        icb->RegisterSharedControl(EnableSyncLogChunkCompressionSSD, "VDiskControls.EnableSyncLogChunkCompressionSSD");
        icb->RegisterSharedControl(MaxSyncLogChunksInFlightHDD, "VDiskControls.MaxSyncLogChunksInFlightHDD");
        icb->RegisterSharedControl(MaxSyncLogChunksInFlightSSD, "VDiskControls.MaxSyncLogChunksInFlightSSD");
        icb->RegisterSharedControl(DefaultHugeGarbagePerMille, "VDiskControls.DefaultHugeGarbagePerMille");

        icb->RegisterSharedControl(CostMetricsParametersByMedia[NPDisk::DEVICE_TYPE_ROT].BurstThresholdNs,
                "VDiskControls.BurstThresholdNsHDD");
        icb->RegisterSharedControl(CostMetricsParametersByMedia[NPDisk::DEVICE_TYPE_SSD].BurstThresholdNs,
                "VDiskControls.BurstThresholdNsSSD");
        icb->RegisterSharedControl(CostMetricsParametersByMedia[NPDisk::DEVICE_TYPE_NVME].BurstThresholdNs,
                "VDiskControls.BurstThresholdNsNVME");
        icb->RegisterSharedControl(CostMetricsParametersByMedia[NPDisk::DEVICE_TYPE_ROT].DiskTimeAvailableScale,
                "VDiskControls.DiskTimeAvailableScaleHDD");
        icb->RegisterSharedControl(CostMetricsParametersByMedia[NPDisk::DEVICE_TYPE_SSD].DiskTimeAvailableScale,
                "VDiskControls.DiskTimeAvailableScaleSSD");
        icb->RegisterSharedControl(CostMetricsParametersByMedia[NPDisk::DEVICE_TYPE_NVME].DiskTimeAvailableScale,
                "VDiskControls.DiskTimeAvailableScaleNVME");

        icb->RegisterLocalControl(SlowDiskThreshold, "DSProxyControls.SlowDiskThreshold");
    }

    // start replication broker
    const auto& replBrokerConfig = Cfg->BlobStorageConfig.GetServiceSet().GetReplBrokerConfig();

    ui64 requestBytesPerSecond = 500000000; // 500 MB/s by default
    if (replBrokerConfig.HasTotalRequestBytesPerSecond()) {
        requestBytesPerSecond = replBrokerConfig.GetTotalRequestBytesPerSecond();
    } else if (replBrokerConfig.HasRateBytesPerSecond()) { // compatibility option
        requestBytesPerSecond = replBrokerConfig.GetRateBytesPerSecond();
    }
    ReplNodeRequestQuoter = std::make_shared<TReplQuoter>(requestBytesPerSecond);

    ui64 responseBytesPerSecond = 500000000; // the same as for request
    if (replBrokerConfig.HasTotalResponseBytesPerSecond()) {
        responseBytesPerSecond = replBrokerConfig.GetTotalResponseBytesPerSecond();
    }
    ReplNodeResponseQuoter = std::make_shared<TReplQuoter>(responseBytesPerSecond);

    const ui64 maxBytes = replBrokerConfig.GetMaxInFlightReadBytes();
    actorSystem->RegisterLocalService(MakeBlobStorageReplBrokerID(), Register(CreateReplBrokerActor(maxBytes)));

    // determine if we are running in 'mock' mode
    EnableProxyMock = Cfg->BlobStorageConfig.GetServiceSet().GetEnableProxyMock();

    // fill in a storage config
    StorageConfig.MutableBlobStorageConfig()->CopyFrom(Cfg->BlobStorageConfig);
    for (const auto& node : Cfg->NameserviceConfig.GetNode()) {
        auto *r = StorageConfig.AddAllNodes();
        r->SetHost(node.GetInterconnectHost());
        r->SetPort(node.GetPort());
        r->SetNodeId(node.GetNodeId());
        if (node.HasLocation()) {
            r->MutableLocation()->CopyFrom(node.GetLocation());
        } else if (node.HasWalleLocation()) {
            r->MutableLocation()->CopyFrom(node.GetWalleLocation());
        }
    }
    StorageConfig.SetClusterUUID(Cfg->NameserviceConfig.GetClusterUUID());

    // Start a statically configured set
    if (Cfg->BlobStorageConfig.HasServiceSet()) {
        const auto& serviceSet = Cfg->BlobStorageConfig.GetServiceSet();
        if (serviceSet.GroupsSize()) {
            ApplyServiceSet(Cfg->BlobStorageConfig.GetServiceSet(), true, false, false, "initial");
        } else {
            Groups.try_emplace(0); // group is gonna be configured soon by DistributedConfigKeeper
        }
        StartStaticProxies();
    }
    EstablishPipe();

    Send(GetNameserviceActorId(), new TEvInterconnect::TEvGetNode(LocalNodeId));

    if (Cfg->IsCacheEnabled()) {
        TActivationContext::Schedule(TDuration::Seconds(5), new IEventHandle(TEvPrivate::EvReadCache, 0, SelfId(), {}, nullptr, 0));
    }

    StartInvalidGroupProxy();

    StartDistributedConfigKeeper();

    HandleGroupPendingQueueTick();
}

void TNodeWarden::HandleReadCache() {
    if (IgnoreCache) {
        return;
    }
    EnqueueSyncOp([this, cfg = Cfg](const TActorContext&) {
        TString data;
        std::exception_ptr ex;
        try {
            data = cfg->CacheAccessor->Read();
        } catch (...) {
            ex = std::current_exception();
        }

        return [=] {
            NKikimrBlobStorage::TNodeWardenCache proto;
            try {
                if (IgnoreCache) {
                    return;
                }

                if (ex) {
                    std::rethrow_exception(ex);
                } else if (!google::protobuf::TextFormat::ParseFromString(data, &proto)) {
                    throw yexception() << "failed to parse node warden cache protobuf";
                }

                STLOG(PRI_INFO, BS_NODE, NW07, "Bootstrap", (Cache, proto));

                if (!proto.HasInstanceId() && !proto.HasAvailDomain() && !proto.HasServiceSet()) {
                    return;
                }

                Y_ABORT_UNLESS(proto.HasInstanceId());
                Y_ABORT_UNLESS(proto.HasAvailDomain() && proto.GetAvailDomain() == AvailDomainId);
                if (!InstanceId) {
                    InstanceId.emplace(proto.GetInstanceId());
                }

                ApplyServiceSet(proto.GetServiceSet(), false, false, false, "cache");
            } catch (...) {
                STLOG(PRI_INFO, BS_NODE, NW16, "Bootstrap failed to fetch cache", (Error, CurrentExceptionMessage()));
                // ignore exception
            }
        };
    });
}

void TNodeWarden::Handle(TEvInterconnect::TEvNodeInfo::TPtr ev) {
    if (const auto& node = ev->Get()->Node) {
        Send(WhiteboardId, new NNodeWhiteboard::TEvWhiteboard::TEvSystemStateUpdate(node->Location));
    }
}

void TNodeWarden::Handle(NPDisk::TEvSlayResult::TPtr ev) {
    const NPDisk::TEvSlayResult &msg = *ev->Get();
    const TVSlotId vslotId(LocalNodeId, msg.PDiskId, msg.VSlotId);
    const auto it = SlayInFlight.find(vslotId);
    Y_DEBUG_ABORT_UNLESS(it != SlayInFlight.end());
    STLOG(PRI_INFO, BS_NODE, NW28, "Handle(NPDisk::TEvSlayResult)", (Msg, msg.ToString()),
        (ExpectedRound, it != SlayInFlight.end() ? std::make_optional(it->second) : std::nullopt));
    if (it == SlayInFlight.end() || it->second != msg.SlayOwnerRound) {
        return; // outdated response
    }
    switch (msg.Status) {
        case NKikimrProto::NOTREADY: {
            const ui64 round = NextLocalPDiskInitOwnerRound();
            TActivationContext::Schedule(TDuration::Seconds(1), new IEventHandle(MakeBlobStoragePDiskID(LocalNodeId,
                msg.PDiskId), SelfId(), new NPDisk::TEvSlay(msg.VDiskId, round, msg.PDiskId, msg.VSlotId)));
            it->second = round;
            break;
        }

        case NKikimrProto::OK:
        case NKikimrProto::ALREADY:
            SlayInFlight.erase(it);
            if (const auto vdiskIt = LocalVDisks.find(vslotId); vdiskIt == LocalVDisks.end()) {
                SendVDiskReport(vslotId, msg.VDiskId, NKikimrBlobStorage::TEvControllerNodeReport::DESTROYED);
            } else {
                SendVDiskReport(vslotId, msg.VDiskId, NKikimrBlobStorage::TEvControllerNodeReport::WIPED);
                TVDiskRecord& vdisk = vdiskIt->second;
                StartLocalVDiskActor(vdisk); // restart actor after successful wiping
            }
            break;

        case NKikimrProto::CORRUPTED: // this branch doesn't really work
        case NKikimrProto::ERROR:
            SlayInFlight.erase(it);
            STLOG(PRI_ERROR, BS_NODE, NW29, "Handle(NPDisk::TEvSlayResult) error", (Msg, msg.ToString()));
            SendVDiskReport(vslotId, msg.VDiskId, NKikimrBlobStorage::TEvControllerNodeReport::OPERATION_ERROR);
            break;

        case NKikimrProto::RACE:
            Y_ABORT("Unexpected# %s", msg.ToString().data());
            break;

        default:
            Y_ABORT("Unexpected status# %s", msg.ToString().data());
            break;
    };
}

void TNodeWarden::Handle(TEvRegisterPDiskLoadActor::TPtr ev) {
    Send(ev.Get()->Sender, new TEvRegisterPDiskLoadActorResult(NextLocalPDiskInitOwnerRound()));
}

void TNodeWarden::Handle(TEvBlobStorage::TEvControllerNodeServiceSetUpdate::TPtr ev) {
    const auto& record = ev->Get()->Record;

    if (record.HasAvailDomain() && record.GetAvailDomain() != AvailDomainId) {
        // AvailDomain may arrive unset
        STLOG_DEBUG_FAIL(BS_NODE, NW02, "unexpected AvailDomain from BS_CONTROLLER", (Msg, record), (AvailDomainId, AvailDomainId));
        return;
    }
    if (record.HasInstanceId()) {
        if (record.GetInstanceId() != InstanceId.value_or(record.GetInstanceId())) {
            STLOG_DEBUG_FAIL(BS_NODE, NW14, "unexpected/unset InstanceId from BS_CONTROLLER", (Msg, record), (InstanceId, InstanceId));
            return;
        }
        InstanceId.emplace(record.GetInstanceId());
    }

    if (record.HasServiceSet()) {
        const bool comprehensive = record.GetComprehensive();
        IgnoreCache |= comprehensive;
        STLOG(PRI_DEBUG, BS_NODE, NW17, "Handle(TEvBlobStorage::TEvControllerNodeServiceSetUpdate)", (Msg, record));
        ApplyServiceSet(record.GetServiceSet(), false, comprehensive, true, "controller");
    }

    for (const auto& item : record.GetGroupMetadata()) {
        const ui32 groupId = item.GetGroupId();
        const ui32 generation = item.GetCurrentGeneration();
        if (const auto it = Groups.find(groupId); it != Groups.end() && it->second.MaxKnownGeneration < generation) {
            ApplyGroupInfo(groupId, generation, nullptr, false, false);
        }
    }
}

void TNodeWarden::SendDropDonorQuery(ui32 nodeId, ui32 pdiskId, ui32 vslotId, const TVDiskID& vdiskId, TDuration backoff) {
    STLOG(PRI_NOTICE, BS_NODE, NW87, "SendDropDonorQuery", (NodeId, nodeId), (PDiskId, pdiskId), (VSlotId, vslotId),
        (VDiskId, vdiskId));
    if (TGroupID groupId(vdiskId.GroupID); groupId.ConfigurationType() == EGroupConfigurationType::Static) {
        auto ev = std::make_unique<TEvNodeConfigInvokeOnRoot>();
        auto *record = &ev->Record;
        auto *cmd = record->MutableDropDonor();
        VDiskIDFromVDiskID(vdiskId, cmd->MutableVDiskId());
        auto *slot = cmd->MutableVSlotId();
        slot->SetNodeId(nodeId);
        slot->SetPDiskId(pdiskId);
        slot->SetVSlotId(vslotId);
        const ui64 cookie = NextInvokeCookie++;
        if (backoff != TDuration::Zero()) {
            TActivationContext::Schedule(backoff, new IEventHandle(DistributedConfigKeeperId, SelfId(), ev.release(), 0, cookie));
        } else {
            Send(DistributedConfigKeeperId, ev.release(), 0, cookie);
        }
        InvokeCallbacks.emplace(cookie, [=](TEvNodeConfigInvokeOnRootResult& msg) {
            if (msg.Record.GetStatus() != NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK) {
                for (const auto& vdisk : StorageConfig.GetBlobStorageConfig().GetServiceSet().GetVDisks()) {
                    const TVDiskID currentVDiskId = VDiskIDFromVDiskID(vdisk.GetVDiskID());
                    const auto& loc = vdisk.GetVDiskLocation();
                    if (currentVDiskId.SameExceptGeneration(vdiskId) &&
                            (currentVDiskId.GroupGeneration == vdiskId.GroupGeneration || !vdiskId.GroupGeneration) &&
                            loc.GetNodeID() == nodeId && loc.GetPDiskID() == pdiskId &&
                            loc.GetVDiskSlotID() == vslotId) {
                        SendDropDonorQuery(nodeId, pdiskId, vslotId, vdiskId, TDuration::Seconds(3));
                        break;
                    }
                }
            }
        });
    } else {
        auto ev = std::make_unique<TEvBlobStorage::TEvControllerConfigRequest>();
        auto& record = ev->Record;
        auto *request = record.MutableRequest();
        auto *cmd = request->AddCommand()->MutableDropDonorDisk();
        auto *p = cmd->MutableVSlotId();
        p->SetNodeId(nodeId);
        p->SetPDiskId(pdiskId);
        p->SetVSlotId(vslotId);
        VDiskIDFromVDiskID(vdiskId, cmd->MutableVDiskId());
        SendToController(std::move(ev));
    }
}

void TNodeWarden::SendVDiskReport(TVSlotId vslotId, const TVDiskID &vDiskId,
        NKikimrBlobStorage::TEvControllerNodeReport::EVDiskPhase phase, TDuration backoff) {
    STLOG(PRI_DEBUG, BS_NODE, NW32, "SendVDiskReport", (VSlotId, vslotId), (VDiskId, vDiskId), (Phase, phase));

    if (TGroupID groupId(vDiskId.GroupID); groupId.ConfigurationType() == EGroupConfigurationType::Static &&
            phase == NKikimrBlobStorage::TEvControllerNodeReport::DESTROYED) {
        auto ev = std::make_unique<TEvNodeConfigInvokeOnRoot>();
        auto *record = &ev->Record;
        auto *cmd = record->MutableStaticVDiskSlain();
        VDiskIDFromVDiskID(vDiskId, cmd->MutableVDiskId());
        auto *slot = cmd->MutableVSlotId();
        slot->SetNodeId(vslotId.NodeId);
        slot->SetPDiskId(vslotId.PDiskId);
        slot->SetVSlotId(vslotId.VDiskSlotId);
        const ui64 cookie = NextInvokeCookie++;
        if (backoff != TDuration::Zero()) {
            TActivationContext::Schedule(backoff, new IEventHandle(DistributedConfigKeeperId, SelfId(), ev.release(), 0, cookie));
        } else {
            Send(DistributedConfigKeeperId, ev.release(), 0, cookie);
        }
        InvokeCallbacks.emplace(cookie, [=](TEvNodeConfigInvokeOnRootResult& msg) {
            if (msg.Record.GetStatus() != NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK) {
                for (const auto& vdisk : StorageConfig.GetBlobStorageConfig().GetServiceSet().GetVDisks()) {
                    const TVDiskID currentVDiskId = VDiskIDFromVDiskID(vdisk.GetVDiskID());
                    const auto& loc = vdisk.GetVDiskLocation();
                    if (currentVDiskId == vDiskId && loc.GetNodeID() == vslotId.NodeId &&
                            loc.GetPDiskID() == vslotId.PDiskId &&
                            loc.GetVDiskSlotID() == vslotId.VDiskSlotId &&
                            vdisk.GetEntityStatus() == NKikimrBlobStorage::EEntityStatus::DESTROY) {
                        SendVDiskReport(vslotId, vDiskId, phase, TDuration::Seconds(3));
                        break;
                    }
                }
            }
        });
    } else {
        auto report = std::make_unique<TEvBlobStorage::TEvControllerNodeReport>(vslotId.NodeId);
        auto *vReport = report->Record.AddVDiskReports();
        auto *id = vReport->MutableVSlotId();
        id->SetNodeId(vslotId.NodeId);
        id->SetPDiskId(vslotId.PDiskId);
        id->SetVSlotId(vslotId.VDiskSlotId);
        VDiskIDFromVDiskID(vDiskId, vReport->MutableVDiskId());
        vReport->SetPhase(phase);
        SendToController(std::move(report));
    }
}

void TNodeWarden::Handle(TEvNodeConfigInvokeOnRootResult::TPtr ev) {
    if (auto nh = InvokeCallbacks.extract(ev->Cookie)) {
        nh.mapped()(*ev->Get());
    }
}

void TNodeWarden::Handle(TEvBlobStorage::TEvAskWardenRestartPDisk::TPtr ev) {
    auto pdiskId = ev->Get()->PDiskId;
    auto requestCookie = ev->Cookie;

    for (auto it = PDiskRestartRequests.begin(); it != PDiskRestartRequests.end(); it++) {
        if (it->second == pdiskId) {
            const TActorId actorId = MakeBlobStoragePDiskID(LocalNodeId, pdiskId);

            Cfg->PDiskKey.Initialize();
            Send(actorId, new TEvBlobStorage::TEvAskWardenRestartPDiskResult(pdiskId, Cfg->PDiskKey, false, nullptr, "Restart already requested"));

            return;
        }
    }

    PDiskRestartRequests[requestCookie] = pdiskId;

    AskBSCToRestartPDisk(pdiskId, requestCookie);
}

void TNodeWarden::Handle(TEvBlobStorage::TEvNotifyWardenPDiskRestarted::TPtr ev) {
    OnPDiskRestartFinished(ev->Get()->PDiskId, ev->Get()->Status);
}

void TNodeWarden::Handle(TEvBlobStorage::TEvControllerConfigResponse::TPtr ev) {
    if (auto nh = ConfigInFlight.extract(ev->Cookie)) {
        nh.mapped()(ev->Get());
    }
}

void TNodeWarden::Handle(TEvBlobStorage::TEvControllerUpdateDiskStatus::TPtr ev) {
    STLOG(PRI_TRACE, BS_NODE, NW38, "Handle(TEvBlobStorage::TEvControllerUpdateDiskStatus)");

    auto differs = [](const auto& updated, const auto& current) {
        TString xUpdated, xCurrent;
        bool success = updated.SerializeToString(&xUpdated);
        Y_ABORT_UNLESS(success);
        success = current.SerializeToString(&xCurrent);
        Y_ABORT_UNLESS(success);
        return xUpdated != xCurrent;
    };

    auto& record = ev->Get()->Record;

    std::unique_ptr<TEvBlobStorage::TEvControllerUpdateDiskStatus> updateDiskStatus;

    for (const NKikimrBlobStorage::TVDiskMetrics& m : record.GetVDisksMetrics()) {
        Y_ABORT_UNLESS(m.HasVSlotId());
        const TVSlotId vslotId(m.GetVSlotId());
        if (const auto it = LocalVDisks.find(vslotId); it != LocalVDisks.end()) {
            TVDiskRecord& vdisk = it->second;
            if (vdisk.VDiskMetrics) {
                auto& current = *vdisk.VDiskMetrics;
                NKikimrBlobStorage::TVDiskMetrics updated(current);
                updated.MergeFrom(m);
                if (differs(updated, current)) {
                    current.Swap(&updated);
                    VDisksWithUnreportedMetrics.PushBack(&vdisk);
                }
            } else {
                if (!updateDiskStatus) {
                    updateDiskStatus.reset(new TEvBlobStorage::TEvControllerUpdateDiskStatus);
                }
                updateDiskStatus->Record.AddVDisksMetrics()->CopyFrom(m);
                vdisk.VDiskMetrics.emplace(m);
            }
        }
    }

    for (const NKikimrBlobStorage::TPDiskMetrics& m : record.GetPDisksMetrics()) {
        Y_ABORT_UNLESS(m.HasPDiskId());
        if (const auto it = LocalPDisks.find({LocalNodeId, m.GetPDiskId()}); it != LocalPDisks.end()) {
            TPDiskRecord& pdisk = it->second;
            if (pdisk.PDiskMetrics) {
                auto& current = *pdisk.PDiskMetrics;
                if (differs(m, current)) {
                    current.CopyFrom(m);
                    PDisksWithUnreportedMetrics.PushBack(&pdisk);
                }
            } else {
                if (!updateDiskStatus) {
                    updateDiskStatus.reset(new TEvBlobStorage::TEvControllerUpdateDiskStatus);
                }
                updateDiskStatus->Record.AddPDisksMetrics()->CopyFrom(m);
                pdisk.PDiskMetrics.emplace(m);
            }
        }
    }

    if (updateDiskStatus) {
        SendToController(std::move(updateDiskStatus));
    }
}

void TNodeWarden::Handle(TEvBlobStorage::TEvControllerGroupMetricsExchange::TPtr ev) {
    SendToController(std::unique_ptr<IEventBase>(ev->Release().Release()), ev->Cookie, ev->Sender);
}

void TNodeWarden::Handle(TEvPrivate::TEvSendDiskMetrics::TPtr&) {
    STLOG(PRI_TRACE, BS_NODE, NW39, "Handle(TEvPrivate::TEvSendDiskMetrics)");
    SendDiskMetrics(true);
    ReportLatencies();
    Schedule(TDuration::Seconds(10), new TEvPrivate::TEvSendDiskMetrics());
}

void TNodeWarden::Handle(TEvPrivate::TEvUpdateNodeDrives::TPtr&) {
    STLOG(PRI_TRACE, BS_NODE, NW88, "Handle(TEvPrivate::UpdateNodeDrives)");
    EnqueueSyncOp([this] (const TActorContext&) {
        auto drives = ListLocalDrives();

        return [this, drives = std::move(drives)] () {
            if (drives != WorkingLocalDrives) {
                SendToController(std::make_unique<TEvBlobStorage::TEvControllerUpdateNodeDrives>(LocalNodeId, drives));
                WorkingLocalDrives = std::move(drives);
            }
        };
    });
    Schedule(TDuration::Seconds(10), new TEvPrivate::TEvUpdateNodeDrives());
}

void TNodeWarden::SendDiskMetrics(bool reportMetrics) {
    STLOG(PRI_TRACE, BS_NODE, NW45, "SendDiskMetrics", (ReportMetrics, reportMetrics));

    auto ev = std::make_unique<TEvBlobStorage::TEvControllerUpdateDiskStatus>();
    auto& record = ev->Record;

    if (reportMetrics) {
        for (auto& vdisk : std::exchange(VDisksWithUnreportedMetrics, {})) {
            Y_ABORT_UNLESS(vdisk.VDiskMetrics);
            record.AddVDisksMetrics()->CopyFrom(*vdisk.VDiskMetrics);
        }
        for (auto& pdisk : std::exchange(PDisksWithUnreportedMetrics, {})) {
            Y_ABORT_UNLESS(pdisk.PDiskMetrics);
            record.AddPDisksMetrics()->CopyFrom(*pdisk.PDiskMetrics);
        }
    }

    FillInVDiskStatus(record.MutableVDiskStatus(), false);

    if (record.VDisksMetricsSize() || record.PDisksMetricsSize() || record.VDiskStatusSize()) { // anything to report?
        SendToController(std::move(ev));
    }
}

void TNodeWarden::Handle(TEvStatusUpdate::TPtr ev) {
    STLOG(PRI_DEBUG, BS_NODE, NW47, "Handle(TEvStatusUpdate)");
    auto *msg = ev->Get();
    const TVSlotId vslotId(msg->NodeId, msg->PDiskId, msg->VSlotId);
    if (const auto it = LocalVDisks.find(vslotId); it != LocalVDisks.end() && (it->second.Status != msg->Status ||
            it->second.OnlyPhantomsRemain != msg->OnlyPhantomsRemain)) {
        auto& vdisk = it->second;
        vdisk.Status = msg->Status;
        vdisk.OnlyPhantomsRemain = msg->OnlyPhantomsRemain;
        VDiskStatusChanged = true;

        if (msg->Status == NKikimrBlobStorage::EVDiskStatus::READY && vdisk.WhiteboardVDiskId) {
            Send(WhiteboardId, new NNodeWhiteboard::TEvWhiteboard::TEvVDiskDropDonors(*vdisk.WhiteboardVDiskId,
                vdisk.WhiteboardInstanceGuid, NNodeWhiteboard::TEvWhiteboard::TEvVDiskDropDonors::TDropAllDonors()));
        }

        if (vdisk.Status == NKikimrBlobStorage::EVDiskStatus::READY && vdisk.RuntimeData) {
            const auto& r = vdisk.RuntimeData;
            const auto& info = r->GroupInfo;

            if (const ui32 groupId = info->GroupID.GetRawId(); TGroupID(groupId).ConfigurationType() == EGroupConfigurationType::Static) {
                for (const auto& item : StorageConfig.GetBlobStorageConfig().GetServiceSet().GetVDisks()) {
                    const TVDiskID vdiskId = VDiskIDFromVDiskID(item.GetVDiskID());
                    if (vdiskId.GroupID.GetRawId() == groupId && info->GetTopology().GetOrderNumber(vdiskId) == r->OrderNumber &&
                            item.HasDonorMode() && item.GetEntityStatus() != NKikimrBlobStorage::EEntityStatus::DESTROY) {
                        SendDropDonorQuery(vslotId.NodeId, vslotId.PDiskId, vslotId.VDiskSlotId, TVDiskID(TGroupId::FromValue(groupId), 0,
                            info->GetVDiskId(r->OrderNumber)));
                        break;
                    }
                }
            }
        }
    }
}

void TNodeWarden::FillInVDiskStatus(google::protobuf::RepeatedPtrField<NKikimrBlobStorage::TVDiskStatus> *pb, bool initial) {
    for (auto& [vslotId, vdisk] : LocalVDisks) {
        const NKikimrBlobStorage::EVDiskStatus status = vdisk.RuntimeData
            ? vdisk.Status
            : NKikimrBlobStorage::EVDiskStatus::ERROR;

        const bool onlyPhantomsRemain = status == NKikimrBlobStorage::EVDiskStatus::REPLICATING ? vdisk.OnlyPhantomsRemain : false;

        if (initial || status != vdisk.ReportedVDiskStatus || onlyPhantomsRemain != vdisk.ReportedOnlyPhantomsRemain) {
            auto *item = pb->Add();
            VDiskIDFromVDiskID(vdisk.GetVDiskId(), item->MutableVDiskId());
            item->SetNodeId(vslotId.NodeId);
            item->SetPDiskId(vslotId.PDiskId);
            item->SetVSlotId(vslotId.VDiskSlotId);
            item->SetPDiskGuid(vdisk.Config.GetVDiskLocation().GetPDiskGuid());
            item->SetStatus(status);
            item->SetOnlyPhantomsRemain(onlyPhantomsRemain);
            vdisk.ReportedVDiskStatus = status;
            vdisk.ReportedOnlyPhantomsRemain = onlyPhantomsRemain;
        }
    }
}

bool ObtainKey(TEncryptionKey *key, const NKikimrProto::TKeyRecord& record) {
    TString containerPath = record.GetContainerPath();
    TString pin = record.GetPin();
    TString keyId = record.GetId();
    ui64 version = record.GetVersion();

    TFileHandle containerFile(containerPath, OpenExisting | RdOnly);
    if (!containerFile.IsOpen()) {
        Cerr << "Can't open key container file# \"" << EscapeC(containerPath) << "\", make sure the file actually exists." << Endl;
        return false;
    }
    ui64 length = containerFile.GetLength();
    if (length == 0) {
        Cerr << "Key container file# \"" << EscapeC(containerPath) << "\" size is 0, make sure the file actually contains the key!" << Endl;
        return false;
    }
    TString data = TString::Uninitialized(length);
    size_t bytesRead = containerFile.Read(data.Detach(), length);
    if (bytesRead != length) {
        Cerr << "Key container file# \"" << EscapeC(containerPath) << "\" could not be read! Expected length# " << length
            << " bytesRead# " << bytesRead << ", make sure the file stays put!" << Endl;
        return false;
    }
    THashCalculator hasher;
    if (pin.size() == 0) {
        pin = "EmptyPin";
    }

    ui8 *keyBytes = 0;
    ui32 keySize = 0;
    key->Key.MutableKeyBytes(&keyBytes, &keySize);
    Y_ABORT_UNLESS(keySize == 4 * sizeof(ui64));
    ui64 *p = (ui64*)keyBytes;

    hasher.SetKey((const ui8*)pin.data(), pin.size());
    hasher.Hash(data.Detach(), data.size());
    p[0] = hasher.GetHashResult(&p[1]);
    hasher.Clear();
    hasher.SetKey((const ui8*)pin.data(), pin.size());
    TString saltBefore = "SaltBefore";
    TString saltAfter = "SaltAfter";
    hasher.Hash(saltBefore.data(), saltBefore.size());
    hasher.Hash(data.Detach(), data.size());
    hasher.Hash(saltAfter.data(), saltAfter.size());
    p[2] = hasher.GetHashResult(&p[3]);

    key->Version = version;
    key->Id = keyId;

    SecureWipeBuffer((ui8*)data.Detach(), data.size());

    return true;
}

bool NKikimr::ObtainTenantKey(TEncryptionKey *key, const NKikimrProto::TKeyConfig& keyConfig) {
    if (keyConfig.KeysSize()) {
        // TODO(cthulhu): process muliple keys here.
        auto &record = keyConfig.GetKeys(0);
        return ObtainKey(key, record);
    } else {
        STLOG(PRI_INFO, BS_NODE, NW66, "No Keys in KeyConfig! Encrypted group DsProxies will not start");
        return false;
    }
}

bool NKikimr::ObtainPDiskKey(NPDisk::TMainKey *mainKey, const NKikimrProto::TKeyConfig& keyConfig) {
    Y_ABORT_UNLESS(mainKey);
    *mainKey = NPDisk::TMainKey{};

    ui32 keysSize = keyConfig.KeysSize();
    if (!keysSize) {
        STLOG(PRI_INFO, BS_NODE, NW69, "No Keys in PDiskKeyConfig! Encrypted pdisks will not start");
        mainKey->ErrorReason = "Empty PDiskKeyConfig";
        mainKey->Keys = { NPDisk::YdbDefaultPDiskSequence };
        mainKey->IsInitialized = true;
        return false;
    }

    TVector<TEncryptionKey> keys(keysSize);
    for (ui32 i = 0; i < keysSize; ++i) {
        auto &record = keyConfig.GetKeys(i);
        if (record.GetId() == "0" && record.GetContainerPath() == "") {
            // use default pdisk key
            keys[i].Id = "0";
            keys[i].Version = record.GetVersion();

            ui8 *keyBytes = 0;
            ui32 keySize = 0;
            keys[i].Key.MutableKeyBytes(&keyBytes, &keySize);

            ui64* p = (ui64*)keyBytes;
            p[0] = NPDisk::YdbDefaultPDiskSequence;
        } else {
            if (!ObtainKey(&keys[i], record)) {
                mainKey->Keys = {};
                mainKey->ErrorReason = "Cannot obtain key, ContainerPath# " + record.GetContainerPath();
                mainKey->IsInitialized = true;
                return false;
            }
        }
    }

    std::sort(keys.begin(), keys.end(), [&](const TEncryptionKey& l, const TEncryptionKey& r) {
        return l.Version < r.Version;
    });

    for (ui32 i = 0; i < keys.size(); ++i) {
        const ui8 *key;
        ui32 keySize;
        keys[i].Key.GetKeyBytes(&key, &keySize);
        Y_DEBUG_ABORT_UNLESS(keySize == 4 * sizeof(ui64));
        mainKey->Keys.push_back(*(ui64*)key);
    }
    mainKey->IsInitialized = true;
    return true;
}


bool NKikimr::ObtainStaticKey(TEncryptionKey *key) {
    // TODO(cthulhu): Replace this with real data
    key->Key.SetKey((ui8*)"TestStaticKey", 13);
    key->Version = 1;
    key->Id = "TestStaticKeyId";
    return true;
}

IActor* NKikimr::CreateBSNodeWarden(const TIntrusivePtr<TNodeWardenConfig> &cfg) {
    return new NStorage::TNodeWarden(cfg);
}
