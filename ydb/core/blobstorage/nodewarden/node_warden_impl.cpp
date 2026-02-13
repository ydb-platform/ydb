#include "node_warden.h"
#include "node_warden_events.h"
#include "node_warden_impl.h"
#include "distconf.h"

#include <google/protobuf/util/message_differencer.h>
#include <ydb/core/config/validation/validators.h>
#include <ydb/core/blobstorage/common/immediate_control_defaults.h>
#include <ydb/core/blobstorage/crypto/secured_block.h>
#include <ydb/core/blobstorage/dsproxy/dsproxy.h>
#include <ydb/core/blobstorage/dsproxy/dsproxy_request_reporting.h>
#include <ydb/core/blobstorage/dsproxy/dsproxy_nodemonactor.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_data.h>
#include <ydb/core/blobstorage/pdisk/drivedata_serializer.h>
#include <ydb/core/blobstorage/vdisk/repl/blobstorage_replbroker.h>
#include <ydb/core/blobstorage/vdisk/syncer/blobstorage_syncer_broker.h>
#include <ydb/library/pdisk_io/file_params.h>
#include <ydb/core/control/lib/immediate_control_board_impl.h>
#include <ydb/core/mind/bscontroller/yaml_config_helpers.h>
#include <ydb/core/base/nameservice.h>
#include <ydb/core/protos/key.pb.h>
#include <util/folder/dirut.h>

#include <library/cpp/lwtrace/mon/mon_lwtrace.h>

using namespace NKikimr;
using namespace NStorage;

TNodeWarden::TNodeWarden(const TIntrusivePtr<TNodeWardenConfig> &cfg)
    : Cfg(cfg)
    , EnablePutBatching(Cfg->FeatureFlags.GetEnablePutBatchingForBlobStorage(), false, true)
    , EnableVPatch(Cfg->FeatureFlags.GetEnableVPatch(), false, true)
    , EnableLocalSyncLogDataCutting(0, 0, 1)
    , EnableSyncLogChunkCompressionHDD(1, 0, 1)
    , EnableSyncLogChunkCompressionSSD(0, 0, 1)
    , MaxSyncLogChunksInFlightHDD(10, 1, 1024)
    , MaxSyncLogChunksInFlightSSD(10, 1, 1024)
    , DefaultHugeGarbagePerMille(300, 1, 1000)
    , HugeDefragFreeSpaceBorderPerMille(260, 1, 1000)
    , MaxChunksToDefragInflight(10, 1, 1000)
    , FreshCompMaxInFlightWrites(10, 1, 1000)
    , FreshCompMaxInFlightReads(10, 1, 1000)
    , HullCompMaxInFlightWrites(10, 1, 1000)
    , HullCompMaxInFlightReads(20, 1, 1000)
    , HullCompFullCompPeriodSec(0, 0, 7 * 24 * 60 * 60)
    , HullCompThrottlerBytesRate(0, 0, 10'000'000'000) // 10 GB/s
    , GarbageThresholdToRunFullCompactionPerMille(0, 0, 300)
    , DefragThrottlerBytesRate(0, 0, 10'000'000'000) // 10 GB/s
    , ThrottlingDryRun(1, 0, 1)
    , ThrottlingMinLevel0SstCount(100, 1, 100000)
    , ThrottlingMaxLevel0SstCount(250, 1, 100000)
    , ThrottlingMinInplacedSizeHDD(20ull << 30, 1 << 20, 500ull << 40)
    , ThrottlingMaxInplacedSizeHDD(60ull << 30, 1 << 20, 500ull << 40)
    , ThrottlingMinInplacedSizeSSD(20ull << 30, 1 << 20, 500ull << 40)
    , ThrottlingMaxInplacedSizeSSD(60ull << 30, 1 << 20, 500ull << 40)
    , ThrottlingMinOccupancyPerMille(900, 1, 1000)
    , ThrottlingMaxOccupancyPerMille(950, 1, 1000)
    , ThrottlingMinLogChunkCount(100, 1, 100000)
    , ThrottlingMaxLogChunkCount(130, 1, 100000)
    , MaxInProgressSyncCount(0, 0, 1000)
    , EnablePhantomFlagStorage(0, 0, 1)
    , PhantomFlagStorageLimitPerVDiskBytes(10'000'000, 0, 100'000'000'000)
    , MaxCommonLogChunksHDD(NPDisk::MaxCommonLogChunks, 1, 1'000'000)
    , MaxCommonLogChunksSSD(NPDisk::MaxCommonLogChunks, 1, 1'000'000)
    , CommonStaticLogChunks(NPDisk::CommonStaticLogChunks, 1, 1'000'000)
    , CostMetricsParametersByMedia({
        TCostMetricsParameters{200},
        TCostMetricsParameters{50},
        TCostMetricsParameters{32},
    })
    , SlowDiskThreshold(std::round(DefaultSlowDiskThreshold * 1000), 1, 1'000'000)
    , SlowDiskThresholdHDD(std::round(DefaultSlowDiskThreshold * 1000), 1, 1'000'000)
    , SlowDiskThresholdSSD(std::round(DefaultSlowDiskThreshold * 1000), 1, 1'000'000)
    , PredictedDelayMultiplier(std::round(DefaultPredictedDelayMultiplier * 1000), 0, 1'000'000)
    , PredictedDelayMultiplierHDD(std::round(DefaultPredictedDelayMultiplier * 1000), 0, 1'000'000)
    , PredictedDelayMultiplierSSD(std::round(DefaultPredictedDelayMultiplier * 1000), 0, 1'000'000)
    , MaxNumOfSlowDisks(DefaultMaxNumOfSlowDisks, 1, 2)
    , MaxNumOfSlowDisksHDD(DefaultMaxNumOfSlowDisks, 1, 2)
    , MaxNumOfSlowDisksSSD(DefaultMaxNumOfSlowDisks, 1, 2)
    , LongRequestThresholdMs(50'000, 1, 1'000'000)
    , ReportingControllerBucketSize(1, 1, 100'000)
    , ReportingControllerLeakDurationMs(60'000, 1, 3'600'000)
    , ReportingControllerLeakRate(1, 1, 100'000)
    , MaxPutTimeoutSeconds(DefaultMaxPutTimeout.Seconds(), 1, 1'000'000)
    , EnableDeepScrubbing(false, false, true)
{
    Y_ABORT_UNLESS(Cfg->BlobStorageConfig.GetServiceSet().AvailabilityDomainsSize() <= 1);
    AvailDomainId = 1;
    for (const auto& domain : Cfg->BlobStorageConfig.GetServiceSet().GetAvailabilityDomains()) {
        AvailDomainId = domain;
    }
    if (Cfg->DomainsConfig) {
        for (const auto& ssconf : Cfg->DomainsConfig->GetStateStorage()) {
            BuildStateStorageInfos(ssconf, StateStorageInfo, BoardInfo, SchemeBoardInfo);
            StateStorageProxyConfigured = true;
        }
    }
}

STATEFN(TNodeWarden::StateOnline) {
    switch (ev->GetTypeRewrite()) {
        fFunc(TEvBlobStorage::TEvPut::EventType, HandleForwarded);
        fFunc(TEvBlobStorage::TEvGet::EventType, HandleForwarded);
        fFunc(TEvBlobStorage::TEvGetBlock::EventType, HandleForwarded);
        fFunc(TEvBlobStorage::TEvBlock::EventType, HandleForwarded);
        fFunc(TEvBlobStorage::TEvPatch::EventType, HandleForwarded);
        fFunc(TEvBlobStorage::TEvDiscover::EventType, HandleForwarded);
        fFunc(TEvBlobStorage::TEvRange::EventType, HandleForwarded);
        fFunc(TEvBlobStorage::TEvCollectGarbage::EventType, HandleForwarded);
        fFunc(TEvBlobStorage::TEvStatus::EventType, HandleForwarded);
        fFunc(TEvBlobStorage::TEvAssimilate::EventType, HandleForwarded);
        fFunc(TEvBlobStorage::TEvBunchOfEvents::EventType, HandleForwarded);
        fFunc(TEvBlobStorage::TEvCheckIntegrity::EventType, HandleForwarded);
        fFunc(TEvRequestProxySessionsState::EventType, HandleForwarded);

        cFunc(TEvPrivate::EvGroupPendingQueueTick, HandleGroupPendingQueueTick);

        hFunc(NIncrHuge::TEvIncrHugeInit, HandleIncrHugeInit);

        hFunc(TEvInterconnect::TEvNodeInfo, Handle);
        hFunc(TEvInterconnect::TEvNodesInfo, Handle);

        hFunc(TEvTabletPipe::TEvClientConnected, Handle);
        hFunc(TEvTabletPipe::TEvClientDestroyed, Handle);

        hFunc(NPDisk::TEvSlayResult, Handle);
        hFunc(NPDisk::TEvShredPDiskResult, Handle);
        hFunc(NPDisk::TEvShredPDisk, Handle);
        hFunc(NPDisk::TEvChangeExpectedSlotCountResult, Handle);

        hFunc(TEvRegisterPDiskLoadActor, Handle);

        hFunc(TEvStatusUpdate, Handle);
        hFunc(TEvBlobStorage::TEvDropDonor, Handle);
        hFunc(TEvBlobStorage::TEvAskRestartVDisk, Handle);
        hFunc(TEvBlobStorage::TEvAskWardenRestartPDisk, Handle);
        hFunc(TEvBlobStorage::TEvNotifyWardenPDiskRestarted, Handle);

        hFunc(TEvGroupStatReport, Handle);

        hFunc(TEvBlobStorage::TEvControllerNodeServiceSetUpdate, Handle);
        hFunc(TEvBlobStorage::TEvUpdateGroupInfo, Handle);
        hFunc(TEvBlobStorage::TEvControllerUpdateDiskStatus, Handle);
        hFunc(TEvBlobStorage::TEvControllerGroupMetricsExchange, Handle);
        hFunc(TEvPrivate::TEvSendDiskMetrics, Handle);
        hFunc(TEvPrivate::TEvUpdateNodeDrives, Handle);
        hFunc(TEvPrivate::TEvRetrySaveConfig, Handle);

        hFunc(NMon::TEvHttpInfo, Handle);
        cFunc(NActors::TEvents::TSystem::Poison, PassAway);

        hFunc(TEvBlobStorage::TEvControllerScrubQueryStartQuantum, Handle);
        hFunc(TEvBlobStorage::TEvControllerScrubStartQuantum, Handle);
        hFunc(TEvBlobStorage::TEvControllerScrubQuantumFinished, Handle);

        hFunc(TEvents::TEvInvokeResult, Handle);

        hFunc(TEvNodeWardenQueryGroupInfo, Handle);
        hFunc(TEvNodeWardenQueryStorageConfig, Handle);
        hFunc(TEvNodeWardenStorageConfig, Handle);
        fFunc(TEvents::TSystem::Unsubscribe, HandleUnsubscribe);

        // proxy requests for the NodeWhiteboard to prevent races
        hFunc(NNodeWhiteboard::TEvWhiteboard::TEvBSGroupStateUpdate, Handle);

        hFunc(TEvBlobStorage::TEvControllerConfigRequest, Handle);
        hFunc(TEvBlobStorage::TEvControllerConfigResponse, Handle);

        cFunc(TEvPrivate::EvReadCache, HandleReadCache);
        fFunc(TEvPrivate::EvGetGroup, HandleGetGroup);

        fFunc(TEvBlobStorage::EvNodeConfigPush, ForwardToDistributedConfigKeeper);
        fFunc(TEvBlobStorage::EvNodeConfigReversePush, ForwardToDistributedConfigKeeper);
        fFunc(TEvBlobStorage::EvNodeConfigUnbind, ForwardToDistributedConfigKeeper);
        fFunc(TEvBlobStorage::EvNodeConfigScatter, ForwardToDistributedConfigKeeper);
        fFunc(TEvBlobStorage::EvNodeConfigGather, ForwardToDistributedConfigKeeper);
        fFunc(TEvBlobStorage::EvNodeConfigInvokeOnRoot, ForwardToDistributedConfigKeeper);
        fFunc(TEvBlobStorage::EvNodeWardenDynamicConfigSubscribe, ForwardToDistributedConfigKeeper);
        fFunc(TEvBlobStorage::EvNodeWardenDynamicConfigPush, ForwardToDistributedConfigKeeper);
        fFunc(TEvBlobStorage::EvNodeWardenUpdateCache, ForwardToDistributedConfigKeeper);
        fFunc(TEvBlobStorage::EvNodeWardenQueryCache, ForwardToDistributedConfigKeeper);
        fFunc(TEvBlobStorage::EvNodeWardenUnsubscribeFromCache, ForwardToDistributedConfigKeeper);
        fFunc(TEvBlobStorage::EvNodeWardenUpdateConfigFromPeer, ForwardToDistributedConfigKeeper);

        hFunc(TEvNodeWardenQueryBaseConfig, Handle);
        hFunc(TEvNodeConfigInvokeOnRootResult, Handle);
        hFunc(TEvNodeWardenNotifyConfigMismatch, Handle);

        fFunc(TEvents::TSystem::Gone, HandleGone);

        hFunc(TEvNodeWardenReadMetadata, Handle);
        hFunc(TEvNodeWardenWriteMetadata, Handle);
        hFunc(TEvPrivate::TEvDereferencePDisk, Handle);

        hFunc(TEvNodeWardenQueryCacheResult, Handle);

        hFunc(TEvNodeWardenNotifySyncerFinished, Handle);

        hFunc(NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse, Handle);
        hFunc(NConsole::TEvConfigsDispatcher::TEvRemoveConfigSubscriptionResponse, Handle);
        hFunc(NConsole::TEvConsole::TEvConfigNotificationRequest, Handle);

        default:
            EnqueuePendingMessage(ev);
            break;
    }

    if (VDiskStatusChanged) {
        SendDiskMetrics(false);
        VDiskStatusChanged = false;
    }
}

void TNodeWarden::RemoveDrivesWithBadSerialsAndReport(TVector<NPDisk::TDriveData>& drives, TStringStream& details) {
    // Serial number's size definitely won't exceed this number of bytes.
    size_t maxSerialSizeInBytes = 100;

    auto isValidSerial = [maxSerialSizeInBytes](TString& serial) {
        if (serial.size() > maxSerialSizeInBytes) {
            // Not sensible size.
            return false;
        }

        // Check if serial number contains only ASCII characters.
        for (size_t i = 0; i < serial.size(); ++i) {
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
        size_t size = std::min(serial.size(), maxSerialSizeInBytes);

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
    if (!AppData()->FeatureFlags.GetEnableDriveSerialsDiscovery()) {
        return {};
    }

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
    TActivationContext::ActorSystem()->RegisterLocalService(MakeBlobStorageProxyID(groupId), Register(
        CreateBlobStorageGroupEjectedProxy(groupId, DsProxyNodeMon), TMailboxType::ReadAsFilled, AppData()->SystemPoolId));
}

void TNodeWarden::StopInvalidGroupProxy() {
    ui32 groupId = Max<ui32>();
    STLOG(PRI_DEBUG, BS_NODE, NW15, "StopInvalidGroupProxy", (GroupId, groupId));
    TActivationContext::Send(new IEventHandle(TEvents::TSystem::Poison, 0, MakeBlobStorageProxyID(groupId), {}, nullptr, 0));
}

void TNodeWarden::StartRequestReportingThrottler() {
    STLOG(PRI_DEBUG, BS_NODE, NW62, "StartRequestReportingThrottler");
    Register(CreateRequestReportingThrottler(ReportingControllerBucketSize, ReportingControllerLeakDurationMs, ReportingControllerLeakRate));
}

void TNodeWarden::PassAway() {
    STLOG(PRI_DEBUG, BS_NODE, NW25, "PassAway");

    Send(NConsole::MakeConfigsDispatcherID(SelfId().NodeId()),
        new NConsole::TEvConfigsDispatcher::TEvRemoveConfigSubscriptionRequest(SelfId()));

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

    TActorSystem *actorSystem = TActivationContext::ActorSystem();
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


        TControlBoard::RegisterLocalControl(EnablePutBatching, icb->BlobStorage.EnablePutBatching);
        TControlBoard::RegisterLocalControl(EnableVPatch, icb->BlobStorage.EnableVPatch);
        TControlBoard::RegisterSharedControl(EnableLocalSyncLogDataCutting, icb->VDiskControls.EnableLocalSyncLogDataCutting);
        TControlBoard::RegisterSharedControl(EnableSyncLogChunkCompressionHDD, icb->VDiskControls.EnableSyncLogChunkCompressionHDD);
        TControlBoard::RegisterSharedControl(EnableSyncLogChunkCompressionSSD, icb->VDiskControls.EnableSyncLogChunkCompressionSSD);
        TControlBoard::RegisterSharedControl(MaxSyncLogChunksInFlightHDD, icb->VDiskControls.MaxSyncLogChunksInFlightHDD);
        TControlBoard::RegisterSharedControl(MaxSyncLogChunksInFlightSSD, icb->VDiskControls.MaxSyncLogChunksInFlightSSD);
        TControlBoard::RegisterSharedControl(DefaultHugeGarbagePerMille, icb->VDiskControls.DefaultHugeGarbagePerMille);
        TControlBoard::RegisterSharedControl(HugeDefragFreeSpaceBorderPerMille, icb->VDiskControls.HugeDefragFreeSpaceBorderPerMille);
        TControlBoard::RegisterSharedControl(MaxChunksToDefragInflight, icb->VDiskControls.MaxChunksToDefragInflight);
        TControlBoard::RegisterSharedControl(FreshCompMaxInFlightWrites, icb->VDiskControls.FreshCompMaxInFlightWrites);
        TControlBoard::RegisterSharedControl(FreshCompMaxInFlightReads, icb->VDiskControls.FreshCompMaxInFlightReads);
        TControlBoard::RegisterSharedControl(HullCompMaxInFlightWrites, icb->VDiskControls.HullCompMaxInFlightWrites);
        TControlBoard::RegisterSharedControl(HullCompMaxInFlightReads, icb->VDiskControls.HullCompMaxInFlightReads);
        TControlBoard::RegisterSharedControl(HullCompFullCompPeriodSec, icb->VDiskControls.HullCompFullCompPeriodSec);
        TControlBoard::RegisterSharedControl(HullCompThrottlerBytesRate, icb->VDiskControls.HullCompThrottlerBytesRate);
        TControlBoard::RegisterSharedControl(GarbageThresholdToRunFullCompactionPerMille, icb->VDiskControls.GarbageThresholdToRunFullCompactionPerMille);
        TControlBoard::RegisterSharedControl(DefragThrottlerBytesRate, icb->VDiskControls.DefragThrottlerBytesRate);

        TControlBoard::RegisterSharedControl(ThrottlingDryRun, icb->VDiskControls.ThrottlingDryRun);
        TControlBoard::RegisterSharedControl(ThrottlingMinLevel0SstCount, icb->VDiskControls.ThrottlingMinLevel0SstCount);
        TControlBoard::RegisterSharedControl(ThrottlingMaxLevel0SstCount, icb->VDiskControls.ThrottlingMaxLevel0SstCount);
        TControlBoard::RegisterSharedControl(ThrottlingMinInplacedSizeHDD, icb->VDiskControls.ThrottlingMinInplacedSizeHDD);
        TControlBoard::RegisterSharedControl(ThrottlingMaxInplacedSizeHDD, icb->VDiskControls.ThrottlingMaxInplacedSizeHDD);
        TControlBoard::RegisterSharedControl(ThrottlingMinInplacedSizeSSD, icb->VDiskControls.ThrottlingMinInplacedSizeSSD);
        TControlBoard::RegisterSharedControl(ThrottlingMaxInplacedSizeSSD, icb->VDiskControls.ThrottlingMaxInplacedSizeSSD);
        TControlBoard::RegisterSharedControl(ThrottlingMinOccupancyPerMille, icb->VDiskControls.ThrottlingMinOccupancyPerMille);
        TControlBoard::RegisterSharedControl(ThrottlingMaxOccupancyPerMille, icb->VDiskControls.ThrottlingMaxOccupancyPerMille);
        TControlBoard::RegisterSharedControl(ThrottlingMinLogChunkCount, icb->VDiskControls.ThrottlingMinLogChunkCount);
        TControlBoard::RegisterSharedControl(ThrottlingMaxLogChunkCount, icb->VDiskControls.ThrottlingMaxLogChunkCount);

        TControlBoard::RegisterSharedControl(MaxInProgressSyncCount, icb->VDiskControls.MaxInProgressSyncCount);
        TControlBoard::RegisterSharedControl(EnablePhantomFlagStorage, icb->VDiskControls.EnablePhantomFlagStorage);
        TControlBoard::RegisterSharedControl(PhantomFlagStorageLimitPerVDiskBytes, icb->VDiskControls.PhantomFlagStorageLimitPerVDiskBytes);

        TControlBoard::RegisterSharedControl(MaxCommonLogChunksHDD, icb->PDiskControls.MaxCommonLogChunksHDD);
        TControlBoard::RegisterSharedControl(MaxCommonLogChunksSSD, icb->PDiskControls.MaxCommonLogChunksSSD);
        TControlBoard::RegisterSharedControl(CommonStaticLogChunks, icb->PDiskControls.CommonStaticLogChunks);

        TControlBoard::RegisterSharedControl(CostMetricsParametersByMedia[NPDisk::DEVICE_TYPE_ROT].BurstThresholdNs,
                icb->VDiskControls.BurstThresholdNsHDD);
        TControlBoard::RegisterSharedControl(CostMetricsParametersByMedia[NPDisk::DEVICE_TYPE_SSD].BurstThresholdNs,
                icb->VDiskControls.BurstThresholdNsSSD);
        TControlBoard::RegisterSharedControl(CostMetricsParametersByMedia[NPDisk::DEVICE_TYPE_NVME].BurstThresholdNs,
                icb->VDiskControls.BurstThresholdNsNVME);
        TControlBoard::RegisterSharedControl(CostMetricsParametersByMedia[NPDisk::DEVICE_TYPE_ROT].DiskTimeAvailableScale,
                icb->VDiskControls.DiskTimeAvailableScaleHDD);
        TControlBoard::RegisterSharedControl(CostMetricsParametersByMedia[NPDisk::DEVICE_TYPE_SSD].DiskTimeAvailableScale,
                icb->VDiskControls.DiskTimeAvailableScaleSSD);
        TControlBoard::RegisterSharedControl(CostMetricsParametersByMedia[NPDisk::DEVICE_TYPE_NVME].DiskTimeAvailableScale,
                icb->VDiskControls.DiskTimeAvailableScaleNVME);

        TControlBoard::RegisterSharedControl(SlowDiskThreshold, icb->DSProxyControls.SlowDiskThreshold);
        TControlBoard::RegisterSharedControl(SlowDiskThresholdHDD, icb->DSProxyControls.SlowDiskThresholdHDD);
        TControlBoard::RegisterSharedControl(SlowDiskThresholdSSD, icb->DSProxyControls.SlowDiskThresholdSSD);

        TControlBoard::RegisterSharedControl(PredictedDelayMultiplier, icb->DSProxyControls.PredictedDelayMultiplier);
        TControlBoard::RegisterSharedControl(PredictedDelayMultiplierHDD, icb->DSProxyControls.PredictedDelayMultiplierHDD);
        TControlBoard::RegisterSharedControl(PredictedDelayMultiplierSSD, icb->DSProxyControls.PredictedDelayMultiplierSSD);

        TControlBoard::RegisterSharedControl(MaxNumOfSlowDisks, icb->DSProxyControls.MaxNumOfSlowDisks);
        TControlBoard::RegisterSharedControl(MaxNumOfSlowDisksHDD, icb->DSProxyControls.MaxNumOfSlowDisksHDD);
        TControlBoard::RegisterSharedControl(MaxNumOfSlowDisksSSD, icb->DSProxyControls.MaxNumOfSlowDisksSSD);

        TControlBoard::RegisterSharedControl(EnableDeepScrubbing, icb->VDiskControls.EnableDeepScrubbing);

        TControlBoard::RegisterSharedControl(LongRequestThresholdMs, icb->DSProxyControls.LongRequestThresholdMs);
        TControlBoard::RegisterSharedControl(ReportingControllerBucketSize, icb->DSProxyControls.RequestReportingSettings.BucketSize);
        TControlBoard::RegisterSharedControl(ReportingControllerLeakDurationMs, icb->DSProxyControls.RequestReportingSettings.LeakDurationMs);
        TControlBoard::RegisterSharedControl(ReportingControllerLeakRate, icb->DSProxyControls.RequestReportingSettings.LeakRate);
        TControlBoard::RegisterSharedControl(MaxPutTimeoutSeconds, icb->DSProxyControls.MaxPutTimeoutSeconds);
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

    actorSystem->RegisterLocalService(MakeBlobStorageSyncBrokerID(), Register(
        CreateSyncBrokerActor(MaxInProgressSyncCount)));

    // create bridge syncer rate quoter
    SyncRateQuoter = std::make_shared<TReplQuoter>(Cfg->BlobStorageConfig.GetBridgeSyncRateBytesPerSecond());

    // determine if we are running in 'mock' mode
    EnableProxyMock = Cfg->BlobStorageConfig.GetServiceSet().GetEnableProxyMock();

    // fill in a base storage config (from the file)
    NKikimrConfig::TAppConfig appConfig;
    appConfig.MutableBlobStorageConfig()->CopyFrom(Cfg->BlobStorageConfig);
    appConfig.MutableNameserviceConfig()->CopyFrom(Cfg->NameserviceConfig);
    if (Cfg->DomainsConfig) {
        appConfig.MutableDomainsConfig()->CopyFrom(*Cfg->DomainsConfig);
    }
    if (Cfg->SelfManagementConfig) {
        appConfig.MutableSelfManagementConfig()->CopyFrom(*Cfg->SelfManagementConfig);
    }
    if (Cfg->BridgeConfig) {
        appConfig.MutableBridgeConfig()->CopyFrom(*Cfg->BridgeConfig);
    }
    TString errorReason;
    auto config = std::make_shared<NKikimrBlobStorage::TStorageConfig>();
    const bool success = DeriveStorageConfig(appConfig, config.get(), &errorReason);
    Y_VERIFY_S(success, "failed to generate initial TStorageConfig: " << errorReason);
    TDistributedConfigKeeper::GenerateBridgeInitialState(*Cfg, config.get());
    TDistributedConfigKeeper::UpdateFingerprint(config.get());
    StorageConfig = std::move(config);

    YamlConfig = std::move(Cfg->YamlConfig);

    InferPDiskSlotCountSettings.CopyFrom(Cfg->BlobStorageConfig.GetInferPDiskSlotCountSettings());
    ui32 blobStorageConfigItem = NKikimrConsole::TConfigItem::BlobStorageConfigItem;
    Send(NConsole::MakeConfigsDispatcherID(SelfId().NodeId()),
        new NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionRequest(blobStorageConfigItem));

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
    Send(GetNameserviceActorId(), new TEvInterconnect::TEvListNodes(true));

    if (Cfg->IsCacheEnabled()) {
        TActivationContext::Schedule(TDuration::Seconds(5), new IEventHandle(TEvPrivate::EvReadCache, 0, SelfId(), {}, nullptr, 0));
    }

    StartInvalidGroupProxy();

    StartDistributedConfigKeeper();

    HandleGroupPendingQueueTick();

    StartRequestReportingThrottler();
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

        return [=, this] {
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

void TNodeWarden::Handle(TEvInterconnect::TEvNodesInfo::TPtr ev) {
    NodeLocationMap.clear();
    for (const auto& info : ev->Get()->Nodes) {
        NodeLocationMap.emplace(info.NodeId, std::move(info.Location));
    }
    for (auto& [groupId, group] : Groups) {
        if (group.Info && group.Info->Type.GetErasure() == TBlobStorageGroupType::ErasureMirror3dc) {
            group.NodeLayoutInfo = MakeIntrusive<TNodeLayoutInfo>(NodeLocationMap[LocalNodeId], group.Info, NodeLocationMap);
            if (group.ProxyId) {
                Send(group.ProxyId, new TEvBlobStorage::TEvConfigureProxy(group.Info, group.NodeLayoutInfo));
            }
        }
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

void TNodeWarden::Handle(NPDisk::TEvShredPDiskResult::TPtr ev) {
    ProcessShredStatus(ev->Cookie, ev->Get()->ShredGeneration, ev->Get()->Status == NKikimrProto::OK ? std::nullopt :
        std::make_optional(TStringBuilder() << "failed to shred PDisk Status# " << NKikimrProto::EReplyStatus_Name(
        ev->Get()->Status)));
}

void TNodeWarden::Handle(NPDisk::TEvChangeExpectedSlotCountResult::TPtr ev) {
    const NPDisk::TEvChangeExpectedSlotCountResult &msg = *ev->Get();
    STLOG(PRI_DEBUG, BS_NODE, NW108, "Handle(NPDisk::TEvChangeExpectedSlotCountResult)", (Msg, msg.ToString()));

    // For now, just log the result. In the future, we might want to track this or take action based on the result.
    if (msg.Status != NKikimrProto::OK) {
        STLOG(PRI_ERROR, BS_NODE, NW109, "ChangeExpectedSlotCount failed", (Status, msg.Status), (ErrorReason, msg.ErrorReason));
    }
}

void TNodeWarden::Handle(NPDisk::TEvShredPDisk::TPtr ev) {
    // the message has returned to sender -- PDisk was terminated before processing it; normally it must never happen,
    // because NodeWarden issues PoisonPill synchronously with removing PDisk from the LocalPDisks set
    ProcessShredStatus(ev->Cookie, ev->Get()->ShredGeneration, "PDisk has been terminated before it got shredded");
    Y_DEBUG_ABORT("unexpected case");
}

void TNodeWarden::ProcessShredStatus(ui64 cookie, ui64 generation, std::optional<TString> error) {
    const auto it = ShredInFlight.find(cookie);
    const std::optional<TPDiskKey> key = it != ShredInFlight.end() ? std::make_optional(it->second) : std::nullopt;
    if (it != ShredInFlight.end()) {
        ShredInFlight.erase(it);
    }

    const auto pdiskIt = key ? LocalPDisks.find(*key) : LocalPDisks.end();
    TPDiskRecord *pdisk = pdiskIt != LocalPDisks.end() ? &pdiskIt->second : nullptr;
    if (pdisk) {
        const size_t numErased = pdisk->ShredCookies.erase(cookie);
        Y_ABORT_UNLESS(numErased);
    }

    STLOG(PRI_DEBUG, BS_SHRED, BSSN00, "processing shred result from PDisk",
        (Cookie, cookie),
        (PDiskId, key),
        (ShredGeneration, generation),
        (ErrorReason, error),
        (ShredGenerationIssued, pdisk ? pdisk->ShredGenerationIssued : std::nullopt));

    if (pdisk && generation == pdisk->ShredGenerationIssued) {
        if (error) {
            pdisk->ShredState.emplace<TString>(std::move(*error));
        } else {
            pdisk->ShredState.emplace<ui64>(generation);
        }
        SendPDiskReport(key->PDiskId, NKikimrBlobStorage::TEvControllerNodeReport::PD_SHRED, pdisk->ShredState);
    }
}

void TNodeWarden::PersistConfig(std::optional<TString> mainYaml, ui64 mainYamlVersion, std::optional<TString> storageYaml,
        std::optional<ui64> storageYamlVersion) {
    if (!Cfg->ConfigDirPath) {
        // no storage directory specified
        return;
    } else if (auto *appData = AppData(); appData->DynamicNameserviceConfig &&
            appData->DynamicNameserviceConfig->MaxStaticNodeId < LocalNodeId) {
        // this is a dynamic node
        return;
    }

    auto escape = [&](auto& value) { return value ? std::make_optional('"' + EscapeC(*value) + '"') : std::nullopt; };

    STLOG(PRI_DEBUG, BS_NODE, NW63, "persisting new configurations",
        (MainYaml, escape(mainYaml)),
        (MainYamlVersion, mainYamlVersion),
        (StorageYaml, escape(storageYaml)),
        (StorageYamlVersion, storageYamlVersion),
        (YamlConfig, YamlConfig));

    const bool updateMain = mainYaml && (!YamlConfig || !YamlConfig->HasMainConfigVersion() ||
        YamlConfig->GetMainConfigVersion() < mainYamlVersion);

    const bool updateStorage = !storageYamlVersion || // delete storage config file in single-config mode
        storageYaml && (!YamlConfig || !YamlConfig->HasStorageConfigVersion() ||
        YamlConfig->GetStorageConfigVersion() < storageYamlVersion);

    if (!updateMain && !updateStorage) {
        return; // nothing to do
    }

    struct TSaveContext {
        TString ConfigDirPath;
        std::optional<TString> MainYaml;
        ui64 MainYamlVersion;
        std::optional<TString> StorageYaml;
        std::optional<ui64> StorageYamlVersion;
        bool UpdateMain;
        bool UpdateStorage;
        bool DeleteStorage;
    };

    auto saveCtx = std::make_shared<TSaveContext>(TSaveContext{
        .ConfigDirPath = Cfg->ConfigDirPath,
        .MainYaml = std::move(mainYaml),
        .MainYamlVersion = mainYamlVersion,
        .StorageYaml = std::move(storageYaml),
        .StorageYamlVersion = storageYamlVersion,
        .UpdateMain = updateMain,
        .UpdateStorage = updateStorage,
        .DeleteStorage = !storageYamlVersion,
    });

    EnqueueSyncOp([this, saveCtx](const TActorContext&) {
        bool success = true;
        try {
            MakePathIfNotExist(saveCtx->ConfigDirPath.c_str());
        } catch (const yexception& e) {
            STLOG(PRI_ERROR, BS_NODE, NW91, "Failed to create config store path", (Error, e.what()));
            success = false;
        }

        auto saveConfig = [&](const TString& yaml, const TString& configFileName) -> bool {
            try {
                TString tempPath = TStringBuilder() << saveCtx->ConfigDirPath << "/temp_" << configFileName;
                TString configPath = TStringBuilder() << saveCtx->ConfigDirPath << "/" << configFileName;

                {
                    TFileOutput tempFile(tempPath);
                    tempFile << yaml;
                    tempFile.Flush();
                    if (Chmod(tempPath.c_str(), S_IRUSR | S_IRGRP | S_IROTH) != 0) {
                        STLOG(PRI_ERROR, BS_NODE, NW92, "Failed to set permissions for temporary file", (Error, LastSystemErrorText()));
                        success = false;
                        return false;
                    }
                }

                if (!NFs::Rename(tempPath, configPath)) {
                    STLOG(PRI_ERROR, BS_NODE, NW53, "Failed to rename temporary file", (Error, LastSystemErrorText()));
                    success = false;
                    return false;
                }
                return true;
            } catch (const std::exception& e) {
                STLOG(PRI_ERROR, BS_NODE, NW93, "Failed to save config file", (Error, e.what()));
                success = false;
                return false;
            }
        };

        if (success && saveCtx->UpdateMain) {
            success = saveConfig(*saveCtx->MainYaml, YamlConfigFileName);
            if (success) {
                STLOG(PRI_INFO, BS_NODE, NW94, "Yaml config saved");
            }
        }

        if (saveCtx->DeleteStorage) {
            std::filesystem::remove(std::filesystem::path(saveCtx->ConfigDirPath.c_str()) / StorageConfigFileName);
        } else if (success && saveCtx->UpdateStorage) {
            success = saveConfig(*saveCtx->StorageYaml, StorageConfigFileName);
            if (success) {
                STLOG(PRI_INFO, BS_NODE, NW95, "Storage config saved");
            }
        }

        return [this, saveCtx, success]() {
            if (success) {
                if (!YamlConfig) {
                    YamlConfig.emplace();
                }
                if (saveCtx->UpdateMain) {
                    YamlConfig->SetMainConfig(*saveCtx->MainYaml);
                    YamlConfig->SetMainConfigVersion(saveCtx->MainYamlVersion);
                }
                if (saveCtx->DeleteStorage) {
                    YamlConfig->ClearStorageConfig();
                    YamlConfig->ClearStorageConfigVersion();
                } else if (saveCtx->UpdateStorage) {
                    YamlConfig->SetStorageConfig(*saveCtx->StorageYaml);
                    YamlConfig->SetStorageConfigVersion(*saveCtx->StorageYamlVersion);
                }
                ConfigSaveTimer.Reset();
            } else {
                TActivationContext::Schedule(ConfigSaveTimer.Next(), new IEventHandle(
                    SelfId(), {}, new TEvPrivate::TEvRetrySaveConfig(std::move(saveCtx->MainYaml), saveCtx->MainYamlVersion,
                    std::move(saveCtx->StorageYaml), saveCtx->StorageYamlVersion), 0, ExpectedSaveConfigCookie));
            }
        };
    });
}

void TNodeWarden::Handle(TEvRegisterPDiskLoadActor::TPtr ev) {
    Send(ev.Get()->Sender, new TEvRegisterPDiskLoadActorResult(NextLocalPDiskInitOwnerRound()));
}

void TNodeWarden::Handle(TEvBlobStorage::TEvControllerNodeServiceSetUpdate::TPtr ev) {
    auto& record = ev->Get()->Record;

    STLOG(PRI_DEBUG, BS_NODE, NW52, "TEvControllerNodeServiceSetUpdate", (Record, record));

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

    if (record.HasShredRequest()) {
        const auto& request = record.GetShredRequest();
        const ui64 generation = request.GetShredGeneration();
        for (ui32 pdiskId : request.GetPDiskIds()) {
            const TPDiskKey key(LocalNodeId, pdiskId);
            if (const auto it = LocalPDisks.find(key); it != LocalPDisks.end()) {
                TPDiskRecord& pdisk = it->second;

                auto issueShredRequestToPDisk = [&] {
                    const ui64 cookie = ++LastShredCookie;
                    ShredInFlight.emplace(cookie, key);
                    pdisk.ShredCookies.emplace(cookie, generation);

                    const TActorId actorId = SelfId();
                    auto ev = std::make_unique<NPDisk::TEvShredPDisk>(generation);
                    TActivationContext::Send(new IEventHandle(MakeBlobStoragePDiskID(LocalNodeId, pdiskId), SelfId(),
                        ev.release(), IEventHandle::FlagForwardOnNondelivery, cookie, &actorId));
                    pdisk.ShredGenerationIssued.emplace(generation);

                    STLOG(PRI_DEBUG, BS_SHRED, BSSN01, "sending shred query to PDisk",
                        (Cookie, cookie),
                        (PDiskId, key),
                        (ShredGeneration, generation));
                };

                if (pdisk.ShredGenerationIssued == generation) {
                    std::visit(TOverloaded{
                        [&](std::monostate&) {
                            // shredding is in progress, do nothing
                        },
                        [&](ui64& generation) {
                            // shredding has already completed for this generation, report it
                            SendPDiskReport(pdiskId, NKikimrBlobStorage::TEvControllerNodeReport::PD_SHRED, generation);
                        },
                        [&](TString& /*aborted*/) {
                            // shredding finished with error last time, restart
                            issueShredRequestToPDisk();
                        }
                    }, pdisk.ShredState);
                } else if (pdisk.ShredGenerationIssued < generation) {
                    issueShredRequestToPDisk();
                } else {
                    SendPDiskReport(pdiskId, NKikimrBlobStorage::TEvControllerNodeReport::PD_SHRED, "obsolete generation");
                }
            } else {
                SendPDiskReport(pdiskId, NKikimrBlobStorage::TEvControllerNodeReport::PD_SHRED, "PDisk not found");
            }
        }
    }

    if (record.HasYamlConfig()) {
        auto& yaml = *record.MutableYamlConfig();

        if (yaml.HasCompressedMainConfig()) {
            Y_DEBUG_ABORT_UNLESS(!yaml.HasMainConfig());
            yaml.SetMainConfig(NYamlConfig::DecompressYamlString(yaml.GetCompressedMainConfig()));
            yaml.ClearCompressedMainConfig();
        }

        if (yaml.HasCompressedStorageConfig()) {
            Y_DEBUG_ABORT_UNLESS(!yaml.HasStorageConfig());
            yaml.SetStorageConfig(NYamlConfig::DecompressYamlString(yaml.GetCompressedStorageConfig()));
            yaml.ClearCompressedStorageConfig();
        }

        PersistConfig(yaml.HasMainConfig() ? std::make_optional(yaml.GetMainConfig()) : std::nullopt,
            yaml.GetMainConfigVersion(),
            yaml.HasStorageConfig() ? std::make_optional(yaml.GetStorageConfig()) : std::nullopt,
            yaml.HasStorageConfigVersion() ? std::make_optional(yaml.GetStorageConfigVersion()) : std::nullopt);

        ExpectedSaveConfigCookie++;
    }

    if (record.GetUpdateSyncers()) {
        ApplyWorkingSyncers(record);
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
        InvokeCallbacks.emplace(cookie, [=, this](TEvNodeConfigInvokeOnRootResult& msg) {
            if (msg.Record.GetStatus() != NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK) {
                for (const auto& vdisk : StorageConfig->GetBlobStorageConfig().GetServiceSet().GetVDisks()) {
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
        InvokeCallbacks.emplace(cookie, [=, this](TEvNodeConfigInvokeOnRootResult& msg) {
            if (msg.Record.GetStatus() != NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK) {
                for (const auto& vdisk : StorageConfig->GetBlobStorageConfig().GetServiceSet().GetVDisks()) {
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

    AskBSCToRestartPDisk(pdiskId, ev->Get()->IgnoreChecks, requestCookie);
}

void TNodeWarden::Handle(TEvBlobStorage::TEvNotifyWardenPDiskRestarted::TPtr ev) {
    OnPDiskRestartFinished(ev->Get()->PDiskId, ev->Get()->Status);
}

void TNodeWarden::Handle(TEvBlobStorage::TEvControllerConfigRequest::TPtr ev) {
    const ui64 cookie = NextConfigCookie++;
    UnfinishedRequests[cookie].CopyFrom(ev->Get()->Record);
    ConfigInFlight.emplace(cookie, [this, cookie = cookie, origSender = ev->Sender, origCookie = ev->Cookie](
            TEvBlobStorage::TEvControllerConfigResponse *ev) {
        if (ev) {
            Send(origSender, ev, 0, origCookie);
            UnfinishedRequests.erase(cookie);
        }
    });
    SendToController(std::unique_ptr<IEventBase>(ev->ReleaseBase().Release()), cookie);
}

void TNodeWarden::Handle(TEvBlobStorage::TEvControllerConfigResponse::TPtr ev) {
    if (auto nh = ConfigInFlight.extract(ev->Cookie)) {
        nh.mapped()(ev->Get());
    }
}

void TNodeWarden::SendUnfinishedRequests() {
    for (const auto& [cookie, record] : UnfinishedRequests) {
        auto ev = std::make_unique<TEvBlobStorage::TEvControllerConfigRequest>();
        ev->Record.CopyFrom(record);
        SendToController(std::move(ev), cookie);
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
    NotifySyncersProgress();
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

void TNodeWarden::Handle(TEvPrivate::TEvRetrySaveConfig::TPtr& ev) {
    STLOG(PRI_TRACE, BS_NODE, NW97, "Handle(TEvRetrySaveConfig)");
    if (ev->Cookie == ExpectedSaveConfigCookie) {
        auto *msg = ev->Get();
        PersistConfig(std::move(msg->MainYaml), msg->MainYamlVersion, std::move(msg->StorageYaml), msg->StorageYamlVersion);
        ExpectedSaveConfigCookie++;
    }
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
                for (const auto& item : StorageConfig->GetBlobStorageConfig().GetServiceSet().GetVDisks()) {
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

void TNodeWarden::Handle(NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse::TPtr /*ev*/)
{}

void TNodeWarden::Handle(NConsole::TEvConfigsDispatcher::TEvRemoveConfigSubscriptionResponse::TPtr /*ev*/)
{}

void TNodeWarden::Handle(NConsole::TEvConsole::TEvConfigNotificationRequest::TPtr ev) {
    auto& record = ev->Get()->Record;
    if (record.HasConfig() && record.GetConfig().HasBlobStorageConfig()) {
        auto inferSettings = record.GetConfig().GetBlobStorageConfig().GetInferPDiskSlotCountSettings();
        auto equals = ::google::protobuf::util::MessageDifferencer::Equals;
        if (!equals(InferPDiskSlotCountSettings, inferSettings)) {
            InferPDiskSlotCountSettings.CopyFrom(inferSettings);
            for (auto& [key, localPDisk] : LocalPDisks) {
                TIntrusivePtr<TPDiskConfig> newPDiskConfig = CreatePDiskConfig(localPDisk.Record);
                ui64 newExpectedSlotCount = newPDiskConfig->ExpectedSlotCount;
                ui32 newSlotSizeInUnits = newPDiskConfig->SlotSizeInUnits;

                if (newExpectedSlotCount != localPDisk.ExpectedSlotCount ||
                        newSlotSizeInUnits != localPDisk.SlotSizeInUnits) {
                    STLOG(PRI_DEBUG, BS_NODE, NW112, "SendChangeExpectedSlotCount from config notification",
                        (PDiskId, key.PDiskId),
                        (ExpectedSlotCount, newExpectedSlotCount),
                        (SlotSizeInUnits, newSlotSizeInUnits));

                    const TActorId pdiskActorId = MakeBlobStoragePDiskID(LocalNodeId, key.PDiskId);
                    Send(pdiskActorId, new NPDisk::TEvChangeExpectedSlotCount(newExpectedSlotCount, newSlotSizeInUnits));

                    localPDisk.ExpectedSlotCount = newExpectedSlotCount;
                    localPDisk.SlotSizeInUnits = newSlotSizeInUnits;
                }
            }
        }
    }
    Send(ev->Sender, new NConsole::TEvConsole::TEvConfigNotificationResponse(record), 0, ev->Cookie);
}

void TNodeWarden::FillInVDiskStatus(google::protobuf::RepeatedPtrField<NKikimrBlobStorage::TVDiskStatus> *pb, bool initial) {
    for (auto& [vslotId, vdisk] : LocalVDisks) {
        if (vdisk.RuntimeData && vdisk.RuntimeData->DDisk) {
            continue; // do not report DDisks here
        }

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

bool NKikimr::NStorage::DeriveStorageConfig(const NKikimrConfig::TAppConfig& appConfig,
        NKikimrBlobStorage::TStorageConfig *config, TString *errorReason) {
    // copy blob storage config
    if (!appConfig.HasBlobStorageConfig()) {
        *errorReason = "original config missing mandatory BlobStorageConfig section";
        return false;
    }

    if (appConfig.HasSelfManagementConfig()) {
        const auto& smFrom = appConfig.GetSelfManagementConfig();
        auto *smTo = config->MutableSelfManagementConfig();
        if (smFrom.HasGeneration() && smTo->HasGeneration() && smFrom.GetGeneration() != smTo->GetGeneration() + 1) {
            *errorReason = TStringBuilder() << "generation mismatch for SelfManagementConfig section existing Generation# "
                << smTo->GetGeneration() << " newly provided Generation# " << smFrom.GetGeneration();
            return false;
        }
        smTo->CopyFrom(smFrom);
    } else {
        config->ClearSelfManagementConfig();
    }

    const auto& bsFrom = appConfig.GetBlobStorageConfig();
    auto *bsTo = config->MutableBlobStorageConfig();

    const auto hasStaticGroupInfo = [](const NKikimrBlobStorage::TNodeWardenServiceSet& ss) {
        return ss.PDisksSize() && ss.VDisksSize() && ss.GroupsSize();
    };

    if (bsFrom.HasServiceSet()) {
        const auto& ssFrom = bsFrom.GetServiceSet();
        auto *ssTo = bsTo->MutableServiceSet();

        // update availability domains if set
        if (ssFrom.AvailabilityDomainsSize()) {
            ssTo->MutableAvailabilityDomains()->CopyFrom(ssFrom.GetAvailabilityDomains());
        }

        // replace replication broker configuration
        if (ssFrom.HasReplBrokerConfig()) {
            ssTo->MutableReplBrokerConfig()->CopyFrom(ssFrom.GetReplBrokerConfig());
        } else {
            ssTo->ClearReplBrokerConfig();
        }

        // update static group information unless distconf is enabled
        if (!hasStaticGroupInfo(ssFrom) && config->GetSelfManagementConfig().GetEnabled()) {
            // distconf enabled, keep it as is
        } else if (!hasStaticGroupInfo(*ssTo)) {
            ssTo->MutablePDisks()->CopyFrom(ssFrom.GetPDisks());
            ssTo->MutableVDisks()->CopyFrom(ssFrom.GetVDisks());
            ssTo->MutableGroups()->CopyFrom(ssFrom.GetGroups());
        } else {
            NProtoBuf::util::MessageDifferencer differ;

            auto error = [&](auto&& key, const char *error) {
                *errorReason = TStringBuilder() << key() << ' ' << error;
                return false;
            };

            auto pdiskKey = [](const auto *item) {
                return TStringBuilder() << "PDisk [" << item->GetNodeID() << ':' << item->GetPDiskID() << ']';
            };

            auto vdiskKey = [](const auto *item) {
                return TStringBuilder() << "VSlot [" << item->GetNodeID() << ':' << item->GetPDiskID() << ':'
                    << item->GetVDiskSlotID() << ']';
            };

            auto groupKey = [](const auto *item) {
                return TStringBuilder() << "group " << item->GetGroupID();
            };

            auto duplicateKey = [&](auto&& key) { return error(std::move(key), "duplicate key in existing StorageConfig"); };
            auto removed = [&](auto&& key) { return error(std::move(key), "was removed from BlobStorageConfig of newly provided configuration"); };
            auto mismatch = [&](auto&& key) { return error(std::move(key), "configuration item mismatch"); };

            THashMap<std::tuple<ui32, ui32>, const NKikimrBlobStorage::TNodeWardenServiceSet::TPDisk*> pdiskMap;
            for (const auto& item : ssTo->GetPDisks()) {
                if (const auto [it, inserted] = pdiskMap.emplace(std::make_tuple(item.GetNodeID(), item.GetPDiskID()),
                        &item); !inserted) {
                    return duplicateKey(std::bind(pdiskKey, &item));
                }
            }
            for (const auto& item : ssFrom.GetPDisks()) {
                if (const auto it = pdiskMap.find(std::make_tuple(item.GetNodeID(), item.GetPDiskID())); it == pdiskMap.end()) {
                    return removed(std::bind(pdiskKey, &item));
                } else if (!differ.Equals(item, *it->second)) {
                    return mismatch(std::bind(pdiskKey, &item));
                } else {
                    pdiskMap.erase(it);
                }
            }
            if (!pdiskMap.empty()) {
                TStringStream err;
                err << "some static PDisks were removed in newly provided configuration:";
                for (const auto& [id, _] : pdiskMap) {
                    const auto& [nodeId, pdiskId] = id;
                    err << " [" << nodeId << ':' << pdiskId << ']';
                }
                *errorReason = std::move(err.Str());
                return false;
            }

            THashMap<std::tuple<ui32, ui32, ui32>, const NKikimrBlobStorage::TNodeWardenServiceSet::TVDisk*> vdiskMap;
            for (const auto& item : ssTo->GetVDisks()) {
                if (!item.HasVDiskLocation()) {
                    *errorReason = "VDisk in existing StorageConfig doesn't have VDiskLocation field set";
                    return false;
                }
                const auto& loc = item.GetVDiskLocation();
                if (const auto [it, inserted] = vdiskMap.emplace(std::make_tuple(loc.GetNodeID(), loc.GetPDiskID(),
                        loc.GetVDiskSlotID()), &item); !inserted) {
                    return duplicateKey(std::bind(vdiskKey, &loc));
                }
            }
            for (const auto& item : ssFrom.GetVDisks()) {
                if (!item.HasVDiskLocation()) {
                    *errorReason = "VDisk in newly provided configuration doesn't have VDiskLocation field set";
                    return false;
                }
                const auto& loc = item.GetVDiskLocation();
                if (const auto it = vdiskMap.find(std::make_tuple(loc.GetNodeID(), loc.GetPDiskID(),
                        loc.GetVDiskSlotID())); it == vdiskMap.end()) {
                    return removed(std::bind(vdiskKey, &loc));
                } else if (!differ.Equals(item, *it->second)) {
                    return mismatch(std::bind(vdiskKey, &loc));
                } else {
                    vdiskMap.erase(it);
                }
            }
            if (!vdiskMap.empty()) {
                TStringStream err;
                err << "some static VDisks were removed in newly provided configuration:";
                for (const auto& [id, _] : vdiskMap) {
                    const auto& [nodeId, pdiskId, vdiskSlotId] = id;
                    err << " [" << nodeId << ':' << pdiskId << ':' << vdiskSlotId << ']';
                }
                *errorReason = std::move(err.Str());
                return false;
            }

            THashMap<ui32, const NKikimrBlobStorage::TGroupInfo*> groupMap;
            for (const auto& item : ssTo->GetGroups()) {
                if (const auto [it, inserted] = groupMap.emplace(item.GetGroupID(), &item); !inserted) {
                    return duplicateKey(std::bind(groupKey, &item));
                }
            }
            for (const auto& item : ssFrom.GetGroups()) {
                if (const auto it = groupMap.find(item.GetGroupID()); it == groupMap.end()) {
                    return removed(std::bind(groupKey, &item));
                } else if (!differ.Equals(item, *it->second)) {
                    return mismatch(std::bind(groupKey, &item));
                } else {
                    groupMap.erase(it);
                }
            }
            if (!groupMap.empty()) {
                *errorReason = "some static groups were removed in newly provided configuration";
                return false;
            }
        }
    }

    // copy define box
    if (bsFrom.HasDefineBox()) {
        bsTo->MutableDefineBox()->CopyFrom(bsFrom.GetDefineBox());
    } else {
        bsTo->ClearDefineBox();
    }
    bsTo->MutableDefineHostConfig()->CopyFrom(bsFrom.GetDefineHostConfig());

    if (bsFrom.HasBscSettings()) {
        bsTo->MutableBscSettings()->CopyFrom(bsFrom.GetBscSettings());
    } else {
        bsTo->ClearBscSettings();
    }

    // copy PDiskConfig from DefineHostConfig/DefineBox if this section is managed automatically
    if (!hasStaticGroupInfo(bsFrom.GetServiceSet()) && config->GetSelfManagementConfig().GetEnabled()) {
        THashMap<std::tuple<ui32, TString>, NKikimrBlobStorage::TPDiskConfig> pdiskConfigs;
        auto callback = [&](const auto& node, const auto& drive) {
            if (drive.HasPDiskConfig()) {
                pdiskConfigs.emplace(std::make_tuple(node.GetNodeId(), drive.GetPath()), drive.GetPDiskConfig());
            }
        };
        EnumerateConfigDrives(*config, 0, callback, nullptr, true);
        for (auto& pdisk : *bsTo->MutableServiceSet()->MutablePDisks()) {
            const auto key = std::make_tuple(pdisk.GetNodeID(), pdisk.GetPath());
            if (const auto it = pdiskConfigs.find(key); it != pdiskConfigs.end()) {
                pdisk.MutablePDiskConfig()->CopyFrom(it->second);
            } else {
                pdisk.ClearPDiskConfig();
            }
        }
    }

    // copy nameservice-related things
    if (!appConfig.HasNameserviceConfig()) {
        *errorReason = "origin config missing mandatory NameserviceConfig section";
        return false;
    }

    const auto& nsFrom = appConfig.GetNameserviceConfig();
    auto *nodes = config->MutableAllNodes();

    THashMap<TString, TBridgePileId> piles;
    if (appConfig.HasBridgeConfig()) {
        const auto& p = appConfig.GetBridgeConfig().GetPiles();
        for (int i = 0; i < p.size(); ++i) {
            if (!p[i].HasName()) {
                *errorReason = "missing pile name";
                return false;
            }
            const auto [it, inserted] = piles.try_emplace(p[i].GetName(), TBridgePileId::FromPileIndex(i));
            if (!inserted) {
                *errorReason = TStringBuilder() << "duplicate pile name " << p[i].GetName();
                return false;
            }
        }
        if (piles.size() < 2) {
            *errorReason = "pile set can't be empty or contain less than two elements when bridge mode is enabled";
            return false;
        }
    }

    // just copy AllNodes from TAppConfig into TStorageConfig
    nodes->Clear();
    for (const auto& node : nsFrom.GetNode()) {
        auto *r = nodes->Add();
        r->SetHost(node.GetInterconnectHost());
        r->SetPort(node.GetPort());
        r->SetNodeId(node.GetNodeId());
        if (node.HasLocation()) {
            r->MutableLocation()->CopyFrom(node.GetLocation());
        } else if (node.HasWalleLocation()) {
            r->MutableLocation()->CopyFrom(node.GetWalleLocation());
        }
        const auto& bridgePileName = TNodeLocation(r->GetLocation()).GetBridgePileName();
        if (!piles.empty()) {
            if (!bridgePileName) {
                *errorReason = TStringBuilder() << "mandatory pile name is missing for node " << r->GetNodeId();
                return false;
            }
        } else if (bridgePileName) {
            *errorReason = "pile name can't be specified when Bridge mode is not enabled";
            return false;
        }
    }

    // and copy ClusterUUID from there too
    config->SetClusterUUID(nsFrom.GetClusterUUID());

    if (appConfig.HasDomainsConfig()) {
        const auto& domains = appConfig.GetDomainsConfig();

        // we expect strictly one domain
        if (domains.DomainSize() == 1) {
            const auto& domain = domains.GetDomain(0);

            auto updateConfig = [&](bool needMerge, auto *to, const auto& from, const char *entity) {
                if (needMerge) {
                    *errorReason = NKikimr::NConfig::ValidateStateStorageConfig(entity, from, *to);
                    if (!errorReason->empty()) {
                        return false;
                    }
                }

                to->CopyFrom(from);
                return true;
            };

            // find state storage setup for that domain
            for (const auto& ss : domains.GetStateStorage()) {
                if (domain.SSIdSize() == 0 || (domain.SSIdSize() == 1 && ss.GetSSId() == domain.GetSSId(0))) {
                    const bool hadStateStorageConfig = config->HasStateStorageConfig();
                    const bool hadStateStorageBoardConfig = config->HasStateStorageBoardConfig();
                    const bool hadSchemeBoardConfig = config->HasSchemeBoardConfig();
                    if (!updateConfig(hadStateStorageConfig, config->MutableStateStorageConfig(), ss, "StateStorage") ||
                            !updateConfig(hadStateStorageBoardConfig, config->MutableStateStorageBoardConfig(), ss, "StateStorageBoard") ||
                            !updateConfig(hadSchemeBoardConfig, config->MutableSchemeBoardConfig(), ss, "SchemeBoard")) {
                        return false;
                    }
                    break;
                }
            }
        }

#define UPDATE_EXPLICIT_CONFIG(NAME) \
        if (domains.HasExplicit##NAME##Config()) { \
            if (config->Has##NAME##Config()) { \
                *errorReason = NKikimr::NConfig::ValidateStateStorageConfig(#NAME, config->Get##NAME##Config(), domains.GetExplicit##NAME##Config()); \
                if (!errorReason->empty()) { \
                    return false; \
                } \
            } \
            config->Mutable##NAME##Config()->CopyFrom(domains.GetExplicit##NAME##Config()); \
        }

        UPDATE_EXPLICIT_CONFIG(StateStorage)
        UPDATE_EXPLICIT_CONFIG(StateStorageBoard)
        UPDATE_EXPLICIT_CONFIG(SchemeBoard)
    }

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
