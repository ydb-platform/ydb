#include <ydb/core/base/blobstorage.h>
#include <ydb/core/base/blobstorage_write_source.h>
#include <ydb/core/base/hive.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_color_limits.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_quota_record.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_util_space_color.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/util/lz4_data_generator.h>

#include <map>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace {

// How the group under test is sized.
//
// The numbers that matter are ratios, not absolutes. A production disk is ~100 GB
// with 128 MB chunks, i.e. ~800 chunks, and the two things that can make a write
// cost more than the colour it was admitted against says are both fixed byte
// amounts: the Fresh segment waiting to be compacted (FreshBufSizeLogoBlobs,
// 64 MB) and the bytes admitted but not yet spent. Both matter in proportion to
// the disk, so shrinking the disk inflates them:
//
//   band between PRE_ORANGE and RED  = (50 - 10) / 1000 of the chunks = 4% of the disk
//   Fresh debt as a share of that band = 64 MB / (0.04 * PDiskSize) = 1600 MB / PDiskSize
//
// which is 1.6% on a 100 GB disk, 20% on 8 GB and 80% on 2 GB. Chunk count is a
// separate axis: it decides how much capacity is lost to rounding every SST up to
// a whole chunk.
struct TOosSettings {
    ui64 PDiskSize = 8_GB;
    ui64 PDiskChunkSize = 32_MB;
    // Blobs must stay below MinHugeBlobInBytes or they land in the huge heap and
    // never touch Fresh, which is what this is all about. MinHugeBlobInBytes in
    // turn must stay well below the chunk size: the Fresh projection reserves
    // TotalPartCount * MinHugeBlobInBytes per chunk for the item that may not fit
    // the tail, so a large threshold makes the projection pessimistic.
    ui32 MinHugeBlobInBytes = 4_MB;
    ui32 BlobSize = 2_MB;
    bool Projection = true;
    // Whether to bootstrap the tenant Hive on the group under test. Needed to watch
    // the tablet die, but a Hive that cannot write goes into a boot loop that costs
    // no simulated time, which freezes the clock and hangs any wait with a deadline.
    // The measurements do not need it: Hive writes 42-byte log records, nothing that
    // moves the space numbers.
    bool RunHive = true;
    // Blobs sent before any reply is collected. 1 means strictly sequential.
    ui32 InFlight = 256;
    ui32 DatasetBlobs = 100;
    ui32 MaxFillBlobs = 40'000;
    ui32 ReportEveryBlobs = 2'000;
    TString Label = "default";
};

struct TVDiskSpaceStats {
    NKikimrBlobStorage::TPDiskSpaceColor::E Color = NKikimrBlobStorage::TPDiskSpaceColor::GREEN;
    bool HasColor = false;
    ui64 DskTotalBytes = 0;
    ui64 DskFreeBytes = 0;
    ui64 DskUsedBytes = 0;
    ui64 HugeUsedChunks = 0;
    ui64 IndexBytes = 0;
    ui64 InplacedDataBytes = 0;
    ui64 HugeDataBytes = 0;
};

// Prod: 3 nodes, 3 disks/node, mirror-3-dc, fail_domain_type: disk.
// Hive (the tenant one) stores its own channels on the overflowing pool, so
// when disks go RED its compaction TEvPut comes back ERROR + RED and the tablet
// dies — after which `ydb workload testshard clean` cannot finish.
struct TTenantHiveOosEnv {
    static TFeatureFlags MakeFeatureFlags(bool projection) {
        TFeatureFlags ff;
        ff.SetEnableVDiskHeapAllocator(true);
        ff.SetEnableVDiskFreshSpaceProjection(projection);
        return ff;
    }

    const TOosSettings Oos;
    TEnvironmentSetup Env;
    TActorId Edge;
    ui64 HiveId = MakeDefaultHiveID();
    ui32 GroupId = 0;
    TIntrusivePtr<TBlobStorageGroupInfo> Info;

    ui32 FailedHivePuts = 0;
    ui32 FailedHiveCompactionPuts = 0;
    std::unordered_set<TLogoBlobID> HiveCompactionPuts;
    std::unordered_map<TLogoBlobID, TWriteSource> HivePutSources;
    TString LastFailedPut;
    TString LastPutError;

    // Filled in as the fill runs, so the report can say what the group looked like
    // the moment it stopped taking user data rather than after it settled.
    ui32 AcceptedAtFirstRefusal = 0;
    TString ColorsAtFirstRefusal;
    TString ChunksAtFirstRefusal;
    ui32 AcceptedAtFirstRed = 0;
    TString ChunksAtFirstRed;
    ui32 AcceptedAtFirstPreOrange = 0;

    static constexpr ui64 DatasetTabletId = 0x10000;
    static constexpr ui64 SystemProbeTabletId = 0x20000;

    explicit TTenantHiveOosEnv(TOosSettings oos = {})
        : Oos(std::move(oos))
        , Env({
            .NodeCount = 3,
            .Erasure = TBlobStorageGroupType::ErasureMirror3dc,
            // The domain must not declare a Hive that never gets bootstrapped: the
            // node Locals then retry finding it forever at no simulated cost, which
            // stops the clock and hangs every Sim() in the test.
            .SetupHive = Oos.RunHive,
            .DeferHiveBootstrap = Oos.RunHive,
            .FeatureFlags = MakeFeatureFlags(Oos.Projection),
            .MinHugeBlobInBytes = Oos.MinHugeBlobInBytes,
            .PDiskSize = Oos.PDiskSize,
            // The huge heap builds a chain per slot size up to MaxLogoBlobDataSize
            // (10 MB) and refuses a chain that fits a single slot per chunk, so the
            // chunk has to stay above ~21 MB.
            .PDiskChunkSize = Oos.PDiskChunkSize,
            .TrackSharedQuotaInPDiskMock = true,
            .SetupResourceBroker = true,
        })
    {
        Env.Runtime->FilterFunction = [this](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvPut) {
                if (auto* msg = ev->Get<TEvBlobStorage::TEvPut>()) {
                    if (msg->Id.TabletID() == HiveId) {
                        HivePutSources[msg->Id] = msg->WriteSource;
                        if (msg->WriteSource == TWriteSource::FlatCompactionPut) {
                            HiveCompactionPuts.insert(msg->Id);
                        }
                    }
                }
            } else if (ev->GetTypeRewrite() == TEvBlobStorage::EvPutResult) {
                if (auto* msg = ev->Get<TEvBlobStorage::TEvPutResult>()) {
                    if (msg->Status != NKikimrProto::OK && msg->Id.TabletID() == HiveId) {
                        ++FailedHivePuts;
                        if (HiveCompactionPuts.contains(msg->Id)) {
                            ++FailedHiveCompactionPuts;
                        }
                        const auto source = HivePutSources.contains(msg->Id)
                            ? HivePutSources[msg->Id]
                            : TWriteSource::Unknown;
                        LastFailedPut = TStringBuilder()
                            << "status# " << NKikimrProto::EReplyStatus_Name(msg->Status)
                            << " flags# " << msg->StatusFlags.ToString()
                            << " red# " << msg->StatusFlags.Check(NKikimrBlobStorage::StatusDiskSpaceRed)
                            << " source# " << WriteSourceName(source)
                            << " id# " << msg->Id.ToString()
                            << " reason# " << msg->ErrorReason;
                        Cerr << "HIVE PUT FAILED " << LastFailedPut << Endl;
                    }
                }
            }
            return true;
        };

        NKikimrBlobStorage::TGroupGeometry geometry;
        geometry.SetNumFailRealms(3);
        geometry.SetNumFailDomainsPerFailRealm(3);
        geometry.SetNumVDisksPerFailDomain(1);
        geometry.SetRealmLevelBegin(10);
        geometry.SetRealmLevelEnd(20);
        geometry.SetDomainLevelBegin(10);
        geometry.SetDomainLevelEnd(256); // fail_domain_type: disk

        Env.CreateBoxAndPool(3, 1, 0, NKikimrBlobStorage::EPDiskType::NVME, geometry);
        Env.Sim(TDuration::Seconds(30));

        auto groups = Env.GetGroups();
        UNIT_ASSERT_VALUES_EQUAL(groups.size(), 1);
        GroupId = groups.front();
        Info = Env.GetGroupInfo(GroupId);
        UNIT_ASSERT_VALUES_EQUAL(Info->GetTotalVDisksNum(), 9);

        WaitForGroupReady();

        Edge = Env.Runtime->AllocateEdgeActor(Env.Settings.ControllerNodeId, __FILE__, __LINE__);

        if (Oos.RunHive) {
            Env.Runtime->CreateTestBootstrapper(
                TTestActorSystem::CreateTestTabletInfo(
                    HiveId, TTabletTypes::Hive, Env.Settings.Erasure.GetErasure(), GroupId, /*numChannels=*/3),
                &CreateDefaultHive, Env.Settings.ControllerNodeId);

            TActorId hivePipe = OpenPipe(HiveId); // wait until the tenant Hive is up on the dynamic group
            ClosePipe(hivePipe);
            WaitForGroupReady();
        }
    }

    ~TTenantHiveOosEnv() {
        if (Env.Runtime) {
            Env.Runtime->FilterFunction = {};
        }
    }

    TInstant Deadline(TDuration d) {
        return Env.Runtime->GetClock() + d;
    }

    void WaitForGroupReady() {
        for (ui32 i = 0; i < 120; ++i) {
            const TActorId edge = Env.Runtime->AllocateEdgeActor(Env.Settings.ControllerNodeId, __FILE__, __LINE__);
            Env.Runtime->WrapInActorContext(edge, [&] {
                SendToBSProxy(edge, GroupId, new TEvBlobStorage::TEvStatus(TInstant::Max()));
            });
            auto res = Env.WaitForEdgeActorEvent<TEvBlobStorage::TEvStatusResult>(
                edge, /*termOnCapture=*/true, Deadline(TDuration::Seconds(5)));
            if (res && res->Get()->Status == NKikimrProto::OK) {
                Cerr << "group ready after iter# " << i << " flags# " << res->Get()->StatusFlags.ToString() << Endl;
                return;
            }
            Env.Sim(TDuration::Seconds(1));
        }
        UNIT_FAIL("group did not become ready");
    }

    TActorId OpenPipe(ui64 tabletId) {
        TActorId pipe = Env.Runtime->Register(
            NTabletPipe::CreateClient(Edge, tabletId, NTabletPipe::TClientRetryPolicy::WithRetries()),
            Edge.NodeId());
        auto resp = Env.WaitForEdgeActorEvent<TEvTabletPipe::TEvClientConnected>(
            Edge, /*termOnCapture=*/false, Deadline(TDuration::Seconds(60)));
        UNIT_ASSERT(resp);
        UNIT_ASSERT_VALUES_EQUAL_C(resp->Get()->Status, NKikimrProto::OK,
            "pipe to tablet# " << tabletId << " failed");
        return pipe;
    }

    void ClosePipe(TActorId pipe) {
        Env.Runtime->WrapInActorContext(Edge, [&] {
            NTabletPipe::CloseClient(TActivationContext::AsActorContext(), pipe);
        });
        Env.WaitForEdgeActorEvent<TEvTabletPipe::TEvClientDestroyed>(
            Edge, /*termOnCapture=*/false, Deadline(TDuration::Seconds(10)));
    }

    ui32 VDiskStatusFlags(ui32 orderNumber) {
        const TVDiskID vdiskId = Info->GetVDiskId(orderNumber);
        ui32 flags = 0;
        Env.WithQueueId(vdiskId, NKikimrBlobStorage::EVDiskQueueId::PutTabletLog, [&](TActorId queueId) {
            const TActorId edge = Env.Runtime->AllocateEdgeActor(queueId.NodeId(), __FILE__, __LINE__);
            Env.Runtime->Send(new IEventHandle(queueId, edge, new TEvBlobStorage::TEvVStatus(vdiskId)), queueId.NodeId());
            auto r = Env.WaitForEdgeActorEvent<TEvBlobStorage::TEvVStatusResult>(edge);
            flags = r->Get()->Record.GetStatusFlags();
        });
        return flags;
    }

    bool AllVDisksAtLeast(ui32 flag) {
        for (ui32 i = 0; i < Info->GetTotalVDisksNum(); ++i) {
            if (!(VDiskStatusFlags(i) & flag)) {
                return false;
            }
        }
        return true;
    }

    bool AnyVDiskHas(ui32 flag) {
        for (ui32 i = 0; i < Info->GetTotalVDisksNum(); ++i) {
            if (VDiskStatusFlags(i) & flag) {
                return true;
            }
        }
        return false;
    }

    ui32 CountVDisksWith(ui32 flag) {
        ui32 n = 0;
        for (ui32 i = 0; i < Info->GetTotalVDisksNum(); ++i) {
            if (VDiskStatusFlags(i) & flag) {
                ++n;
            }
        }
        return n;
    }

    ui64 ChunkSize() const {
        return Oos.PDiskChunkSize;
    }

    ui64 ChunksPerPDisk() const {
        return Oos.PDiskSize / ChunkSize();
    }

    ui64 TotalCapacityBytes() const {
        return ChunksPerPDisk() * ChunkSize() * Env.PDiskMockStates.size();
    }

    // Chunks the PDisk mock has actually been written to. It counts allocation that
    // has been used, so it lags a reservation by one write; the authoritative used
    // count is the one the VDisk polls out of PDisk (TVDiskSpaceStats::DskUsedBytes).
    TString DescribeChunks() {
        TStringBuilder sb;
        for (auto& [key, state] : Env.PDiskMockStates) {
            sb << " pdisk[" << key.first << ":" << key.second << "]=" << state->GetChunks().size();
        }
        return sb;
    }

    ui64 WrittenChunksTotal() {
        ui64 n = 0;
        for (auto& [key, state] : Env.PDiskMockStates) {
            n += state->GetChunks().size();
        }
        return n;
    }

    // The colour a PDisk holding `used` chunks reports. The PDisk mock keeps one
    // shared chunk quota per disk and derives the colour from the free chunk count
    // exactly this way, so this reproduces what the VDisks will be told — without
    // waiting for the disk space tracker's next poll.
    //
    // This matters for the fill loop: the tracker polls every DskTrackerInterval of
    // *simulated* time, and a batched fill can run start to finish in less than one
    // interval, which is why the "colour" printed from the monitoring counters can
    // still say GREEN on a disk that is one chunk from PRE_ORANGE.
    NKikimrBlobStorage::TPDiskSpaceColor::E ColorForUsedChunks(ui64 used) const {
        NPDisk::TQuotaRecord quota;
        quota.ForceHardLimit(ChunksPerPDisk(), NPDisk::TColorLimits::MakeChunkLimits(130));
        quota.ForceAllocate(used);
        double occupancy = 0;
        return quota.EstimateSpaceColor(0, &occupancy);
    }

    // Used chunks at which the disk enters each colour, i.e. how wide the band a fill
    // has to cross between "user writes stop" and "system writes stop" is here.
    TString DescribeColorBands() const {
        using TColor = NKikimrBlobStorage::TPDiskSpaceColor;
        const ui64 total = ChunksPerPDisk();
        std::map<int, ui64> firstUsed;
        for (ui64 used = 0; used <= total; ++used) {
            firstUsed.try_emplace(int(ColorForUsedChunks(used)), used);
        }
        TStringBuilder sb;
        sb << " totalChunks# " << total;
        for (const auto& [color, used] : firstUsed) {
            sb << " " << TColor::E_Name(TColor::E(color)) << "At# " << used;
        }
        return sb;
    }

    ui32 CountPDisksAtLeast(NKikimrBlobStorage::TPDiskSpaceColor::E color) {
        ui32 n = 0;
        for (auto& [key, state] : Env.PDiskMockStates) {
            if (ColorForUsedChunks(state->GetChunks().size()) >= color) {
                ++n;
            }
        }
        return n;
    }

    TString DescribeEstimatedColors() {
        TStringBuilder sb;
        for (auto& [key, state] : Env.PDiskMockStates) {
            const ui64 used = state->GetChunks().size();
            sb << " pdisk[" << key.first << ":" << key.second << "]="
                << NKikimrBlobStorage::TPDiskSpaceColor_E_Name(ColorForUsedChunks(used)) << "/" << used;
        }
        return sb;
    }

    // How much of the chunks the VDisks hold has actually been written to. A chunk
    // is charged to the space colour in full the moment it is reserved, so a low
    // fill factor means the disk is being consumed by allocation rather than by
    // data, which is the difference between "the LSM holds N bytes" and "the LSM
    // owns N bytes of the disk".
    ui64 WrittenBytesInChunksTotal() {
        ui64 bytes = 0;
        for (auto& [key, state] : Env.PDiskMockStates) {
            for (const ui32 chunkIdx : state->GetChunks()) {
                for (const auto& [begin, end] : state->GetWrittenAreas(chunkIdx)) {
                    bytes += end - begin;
                }
            }
        }
        return bytes;
    }

    struct TLevelStats {
        ui64 SstNum = 0;
        ui64 NumItems = 0;
        ui64 DataInplaced = 0;
    };

    // SSTs and bytes per LSM level, summed over the group. `level` is the label the
    // VDisk registers; level 0 is the unsorted set of freshly compacted segments.
    std::map<TString, TLevelStats> CollectLevelStats() {
        std::map<TString, TLevelStats> res;
        for (ui32 nodeId : Env.Runtime->GetNodes()) {
            auto* appData = Env.Runtime->GetNode(nodeId)->AppData.get();
            WalkLevelCounters(GetServiceCounters(appData->Counters, "vdisks"), false, res);
        }
        return res;
    }

    void WalkLevelCounters(const TIntrusivePtr<::NMonitoring::TDynamicCounters>& node, bool groupMatched,
            std::map<TString, TLevelStats>& out) {
        if (!node) {
            return;
        }
        if (groupMatched) {
            if (auto levels = node->FindSubgroup("subsystem", "levels")) {
                std::vector<std::pair<TString, TString>> perLevel;
                levels->EnumerateSubgroups([&](const TString& name, const TString& value) {
                    if (name == "level") {
                        perLevel.emplace_back(name, value);
                    }
                });
                for (const auto& [name, value] : perLevel) {
                    auto group = levels->FindSubgroup(name, value);
                    TLevelStats& stats = out[value];
                    stats.SstNum += CounterValue(group, "SstNum");
                    stats.NumItems += CounterValue(group, "NumItems");
                    stats.DataInplaced += CounterValue(group, "DataInplaced");
                }
            }
        }
        std::vector<std::pair<TString, TString>> children;
        node->EnumerateSubgroups([&](const TString& name, const TString& value) {
            children.emplace_back(name, value);
        });
        for (const auto& [name, value] : children) {
            if (name == "subsystem") {
                continue;
            }
            bool matched = groupMatched;
            if (name == "group") {
                matched = value == std::to_string(GroupId);
            }
            WalkLevelCounters(node->FindSubgroup(name, value), matched, out);
        }
    }

    TString DescribeColors() {
        TStringBuilder sb;
        for (ui32 i = 0; i < Info->GetTotalVDisksNum(); ++i) {
            const ui32 flags = VDiskStatusFlags(i);
            sb << " vdisk[" << i << "]=" << NKikimrBlobStorage::TPDiskSpaceColor_E_Name(
                StatusFlagToSpaceColor(flags));
        }
        return sb;
    }

    ////////////////////////////////////////////////////////////////////////////
    // Space accounting read straight out of the VDisk monitoring counters. Asking
    // the VDisks over the wire needs a backpressure queue client per VDisk per
    // query, which is far too expensive to do inside the fill loop; the counters
    // are refreshed by the disk space tracker's own TEvCheckSpace poll.
    ////////////////////////////////////////////////////////////////////////////

    static ui64 CounterValue(const TIntrusivePtr<::NMonitoring::TDynamicCounters>& group, const TString& name) {
        if (!group) {
            return 0;
        }
        auto counter = group->FindCounter(name);
        return counter ? static_cast<ui64>(counter->Val()) : 0;
    }

    static void ReadOutOfSpaceGroup(const TIntrusivePtr<::NMonitoring::TDynamicCounters>& group,
            TVDiskSpaceStats& stats) {
        stats.DskTotalBytes = CounterValue(group, "DskTotalBytes");
        stats.DskFreeBytes = CounterValue(group, "DskFreeBytes");
        stats.DskUsedBytes = CounterValue(group, "DskUsedBytes");
        stats.HugeUsedChunks = CounterValue(group, "HugeUsedChunks");
        stats.IndexBytes = CounterValue(group, "DskSpaceCurIndex");
        stats.InplacedDataBytes = CounterValue(group, "DskSpaceCurInplacedData");
        stats.HugeDataBytes = CounterValue(group, "DskSpaceCurHugeData");

        using TColor = NKikimrBlobStorage::TPDiskSpaceColor;
        static const std::pair<const char*, TColor::E> alerts[] = {
            {"CapacityAlertBlack", TColor::BLACK},
            {"CapacityAlertRed", TColor::RED},
            {"CapacityAlertOrange", TColor::ORANGE},
            {"CapacityAlertPreOrange", TColor::PRE_ORANGE},
            {"CapacityAlertLightOrange", TColor::LIGHT_ORANGE},
            {"CapacityAlertYellow", TColor::YELLOW},
            {"CapacityAlertLightYellow", TColor::LIGHT_YELLOW},
            {"CapacityAlertCyan", TColor::CYAN},
            {"CapacityAlertGreen", TColor::GREEN},
        };
        for (const auto& [name, color] : alerts) {
            if (CounterValue(group, name)) {
                stats.Color = color;
                stats.HasColor = true;
                break;
            }
        }
    }

    void WalkCounters(const TIntrusivePtr<::NMonitoring::TDynamicCounters>& node, bool groupMatched,
            ui32 orderNumber, std::map<ui32, TVDiskSpaceStats>& out) {
        if (!node) {
            return;
        }
        if (groupMatched && orderNumber != Max<ui32>()) {
            if (auto oos = node->FindSubgroup("subsystem", "outofspace")) {
                ReadOutOfSpaceGroup(oos, out[orderNumber]);
            }
        }
        std::vector<std::pair<TString, TString>> children;
        node->EnumerateSubgroups([&](const TString& name, const TString& value) {
            children.emplace_back(name, value);
        });
        for (const auto& [name, value] : children) {
            if (name == "subsystem") {
                continue; // leaf groups; nothing below carries a VDisk identity
            }
            bool matched = groupMatched;
            ui32 next = orderNumber;
            if (name == "group") {
                matched = value == std::to_string(GroupId);
            } else if (name == "orderNumber") {
                next = FromString<ui32>(value);
            }
            WalkCounters(node->FindSubgroup(name, value), matched, next, out);
        }
    }

    std::map<ui32, TVDiskSpaceStats> CollectVDiskSpaceStats() {
        std::map<ui32, TVDiskSpaceStats> res;
        for (ui32 nodeId : Env.Runtime->GetNodes()) {
            auto* appData = Env.Runtime->GetNode(nodeId)->AppData.get();
            WalkCounters(GetServiceCounters(appData->Counters, "vdisks"), false, Max<ui32>(), res);
        }
        return res;
    }

    TString DescribeColorsFromCounters() {
        TStringBuilder sb;
        for (const auto& [orderNumber, stats] : CollectVDiskSpaceStats()) {
            sb << " vdisk[" << orderNumber << "]="
                << (stats.HasColor ? NKikimrBlobStorage::TPDiskSpaceColor_E_Name(stats.Color) : "?");
        }
        return sb;
    }

    ui32 CountVDisksAtLeastFromCounters(NKikimrBlobStorage::TPDiskSpaceColor::E color) {
        ui32 n = 0;
        for (const auto& [orderNumber, stats] : CollectVDiskSpaceStats()) {
            if (stats.HasColor && stats.Color >= color) {
                ++n;
            }
        }
        return n;
    }

    // Everything the task asks to see at the point the fill stops, on one screen.
    void Report(const TString& stage, ui32 accepted) {
        // Let the disk space tracker poll once so the counters describe the state
        // that the fill left behind rather than the one it saw a moment earlier.
        Env.Sim(TDuration::Seconds(5));

        const ui64 chunkSize = ChunkSize();
        const ui64 chunksPerDisk = ChunksPerPDisk();
        const ui64 numDisks = Env.PDiskMockStates.size();
        const ui64 capacity = TotalCapacityBytes();
        const ui64 logical = ui64(accepted) * Oos.BlobSize;
        const ui64 physical = logical * 3; // mirror-3-dc keeps three full copies

        auto stats = CollectVDiskSpaceStats();
        ui64 usedBytes = 0;
        ui64 hugeChunks = 0;
        ui64 indexBytes = 0;
        ui64 inplacedBytes = 0;
        ui64 hugeDataBytes = 0;
        for (const auto& [orderNumber, s] : stats) {
            usedBytes += s.DskUsedBytes;
            hugeChunks += s.HugeUsedChunks;
            indexBytes += s.IndexBytes;
            inplacedBytes += s.InplacedDataBytes;
            hugeDataBytes += s.HugeDataBytes;
        }
        const ui64 writtenChunks = WrittenChunksTotal();
        const ui64 writtenBytes = WrittenBytesInChunksTotal();

        auto pct = [&](ui64 num, ui64 den) {
            return den ? 100.0 * double(num) / double(den) : 0.0;
        };

        Cerr << "OOSMEAS ===== " << Oos.Label << " / " << stage << " =====" << Endl;
        Cerr << "OOSMEAS config"
            << " pdiskSize# " << Oos.PDiskSize
            << " chunkSize# " << chunkSize
            << " chunksPerDisk# " << chunksPerDisk
            << " numDisks# " << numDisks
            << " capacityBytes# " << capacity
            << " blobSize# " << Oos.BlobSize
            << " minHugeBlobInBytes# " << Oos.MinHugeBlobInBytes
            << " inFlight# " << Oos.InFlight
            << " inFlightBytes# " << ui64(Oos.InFlight) * Oos.BlobSize
            << " projection# " << Oos.Projection
            << " simClockSec# " << (Env.Runtime->GetClock() - TInstant::Zero()).Seconds()
            << Endl;
        Cerr << "OOSMEAS bands" << DescribeColorBands() << Endl;
        Cerr << "OOSMEAS volume"
            << " acceptedBlobs# " << accepted
            << " logicalBytes# " << logical
            << " physicalBytes# " << physical
            << " userOccupancyPct# " << pct(physical, capacity)
            << " allocatedBytes# " << usedBytes
            << " allocatedOccupancyPct# " << pct(usedBytes, capacity)
            << " writtenChunks# " << writtenChunks
            << " writtenOccupancyPct# " << pct(writtenChunks * chunkSize, capacity)
            << " userShareOfAllocatedPct# " << pct(physical, usedBytes)
            << Endl;
        Cerr << "OOSMEAS chunkbreakdown"
            << " allocatedChunks# " << (chunkSize ? usedBytes / chunkSize : 0)
            << " hugeHeapChunks# " << hugeChunks
            << " lsmIndexBytes# " << indexBytes
            << " lsmInplacedDataBytes# " << inplacedBytes
            << " lsmHugeDataBytes# " << hugeDataBytes
            << " lsmChunksEquivalent# " << (chunkSize ? (indexBytes + inplacedBytes) / chunkSize : 0)
            << " writtenBytesInChunks# " << writtenBytes
            << " chunkFillPct# " << pct(writtenBytes, writtenChunks * chunkSize)
            << " logChunks# n/a(pdisk mock keeps the recovery log in memory)"
            << Endl;
        for (const auto& [level, stats] : CollectLevelStats()) {
            if (!stats.SstNum && !stats.NumItems) {
                continue;
            }
            Cerr << "OOSMEAS level[" << level << "]"
                << " ssts# " << stats.SstNum
                << " items# " << stats.NumItems
                << " dataInplacedBytes# " << stats.DataInplaced
                << Endl;
        }
        for (const auto& [orderNumber, s] : stats) {
            Cerr << "OOSMEAS vdisk[" << orderNumber << "]"
                << " color# " << (s.HasColor ? NKikimrBlobStorage::TPDiskSpaceColor_E_Name(s.Color) : "?")
                << " totalChunks# " << (chunkSize ? s.DskTotalBytes / chunkSize : 0)
                << " usedChunks# " << (chunkSize ? s.DskUsedBytes / chunkSize : 0)
                << " freeChunks# " << (chunkSize ? s.DskFreeBytes / chunkSize : 0)
                << " hugeChunks# " << s.HugeUsedChunks
                << " lsmIndexBytes# " << s.IndexBytes
                << " lsmInplacedDataBytes# " << s.InplacedDataBytes
                << " lsmHugeDataBytes# " << s.HugeDataBytes
                << Endl;
        }
        Cerr << "OOSMEAS colorsFromVStatus#" << DescribeColors() << Endl;
        Cerr << "OOSMEAS colorsFromChunks#" << DescribeEstimatedColors() << Endl;
        Cerr << "OOSMEAS trace"
            << " acceptedAtFirstPreOrange# " << AcceptedAtFirstPreOrange
            << " acceptedAtFirstRefusal# " << AcceptedAtFirstRefusal
            << " acceptedAtFirstRed# " << AcceptedAtFirstRed
            << Endl;
        Cerr << "OOSMEAS ===== end " << Oos.Label << " / " << stage << " =====" << Endl;
    }

    // Analog of `ydb workload tpcc init -w … && import`: occupy some user data that
    // must remain after TestShard is dropped.
    void WriteDataset(ui32 blobs, ui32 blobSize) {
        TString data = FastGenDataForLZ4(blobSize);
        for (ui32 step = 1; step <= blobs; ++step) {
            const TLogoBlobID id(DatasetTabletId, 1, step, 0, blobSize, 0);
            const TActorId edge = Env.Runtime->AllocateEdgeActor(Env.Settings.ControllerNodeId, __FILE__, __LINE__);
            Env.Runtime->WrapInActorContext(edge, [&] {
                SendToBSProxy(edge, GroupId, new TEvBlobStorage::TEvPut(
                    TEvBlobStorage::TEvPut::TParameters{
                        .BlobId = id,
                        .Buffer = TRope(data),
                        .Deadline = TInstant::Max(),
                        .HandleClass = NKikimrBlobStorage::TabletLog,
                        .Tactic = TEvBlobStorage::TEvPut::TacticMaxThroughput,
                        .DataKind = NKikimrBlobStorage::TDataKind::USER,
                    }));
            });
            auto res = Env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(edge);
            UNIT_ASSERT_VALUES_EQUAL_C(res->Get()->Status, NKikimrProto::OK,
                "dataset put step# " << step << " " << res->Get()->ErrorReason);
        }
        Cerr << "dataset written blobs# " << blobs << " size# " << blobSize << Endl;
    }

    void WriteDataset() {
        WriteDataset(Oos.DatasetBlobs, Oos.BlobSize);
    }

    NKikimrProto::EReplyStatus PutBlob(ui64 tabletId, ui32 step, ui32 blobSize,
            NKikimrBlobStorage::TDataKind::E dataKind) {
        TString data = FastGenDataForLZ4(blobSize, step);
        const TLogoBlobID id(tabletId, 1, step, 0, blobSize, 0);
        const TActorId edge = Env.Runtime->AllocateEdgeActor(Env.Settings.ControllerNodeId, __FILE__, __LINE__);
        Env.Runtime->WrapInActorContext(edge, [&] {
            SendToBSProxy(edge, GroupId, new TEvBlobStorage::TEvPut(
                TEvBlobStorage::TEvPut::TParameters{
                    .BlobId = id,
                    .Buffer = TRope(data),
                    .Deadline = Env.Runtime->GetClock() + TDuration::Seconds(5),
                    .HandleClass = NKikimrBlobStorage::TabletLog,
                    .Tactic = TEvBlobStorage::TEvPut::TacticMaxThroughput,
                    .DataKind = dataKind,
                }));
        });
        auto res = Env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(
            edge, /*termOnCapture=*/true, Deadline(TDuration::Seconds(10)));
        LastPutError = res ? res->Get()->ErrorReason : "timeout";
        return res ? res->Get()->Status : NKikimrProto::TIMEOUT;
    }

    // Stands in for the writes Hive does: same group, same fail model, SYSTEM
    // DataKind. It cannot use HiveId itself, because the running Hive has blocked
    // generation 1 for its own tablet id.
    NKikimrProto::EReplyStatus PutSystemBlob(ui32 step, ui32 blobSize) {
        return PutBlob(SystemProbeTabletId, step, blobSize, NKikimrBlobStorage::TDataKind::SYSTEM);
    }

    // Analog of the TestShard load: pour user data in until the group stops taking it.
    // Writes go out InFlight at a time; a whole batch is admitted against the colour
    // the disks report at the moment it arrives, and the space it costs is only spent
    // afterwards, so the batch size is one of the variables under study. InFlight == 1
    // is the strictly sequential case, where the colour always keeps up.
    // Returns the number of blobs that were accepted.
    ui32 FillWithUserData() {
        const ui32 inFlight = Max<ui32>(Oos.InFlight, 1);
        const TActorId edge = Env.Runtime->AllocateEdgeActor(Env.Settings.ControllerNodeId, __FILE__, __LINE__);
        const TString data = FastGenDataForLZ4(Oos.BlobSize);
        ui32 step = 1;
        ui32 accepted = 0;
        ui32 refusedBatches = 0;
        ui32 nextReport = Oos.ReportEveryBlobs;
        bool reportedRefusal = false;
        bool reportedRed = false;
        bool reportedPreOrange = false;

        while (step < Oos.MaxFillBlobs && refusedBatches < 3) {
            ui32 sent = 0;
            while (sent < inFlight && step < Oos.MaxFillBlobs) {
                const TLogoBlobID id(DatasetTabletId + 1, 1, step++, 0, Oos.BlobSize, 0);
                Env.Runtime->WrapInActorContext(edge, [&] {
                    SendToBSProxy(edge, GroupId, new TEvBlobStorage::TEvPut(
                        TEvBlobStorage::TEvPut::TParameters{
                            .BlobId = id,
                            .Buffer = TRope(data),
                            .Deadline = TInstant::Max(),
                            .HandleClass = NKikimrBlobStorage::TabletLog,
                            .Tactic = TEvBlobStorage::TEvPut::TacticMaxThroughput,
                            .DataKind = NKikimrBlobStorage::TDataKind::USER,
                        }));
                });
                ++sent;
            }

            // Everything that was sent has to be collected before anything else talks to
            // the disks: a status query in the middle would be handed a put reply meant
            // for this actor.
            ui32 refused = 0;
            TString firstRefusal;
            for (ui32 i = 0; i < sent; ++i) {
                auto res = Env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(
                    edge, /*termOnCapture=*/false, Deadline(TDuration::Seconds(120)));
                if (!res) {
                    refused += sent - i; // nothing else is coming for this batch
                    break;
                }
                if (res->Get()->Status == NKikimrProto::OK) {
                    ++accepted;
                } else {
                    if (!firstRefusal) {
                        firstRefusal = TStringBuilder()
                            << NKikimrProto::EReplyStatus_Name(res->Get()->Status)
                            << " reason# " << res->Get()->ErrorReason;
                    }
                    ++refused;
                }
            }

            using TColor = NKikimrBlobStorage::TPDiskSpaceColor;
            if (!reportedPreOrange && CountPDisksAtLeast(TColor::PRE_ORANGE)) {
                reportedPreOrange = true;
                AcceptedAtFirstPreOrange = accepted;
                Cerr << "OOSMEAS event FIRST PRE_ORANGE accepted# " << accepted
                    << DescribeEstimatedColors() << Endl;
            }
            if (!reportedRed) {
                const ui32 red = CountPDisksAtLeast(TColor::RED);
                if (red) {
                    reportedRed = true;
                    AcceptedAtFirstRed = accepted;
                    ChunksAtFirstRed = DescribeChunks();
                    Cerr << "OOSMEAS event FIRST RED accepted# " << accepted
                        << " redDisks# " << red << DescribeEstimatedColors() << Endl;
                }
            }
            if (firstRefusal && !reportedRefusal) {
                reportedRefusal = true;
                AcceptedAtFirstRefusal = accepted;
                ColorsAtFirstRefusal = DescribeEstimatedColors();
                ChunksAtFirstRefusal = DescribeChunks();
                Cerr << "OOSMEAS event FIRST REFUSAL accepted# " << accepted
                    << " status# " << firstRefusal << ColorsAtFirstRefusal << Endl;
            }
            refusedBatches = refused == sent ? refusedBatches + 1 : 0;
            if (accepted >= nextReport || refused) {
                nextReport = accepted + Oos.ReportEveryBlobs;
                Cerr << "user fill step# " << step << " accepted# " << accepted << " refusedInBatch# " << refused
                    << DescribeEstimatedColors() << Endl;
            }
        }

        Cerr << "user fill done step# " << step << " accepted# " << accepted
            << DescribeEstimatedColors() << Endl;
        return accepted;
    }
};

// The disk the original reproducer used: 2 GB in 32 MB chunks, i.e. 64 chunks, on
// which PRE_ORANGE and RED are four chunks apart.
TOosSettings TinyDiskSettings() {
    return TOosSettings{
        .PDiskSize = 2_GB,
        .PDiskChunkSize = 32_MB,
        .MinHugeBlobInBytes = 1_MB,
        .BlobSize = 512 * 1024,
        .Projection = true,
        .RunHive = false,
        .InFlight = 1024,
        .DatasetBlobs = 200,
        .MaxFillBlobs = 20'000,
        .ReportEveryBlobs = 1'000,
        .Label = "user-fill",
    };
}

} // namespace

Y_UNIT_TEST_SUITE(TenantHiveOutOfSpace) {

    // Parallel USER puts are judged by the color the disk would be in after
    // compacting Fresh, so they must stop before ORANGE. SYSTEM puts may continue.
    Y_UNIT_TEST(UserPutsStopBeforeOrange) {
        TTenantHiveOosEnv env(TinyDiskSettings());

        env.WriteDataset();
        const ui32 accepted = env.FillWithUserData();
        UNIT_ASSERT_C(accepted > 0, "no user data was accepted at all");
        env.Report("fill stopped", accepted);

        UNIT_ASSERT_VALUES_EQUAL_C(
            env.CountVDisksAtLeastFromCounters(NKikimrBlobStorage::TPDiskSpaceColor::ORANGE), 0,
            "USER puts pushed VDisks into ORANGE:" << env.DescribeColorsFromCounters());

        UNIT_ASSERT_VALUES_EQUAL_C(env.PutSystemBlob(1, 64 * 1024), NKikimrProto::OK,
            "SYSTEM put refused while disks are at" << env.DescribeColorsFromCounters());
    }
}
