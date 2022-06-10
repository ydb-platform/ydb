#include <ydb/core/base/blobstorage.h>
#include <ydb/core/protos/blobstorage.pb.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/core/mind/bscontroller/grouper.h>
#include "cli.h"
#include "cli_cmds.h"
#include "proto_common.h"

namespace NKikimr {
namespace NDriverClient {

using NBsController::TCandidate;
using NBsController::GroupFromCandidates;

class TClientCommandGenConfigStatic : public TClientCommandConfig {
    TString ErasureList;

    TString ErasureStr;
    TString DesiredPDiskType = "ROT";
    TString DistinctionLevelStr = "body";
    TString BsFormatFile;
    TString RingDistinctionLevelStr;

    TBlobStorageGroupType Type;
    ui32 AvailabilityDomain = 1;
    ui32 GroupId = 0;
    ui32 GroupGen = 1;
    ui32 FailDomains = 0;
    ui32 VDisksPerFailDomain = 1;
    ui32 BeginLevel;
    ui32 EndLevel;
    ui32 RingBeginLevel;
    ui32 RingEndLevel;
    ui32 VSlot = 0;
    ui32 NumRings = 0;
    TString VDiskKind = "Default";
    int VDiskKindVal = 0;

    int ExpectedSlotCount = -1;

    ui64 Dc = -1;
    ui64 Room = -1;
    ui64 Rack = -1;
    ui64 Body = -1;

    TFailDomain Prefix;

    struct TPDiskInfo {
        const TString      Type;      // PDisk type as mentioned in bs_format.txt
        const TString      Path;      // path to device on a node
        const TFailDomain FailDom;   // physical location of a PDisk
        const ui64        PDiskGuid; // PDisk unique identifier
        const ui32        PDiskId;   // PDisk per-node identifier
        const ui32        NodeId;    // the node PDisk resides on

        std::optional<NKikimrBlobStorage::TPDiskConfig> PDiskConfig;

        explicit TPDiskInfo(const NKikimrConfig::TBlobStorageFormatConfig::TDrive& drive)
            : Type(drive.GetType())
            , Path(drive.GetPath())
            , FailDom(ParseFailDomain(drive))
            , PDiskGuid(drive.GetGuid())
            , PDiskId(drive.GetPDiskId())
            , NodeId(drive.GetNodeId())
        {
#define MANDATORY_FIELD(NAME) if (!drive.Has##NAME()) { ythrow yexception() << "missing \"" #NAME "\" field in Drive record"; }
            MANDATORY_FIELD(Type)
            MANDATORY_FIELD(Path)
            MANDATORY_FIELD(Guid)
            MANDATORY_FIELD(PDiskId)
            MANDATORY_FIELD(NodeId)
#undef MANDATORY_FIELD
            if (drive.HasPDiskConfig()) {
                PDiskConfig.emplace(drive.GetPDiskConfig());
            }
        }

        TString ToString() const {
            TStringStream str;
            str << "{Type# " << Type << " FailDom# " << FailDom.ToString() << " PDiskGuid# " << PDiskGuid << " PDiskId# "
                << PDiskId << " NodeId# " << NodeId << "}";
            return str.Str();
        }

    private:
        static TFailDomain ParseFailDomain(const NKikimrConfig::TBlobStorageFormatConfig::TDrive& drive) {
            TFailDomain fdom;

            using T = NKikimrConfig::TBlobStorageFormatConfig::TDrive;
            TVector<std::tuple<int, bool(T::*)() const, ui64(T::*)() const>> levels = {
                std::make_tuple(10, &T::HasDataCenterId, &T::GetDataCenterId),
                std::make_tuple(20, &T::HasRoomId,       &T::GetRoomId      ),
                std::make_tuple(30, &T::HasRackId,       &T::GetRackId      ),
                std::make_tuple(40, &T::HasBodyId,       &T::GetBodyId      )
            };
            for (const auto& triplet : levels) {
                ui32 level;
                bool (T::*pfnHas)() const;
                ui64 (T::*pfnGet)() const;
                std::tie(level, pfnHas, pfnGet) = triplet;
                if ((drive.*pfnHas)()) {
                    fdom.Levels[level] = (drive.*pfnGet)();
                }
            }

            return fdom;
        }
    };

public:
    TClientCommandGenConfigStatic()
        : TClientCommandConfig("static", {}, "Generate configuration for static group")
    {
        TStringStream erasureList("{");
        for (ui32 species = 0; species < TBlobStorageGroupType::ErasureSpeciesCount; ++species) {
            erasureList << (species ? "|" : "") << TBlobStorageGroupType::ErasureName[species];
        }
        erasureList << "}";
        ErasureList = erasureList.Str();
    }

    void Config(TConfig& config) override {
        TClientCommand::Config(config);

        config.Opts->AddLongOption("erasure", "erasure species").Required().RequiredArgument(ErasureList).StoreResult(&ErasureStr);
        config.Opts->AddLongOption("avdomain", "availability domain").Optional().RequiredArgument("NUM").StoreResult(&AvailabilityDomain);
        config.Opts->AddLongOption("groupid", "group identifier").Optional().RequiredArgument("NUM").StoreResult(&GroupId);
        config.Opts->AddLongOption("pdisktype", "desired pdisk type").Optional().RequiredArgument("{SSD|ROT}").StoreResult(&DesiredPDiskType);
        config.Opts->AddLongOption("vdiskkind", "vdisk kind")
            .Optional()
            .RequiredArgument(GetProtobufEnumOptions(NKikimrBlobStorage::TVDiskKind_EVDiskKind_descriptor()))
            .StoreResult(&VDiskKind);
        config.Opts->AddLongOption("expected-slot-count", "expected (maximum) slot count for PDisk")
            .Optional()
            .RequiredArgument("NUM")
            .StoreResult(&ExpectedSlotCount);
        config.Opts->AddLongOption("faildomains", "desired fail domains").RequiredArgument("NUM").StoreResult(&FailDomains);
        config.Opts->AddLongOption("vdisks", "vdisks per fail domain").RequiredArgument("NUM").StoreResult(&VDisksPerFailDomain);
        config.Opts->AddLongOption("vslot", "vslot id for created group").Optional().RequiredArgument("NUM").StoreResult(&VSlot);
        config.Opts->AddLongOption("dc", "location: dc").RequiredArgument("NUM").StoreResult(&Dc);
        config.Opts->AddLongOption("room", "location: room").RequiredArgument("NUM").StoreResult(&Room);
        config.Opts->AddLongOption("rack", "location: rack").RequiredArgument("NUM").StoreResult(&Rack);
        config.Opts->AddLongOption("body", "location: body").RequiredArgument("NUM").StoreResult(&Body);
        config.Opts->AddLongOption("dx", "distinction level").RequiredArgument("{dc|room|rack|body}").StoreResult(&DistinctionLevelStr);
        config.Opts->AddLongOption("ringdx", "ring distinction level").RequiredArgument("{dc|room|rack|body}").StoreResult(&RingDistinctionLevelStr);
        config.Opts->AddLongOption("rings", "number of rings").RequiredArgument("NUM").StoreResult(&NumRings);

        config.Opts->AddLongOption("bs-format-file", "path to bs_format.txt file").Required().RequiredArgument("PATH")
            .StoreResult(&BsFormatFile);
    }

    ui32 ParseDistinctionLevel(const TString& s) {
        const ui32 level =
            s == "dc"   ? 10 :
            s == "room" ? 20 :
            s == "rack" ? 30 :
            s == "body" ? 40 : 0;
        if (!level) {
            ythrow yexception() << "unknown distinction level provided: \"" << s << "\"";
        }
        return level;
    }

    void Parse(TConfig& config) override {
        TClientCommand::Parse(config);

        auto erasure = TBlobStorageGroupType::ErasureSpeciesByName(ErasureStr);
        Type = TBlobStorageGroupType(erasure);
        if (Type.GetErasure() == TBlobStorageGroupType::ErasureSpeciesCount) {
            ythrow TWithBackTrace<yexception>() << "unknown erasure species: \"" << ErasureStr << "\", valid values are: " << ErasureList;
        }

        if (!RingDistinctionLevelStr && Type.GetErasure() == TBlobStorageGroupType::ErasureMirror3dc) {
            RingDistinctionLevelStr = "dc";
        }

        BeginLevel = 0;
        EndLevel = ParseDistinctionLevel(DistinctionLevelStr);
        RingBeginLevel = 0;
        RingEndLevel = RingDistinctionLevelStr ? ParseDistinctionLevel(RingDistinctionLevelStr) : 0;

        if (Dc != (ui64)-1) {
            Prefix.Levels[10] = Dc;
        }
        if (Room != (ui64)-1) {
            Prefix.Levels[20] = Room;
        }
        if (Rack != (ui64)-1) {
            Prefix.Levels[30] = Rack;
        }
        if (Body != (ui64)-1) {
            Prefix.Levels[40] = Body;
        }

        if (!FailDomains) {
            FailDomains = Type.GetErasure() == TBlobStorageGroupType::ErasureMirror3dc ? 3 : Type.BlobSubgroupSize();
        }

        ui32 minRings = Type.GetErasure() == TBlobStorageGroupType::ErasureMirror3dc ? 3 : 1;
        if (!NumRings) {
            NumRings = minRings;
        } else if (NumRings < minRings) {
            ythrow yexception() << "number of rings (" << NumRings << ") is less than minimum possible number of rings ("
                << minRings << ") for this erasure type";
        }

        VDiskKindVal = GetProtobufOption(NKikimrBlobStorage::TVDiskKind_EVDiskKind_descriptor(), "--vdiskkind", VDiskKind);
    }

    int Run(TConfig& config) override {
        const ui32 minSubgroupSize = Type.GetErasure() == TBlobStorageGroupType::ErasureMirror3dc ? 3 : Type.BlobSubgroupSize();
        if (FailDomains < minSubgroupSize) {
            Cerr << "not enough fail domains (" << FailDomains << ") to build group; minimum " << minSubgroupSize
                << " domains required" << Endl;
            return EXIT_FAILURE;
        }

        NKikimrConfig::TBlobStorageFormatConfig bsFormat;
        try {
            TUnbufferedFileInput input(BsFormatFile);
            const bool status = google::protobuf::TextFormat::ParseFromString(input.ReadAll(), &bsFormat);
            if (!status) {
                ythrow yexception() << "failed to parse protobuf";
            }
        } catch (const yexception& ex) {
            Cerr << "Failed to read BS format \"" << BsFormatFile << "\": " << ex.what() << Endl;
            return EXIT_FAILURE;
        }

        // make a set of PDisks that fit our filtering condition
        TVector<TPDiskInfo> pdisks;
        for (const NKikimrConfig::TBlobStorageFormatConfig::TDrive& drive : bsFormat.GetDrive()) {
            TPDiskInfo pdiskInfo(drive);
            if (pdiskInfo.Type == DesiredPDiskType && Prefix.IsSubdomainOf(pdiskInfo.FailDom)) {
                pdisks.push_back(std::move(pdiskInfo));
            }
        }

        TVector<TCandidate> candidates;
        for (const TPDiskInfo& pdisk : pdisks) {
            candidates.emplace_back(pdisk.FailDom, BeginLevel, EndLevel, 0 /*badness*/, pdisk.NodeId, pdisk.PDiskId, VSlot);
        }

        TVector<TVector<TVector<const TCandidate*>>> bestGroup;
        if (!CreateGroupWithRings(candidates, NumRings, FailDomains, VDisksPerFailDomain, RingBeginLevel, RingEndLevel,
                bestGroup)) {
            Cerr << "Can't create group with given parameters"
                << " NumRings = " << NumRings
                << ", FailDomains = " << FailDomains
                << ", VDisksPerFailDomain = " << VDisksPerFailDomain
                << ", RingBeginLevel = " << RingBeginLevel
                << ", RingEndLevel = " << RingEndLevel
                << Endl;
            return EXIT_FAILURE;
        }

        NKikimrConfig::TBlobStorageConfig pb;
        NKikimrBlobStorage::TNodeWardenServiceSet& serviceSet = *pb.MutableServiceSet();
        serviceSet.AddAvailabilityDomains(AvailabilityDomain);

        auto transformVDisk = [&](const TCandidate *vdisk, auto& outFailDomain, ui32 ringIdx, ui32 domainIdx, ui32& vdiskIdx) {
            // calculate index of candidate inside our PDisk vector and find matching PDisk info
            const ui32 index = vdisk - candidates.data();
            const TPDiskInfo& pdiskInfo = pdisks[index];

            // add pdisk meta
            auto& pdiskItem = *serviceSet.AddPDisks();
            pdiskItem.SetNodeID(pdiskInfo.NodeId);
            pdiskItem.SetPDiskID(pdiskInfo.PDiskId);
            pdiskItem.SetPath(pdiskInfo.Path);
            pdiskItem.SetPDiskGuid(pdiskInfo.PDiskGuid);

            NPDisk::EDeviceType deviceType = NPDisk::DeviceTypeFromStr(pdiskInfo.Type);
            if (deviceType == NPDisk::DEVICE_TYPE_UNKNOWN) {
                ythrow yexception() << "invalid PDisk Type " << pdiskInfo.Type;
            }
            const ui64 kind = 0;
            TPDiskCategory cat(deviceType, kind);
            pdiskItem.SetPDiskCategory(cat.GetRaw());

            if (pdiskInfo.PDiskConfig) {
                auto *pdiskConfig = pdiskItem.MutablePDiskConfig();
                pdiskConfig->CopyFrom(*pdiskInfo.PDiskConfig);
            } else if (config.ParseResult->Has("expected-slot-count")) {
                auto *pdiskConfig = pdiskItem.MutablePDiskConfig();
                pdiskConfig->SetExpectedSlotCount(ExpectedSlotCount);
            }

            // add vdisk meta
            auto& vdiskItem = *serviceSet.AddVDisks();
            auto& vdiskId = *vdiskItem.MutableVDiskID();
            vdiskId.SetGroupID(GroupId);
            vdiskId.SetGroupGeneration(GroupGen);
            vdiskId.SetRing(ringIdx);
            vdiskId.SetDomain(domainIdx);
            vdiskId.SetVDisk(vdiskIdx);
            auto& vdiskLocation = *vdiskItem.MutableVDiskLocation();
            vdiskLocation.SetNodeID(vdisk->NodeId);
            vdiskLocation.SetPDiskID(vdisk->PDiskId);
            vdiskLocation.SetVDiskSlotID(vdisk->VDiskSlotId);
            vdiskLocation.SetPDiskGuid(pdiskInfo.PDiskGuid);
            vdiskItem.SetVDiskKind((NKikimrBlobStorage::TVDiskKind_EVDiskKind)VDiskKindVal);

            // add vdisk location to the group descriptor
            *outFailDomain.AddVDiskLocations() = vdiskLocation;

            ++vdiskIdx;
        };

        auto transformDomain = [&](const TVector<const TCandidate*>& inVDisks, auto& outRing, ui32 ringIdx, ui32& domainIdx) {
            ui32 vdiskIdx = 0;
            std::for_each(inVDisks.begin(), inVDisks.end(), std::bind(transformVDisk, std::placeholders::_1,
                    std::ref(*outRing.AddFailDomains()), ringIdx, domainIdx, std::ref(vdiskIdx)));
            ++domainIdx;
        };

        auto transformRing = [&](const TVector<TVector<const TCandidate*>>& inDomains, auto& outGroup, ui32& ringIdx) {
            ui32 domainIdx = 0;
            std::for_each(inDomains.begin(), inDomains.end(), std::bind(transformDomain, std::placeholders::_1,
                    std::ref(*outGroup.AddRings()), ringIdx, std::ref(domainIdx)));
            ++ringIdx;
        };

        auto& group = *serviceSet.AddGroups();
        group.SetGroupID(GroupId);
        group.SetGroupGeneration(GroupGen);
        group.SetErasureSpecies(Type.GetErasure());

        ui32 ringIdx = 0;
        std::for_each(bestGroup.begin(), bestGroup.end(), std::bind(transformRing, std::placeholders::_1,
                std::ref(group), std::ref(ringIdx)));

        TString message;
        if (!google::protobuf::TextFormat::PrintToString(pb, &message)) {
            Cerr << "failed to format resulting protobuf" << Endl;
            return EXIT_FAILURE;
        }
        Cout << message;

        return EXIT_SUCCESS;
    }
};

class TClientCommandGenConfig : public TClientCommandTree {
public:
    TClientCommandGenConfig()
        : TClientCommandTree("genconfig", {}, "Config generation")
    {
        AddCommand(std::make_unique<TClientCommandGenConfigStatic>());
    }
};

std::unique_ptr<TClientCommand> CreateClientCommandGenConfig() {
    return std::make_unique<TClientCommandGenConfig>();
}

} // NDriverClient
} // NKikimr
