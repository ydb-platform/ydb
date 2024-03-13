#include <ydb/core/base/blobstorage.h>
#include <ydb/core/protos/blobstorage.pb.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/core/mind/bscontroller/group_geometry_info.h>
#include <ydb/core/mind/bscontroller/group_mapper.h>
#include "cli.h"
#include "cli_cmds.h"
#include "proto_common.h"

namespace NKikimr {
namespace NDriverClient {

class TClientCommandGenConfigStatic : public TClientCommandBase {
    TString ErasureList;

    TString ErasureStr;
    TString DesiredPDiskType = "ROT";
    TString DistinctionLevelStr;
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

    struct TPDiskInfo {
        const TString       Type;      // PDisk type as mentioned in bs_format.txt
        const TString       Path;      // path to device on a node
        const TNodeLocation Location;
        const ui64          PDiskGuid; // PDisk unique identifier
        const ui32          PDiskId;   // PDisk per-node identifier
        const ui32          NodeId;    // the node PDisk resides on

        std::optional<NKikimrBlobStorage::TPDiskConfig> PDiskConfig;

        explicit TPDiskInfo(const NKikimrConfig::TBlobStorageFormatConfig::TDrive& drive)
            : Type(drive.GetType())
            , Path(drive.GetPath())
            , Location(ParseLocation(drive))
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
            str << "{Type# " << Type << " Location# " << Location.ToString() << " PDiskGuid# " << PDiskGuid << " PDiskId# "
                << PDiskId << " NodeId# " << NodeId << "}";
            return str.Str();
        }

    private:
        static TNodeLocation ParseLocation(const NKikimrConfig::TBlobStorageFormatConfig::TDrive& drive) {
            return TNodeLocation(
                ::ToString(drive.GetDataCenterId()),
                ::ToString(drive.GetRoomId()),
                ::ToString(drive.GetRackId()),
                ::ToString(drive.GetBodyId())
            );
        }
    };

public:
    TClientCommandGenConfigStatic()
        : TClientCommandBase("static", {}, "Generate configuration for static group")
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
        config.Opts->AddLongOption("dx", "distinction level").RequiredArgument("{dc|room|rack|body}").StoreResult(&DistinctionLevelStr);
        config.Opts->AddLongOption("ringdx", "ring distinction level").RequiredArgument("{dc|room|rack|body}").StoreResult(&RingDistinctionLevelStr);
        config.Opts->AddLongOption("rings", "number of rings").RequiredArgument("NUM").StoreResult(&NumRings);
        config.Opts->AddLongOption("domain-level-begin", "domain distinction beginning level").RequiredArgument("NUM").StoreResult(&BeginLevel);
        config.Opts->AddLongOption("domain-level-end", "domain distinction ending level").RequiredArgument("NUM").StoreResult(&EndLevel);
        config.Opts->AddLongOption("ring-level-begin", "ring distinction beginning level").RequiredArgument("NUM").StoreResult(&RingBeginLevel);
        config.Opts->AddLongOption("ring-level-end", "ring distinction ending level").RequiredArgument("NUM").StoreResult(&RingEndLevel);

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

        if (DistinctionLevelStr) {
            BeginLevel = 0;
            EndLevel = ParseDistinctionLevel(DistinctionLevelStr);
        }
        if (BeginLevel == 0 && EndLevel == 0) {
            EndLevel = ParseDistinctionLevel("body");
        }

        if (RingDistinctionLevelStr) {
            RingBeginLevel = 0;
            RingEndLevel = RingDistinctionLevelStr ? ParseDistinctionLevel(RingDistinctionLevelStr) : 0;
        }
        if (RingBeginLevel == 0 && RingEndLevel == 0 && Type.GetErasure() == TBlobStorageGroupType::ErasureMirror3dc) {
            RingEndLevel = ParseDistinctionLevel("dc");
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

        NKikimrBlobStorage::TGroupGeometry g;
        g.SetNumFailRealms(NumRings);
        g.SetNumFailDomainsPerFailRealm(FailDomains);
        g.SetNumVDisksPerFailDomain(VDisksPerFailDomain);
        g.SetRealmLevelBegin(RingBeginLevel);
        g.SetRealmLevelEnd(RingEndLevel);
        g.SetDomainLevelBegin(BeginLevel);
        g.SetDomainLevelEnd(EndLevel);

        NBsController::TGroupMapper mapper(NBsController::TGroupGeometryInfo(Type, g));

        // make a set of PDisks that fit our filtering condition
        TVector<TPDiskInfo> pdisks;
        THashMap<NBsController::TPDiskId, size_t> pdiskMap;
        for (const NKikimrConfig::TBlobStorageFormatConfig::TDrive& drive : bsFormat.GetDrive()) {
            TPDiskInfo pdiskInfo(drive);
            if (pdiskInfo.Type == DesiredPDiskType) {
                pdisks.push_back(pdiskInfo);

                mapper.RegisterPDisk({
                    .PDiskId{pdiskInfo.NodeId, pdiskInfo.PDiskId},
                    .Location = pdiskInfo.Location,
                    .Usable = true,
                    .NumSlots = 0,
                    .MaxSlots = 1,
                    .SpaceAvailable = 0,
                    .Operational = true,
                    .Decommitted = false,
                });

                pdiskMap.emplace(NBsController::TPDiskId{pdiskInfo.NodeId, pdiskInfo.PDiskId}, pdisks.size() - 1);
            }
        }

        NBsController::TGroupMapper::TGroupDefinition group;
        TString error;
        if (!mapper.AllocateGroup(0, group, {}, {}, 0, false, error)) {
            Cerr << "Can't allocate group: " << error << Endl;
            return EXIT_FAILURE;
        }

        NKikimrConfig::TBlobStorageConfig pb;
        NKikimrBlobStorage::TNodeWardenServiceSet& serviceSet = *pb.MutableServiceSet();
        serviceSet.AddAvailabilityDomains(AvailabilityDomain);

        auto& pgroup = *serviceSet.AddGroups();
        pgroup.SetGroupID(GroupId);
        pgroup.SetGroupGeneration(GroupGen);
        pgroup.SetErasureSpecies(Type.GetErasure());

        for (ui32 ringIdx = 0; ringIdx < group.size(); ++ringIdx) {
            auto *ring = pgroup.AddRings();
            for (ui32 domainIdx = 0; domainIdx < group[ringIdx].size(); ++domainIdx) {
                auto *domain = ring->AddFailDomains();
                for (ui32 vdiskIdx = 0; vdiskIdx < group[ringIdx][domainIdx].size(); ++vdiskIdx) {
                    const TPDiskInfo& pdiskInfo = pdisks[pdiskMap[group[ringIdx][domainIdx][vdiskIdx]]];

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
                    vdiskLocation.SetNodeID(pdiskInfo.NodeId);
                    vdiskLocation.SetPDiskID(pdiskInfo.PDiskId);
                    vdiskLocation.SetVDiskSlotID(0);
                    vdiskLocation.SetPDiskGuid(pdiskInfo.PDiskGuid);
                    vdiskItem.SetVDiskKind((NKikimrBlobStorage::TVDiskKind_EVDiskKind)VDiskKindVal);

                    // add vdisk location to the group descriptor
                    domain->AddVDiskLocations()->CopyFrom(vdiskLocation);
                }
            }
        }

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
