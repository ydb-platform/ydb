#include "service_distributed_storage.h"
#include "rpc_deferrable.h"

#include <ydb/core/base/auth.h>
#include <ydb/core/base/blobstorage_tablet_types.h>
#include <ydb/core/base/tabletid.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/blobstorage/nodewarden/node_warden_events.h>
#include <ydb/core/protos/blobstorage_config.pb.h>
#include <ydb/core/protos/blobstorage_disk_color.pb.h>
#include <ydb/core/protos/stream.pb.h>
#include <ydb/public/api/protos/draft/ydb_distributed_storage.pb.h>

#include <util/string/builder.h>

#include <optional>

namespace NKikimr::NGRpcService {

using namespace NActors;

using TEvStreamStorageStateRequest = TGrpcRequestNoOperationCall<Ydb::DistributedStorage::StorageStateRequest,
                                                                 Ydb::DistributedStorage::StorageStateResponse>;
using TEvReassignVDiskRequest = TGrpcRequestOperationCall<Ydb::DistributedStorage::ReassignVDiskRequest,
                                                          Ydb::DistributedStorage::ReassignVDiskResponse>;

namespace {

bool IsStaticGroup(ui32 groupId) {
    return TGroupID(groupId).ConfigurationType() == EGroupConfigurationType::Static;
}

Ydb::DistributedStorage::DeviceType ConvertDeviceType(NKikimrBlobStorage::EPDiskType type) {
    switch (type) {
        case NKikimrBlobStorage::ROT:
            return Ydb::DistributedStorage::DEVICE_TYPE_HDD;
        case NKikimrBlobStorage::SSD:
            return Ydb::DistributedStorage::DEVICE_TYPE_SSD;
        case NKikimrBlobStorage::NVME:
            return Ydb::DistributedStorage::DEVICE_TYPE_NVME;
        case NKikimrBlobStorage::UNKNOWN_TYPE:
        default:
            return Ydb::DistributedStorage::DEVICE_TYPE_UNSPECIFIED;
    }
}

Ydb::DistributedStorage::PDiskStatus ConvertPDiskStatus(NKikimrBlobStorage::EDriveStatus status) {
    switch (status) {
        case NKikimrBlobStorage::ACTIVE:
            return Ydb::DistributedStorage::PDISK_STATUS_ACTIVE;
        case NKikimrBlobStorage::INACTIVE:
            return Ydb::DistributedStorage::PDISK_STATUS_INACTIVE;
        case NKikimrBlobStorage::BROKEN:
            return Ydb::DistributedStorage::PDISK_STATUS_BROKEN;
        case NKikimrBlobStorage::FAULTY:
            return Ydb::DistributedStorage::PDISK_STATUS_FAULTY;
        case NKikimrBlobStorage::TO_BE_REMOVED:
            return Ydb::DistributedStorage::PDISK_STATUS_TO_BE_REMOVED;
        case NKikimrBlobStorage::UNKNOWN:
        default:
            return Ydb::DistributedStorage::PDISK_STATUS_UNSPECIFIED;
    }
}

Ydb::DistributedStorage::PDiskDecommitStatus ConvertPDiskDecommitStatus(NKikimrBlobStorage::EDecommitStatus status) {
    switch (status) {
        case NKikimrBlobStorage::DECOMMIT_NONE:
            return Ydb::DistributedStorage::PDISK_DECOMMIT_STATUS_NONE;
        case NKikimrBlobStorage::DECOMMIT_PENDING:
            return Ydb::DistributedStorage::PDISK_DECOMMIT_STATUS_PENDING;
        case NKikimrBlobStorage::DECOMMIT_IMMINENT:
            return Ydb::DistributedStorage::PDISK_DECOMMIT_STATUS_IMMINENT;
        case NKikimrBlobStorage::DECOMMIT_REJECTED:
            return Ydb::DistributedStorage::PDISK_DECOMMIT_STATUS_REJECTED;
        case NKikimrBlobStorage::DECOMMIT_UNSET:
        default:
            return Ydb::DistributedStorage::PDISK_DECOMMIT_STATUS_UNSPECIFIED;
    }
}

Ydb::DistributedStorage::PDiskMaintenanceStatus ConvertPDiskMaintenanceStatus(NKikimrBlobStorage::TMaintenanceStatus::E status) {
    switch (status) {
        case NKikimrBlobStorage::TMaintenanceStatus::NO_REQUEST:
            return Ydb::DistributedStorage::PDISK_MAINTENANCE_STATUS_NO_REQUEST;
        case NKikimrBlobStorage::TMaintenanceStatus::LONG_TERM_MAINTENANCE_PLANNED:
            return Ydb::DistributedStorage::PDISK_MAINTENANCE_STATUS_LONG_TERM_MAINTENANCE_PLANNED;
        case NKikimrBlobStorage::TMaintenanceStatus::NO_NEW_VDISKS:
            return Ydb::DistributedStorage::PDISK_MAINTENANCE_STATUS_NO_NEW_VDISKS;
        case NKikimrBlobStorage::TMaintenanceStatus::NOT_SET:
        default:
            return Ydb::DistributedStorage::PDISK_MAINTENANCE_STATUS_UNSPECIFIED;
    }
}

Ydb::DistributedStorage::VDiskStatus ConvertVDiskStatus(TStringBuf status) {
    if (status == "ERROR") {
        return Ydb::DistributedStorage::VDISK_STATUS_ERROR;
    }
    if (status == "INIT_PENDING") {
        return Ydb::DistributedStorage::VDISK_STATUS_INIT_PENDING;
    }
    if (status == "REPLICATING") {
        return Ydb::DistributedStorage::VDISK_STATUS_REPLICATING;
    }
    if (status == "READY") {
        return Ydb::DistributedStorage::VDISK_STATUS_READY;
    }
    return Ydb::DistributedStorage::VDISK_STATUS_UNSPECIFIED;
}

Ydb::DistributedStorage::GroupStatus ConvertGroupStatus(NKikimrBlobStorage::TGroupStatus::E status) {
    switch (status) {
        case NKikimrBlobStorage::TGroupStatus::FULL:
            return Ydb::DistributedStorage::GROUP_STATUS_FULL;
        case NKikimrBlobStorage::TGroupStatus::PARTIAL:
            return Ydb::DistributedStorage::GROUP_STATUS_PARTIAL;
        case NKikimrBlobStorage::TGroupStatus::DEGRADED:
            return Ydb::DistributedStorage::GROUP_STATUS_DEGRADED;
        case NKikimrBlobStorage::TGroupStatus::DISINTEGRATED:
            return Ydb::DistributedStorage::GROUP_STATUS_DISINTEGRATED;
        case NKikimrBlobStorage::TGroupStatus::UNKNOWN:
        default:
            return Ydb::DistributedStorage::GROUP_STATUS_UNSPECIFIED;
    }
}

Ydb::DistributedStorage::SpaceColor ConvertSpaceColor(NKikimrBlobStorage::TPDiskSpaceColor::E color) {
    switch (color) {
        case NKikimrBlobStorage::TPDiskSpaceColor::GREEN:
            return Ydb::DistributedStorage::SPACE_COLOR_GREEN;
        case NKikimrBlobStorage::TPDiskSpaceColor::CYAN:
            return Ydb::DistributedStorage::SPACE_COLOR_CYAN;
        case NKikimrBlobStorage::TPDiskSpaceColor::LIGHT_YELLOW:
            return Ydb::DistributedStorage::SPACE_COLOR_LIGHT_YELLOW;
        case NKikimrBlobStorage::TPDiskSpaceColor::YELLOW:
            return Ydb::DistributedStorage::SPACE_COLOR_YELLOW;
        case NKikimrBlobStorage::TPDiskSpaceColor::LIGHT_ORANGE:
            return Ydb::DistributedStorage::SPACE_COLOR_LIGHT_ORANGE;
        case NKikimrBlobStorage::TPDiskSpaceColor::PRE_ORANGE:
            return Ydb::DistributedStorage::SPACE_COLOR_PRE_ORANGE;
        case NKikimrBlobStorage::TPDiskSpaceColor::ORANGE:
            return Ydb::DistributedStorage::SPACE_COLOR_ORANGE;
        case NKikimrBlobStorage::TPDiskSpaceColor::RED:
            return Ydb::DistributedStorage::SPACE_COLOR_RED;
        case NKikimrBlobStorage::TPDiskSpaceColor::BLACK:
            return Ydb::DistributedStorage::SPACE_COLOR_BLACK;
        default:
            return Ydb::DistributedStorage::SPACE_COLOR_UNSPECIFIED;
    }
}

Ydb::DistributedStorage::VirtualGroupState ConvertVirtualGroupState(NKikimrBlobStorage::EVirtualGroupState state) {
    switch (state) {
        case NKikimrBlobStorage::EVirtualGroupState::NEW:
            return Ydb::DistributedStorage::VIRTUAL_GROUP_STATE_NEW;
        case NKikimrBlobStorage::EVirtualGroupState::CREATE_FAILED:
            return Ydb::DistributedStorage::VIRTUAL_GROUP_STATE_CREATE_FAILED;
        case NKikimrBlobStorage::EVirtualGroupState::WORKING:
            return Ydb::DistributedStorage::VIRTUAL_GROUP_STATE_WORKING;
        case NKikimrBlobStorage::EVirtualGroupState::DELETING:
            return Ydb::DistributedStorage::VIRTUAL_GROUP_STATE_DELETING;
        default:
            return Ydb::DistributedStorage::VIRTUAL_GROUP_STATE_UNSPECIFIED;
    }
}

Ydb::DistributedStorage::GroupDecommitStatus ConvertGroupDecommitStatus(NKikimrBlobStorage::TGroupDecommitStatus::E status) {
    switch (status) {
        case NKikimrBlobStorage::TGroupDecommitStatus::NONE:
            return Ydb::DistributedStorage::GROUP_DECOMMIT_STATUS_NONE;
        case NKikimrBlobStorage::TGroupDecommitStatus::PENDING:
            return Ydb::DistributedStorage::GROUP_DECOMMIT_STATUS_PENDING;
        case NKikimrBlobStorage::TGroupDecommitStatus::IN_PROGRESS:
            return Ydb::DistributedStorage::GROUP_DECOMMIT_STATUS_IN_PROGRESS;
        case NKikimrBlobStorage::TGroupDecommitStatus::DONE:
            return Ydb::DistributedStorage::GROUP_DECOMMIT_STATUS_DONE;
        case NKikimrBlobStorage::TGroupDecommitStatus::RECOMMISSIONING:
            return Ydb::DistributedStorage::GROUP_DECOMMIT_STATUS_RECOMMISSIONING;
        default:
            return Ydb::DistributedStorage::GROUP_DECOMMIT_STATUS_UNSPECIFIED;
    }
}

Ydb::DistributedStorage::NodeType ConvertNodeType(NKikimrBlobStorage::ENodeType type) {
    switch (type) {
        case NKikimrBlobStorage::NT_STATIC:
            return Ydb::DistributedStorage::NODE_TYPE_STATIC;
        case NKikimrBlobStorage::NT_DYNAMIC:
            return Ydb::DistributedStorage::NODE_TYPE_DYNAMIC;
        case NKikimrBlobStorage::NT_UNKNOWN:
        default:
            return Ydb::DistributedStorage::NODE_TYPE_UNSPECIFIED;
    }
}

Ydb::DistributedStorage::EncryptionMode ConvertEncryptionMode(ui32 mode) {
    switch (mode) {
        case 0:
            return Ydb::DistributedStorage::ENCRYPTION_MODE_NONE;
        case 1:
            return Ydb::DistributedStorage::ENCRYPTION_MODE_CHACHA8;
        default:
            return Ydb::DistributedStorage::ENCRYPTION_MODE_UNSPECIFIED;
    }
}

Ydb::DistributedStorage::DeviceLifeStage ConvertLifeStage(NKikimrBlobStorage::TDriveLifeStage::E stage) {
    switch (stage) {
        case NKikimrBlobStorage::TDriveLifeStage::FREE:
            return Ydb::DistributedStorage::DEVICE_LIFE_STAGE_FREE;
        case NKikimrBlobStorage::TDriveLifeStage::ADDED_BY_DSTOOL:
            return Ydb::DistributedStorage::DEVICE_LIFE_STAGE_ADDED;
        case NKikimrBlobStorage::TDriveLifeStage::REMOVED_BY_DSTOOL:
            return Ydb::DistributedStorage::DEVICE_LIFE_STAGE_REMOVED;
        default:
            return Ydb::DistributedStorage::DEVICE_LIFE_STAGE_UNSPECIFIED;
    }
}

std::optional<bool> FromTriState(NKikimrBlobStorage::ETriStateBool value) {
    switch (value) {
        case NKikimrBlobStorage::kTrue:
            return true;
        case NKikimrBlobStorage::kFalse:
            return false;
        default:
            return std::nullopt;
    }
}

void SetPDiskId(ui32 nodeId, ui32 pdiskId, Ydb::DistributedStorage::PDiskId& to) {
    to.set_node_id(nodeId);
    to.set_pdisk_id(pdiskId);
}

void CopyVDiskId(const NKikimrBlobStorage::TBaseConfig::TVSlot& from, Ydb::DistributedStorage::VDiskId& to) {
    to.set_group_id(from.GetGroupId());
    to.set_group_generation(from.GetGroupGeneration());
    to.set_fail_realm_idx(from.GetFailRealmIdx());
    to.set_fail_domain_idx(from.GetFailDomainIdx());
    to.set_vdisk_idx(from.GetVDiskIdx());
}

void CopyVDiskId(const NKikimrBlobStorage::TVDiskID& from, Ydb::DistributedStorage::VDiskId& to) {
    to.set_group_id(from.GetGroupID());
    to.set_group_generation(from.GetGroupGeneration());
    to.set_fail_realm_idx(from.GetRing());
    to.set_fail_domain_idx(from.GetDomain());
    to.set_vdisk_idx(from.GetVDisk());
}

void CopyVDiskId(const Ydb::DistributedStorage::VDiskId& from, NKikimrBlobStorage::TVDiskID& to) {
    to.SetGroupID(from.group_id());
    to.SetGroupGeneration(from.group_generation());
    to.SetRing(from.fail_realm_idx());
    to.SetDomain(from.fail_domain_idx());
    to.SetVDisk(from.vdisk_idx());
}

void CopyPDiskId(const Ydb::DistributedStorage::PDiskId& from, NKikimrBlobStorage::TPDiskId& to) {
    to.SetNodeId(from.node_id());
    to.SetPDiskId(from.pdisk_id());
}

bool IsSameVDiskId(const NKikimrBlobStorage::TVDiskID& lhs, const Ydb::DistributedStorage::VDiskId& rhs) {
    return lhs.GetGroupID() == rhs.group_id()
        && lhs.GetGroupGeneration() == rhs.group_generation()
        && lhs.GetRing() == rhs.fail_realm_idx()
        && lhs.GetDomain() == rhs.fail_domain_idx()
        && lhs.GetVDisk() == rhs.vdisk_idx();
}

void CopyVSlotId(const NKikimrBlobStorage::TVSlotId& from, Ydb::DistributedStorage::VSlotId& to) {
    to.set_node_id(from.GetNodeId());
    to.set_pdisk_id(from.GetPDiskId());
    to.set_vslot_id(from.GetVSlotId());
}

void CopyNodeLocation(Ydb::Discovery::NodeLocation* dst, const NActorsInterconnect::TNodeLocation& src) {
    if (src.HasDataCenterNum()) {
        dst->set_data_center_num(src.GetDataCenterNum());
    }
    if (src.HasRoomNum()) {
        dst->set_room_num(src.GetRoomNum());
    }
    if (src.HasRackNum()) {
        dst->set_rack_num(src.GetRackNum());
    }
    if (src.HasBodyNum()) {
        dst->set_body_num(src.GetBodyNum());
    }
    if (src.HasBody()) {
        dst->set_body(src.GetBody());
    }
    if (src.HasBridgePileName()) {
        dst->set_bridge_pile_name(src.GetBridgePileName());
    }
    if (src.HasDataCenter()) {
        dst->set_data_center(src.GetDataCenter());
    }
    if (src.HasModule()) {
        dst->set_module(src.GetModule());
    }
    if (src.HasRack()) {
        dst->set_rack(src.GetRack());
    }
    if (src.HasUnit()) {
        dst->set_unit(src.GetUnit());
    }
}

void ConvertPDisk(const NKikimrBlobStorage::TBaseConfig::TPDisk& from, Ydb::DistributedStorage::PDisk& to) {
    SetPDiskId(from.GetNodeId(), from.GetPDiskId(), *to.mutable_id());

    to.set_path(from.GetPath());
    to.set_device_type(ConvertDeviceType(from.GetType()));
    to.set_box_id(from.GetBoxId());
    to.set_guid(from.GetGuid());
    to.set_kind(from.GetKind());

    to.set_status(ConvertPDiskStatus(from.GetDriveStatus()));
    to.set_decommit_status(ConvertPDiskDecommitStatus(from.GetDecommitStatus()));
    to.set_maintenance_status(ConvertPDiskMaintenanceStatus(from.GetMaintenanceStatus()));
    to.set_read_only(from.GetReadOnly());

    to.set_expected_serial(from.GetExpectedSerial());
    to.set_last_seen_serial(from.GetLastSeenSerial());

    if (const auto value = FromTriState(from.GetSharedWithOs())) {
        to.set_shared_with_os(*value);
    }
    if (const auto value = FromTriState(from.GetReadCentric())) {
        to.set_read_centric(*value);
    }

    to.set_is_static(from.GetBoxId() == 0);

    const auto& metrics = from.GetPDiskMetrics();
    to.set_expected_slot_count(metrics.HasSlotCount() ? metrics.GetSlotCount() : from.GetExpectedSlotCount());
    to.set_slot_size_in_units(metrics.HasSlotSizeInUnits() ? metrics.GetSlotSizeInUnits() : from.GetPDiskConfig().GetSlotSizeInUnits());
    to.set_num_static_slots(from.GetNumStaticSlots());
    to.set_enforced_dynamic_slot_size(metrics.GetEnforcedDynamicSlotSize());

    to.set_total_size(metrics.GetTotalSize());
    to.set_available_size(metrics.GetAvailableSize());
    to.set_max_read_throughput_bytes_per_second(metrics.GetMaxReadThroughput());
    to.set_max_write_throughput_bytes_per_second(metrics.GetMaxWriteThroughput());
    to.set_max_iops(metrics.GetMaxIOPS());
}

void ConvertVDisk(const NKikimrBlobStorage::TBaseConfig::TVSlot& from, Ydb::DistributedStorage::VDisk& to) {
    CopyVDiskId(from, *to.mutable_id());
    CopyVSlotId(from.GetVSlotId(), *to.mutable_slot_id());

    to.set_kind(from.GetVDiskKind());
    to.set_status(ConvertVDiskStatus(from.GetStatus()));
    to.set_ready(from.GetReady());
    to.set_read_only(from.GetReadOnly());
    to.set_is_static(IsStaticGroup(from.GetGroupId()));

    for (const auto& donor : from.GetDonors()) {
        auto* d = to.add_donors();
        CopyVDiskId(donor.GetVDiskId(), *d->mutable_vdisk_id());
        CopyVSlotId(donor.GetVSlotId(), *d->mutable_slot_id());
        d->set_allocated_size(donor.GetVDiskMetrics().GetAllocatedSize());
    }

    const auto& metrics = from.GetVDiskMetrics();
    to.set_allocated_size(metrics.HasAllocatedSize() ? metrics.GetAllocatedSize() : from.GetAllocatedSize());
    to.set_available_size(metrics.GetAvailableSize());

    if (metrics.HasVDiskSlotUsage()) {
        to.set_slot_usage(metrics.GetVDiskSlotUsage() / 100.0);
    }
    if (metrics.HasVDiskRawUsage()) {
        to.set_raw_usage(metrics.GetVDiskRawUsage() / 100.0);
    }
    if (metrics.HasNormalizedOccupancy()) {
        to.set_normalized_occupancy(metrics.GetNormalizedOccupancy());
    }
    if (metrics.HasCapacityAlert()) {
        to.set_space_color(ConvertSpaceColor(metrics.GetCapacityAlert()));
    }
}

void ConvertGroup(const NKikimrBlobStorage::TBaseConfig::TGroup& from, Ydb::DistributedStorage::Group& to) {
    to.set_group_id(from.GetGroupId());
    to.set_generation(from.GetGroupGeneration());
    to.set_erasure_species(from.GetErasureSpecies());
    to.set_box_id(from.GetBoxId());
    to.set_storage_pool_id(from.GetStoragePoolId());

    for (const auto& vslotId : from.GetVSlotId()) {
        CopyVSlotId(vslotId, *to.add_vslot_ids());
    }

    to.set_operating_status(ConvertGroupStatus(from.GetOperatingStatus()));
    to.set_expected_status(ConvertGroupStatus(from.GetExpectedStatus()));
    to.set_seen_operational(from.GetSeenOperational());

    to.set_size_in_units(from.GetGroupSizeInUnits());
    to.set_is_static(IsStaticGroup(from.GetGroupId()));
    to.set_is_proxy(from.GetIsProxyGroup());

    if (from.HasVirtualGroupInfo()) {
        const auto& vgi = from.GetVirtualGroupInfo();
        auto* vg = to.mutable_virtual_group();
        vg->set_state(ConvertVirtualGroupState(vgi.GetState()));
        vg->set_name(vgi.GetName());
        vg->set_blob_depot_id(vgi.GetBlobDepotId());
        vg->set_error_reason(vgi.GetErrorReason());
        vg->set_decommit_status(ConvertGroupDecommitStatus(vgi.GetDecommitStatus()));
    }
}

void ConvertStoragePool(const NKikimrBlobStorage::TDefineStoragePool& from, Ydb::DistributedStorage::StoragePool& to) {
    to.set_box_id(from.GetBoxId());
    to.set_storage_pool_id(from.GetStoragePoolId());
    to.set_name(from.GetName());
    to.set_erasure_species(from.GetErasureSpecies());
    to.set_kind(from.GetKind());
    to.set_vdisk_kind(from.GetVDiskKind());

    to.set_num_groups(from.GetNumGroups());
    to.set_default_group_size_in_units(from.GetDefaultGroupSizeInUnits());

    if (from.HasGeometry()) {
        const auto& g = from.GetGeometry();
        auto* geometry = to.mutable_geometry();
        geometry->set_realm_level_begin(g.GetRealmLevelBegin());
        geometry->set_realm_level_end(g.GetRealmLevelEnd());
        geometry->set_domain_level_begin(g.GetDomainLevelBegin());
        geometry->set_domain_level_end(g.GetDomainLevelEnd());
        geometry->set_num_fail_realms(g.GetNumFailRealms());
        geometry->set_num_fail_domains_per_fail_realm(g.GetNumFailDomainsPerFailRealm());
        geometry->set_num_vdisks_per_fail_domain(g.GetNumVDisksPerFailDomain());
    }

    for (const auto& filter : from.GetPDiskFilter()) {
        auto* f = to.add_pdisk_filters();
        for (const auto& property : filter.GetProperty()) {
            switch (property.GetPropertyCase()) {
                case NKikimrBlobStorage::TPDiskFilter::TRequiredProperty::kType:
                    f->set_device_type(ConvertDeviceType(property.GetType()));
                    break;
                case NKikimrBlobStorage::TPDiskFilter::TRequiredProperty::kSharedWithOs:
                    f->set_shared_with_os(property.GetSharedWithOs());
                    break;
                case NKikimrBlobStorage::TPDiskFilter::TRequiredProperty::kReadCentric:
                    f->set_read_centric(property.GetReadCentric());
                    break;
                case NKikimrBlobStorage::TPDiskFilter::TRequiredProperty::kKind:
                    f->set_kind(property.GetKind());
                    break;
                default:
                    break;
            }
        }
    }

    to.set_encryption_mode(ConvertEncryptionMode(from.GetEncryptionMode()));
    to.set_config_generation(from.GetItemConfigGeneration());
}

void ConvertNode(const NKikimrBlobStorage::TBaseConfig::TNode& from, Ydb::DistributedStorage::Node& to) {
    to.set_node_id(from.GetNodeId());
    to.set_host(from.GetHostKey().GetFqdn());
    to.set_port(from.GetHostKey().GetIcPort());
    to.set_type(ConvertNodeType(from.GetType()));
    CopyNodeLocation(to.mutable_location(), from.GetLocation());
}

void ConvertDevice(const NKikimrBlobStorage::TBaseConfig::TDevice& from, Ydb::DistributedStorage::Device& to) {
    to.set_serial_number(from.GetSerialNumber());
    to.set_box_id(from.GetBoxId());
    to.set_node_id(from.GetNodeId());
    to.set_device_type(ConvertDeviceType(from.GetType()));
    to.set_path(from.GetPath());
    if (from.GetPDiskId()) {
        SetPDiskId(from.GetNodeId(), from.GetPDiskId(), *to.mutable_pdisk_id());
    }
    if (from.GetGuid()) {
        to.set_guid(from.GetGuid());
    }
    to.set_life_stage(ConvertLifeStage(from.GetLifeStage()));
}

void ConvertClusterSettings(const NKikimrBlobStorage::TUpdateSettings& from, Ydb::DistributedStorage::ClusterSettings& to) {
    to.set_default_max_slots(from.GetDefaultMaxSlots(0));
    to.set_enable_self_heal(from.GetEnableSelfHeal(0));
    to.set_enable_donor_mode(from.GetEnableDonorMode(0));
    to.set_scrub_periodicity_seconds(from.GetScrubPeriodicitySeconds(0));
    to.set_pdisk_space_margin_promille(from.GetPDiskSpaceMarginPromille(0));
    to.set_group_reserve_min(from.GetGroupReserveMin(0));
    to.set_group_reserve_part_ppm(from.GetGroupReservePartPPM(0));
    to.set_max_scrubbed_pdisks_at_once(from.GetMaxScrubbedDisksAtOnce(0));
    to.set_pdisk_space_color_border(ConvertSpaceColor(from.GetPDiskSpaceColorBorder(0)));
    to.set_enable_group_layout_sanitizer(from.GetEnableGroupLayoutSanitizer(0));
    to.set_allow_multiple_realms_occupation(from.GetAllowMultipleRealmsOccupation(0));
    to.set_use_self_heal_local_policy(from.GetUseSelfHealLocalPolicy(0));
    to.set_try_to_relocate_broken_disks_locally_first(from.GetTryToRelocateBrokenDisksLocallyFirst(0));
}

void ApplySafetyOptions(const Ydb::DistributedStorage::SafetyOptions& options, NKikimrBlobStorage::TConfigRequest& request) {
    request.SetIgnoreDegradedGroupsChecks(options.ignore_degraded_groups());
    request.SetIgnoreGroupFailModelChecks(options.ignore_group_failure_model());
}

void SetUserSid(const NACLib::TUserToken* token, NKikimrBlobStorage::TConfigRequest& request) {
    if (token) {
        request.SetUserSID(token->GetUserSID());
    }
}

struct TBscMutationError {
    Ydb::StatusIds::StatusCode Status;
    TString Message;
};

Ydb::StatusIds::StatusCode MapBscMutationError(NKikimrBlobStorage::TConfigResponse::TStatus::EFailReason reason) {
    using TStatus = NKikimrBlobStorage::TConfigResponse::TStatus;

    switch (reason) {
        case TStatus::kHostNotFound:
        case TStatus::kPDiskNotFound:
        case TStatus::kHostConfigNotFound:
        case TStatus::kGroupNotFound:
        case TStatus::kVSlotNotFound:
            return Ydb::StatusIds::NOT_FOUND;

        case TStatus::kItemConfigGenerationMismatch:
        case TStatus::kMayLoseData:
        case TStatus::kVDiskIdIncorrect:
        case TStatus::kDiskIsNotDonor:
        case TStatus::kAlready:
        case TStatus::kMayGetDegraded:
        case TStatus::kReassignNotViable:
        case TStatus::kGroupGenerationMismatch:
        case TStatus::kGeneric:
        default:
            return Ydb::StatusIds::PRECONDITION_FAILED;
    }
}

std::optional<TBscMutationError> ValidateBscMutationResponse(const NKikimrBlobStorage::TConfigResponse& response, bool dryRun, size_t expectedStatuses) {
    for (const auto& status : response.GetStatus()) {
        if (!status.GetSuccess()) {
            TString message = status.GetErrorDescription();
            if (message.empty()) {
                message = response.GetErrorDescription();
            }
            if (message.empty()) {
                message = "BSC rejected the operation";
            }
            return TBscMutationError{
                MapBscMutationError(status.GetFailReason()),
                std::move(message),
            };
        }
    }

    if (response.StatusSize() != expectedStatuses) {
        return TBscMutationError{
            Ydb::StatusIds::INTERNAL_ERROR,
            TStringBuilder() << "BSC returned " << response.StatusSize() << " command statuses, expected " << expectedStatuses,
        };
    }

    const bool transactionFinishedAsExpected = dryRun ? response.GetRollbackSuccess() : response.GetSuccess();
    if (!transactionFinishedAsExpected) {
        return TBscMutationError{
            Ydb::StatusIds::PRECONDITION_FAILED,
            !response.GetErrorDescription().empty() ? response.GetErrorDescription() : "BSC rejected the operation",
        };
    }

    return std::nullopt;
}

size_t VarintSize(size_t value) {
    size_t size = 1;
    while (value >= 128) {
        value >>= 7;
        ++size;
    }
    return size;
}

} // namespace

class TStreamStorageStateRequest : public TActorBootstrapped<TStreamStorageStateRequest> {
    using TBase = TActorBootstrapped<TStreamStorageStateRequest>;

    static constexpr size_t kDefaultTargetPartSize = 4 * 1024 * 1024;

    enum class ESection {
        Settings,
        StoragePools,
        Groups,
        VDisks,
        PDisks,
        Nodes,
        Devices,
        Done,
    };

    enum class EBuildResult {
        Ready,
        Done,
        ItemTooLarge,
    };

    enum class EFillResult {
        Empty,
        Ready,
        ItemTooLarge,
    };

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::GRPC_STREAM_REQ;
    }

    explicit TStreamStorageStateRequest(TEvStreamStorageStateRequest* request)
        : Request(request)
    {}

    void Bootstrap(const TActorContext& ctx) {
        const auto& streamConfig = AppData(ctx)->StreamingConfig.GetOutputStreamConfig();
        MaxPartSize = Max<size_t>(1, streamConfig.GetMessageSizeLimit());
        TargetPartSize = Min(kDefaultTargetPartSize, MaxPartSize);
        InactiveClientTimeout = TDuration::FromValue(streamConfig.GetInactiveClientTimeout());

        const auto selfId = SelfId();
        const auto* actorSystem = ctx.ActorSystem();
        Request->SetFinishAction([selfId, actorSystem]() {
            actorSystem->Send(selfId, new TEvents::TEvPoisonPill());
        });
        Request->SetStreamingNotify([selfId, actorSystem](size_t left) {
            actorSystem->Send(selfId, new TRpcServices::TEvGrpcNextReply(left));
        });

        Become(&TStreamStorageStateRequest::StateFunc);

        if (!IsAdministrator(AppData(), Request->GetInternalToken().Get())) {
            ReplyError(Ydb::StatusIds::UNAUTHORIZED, "Storage state queries require administrator privileges",
                       NKikimrIssues::TIssuesIds::ACCESS_DENIED);
            return;
        }

        const auto& request = *Request->GetProtoRequest();
        WantStoragePools = request.include_storage_pools();
        WantGroups = request.include_groups();
        WantVDisks = request.include_vdisks();
        WantPDisks = request.include_pdisks();
        WantNodes = request.include_nodes();
        WantDevices = request.include_devices();
        WantSettings = request.include_settings();

        if (!WantStoragePools && !WantGroups && !WantVDisks && !WantPDisks && !WantNodes && !WantDevices && !WantSettings) {
            FinishSuccess();
            return;
        }

        NTabletPipe::TClientConfig pipeConfig;
        pipeConfig.RetryPolicy = {.RetryLimitCount = 3u};
        BSCPipeClient = Register(NTabletPipe::CreateClient(SelfId(), MakeBSControllerID(), pipeConfig));

        auto req = std::make_unique<TEvBlobStorage::TEvControllerConfigRequest>();
        auto& record = *req->Record.MutableRequest();
        record.SetRollback(true);
        auto* queryBaseConfig = record.AddCommand()->MutableQueryBaseConfig();
        queryBaseConfig->SetRetrieveDevices(WantDevices);
        queryBaseConfig->SetSuppressPDisks(!WantPDisks);
        queryBaseConfig->SetSuppressVSlots(!WantVDisks);
        queryBaseConfig->SetSuppressGroups(!WantGroups);
        queryBaseConfig->SetSuppressNodes(!WantNodes);
        if (WantStoragePools) {
            record.AddCommand()->MutableReadStoragePool()->SetBoxId(Max<ui64>());
        }
        NTabletPipe::SendData(SelfId(), BSCPipeClient, req.release());
    }

    void PassAway() override {
        if (BSCPipeClient) {
            NTabletPipe::CloseClient(SelfId(), BSCPipeClient);
        }
        TBase::PassAway();
    }

private:
    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvBlobStorage::TEvControllerConfigResponse, Handle);
            hFunc(TEvTabletPipe::TEvClientConnected, Handle);
            hFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            hFunc(TRpcServices::TEvGrpcNextReply, Handle);
            hFunc(TEvents::TEvWakeup, Handle);
            hFunc(TEvents::TEvPoisonPill, Handle);
            default:
                ReplyError(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected event in StreamStorageState", NKikimrIssues::TIssuesIds::DEFAULT_ERROR);
        }
    }

    void Handle(TEvBlobStorage::TEvControllerConfigResponse::TPtr& ev) {
        const auto& response = ev->Get()->Record.GetResponse();

        const size_t expectedStatuses = 1 + static_cast<size_t>(WantStoragePools);
        if (response.StatusSize() != expectedStatuses) {
            ReplyError(Ydb::StatusIds::INTERNAL_ERROR,
                       TStringBuilder() << "BSC returned " << response.StatusSize() << " command statuses, expected " << expectedStatuses,
                       NKikimrIssues::TIssuesIds::DEFAULT_ERROR);
            return;
        }
        for (const auto& status : response.GetStatus()) {
            if (!status.GetSuccess()) {
                ReplyError(Ydb::StatusIds::INTERNAL_ERROR,
                           !status.GetErrorDescription().empty() ? status.GetErrorDescription() : "BSC rejected the storage snapshot request",
                           NKikimrIssues::TIssuesIds::DEFAULT_ERROR);
                return;
            }
        }

        BscResponse = ev;
        NTabletPipe::CloseClient(SelfId(), BSCPipeClient);
        BSCPipeClient = {};
        SendNextPart();
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
        if (ev->Get()->Status != NKikimrProto::OK) {
            ReplyError(Ydb::StatusIds::UNAVAILABLE, "Failed to connect to storage controller tablet", NKikimrIssues::TIssuesIds::SHARD_NOT_AVAILABLE);
        }
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr&) {
        if (!BscResponse) {
            ReplyError(Ydb::StatusIds::UNAVAILABLE, "Connection to storage controller tablet was lost", NKikimrIssues::TIssuesIds::SHARD_NOT_AVAILABLE);
        }
    }

    void Handle(TRpcServices::TEvGrpcNextReply::TPtr& ev) {
        if (ev->Get()->LeftInQueue == 0 && PartInFlight) {
            PartInFlight = false;
            ++ClientTimeoutGeneration;
            SendNextPart();
        }
    }

    void Handle(TEvents::TEvWakeup::TPtr& ev) {
        if (!PartInFlight || ev->Get()->Tag != ClientTimeoutGeneration) {
            return;
        }

        const TDuration elapsed = TActivationContext::Now() - LastPartSentAt;
        if (elapsed >= InactiveClientTimeout) {
            ReplyError(Ydb::StatusIds::TIMEOUT, TStringBuilder() << "Client did not consume a storage response part within " << InactiveClientTimeout,
                       NKikimrIssues::TIssuesIds::DEFAULT_ERROR);
            return;
        }

        Schedule(InactiveClientTimeout - elapsed, new TEvents::TEvWakeup(ClientTimeoutGeneration));
    }

    void Handle(TEvents::TEvPoisonPill::TPtr&) {
        // FinishStream is required even after the client has gone away so that
        // the gRPC adapter can release memory owned by the stream.
        Request->FinishStream(Ydb::StatusIds::CANCELLED);
        PassAway();
    }

    template <typename TSource, typename TBatch, typename TConverter>
    EFillResult FillBatch(const TSource& source, TBatch& batch, Ydb::DistributedStorage::StorageStateResponse& part,
                          TConverter converter) {
        if (NextItem >= source.size()) {
            return EFillResult::Empty;
        }

        size_t estimatedPartSize = part.ByteSizeLong();
        while (NextItem < source.size()) {
            auto* item = batch.Add();
            converter(source.Get(NextItem), *item);
            ++NextItem;

            const size_t itemSize = item->ByteSizeLong();
            // Every section is a repeated message field with a one-byte tag.
            // Keep room for the two enclosing message length prefixes and only
            // recalculate the whole part when approaching the hard limit.
            estimatedPartSize += 1 + VarintSize(itemSize) + itemSize;
            if (estimatedPartSize + 20 > MaxPartSize) {
                if (part.ByteSizeLong() > MaxPartSize) {
                    batch.RemoveLast();
                    --NextItem;
                    if (batch.size() == 0) {
                        return EFillResult::ItemTooLarge;
                    }
                }
                return EFillResult::Ready;
            }

            if (estimatedPartSize >= TargetPartSize) {
                return EFillResult::Ready;
            }
        }

        return EFillResult::Ready;
    }

    template <typename TSource, typename TBatch, typename TConverter>
    EBuildResult FillSection(const TSource& source, TBatch& batch, Ydb::DistributedStorage::StorageStateResponse& part,
                             TConverter converter) {
        const EFillResult result = FillBatch(source, batch, part, converter);
        if (result == EFillResult::ItemTooLarge) {
            return EBuildResult::ItemTooLarge;
        }
        if (result == EFillResult::Empty) {
            part.clear_result();
            AdvanceSection();
            return BuildNextPart(part);
        }
        if (NextItem == source.size()) {
            AdvanceSection();
        }
        return EBuildResult::Ready;
    }

    EBuildResult BuildNextPart(Ydb::DistributedStorage::StorageStateResponse& part) {
        part.set_status(Ydb::StatusIds::SUCCESS);

        const auto& response = BscResponse->Get()->Record.GetResponse();
        const auto& baseConfig = response.GetStatus(0).GetBaseConfig();

        switch (CurrentSection) {
            case ESection::Settings:
                if (!WantSettings) {
                    AdvanceSection();
                    return BuildNextPart(part);
                }
                ConvertClusterSettings(baseConfig.GetSettings(), *part.mutable_result()->mutable_settings());
                AdvanceSection();
                return part.ByteSizeLong() <= MaxPartSize
                    ? EBuildResult::Ready
                    : EBuildResult::ItemTooLarge;

            case ESection::StoragePools:
                if (!WantStoragePools) {
                    AdvanceSection();
                    return BuildNextPart(part);
                }
                return FillSection(response.GetStatus(1).GetStoragePool(), *part.mutable_result()->mutable_storage_pools(), part,
                                   [](const auto& from, auto& to) { ConvertStoragePool(from, to); });

            case ESection::Groups:
                if (!WantGroups) {
                    AdvanceSection();
                    return BuildNextPart(part);
                }
                return FillSection(baseConfig.GetGroup(), *part.mutable_result()->mutable_groups(), part,
                                   [](const auto& from, auto& to) { ConvertGroup(from, to); });

            case ESection::VDisks:
                if (!WantVDisks) {
                    AdvanceSection();
                    return BuildNextPart(part);
                }
                return FillSection(baseConfig.GetVSlot(), *part.mutable_result()->mutable_vdisks(), part,
                                   [](const auto& from, auto& to) { ConvertVDisk(from, to); });

            case ESection::PDisks:
                if (!WantPDisks) {
                    AdvanceSection();
                    return BuildNextPart(part);
                }
                return FillSection(baseConfig.GetPDisk(), *part.mutable_result()->mutable_pdisks(), part,
                                   [](const auto& from, auto& to) { ConvertPDisk(from, to); });

            case ESection::Nodes:
                if (!WantNodes) {
                    AdvanceSection();
                    return BuildNextPart(part);
                }
                return FillSection(baseConfig.GetNode(), *part.mutable_result()->mutable_nodes(), part,
                                   [](const auto& from, auto& to) { ConvertNode(from, to); });

            case ESection::Devices:
                if (!WantDevices) {
                    AdvanceSection();
                    return BuildNextPart(part);
                }
                return FillSection(baseConfig.GetDevice(), *part.mutable_result()->mutable_devices(), part,
                                   [](const auto& from, auto& to) { ConvertDevice(from, to); });

            case ESection::Done:
                return EBuildResult::Done;
        }

        return EBuildResult::Done;
    }

    void AdvanceSection() {
        switch (CurrentSection) {
            case ESection::Settings:
                CurrentSection = ESection::StoragePools;
                break;
            case ESection::StoragePools:
                CurrentSection = ESection::Groups;
                break;
            case ESection::Groups:
                CurrentSection = ESection::VDisks;
                break;
            case ESection::VDisks:
                CurrentSection = ESection::PDisks;
                break;
            case ESection::PDisks:
                CurrentSection = ESection::Nodes;
                break;
            case ESection::Nodes:
                CurrentSection = ESection::Devices;
                break;
            case ESection::Devices:
            case ESection::Done:
                CurrentSection = ESection::Done;
                break;
        }
        NextItem = 0;
    }

    void SendNextPart() {
        Ydb::DistributedStorage::StorageStateResponse part;
        switch (BuildNextPart(part)) {
            case EBuildResult::Done:
                FinishSuccess();
                return;
            case EBuildResult::ItemTooLarge:
                ReplyError(Ydb::StatusIds::INTERNAL_ERROR, "A storage item is too large to fit into one response part",
                           NKikimrIssues::TIssuesIds::DEFAULT_ERROR);
                return;
            case EBuildResult::Ready:
                break;
        }

        TString serialized;
        if (!part.SerializeToString(&serialized)) {
            ReplyError(Ydb::StatusIds::INTERNAL_ERROR, "Failed to serialize a storage response part", NKikimrIssues::TIssuesIds::DEFAULT_ERROR);
            return;
        }
        PartInFlight = true;
        LastPartSentAt = TActivationContext::Now();
        if (InactiveClientTimeout) {
            ++ClientTimeoutGeneration;
            Schedule(InactiveClientTimeout, new TEvents::TEvWakeup(ClientTimeoutGeneration));
        }
        Request->SendSerializedResult(std::move(serialized), Ydb::StatusIds::SUCCESS);
    }

    void ReplyError(Ydb::StatusIds::StatusCode status, const TString& message, NKikimrIssues::TIssuesIds::EIssueCode issueCode) {
        Ydb::DistributedStorage::StorageStateResponse part;
        part.set_status(status);
        auto* issue = part.add_issues();
        issue->set_severity(NYql::TSeverityIds::S_ERROR);
        issue->set_message(message);
        issue->set_issue_code(issueCode);

        TString serialized;
        Y_PROTOBUF_SUPPRESS_NODISCARD part.SerializeToString(&serialized);
        Request->SendSerializedResult(std::move(serialized), status);
        Request->FinishStream(status);
        PassAway();
    }

    void FinishSuccess() {
        Request->FinishStream(Ydb::StatusIds::SUCCESS);
        PassAway();
    }

    std::shared_ptr<TEvStreamStorageStateRequest> Request;
    TEvBlobStorage::TEvControllerConfigResponse::TPtr BscResponse;
    bool WantStoragePools = false;
    bool WantGroups = false;
    bool WantVDisks = false;
    bool WantPDisks = false;
    bool WantNodes = false;
    bool WantDevices = false;
    bool WantSettings = false;
    ESection CurrentSection = ESection::Settings;
    int NextItem = 0;
    TActorId BSCPipeClient;
    size_t TargetPartSize = kDefaultTargetPartSize;
    size_t MaxPartSize = kDefaultTargetPartSize;
    TDuration InactiveClientTimeout;
    TInstant LastPartSentAt;
    ui64 ClientTimeoutGeneration = 0;
    bool PartInFlight = false;
};

class TReassignVDiskRequestActor : public TRpcOperationRequestActor<TReassignVDiskRequestActor, TEvReassignVDiskRequest> {
    using TBase = TRpcOperationRequestActor<TReassignVDiskRequestActor, TEvReassignVDiskRequest>;

public:
    using TBase::TBase;

    void Bootstrap(const TActorContext& ctx) {
        TBase::Bootstrap(ctx);

        if (!IsAdministrator(AppData(), Request_->GetInternalToken().Get())) {
            Reply(Ydb::StatusIds::UNAUTHORIZED, "VDisk reassignment requires administrator privileges",
                  NKikimrIssues::TIssuesIds::ACCESS_DENIED, ctx);
            return;
        }

        const auto& request = *GetProtoRequest();
        if (!ValidateRequest(ctx)) {
            return;
        }

        if (IsStaticGroup(request.vdisk_id().group_id())) {
            StartStaticReassignment(ctx);
            return;
        }

        StartDynamicReassignment();
        Become(&TReassignVDiskRequestActor::StateFunc);
    }

    void PassAway() override {
        if (BSCPipeClient) {
            NTabletPipe::CloseClient(SelfId(), BSCPipeClient);
        }
        TBase::PassAway();
    }

private:
    bool ValidateRequest(const TActorContext& ctx) {
        const auto& request = *GetProtoRequest();
        if (!request.has_vdisk_id() || request.vdisk_id().group_generation() == 0) {
            Reply(Ydb::StatusIds::BAD_REQUEST, "vdisk_id with a non-zero group_generation is required; group_id zero is valid for the static group",
                  NKikimrIssues::TIssuesIds::DEFAULT_ERROR, ctx);
            return false;
        }

        if (request.has_target_pdisk_id() && (request.target_pdisk_id().node_id() == 0 || request.target_pdisk_id().pdisk_id() == 0)) {
            Reply(Ydb::StatusIds::BAD_REQUEST, "target_pdisk_id must contain non-zero node_id and pdisk_id",
                  NKikimrIssues::TIssuesIds::DEFAULT_ERROR, ctx);
            return false;
        }

        return true;
    }

    void StartDynamicReassignment() {
        const auto& request = *GetProtoRequest();

        NTabletPipe::TClientConfig pipeConfig;
        pipeConfig.RetryPolicy = {.RetryLimitCount = 3u};
        BSCPipeClient = Register(NTabletPipe::CreateClient(SelfId(), MakeBSControllerID(), pipeConfig));

        auto req = std::make_unique<TEvBlobStorage::TEvControllerConfigRequest>();
        auto& record = *req->Record.MutableRequest();
        record.SetRollback(request.dry_run());
        SetUserSid(Request_->GetInternalToken().Get(), record);

        const auto& options = request.options();
        ApplySafetyOptions(options.safety(), record);
        record.SetAllowUnusableDisks(options.allow_existing_ineligible_pdisks());
        record.SetSettleOnlyOnOperationalDisks(options.settle_only_on_operational_pdisks());
        record.SetIgnoreVSlotQuotaCheck(options.ignore_target_space_check());

        const auto& vdiskId = request.vdisk_id();
        auto* command = record.AddCommand()->MutableReassignGroupDisk();
        command->SetGroupId(vdiskId.group_id());
        command->SetGroupGeneration(vdiskId.group_generation());
        command->SetFailRealmIdx(vdiskId.fail_realm_idx());
        command->SetFailDomainIdx(vdiskId.fail_domain_idx());
        command->SetVDiskIdx(vdiskId.vdisk_idx());
        command->SetSuppressDonorMode(options.suppress_donor_mode());
        if (request.has_target_pdisk_id()) {
            CopyPDiskId(request.target_pdisk_id(), *command->MutableTargetPDiskId());
        }

        NTabletPipe::SendData(SelfId(), BSCPipeClient, req.release());
    }

    void StartStaticReassignment(const TActorContext& ctx) {
        const auto& request = *GetProtoRequest();

        if (request.dry_run()) {
            Reply(Ydb::StatusIds::UNSUPPORTED, "dry_run is not supported for static VDisk reassignment", NKikimrIssues::TIssuesIds::DEFAULT_ERROR, ctx);
            return;
        }

        ctx.Send(MakeBlobStorageNodeWardenID(SelfId().NodeId()), new TEvNodeWardenQueryStorageConfig(false));
        Become(&TReassignVDiskRequestActor::StateFunc);
    }

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvBlobStorage::TEvControllerConfigResponse, Handle);
            hFunc(TEvTabletPipe::TEvClientConnected, Handle);
            hFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            hFunc(TEvNodeWardenStorageConfig, Handle);
            hFunc(NStorage::TEvNodeConfigInvokeOnRootResult, Handle);
            default:
                return TBase::StateFuncBase(ev);
        }
    }

    void Handle(TEvNodeWardenStorageConfig::TPtr& ev) {
        const auto& ctx = ActorContext();
        if (!ev->Get()->SelfManagementEnabled) {
            Reply(Ydb::StatusIds::UNSUPPORTED, "Static VDisk reassignment requires Distconf V2 self-management mode",
                  NKikimrIssues::TIssuesIds::DEFAULT_ERROR, ctx);
            return;
        }

        const auto& request = *GetProtoRequest();
        const auto& options = request.options();

        auto invoke = std::make_unique<NStorage::TEvNodeConfigInvokeOnRoot>();
        auto* command = invoke->Record.MutableReassignGroupDisk();
        CopyVDiskId(request.vdisk_id(), *command->MutableVDiskId());
        if (request.has_target_pdisk_id()) {
            CopyPDiskId(request.target_pdisk_id(), *command->MutablePDiskId());
        }
        command->SetConvertToDonor(!options.suppress_donor_mode());
        command->SetIgnoreGroupFailModelChecks(options.safety().ignore_group_failure_model());
        command->SetIgnoreDegradedGroupsChecks(options.safety().ignore_degraded_groups());
        command->SetIgnoreVSlotQuotaCheck(options.ignore_target_space_check());
        command->SetAllowUnusableDisks(options.allow_existing_ineligible_pdisks());
        command->SetSettleOnlyOnOperationalDisks(options.settle_only_on_operational_pdisks());

        ctx.Send(MakeBlobStorageNodeWardenID(SelfId().NodeId()), invoke.release());
    }

    void Handle(NStorage::TEvNodeConfigInvokeOnRootResult::TPtr& ev) {
        using TResult = NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult;

        const auto& record = ev->Get()->Record;
        const auto& ctx = ActorContext();
        const TString message = record.GetErrorReason().empty()
            ? "Distconf rejected static VDisk reassignment"
            : record.GetErrorReason();

        switch (record.GetStatus()) {
            case TResult::OK: {
                const auto& placement = record.GetReassignGroupDisk();
                Ydb::DistributedStorage::ReassignVDiskResult result;
                result.mutable_vdisk_id()->CopyFrom(GetProtoRequest()->vdisk_id());
                CopyVSlotId(placement.GetSourceSlotId(), *result.mutable_source_slot_id());
                CopyVSlotId(placement.GetTargetSlotId(), *result.mutable_target_slot_id());
                ReplyWithResult(Ydb::StatusIds::SUCCESS, result, ctx);
                return;
            }
            case TResult::NO_QUORUM:
                Reply(Ydb::StatusIds::UNAVAILABLE, message, NKikimrIssues::TIssuesIds::SHARD_NOT_AVAILABLE, ctx);
                return;
            case TResult::RACE:
                Reply(Ydb::StatusIds::ABORTED, message, NKikimrIssues::TIssuesIds::DEFAULT_ERROR, ctx);
                return;
            case TResult::ERROR:
                Reply(Ydb::StatusIds::PRECONDITION_FAILED, message, NKikimrIssues::TIssuesIds::DEFAULT_ERROR, ctx);
                return;
            case TResult::CONTINUE_BSC:
            default:
                Reply(Ydb::StatusIds::INTERNAL_ERROR, message, NKikimrIssues::TIssuesIds::DEFAULT_ERROR, ctx);
                return;
        }
    }

    void Handle(TEvBlobStorage::TEvControllerConfigResponse::TPtr& ev) {
        const auto& response = ev->Get()->Record.GetResponse();
        const auto& ctx = ActorContext();

        const auto& request = *GetProtoRequest();
        if (const auto error = ValidateBscMutationResponse(response, request.dry_run(), 1)) {
            Reply(error->Status, error->Message, NKikimrIssues::TIssuesIds::DEFAULT_ERROR, ctx);
            return;
        }

        const auto& status = response.GetStatus(0);
        if (status.ReassignedItemSize() != 1) {
            Reply(Ydb::StatusIds::INTERNAL_ERROR, TStringBuilder() << "BSC returned " << status.ReassignedItemSize() << " reassigned items, expected one",
                  NKikimrIssues::TIssuesIds::DEFAULT_ERROR, ctx);
            return;
        }

        const auto& item = status.GetReassignedItem(0);
        if (!item.HasVDiskId() || !item.HasFrom() || !item.HasTo()) {
            Reply(Ydb::StatusIds::INTERNAL_ERROR, "BSC returned incomplete placement information", NKikimrIssues::TIssuesIds::DEFAULT_ERROR, ctx);
            return;
        }
        if (!IsSameVDiskId(item.GetVDiskId(), request.vdisk_id())) {
            Reply(Ydb::StatusIds::INTERNAL_ERROR, "BSC returned placement for an unexpected VDisk", NKikimrIssues::TIssuesIds::DEFAULT_ERROR, ctx);
            return;
        }

        Ydb::DistributedStorage::ReassignVDiskResult result;
        CopyVDiskId(item.GetVDiskId(), *result.mutable_vdisk_id());
        CopyVSlotId(item.GetFrom(), *result.mutable_source_slot_id());
        CopyVSlotId(item.GetTo(), *result.mutable_target_slot_id());
        ReplyWithResult(Ydb::StatusIds::SUCCESS, result, ctx);
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
        if (ev->Get()->Status != NKikimrProto::OK) {
            Reply(Ydb::StatusIds::UNAVAILABLE, "Failed to connect to storage controller tablet", NKikimrIssues::TIssuesIds::SHARD_NOT_AVAILABLE,
                  ActorContext());
        }
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr&) {
        Reply(Ydb::StatusIds::UNAVAILABLE, "Connection to storage controller tablet was lost", NKikimrIssues::TIssuesIds::SHARD_NOT_AVAILABLE,
              ActorContext());
    }

    TActorId BSCPipeClient;
};

void DoStreamStorageState(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f) {
    auto* request = dynamic_cast<TEvStreamStorageStateRequest*>(p.release());
    Y_ABORT_UNLESS(request, "Wrong request type for StreamStorageState");
    f.RegisterActor(new TStreamStorageStateRequest(request));
}

void DoReassignVDisk(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TReassignVDiskRequestActor(p.release()));
}

} // namespace NKikimr::NGRpcService
