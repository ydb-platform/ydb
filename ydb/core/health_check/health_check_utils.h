#pragma once

#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <ydb/core/protos/blobstorage_distributed_config.pb.h>
#include <ydb/core/sys_view/common/events.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/library/actors/interconnect/interconnect.h>
#include <ydb/public/api/grpc/ydb_monitoring_v1.grpc.pb.h>

///
#include <ydb/core/base/path.h>
#include <ydb/core/base/statestorage.h>
#include <ydb/core/mon/mon.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/mind/tenant_slot_broker.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/util/proto_duration.h>
#include <ydb/core/util/tuples.h>

#include <ydb/core/protos/config.pb.h>
#include <ydb/library/actors/interconnect/interconnect.h>
#include <ydb/library/actors/wilson/wilson_span.h>
#include <ydb/library/wilson_ids/wilson.h>

namespace NKikimr::NHealthCheck {

using namespace NSchemeCache;
using namespace NNodeWhiteboard;
using namespace NSchemeShard;

using TGroupId = ui32;

static Ydb::Monitoring::StatusFlag::Status MaxStatus(Ydb::Monitoring::StatusFlag::Status a, Ydb::Monitoring::StatusFlag::Status b) {
    return static_cast<Ydb::Monitoring::StatusFlag::Status>(std::max<int>(a, b));
}

static Ydb::Monitoring::StatusFlag::Status MinStatus(Ydb::Monitoring::StatusFlag::Status a, Ydb::Monitoring::StatusFlag::Status b) {
    return static_cast<Ydb::Monitoring::StatusFlag::Status>(std::min<int>(a, b));
}

static bool IsStaticGroup(ui32 groupId) {
    return !(groupId & 0x80000000);
}

static TString GetVSlotId(const NKikimrSysView::TVSlotKey& vSlotKey) {
    return TStringBuilder()
            << vSlotKey.GetNodeId() << '-'
            << vSlotKey.GetPDiskId() << '-'
            << vSlotKey.GetVSlotId();
}

static TString GetVDiskId(const NKikimrBlobStorage::TVDiskID& protoVDiskId) {
    return TStringBuilder()
            << protoVDiskId.groupid() << '-'
            << protoVDiskId.groupgeneration() << '-'
            << protoVDiskId.ring() << '-'
            << protoVDiskId.domain() << '-'
            << protoVDiskId.vdisk();
}

// static TString GetVDiskId(const NKikimrBlobStorage::TBaseConfig::TVSlot& protoVSlot) {
//     return TStringBuilder()
//             << protoVSlot.groupid() << '-'
//             << protoVSlot.groupgeneration() << '-'
//             << protoVSlot.failrealmidx() << '-'
//             << protoVSlot.faildomainidx() << '-'
//             << protoVSlot.vdiskidx();
// }

static TString GetVDiskId(const NKikimrBlobStorage::TNodeWardenServiceSet_TVDisk& protoVDiskId) {
    return GetVDiskId(protoVDiskId.vdiskid());
}

// static TString GetVDiskId(const NKikimrWhiteboard::TVDiskStateInfo vDiskInfo) {
//     return GetVDiskId(vDiskInfo.vdiskid());
// }

static TString GetVDiskId(const NKikimrSysView::TVSlotInfo& vSlot) {
    return TStringBuilder()
            << vSlot.GetGroupId() << '-'
            << vSlot.GetGroupGeneration() << '-'
            << vSlot.GetFailRealm() << '-'
            << vSlot.GetFailDomain() << '-'
            << vSlot.GetVDisk();
}

static TString GetPDiskId(const NKikimrWhiteboard::TVDiskStateInfo vDiskInfo) {
    return TStringBuilder() << vDiskInfo.nodeid() << "-" << vDiskInfo.pdiskid();
}

static TString GetPDiskId(const NKikimrWhiteboard::TPDiskStateInfo pDiskInfo) {
    return TStringBuilder() << pDiskInfo.nodeid() << "-" << pDiskInfo.pdiskid();
}

// static TString GetPDiskId(const NKikimrBlobStorage::TBaseConfig::TPDisk& pDisk) {
//     return TStringBuilder() << pDisk.nodeid() << "-" << pDisk.pdiskid();
// }

// static TString GetPDiskId(const NKikimrBlobStorage::TBaseConfig::TVSlot& vSlot) {
//     return TStringBuilder() << vSlot.vslotid().nodeid() << "-" << vSlot.vslotid().pdiskid();
// }

static TString GetPDiskId(const NKikimrBlobStorage::TNodeWardenServiceSet_TPDisk& pDisk) {
    return TStringBuilder() << pDisk.nodeid() << "-" << pDisk.pdiskid();
}

static TString GetPDiskId(const NKikimrSysView::TVSlotKey& vSlotKey) {
    return TStringBuilder() << vSlotKey.GetNodeId() << "-" << vSlotKey.GetPDiskId();
}

static TString GetPDiskId(const NKikimrSysView::TPDiskKey& pDiskKey) {
    return TStringBuilder() << pDiskKey.GetNodeId() << "-" << pDiskKey.GetPDiskId();
}

static bool IsSuccess(const std::unique_ptr<TEvSchemeShard::TEvDescribeSchemeResult>& ev) {
    return ev->GetRecord().status() == NKikimrScheme::StatusSuccess;
}

static TString GetError(const std::unique_ptr<TEvSchemeShard::TEvDescribeSchemeResult>& ev) {
    return NKikimrScheme::EStatus_Name(ev->GetRecord().status());
}

static bool IsSuccess(const std::unique_ptr<TEvStateStorage::TEvBoardInfo>& ev) {
    return ev->Status == TEvStateStorage::TEvBoardInfo::EStatus::Ok;
}

static TString GetError(const std::unique_ptr<TEvStateStorage::TEvBoardInfo>& ev) {
    switch (ev->Status) {
        case TEvStateStorage::TEvBoardInfo::EStatus::Ok:
            return "Ok";
        case TEvStateStorage::TEvBoardInfo::EStatus::Unknown:
            return "Unknown";
        case TEvStateStorage::TEvBoardInfo::EStatus::NotAvailable:
            return "NotAvailable";
    }
}

static bool IsSuccess(const std::unique_ptr<TEvTxProxySchemeCache::TEvNavigateKeySetResult>& ev) {
    return (ev->Request->ResultSet.size() > 0) && (std::find_if(ev->Request->ResultSet.begin(), ev->Request->ResultSet.end(),
        [](const auto& entry) {
            return entry.Status == TSchemeCacheNavigate::EStatus::Ok;
        }) != ev->Request->ResultSet.end());
}

static TString GetError(const std::unique_ptr<TEvTxProxySchemeCache::TEvNavigateKeySetResult>& ev) {
    if (ev->Request->ResultSet.size() == 0) {
        return "empty response";
    }
    for (const auto& entry : ev->Request->ResultSet) {
        if (entry.Status != TSchemeCacheNavigate::EStatus::Ok) {
            switch (entry.Status) {
                case TSchemeCacheNavigate::EStatus::Ok:
                    return "Ok";
                case TSchemeCacheNavigate::EStatus::Unknown:
                    return "Unknown";
                case TSchemeCacheNavigate::EStatus::RootUnknown:
                    return "RootUnknown";
                case TSchemeCacheNavigate::EStatus::PathErrorUnknown:
                    return "PathErrorUnknown";
                case TSchemeCacheNavigate::EStatus::PathNotTable:
                    return "PathNotTable";
                case TSchemeCacheNavigate::EStatus::PathNotPath:
                    return "PathNotPath";
                case TSchemeCacheNavigate::EStatus::TableCreationNotComplete:
                    return "TableCreationNotComplete";
                case TSchemeCacheNavigate::EStatus::LookupError:
                    return "LookupError";
                case TSchemeCacheNavigate::EStatus::RedirectLookupError:
                    return "RedirectLookupError";
                case TSchemeCacheNavigate::EStatus::AccessDenied:
                    return "AccessDenied";
                default:
                    return ::ToString(static_cast<int>(entry.Status));
            }
        }
    }
    return "no error";
}

static Ydb::Monitoring::StatusFlag::Status GetFlagFromWhiteboardFlag(NKikimrWhiteboard::EFlag flag) {
    switch (flag) {
        case NKikimrWhiteboard::EFlag::Green:
            return Ydb::Monitoring::StatusFlag::GREEN;
        case NKikimrWhiteboard::EFlag::Yellow:
            return Ydb::Monitoring::StatusFlag::YELLOW;
        case NKikimrWhiteboard::EFlag::Orange:
            return Ydb::Monitoring::StatusFlag::ORANGE;
        case NKikimrWhiteboard::EFlag::Red:
            return Ydb::Monitoring::StatusFlag::RED;
        default:
            return Ydb::Monitoring::StatusFlag::UNSPECIFIED;
    }
}

} // NKikimr::NHealthCheck
