#pragma once

#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <ydb/core/protos/blobstorage_distributed_config.pb.h>
#include <ydb/core/sys_view/common/events.h>
#include <ydb/public/api/grpc/ydb_monitoring_v1.grpc.pb.h>

namespace NKikimr::NHealthCheck {

static Ydb::Monitoring::StatusFlag::Status MaxStatus(Ydb::Monitoring::StatusFlag::Status a, Ydb::Monitoring::StatusFlag::Status b) {
    return static_cast<Ydb::Monitoring::StatusFlag::Status>(std::max<int>(a, b));
}

static Ydb::Monitoring::StatusFlag::Status MinStatus(Ydb::Monitoring::StatusFlag::Status a, Ydb::Monitoring::StatusFlag::Status b) {
    return static_cast<Ydb::Monitoring::StatusFlag::Status>(std::min<int>(a, b));
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

static TString GetVDiskId(const NKikimrBlobStorage::TBaseConfig::TVSlot& protoVSlot) {
    return TStringBuilder()
            << protoVSlot.groupid() << '-'
            << protoVSlot.groupgeneration() << '-'
            << protoVSlot.failrealmidx() << '-'
            << protoVSlot.faildomainidx() << '-'
            << protoVSlot.vdiskidx();
}

static TString GetVDiskId(const NKikimrBlobStorage::TNodeWardenServiceSet_TVDisk& protoVDiskId) {
    return GetVDiskId(protoVDiskId.vdiskid());
}

static TString GetVDiskId(const NKikimrWhiteboard::TVDiskStateInfo vDiskInfo) {
    return GetVDiskId(vDiskInfo.vdiskid());
}

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

static TString GetPDiskId(const NKikimrBlobStorage::TBaseConfig::TPDisk& pDisk) {
    return TStringBuilder() << pDisk.nodeid() << "-" << pDisk.pdiskid();
}

static TString GetPDiskId(const NKikimrBlobStorage::TBaseConfig::TVSlot& vSlot) {
    return TStringBuilder() << vSlot.vslotid().nodeid() << "-" << vSlot.vslotid().pdiskid();
}

static TString GetPDiskId(const NKikimrBlobStorage::TNodeWardenServiceSet_TPDisk& pDisk) {
    return TStringBuilder() << pDisk.nodeid() << "-" << pDisk.pdiskid();
}

static TString GetPDiskId(const NKikimrSysView::TVSlotKey& vSlotKey) {
    return TStringBuilder() << vSlotKey.GetNodeId() << "-" << vSlotKey.GetPDiskId();
}

static TString GetPDiskId(const NKikimrSysView::TPDiskKey& pDiskKey) {
    return TStringBuilder() << pDiskKey.GetNodeId() << "-" << pDiskKey.GetPDiskId();
}

} // NKikimr::NHealthCheck
