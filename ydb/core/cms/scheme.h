#pragma once

#include "defs.h"

#include <ydb/core/scheme/scheme_types_defs.h>
#include <ydb/core/tablet_flat/flat_cxx_database.h>

namespace NKikimr::NCms {

struct Schema : NIceDb::Schema {
    struct Param : Table<1> {
        struct ID : Column<1, NScheme::NTypeIds::Uint32> {};
        struct NextPermissionID : Column<2, NScheme::NTypeIds::Uint64> {};
        struct NextRequestID : Column<3, NScheme::NTypeIds::Uint64> {};
        struct NextNotificationID : Column<4, NScheme::NTypeIds::Uint64> {};
        struct Config : Column<5, NScheme::NTypeIds::String> { using Type = NKikimrCms::TCmsConfig; };
        struct LastLogRecordTimestamp : Column<6, NScheme::NTypeIds::Uint64> {};

        using TKey = TableKey<ID>;
        using TColumns = TableColumns<ID, NextPermissionID, NextRequestID, NextNotificationID,
            Config, LastLogRecordTimestamp>;
    };

    struct Permission : Table<2> {
        struct ID : Column<1, NScheme::NTypeIds::Utf8> {};
        struct Owner : Column<2, NScheme::NTypeIds::Utf8> {};
        struct Action : Column<3, NScheme::NTypeIds::Utf8> {};
        struct Deadline : Column<4, NScheme::NTypeIds::Uint64> {};
        struct RequestID : Column<5, NScheme::NTypeIds::Utf8> {};

        using TKey = TableKey<ID>;
        using TColumns = TableColumns<ID, Owner, Action, Deadline, RequestID>;
    };

    struct Request : Table<3> {
        struct ID : Column<1, NScheme::NTypeIds::Utf8> {};
        struct Owner : Column<2, NScheme::NTypeIds::Utf8> {};
        struct Order : Column<3, NScheme::NTypeIds::Uint64> {};
        struct Content : Column<4, NScheme::NTypeIds::Utf8> {};
        struct Priority : Column<5, NScheme::NTypeIds::Int32> {};

        using TKey = TableKey<ID>;
        using TColumns = TableColumns<ID, Owner, Order, Content, Priority>;
    };

    struct WalleTask : Table<4> {
        struct TaskID : Column<1, NScheme::NTypeIds::Utf8> {};
        struct RequestID : Column<2, NScheme::NTypeIds::Utf8> {};

        using TKey = TableKey<TaskID>;
        using TColumns = TableColumns<TaskID, RequestID>;
    };

    struct Notification : Table<5> {
        struct ID : Column<1, NScheme::NTypeIds::Utf8> {};
        struct Owner : Column<2, NScheme::NTypeIds::Utf8> {};
        struct NotificationProto : Column<3, NScheme::NTypeIds::Utf8> {};

        using TKey = TableKey<ID>;
        using TColumns = TableColumns<ID, Owner, NotificationProto>;
    };

    struct NodeTenant : Table<6> {
        struct NodeId : Column<1, NScheme::NTypeIds::Uint32> {};
        struct Tenant : Column<2, NScheme::NTypeIds::Utf8> {};

        using TKey = TableKey<NodeId>;
        using TColumns = TableColumns<NodeId, Tenant>;
    };

    struct HostMarkers : Table<7> {
        struct Host : Column<1, NScheme::NTypeIds::Utf8> {};
        struct Markers : Column<2, NScheme::NTypeIds::String> { using Type = TVector<NKikimrCms::EMarker>; };

        using TKey = TableKey<Host>;
        using TColumns = TableColumns<Host, Markers>;
    };

    struct NodeMarkers : Table<8> {
        struct NodeId : Column<1, NScheme::NTypeIds::Uint32> {};
        struct Markers : Column<2, NScheme::NTypeIds::String> { using Type = TVector<NKikimrCms::EMarker>; };

        using TKey = TableKey<NodeId>;
        using TColumns = TableColumns<NodeId, Markers>;
    };

    struct PDiskMarkers : Table<9> {
        struct NodeId : Column<1, NScheme::NTypeIds::Uint32> {};
        struct DiskId : Column<2, NScheme::NTypeIds::Uint32> {};
        struct Markers : Column<3, NScheme::NTypeIds::String> { using Type = TVector<NKikimrCms::EMarker>; };

        using TKey = TableKey<NodeId, DiskId>;
        using TColumns = TableColumns<NodeId, DiskId, Markers>;
    };

    struct VDiskMarkers : Table<10> {
        struct GroupId : Column<1, NScheme::NTypeIds::Uint32> {};
        struct GroupGeneration : Column<2, NScheme::NTypeIds::Uint32> {};
        struct FailRealm : Column<3, NScheme::NTypeIds::Uint8> {};
        struct FailDomain : Column<4, NScheme::NTypeIds::Uint8> {};
        struct VDisk : Column<5, NScheme::NTypeIds::Uint8> {};
        struct Markers : Column<6, NScheme::NTypeIds::String> { using Type = TVector<NKikimrCms::EMarker>; };

        using TKey = TableKey<GroupId, GroupGeneration, FailRealm, FailDomain, VDisk>;
        using TColumns = TableColumns<GroupId, GroupGeneration, FailRealm, FailDomain, VDisk, Markers>;
    };

    struct LogRecords : Table<11> {
        struct Timestamp : Column<1, NScheme::NTypeIds::Uint64> {};
        struct Data : Column<2, NScheme::NTypeIds::String> { using Type = NKikimrCms::TLogRecordData; };

        using TKey = TableKey<Timestamp>;
        using TColumns = TableColumns<Timestamp, Data>;
    };

    struct NodeDowntimes : Table<12> {
        struct NodeId : Column<1, NScheme::NTypeIds::Uint32> {};
        struct Downtime : Column<2, NScheme::NTypeIds::String> { using Type = NKikimrCms::TAvailabilityStats; };

        using TKey = TableKey<NodeId>;
        using TColumns = TableColumns<NodeId, Downtime>;
    };

    struct PDiskDowntimes : Table<13> {
        struct NodeId : Column<1, NScheme::NTypeIds::Uint32> {};
        struct DiskId : Column<2, NScheme::NTypeIds::Uint32> {};
        struct Downtime : Column<3, NScheme::NTypeIds::String> { using Type = NKikimrCms::TAvailabilityStats; };

        using TKey = TableKey<NodeId, DiskId>;
        using TColumns = TableColumns<NodeId, DiskId, Downtime>;
    };

    struct MaintenanceTasks : Table<14> {
        struct TaskID : Column<1, NScheme::NTypeIds::Utf8> {};
        struct RequestID : Column<2, NScheme::NTypeIds::Utf8> {};
        struct Owner : Column<3, NScheme::NTypeIds::Utf8> {};
        struct HasSingleCompositeActionGroup : Column<4, NScheme::NTypeIds::Bool> {};

        using TKey = TableKey<TaskID>;
        using TColumns = TableColumns<TaskID, RequestID, Owner, HasSingleCompositeActionGroup>;
    };

    using TTables = SchemaTables<Param, Permission, Request, WalleTask, Notification, NodeTenant,
        HostMarkers, NodeMarkers, PDiskMarkers, VDiskMarkers, LogRecords, NodeDowntimes, PDiskDowntimes,
        MaintenanceTasks>;
    using TSettings = SchemaSettings<ExecutorLogBatching<true>,
                                     ExecutorLogFlushPeriod<TDuration::MicroSeconds(512).GetValue()>>;
};

} // namespace NKikimr::NCms
