#pragma once

#include "defs.h"

#include <ydb/core/protos/kesus.pb.h>
#include <ydb/core/tablet_flat/flat_cxx_database.h>
#include <ydb/public/api/protos/ydb_coordination.pb.h>

namespace NKikimr {
namespace NKesus {

struct TKesusSchema : NIceDb::Schema {
    struct SysParams : Table<1> {
        struct Id : Column<1, NScheme::NTypeIds::Uint64> {};
        struct Value : Column<2, NScheme::NTypeIds::Utf8> {};

        using TKey = TableKey<Id>;
        using TColumns = TableColumns<Id, Value>;
    };

    struct Sessions : Table<2> {
        struct Id : Column<1, NScheme::NTypeIds::Uint64> {};
        struct TimeoutMillis : Column<2, NScheme::NTypeIds::Uint64> {};
        struct Description : Column<3, NScheme::NTypeIds::Utf8> {};
        struct ProtectionKey : Column<4, NScheme::NTypeIds::String> {};

        using TKey = TableKey<Id>;
        using TColumns = TableColumns<Id, TimeoutMillis, Description, ProtectionKey>;
    };

    struct Semaphores : Table<3> {
        struct Id : Column<1, NScheme::NTypeIds::Uint64> {};
        struct Name : Column<2, NScheme::NTypeIds::Utf8> {};
        struct Data : Column<3, NScheme::NTypeIds::String> {};
        struct Limit : Column<4, NScheme::NTypeIds::Uint64> {};
        struct Ephemeral : Column<5, NScheme::NTypeIds::Bool> {};

        using TKey = TableKey<Id>;
        using TColumns = TableColumns<Id, Name, Data, Limit, Ephemeral>;
    };

    struct SessionSemaphores : Table<4> {
        struct SessionId : Column<1, NScheme::NTypeIds::Uint64> {};
        struct SemaphoreId : Column<2, NScheme::NTypeIds::Uint64> {};
        struct OrderId : Column<3, NScheme::NTypeIds::Uint64> {};
        struct TimeoutMillis : Column<4, NScheme::NTypeIds::Uint64> {};
        struct Count : Column<5, NScheme::NTypeIds::Uint64> {};
        struct Data : Column<6, NScheme::NTypeIds::String> {};

        using TKey = TableKey<SessionId, SemaphoreId>;
        using TColumns = TableColumns<SessionId, SemaphoreId, OrderId, TimeoutMillis, Count, Data>;
    };

    struct QuoterResources : Table<5> {
        struct Id : Column<1, NScheme::NTypeIds::Uint64> {};
        struct ParentId : Column<2, NScheme::NTypeIds::Uint64> {};
        struct Props : Column<3, NScheme::NTypeIds::String> { using Type = NKikimrKesus::TStreamingQuoterResource; };

        using TKey = TableKey<Id>;
        using TColumns = TableColumns<Id, ParentId, Props>;
    };

    using TTables = SchemaTables<SysParams, Sessions, Semaphores, SessionSemaphores, QuoterResources>;

    using TSettings = SchemaSettings<
        ExecutorLogBatching<true>,
        ExecutorLogFlushPeriod<500 /*500us*/>>;

    static constexpr ui64 SysParam_KesusPath = 1;
    static constexpr ui64 SysParam_NextSessionId = 2;
    static constexpr ui64 SysParam_NextSemaphoreId = 3;
    static constexpr ui64 SysParam_NextSemaphoreOrderId = 4;
    static constexpr ui64 SysParam_LastLeaderActor = 5;
    static constexpr ui64 SysParam_SelfCheckPeriodMillis = 6;
    static constexpr ui64 SysParam_SessionGracePeriodMillis = 7;
    static constexpr ui64 SysParam_SelfCheckCounter = 8;
    static constexpr ui64 SysParam_ConfigVersion = 9;
    static constexpr ui64 SysParam_ReadConsistencyMode = 10;
    static constexpr ui64 SysParam_AttachConsistencyMode = 11;
    static constexpr ui64 SysParam_StrictMarkerCounter = 12;
    static constexpr ui64 SysParam_NextQuoterResourceId = 13;
    static constexpr ui64 SysParam_RateLimiterCountersMode = 14;
};

}
}
