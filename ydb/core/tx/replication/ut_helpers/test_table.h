#pragma once

#include <util/generic/maybe.h>
#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NKikimrSchemeOp {
    class TColumnDescription;
    class TTableDescription;
    class TTableReplicationConfig;
    class TColumnTableDescription;
    class TOlapColumnDescription;
}

namespace NKikimr::NReplication::NTestHelpers {

struct TTestTableDescription {
    struct TColumn {
        TString Name;
        TString Type;

        void SerializeTo(NKikimrSchemeOp::TColumnDescription& proto) const;
        void SerializeTo(NKikimrSchemeOp::TOlapColumnDescription& proto) const;
    };

    struct TReplicationConfig {
        enum EMode {
            MODE_NONE = 0,
            MODE_READ_ONLY = 1,
        };

        enum EConsistencyLevel {
            CONSISTENCY_LEVEL_UNKNOWN = 0,
            CONSISTENCY_LEVEL_GLOBAL = 1,
            CONSISTENCY_LEVEL_ROW = 2,
        };

        EMode Mode;
        EConsistencyLevel ConsistencyLevel;

        void SerializeTo(NKikimrSchemeOp::TTableReplicationConfig& proto) const;
        static TReplicationConfig Default();
    };

    TString Name;
    TVector<TString> KeyColumns;
    TVector<TColumn> Columns;
    TMaybe<TReplicationConfig> ReplicationConfig = TReplicationConfig::Default();
    TMaybe<ui32> UniformPartitions = Nothing();

    void SerializeTo(NKikimrSchemeOp::TTableDescription& proto) const;
    void SerializeTo(NKikimrSchemeOp::TColumnTableDescription& proto) const;
};

THolder<NKikimrSchemeOp::TTableDescription> MakeTableDescription(const TTestTableDescription& desc);
THolder<NKikimrSchemeOp::TColumnTableDescription> MakeColumnTableDescription(const TTestTableDescription& desc);

}
