#pragma once

#include <util/generic/maybe.h>
#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NKikimrSchemeOp {
    class TColumnDescription;
    class TTableDescription;
    class TTableReplicationConfig;
}

namespace NKikimr::NReplication::NTestHelpers {

struct TTestTableDescription {
    struct TColumn {
        TString Name;
        TString Type;

        void SerializeTo(NKikimrSchemeOp::TColumnDescription& proto) const;
    };

    struct TReplicationConfig {
        enum EMode {
            MODE_NONE = 0,
            MODE_READ_ONLY = 1,
        };

        enum EConsistency {
            CONSISTENCY_UNKNOWN = 0,
            CONSISTENCY_STRONG = 1,
            CONSISTENCY_WEAK = 2,
        };

        EMode Mode;
        EConsistency Consistency;

        void SerializeTo(NKikimrSchemeOp::TTableReplicationConfig& proto) const;
        static TReplicationConfig Default();
    };

    TString Name;
    TVector<TString> KeyColumns;
    TVector<TColumn> Columns;
    TMaybe<TReplicationConfig> ReplicationConfig = TReplicationConfig::Default();
    TMaybe<ui32> UniformPartitions = Nothing();

    void SerializeTo(NKikimrSchemeOp::TTableDescription& proto) const;
};

THolder<NKikimrSchemeOp::TTableDescription> MakeTableDescription(const TTestTableDescription& desc);

}
