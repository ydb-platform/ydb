#include "test_table.h"

#include <ydb/core/protos/flat_scheme_op.pb.h>

namespace NKikimr::NReplication::NTestHelpers {

void TTestTableDescription::TColumn::SerializeTo(NKikimrSchemeOp::TColumnDescription& proto) const {
    proto.SetName(Name);
    proto.SetType(Type);
}

void TTestTableDescription::TReplicationConfig::SerializeTo(NKikimrSchemeOp::TTableReplicationConfig& proto) const {
    switch (Mode) {
    case MODE_NONE:
        proto.SetMode(NKikimrSchemeOp::TTableReplicationConfig::REPLICATION_MODE_NONE);
        break;
    case MODE_READ_ONLY:
        proto.SetMode(NKikimrSchemeOp::TTableReplicationConfig::REPLICATION_MODE_READ_ONLY);
        break;
    default:
        Y_ABORT("Unexpected mode");
    }

    switch (Consistency) {
    case CONSISTENCY_UNKNOWN:
        proto.SetConsistency(NKikimrSchemeOp::TTableReplicationConfig::CONSISTENCY_UNKNOWN);
        break;
    case CONSISTENCY_STRONG:
        proto.SetConsistency(NKikimrSchemeOp::TTableReplicationConfig::CONSISTENCY_STRONG);
        break;
    case CONSISTENCY_WEAK:
        proto.SetConsistency(NKikimrSchemeOp::TTableReplicationConfig::CONSISTENCY_WEAK);
        break;
    default:
        Y_ABORT("Unexpected consistency");
    }
}

TTestTableDescription::TReplicationConfig TTestTableDescription::TReplicationConfig::Default() {
    return TReplicationConfig{
        .Mode = MODE_READ_ONLY,
        .Consistency = CONSISTENCY_WEAK,
    };
}

void TTestTableDescription::SerializeTo(NKikimrSchemeOp::TTableDescription& proto) const {
    proto.SetName(Name);

    for (const auto& keyColumn : KeyColumns) {
        proto.AddKeyColumnNames(keyColumn);
    }

    for (const auto& column : Columns) {
        column.SerializeTo(*proto.AddColumns());
    }

    if (ReplicationConfig) {
        ReplicationConfig->SerializeTo(*proto.MutableReplicationConfig());
    }

    if (UniformPartitions) {
        proto.SetUniformPartitionsCount(*UniformPartitions);
    }
}

THolder<NKikimrSchemeOp::TTableDescription> MakeTableDescription(const TTestTableDescription& desc) {
    auto result = MakeHolder<NKikimrSchemeOp::TTableDescription>();
    desc.SerializeTo(*result);
    return result;
}

}
