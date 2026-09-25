#include "test_table.h"

#include <ydb/core/protos/flat_scheme_op.pb.h>

namespace NKikimr::NReplication::NTestHelpers {

void TTestTableDescription::TColumn::SerializeTo(NKikimrSchemeOp::TColumnDescription& proto) const {
    proto.SetName(Name);
    proto.SetType(Type);
}

void TTestTableDescription::TColumn::SerializeTo(NKikimrSchemeOp::TOlapColumnDescription& proto) const {
    proto.SetName(Name);
    proto.SetType(Type);
    proto.SetNotNull(true);
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

    switch (ConsistencyLevel) {
    case CONSISTENCY_LEVEL_UNKNOWN:
        proto.SetConsistencyLevel(NKikimrSchemeOp::TTableReplicationConfig::CONSISTENCY_LEVEL_UNKNOWN);
        break;
    case CONSISTENCY_LEVEL_GLOBAL:
        proto.SetConsistencyLevel(NKikimrSchemeOp::TTableReplicationConfig::CONSISTENCY_LEVEL_GLOBAL);
        break;
    case CONSISTENCY_LEVEL_ROW:
        proto.SetConsistencyLevel(NKikimrSchemeOp::TTableReplicationConfig::CONSISTENCY_LEVEL_ROW);
        break;
    default:
        Y_ABORT("Unexpected consistency");
    }
}

TTestTableDescription::TReplicationConfig TTestTableDescription::TReplicationConfig::Default() {
    return TReplicationConfig{
        .Mode = MODE_READ_ONLY,
        .ConsistencyLevel = CONSISTENCY_LEVEL_ROW,
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

void TTestTableDescription::SerializeTo(NKikimrSchemeOp::TColumnTableDescription& proto) const {
    proto.SetName(Name);

    for (const auto& keyColumn : KeyColumns) {
        proto.MutableSchema()->AddKeyColumnNames(keyColumn);
    }

    for (const auto& column : Columns) {
        column.SerializeTo(*proto.MutableSchema()->AddColumns());
    }
}

std::unique_ptr<NKikimrSchemeOp::TTableDescription> MakeTableDescription(const TTestTableDescription& desc) {
    auto result = std::make_unique<NKikimrSchemeOp::TTableDescription>();
    desc.SerializeTo(*result);
    return result;
}

std::unique_ptr<NKikimrSchemeOp::TColumnTableDescription> MakeColumnTableDescription(const TTestTableDescription& desc) {
    auto result = std::make_unique<NKikimrSchemeOp::TColumnTableDescription>();
    desc.SerializeTo(*result);
    return result;
}

}
