#include "test_table.h"

#include <ydb/core/protos/flat_scheme_op.pb.h>

namespace NKikimr::NReplication::NTestHelpers {

void TTestTableDescription::TColumn::SerializeTo(NKikimrSchemeOp::TColumnDescription& proto) const {
    proto.SetName(Name);
    proto.SetType(Type);
}

void TTestTableDescription::SerializeTo(NKikimrSchemeOp::TTableDescription& proto) const {
    proto.SetName(Name);
    proto.MutableReplicationConfig()->SetMode(NKikimrSchemeOp::TTableReplicationConfig::REPLICATION_MODE_READ_ONLY);
    proto.MutableReplicationConfig()->SetConsistency(NKikimrSchemeOp::TTableReplicationConfig::CONSISTENCY_WEAK);

    for (const auto& keyColumn : KeyColumns) {
        proto.AddKeyColumnNames(keyColumn);
    }

    for (const auto& column : Columns) {
        column.SerializeTo(*proto.AddColumns());
    }
}

THolder<NKikimrSchemeOp::TTableDescription> MakeTableDescription(const TTestTableDescription& desc) {
    auto result = MakeHolder<NKikimrSchemeOp::TTableDescription>();
    desc.SerializeTo(*result);
    return result;
}

}
