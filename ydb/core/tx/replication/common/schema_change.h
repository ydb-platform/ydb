#pragma once

namespace NKikimrReplication {
    class TSchemaChange;
}

namespace NKikimr::NReplication {

bool IsSameSchemaChange(const NKikimrReplication::TSchemaChange& lhs,
    const NKikimrReplication::TSchemaChange& rhs);

}
