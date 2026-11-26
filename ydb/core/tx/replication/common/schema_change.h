#pragma once

namespace NKikimrReplication {
    class TSchemaChange;
}

namespace NKikimr::NReplication {

bool HasFulltextIndexMetadata(const NKikimrReplication::TSchemaChange& schema);
void ClearFulltextIndexMetadata(NKikimrReplication::TSchemaChange& schema);
bool MergeSchemaIndexMetadata(NKikimrReplication::TSchemaChange& into,
    const NKikimrReplication::TSchemaChange& from);

bool IsSameSchemaChange(const NKikimrReplication::TSchemaChange& lhs,
    const NKikimrReplication::TSchemaChange& rhs);

}
