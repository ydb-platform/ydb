#pragma once

#include <util/system/types.h>

namespace NKikimrReplication {
    class TSchemaChange;
}

namespace NKikimr::NReplication {

enum ESchemaCapability : ui64 {
    SchemaIndexMetadata = 1,
    SchemaFulltextIndexSettings = 2,
};

ui64 GetSchemaCapabilities(const NKikimrReplication::TSchemaChange& schema);
void FilterSchemaMetadata(NKikimrReplication::TSchemaChange& schema, ui64 capabilities);
bool MergeSchemaIndexMetadata(NKikimrReplication::TSchemaChange& into,
    const NKikimrReplication::TSchemaChange& from);

bool IsSameSchemaChange(const NKikimrReplication::TSchemaChange& lhs,
    const NKikimrReplication::TSchemaChange& rhs);

}
