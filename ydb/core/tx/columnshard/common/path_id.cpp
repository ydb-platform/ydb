#include "path_id.h"

template<>
void Out<NKikimr::NColumnShard::TInternalPathId>(IOutputStream& s, const NKikimr::NColumnShard::TInternalPathId& v) {
    s << v.GetRawValue();
}

template<>
void Out<NKikimr::NColumnShard::TSchemeShardLocalPathId>(IOutputStream& s, const NKikimr::NColumnShard::TSchemeShardLocalPathId& v) {
    s << v.GetRawValue();
}
