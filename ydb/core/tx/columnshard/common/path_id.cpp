#include "path_id.h"

template<>
void Out<NKikimr::NColumnShard::TInternalPathId>(IOutputStream& s, const NKikimr::NColumnShard::TInternalPathId& v) {
    s << v.GetRawValue();
}

template<>
void Out<NKikimr::NColumnShard::TLocalPathId>(IOutputStream& s, const NKikimr::NColumnShard::TLocalPathId& v) {
    s << v.GetRawValue();
}

template<>
void Out<NKikimr::NColumnShard::TUnifiedPathId>(IOutputStream& s, const NKikimr::NColumnShard::TUnifiedPathId& v) {
    s << "{" << v.GetInternalPathId() << ", " << v.GetLocalPathId() << "}";
}
