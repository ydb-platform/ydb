#include "path_id.h"

template<>
void Out<NKikimr::NColumnShard::TInternalPathId>(IOutputStream& s, TTypeTraits<NKikimr::NColumnShard::TInternalPathId>::TFuncParam v) {
     s << v.GetRawValue();
}
