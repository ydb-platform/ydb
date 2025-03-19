#include "path_id.h"


void Out(IOutputStream& s, NKikimr::NColumnShard::TInternalPathId v){
    s << v.GetInternalPathIdValue();
}



template<>
void Out<NKikimr::NColumnShard::TInternalPathId>(IOutputStream& s, TTypeTraits<NKikimr::NColumnShard::TInternalPathId>::TFuncParam v) {
    return Out(s, v);
}
