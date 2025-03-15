#include "path_id.h"


void Out(IOutputStream& s, NKikimr::NColumnShard::TInternalPathId v){
    s << v.GetInternalPathIdValue();
}


void Out(IOutputStream& s, NKikimr::NColumnShard::TLocalPathId v){
    s << v.GetLocalPathIdValue();
}

void Out(IOutputStream& s, const NKikimr::NColumnShard::TUnifiedPathId& v){
    s << "{" << v.LocalPathId << ", " << v.InternalPathId << "}";
}


template<>
void Out<NKikimr::NColumnShard::TInternalPathId>(IOutputStream& s, TTypeTraits<NKikimr::NColumnShard::TInternalPathId>::TFuncParam v) {
    return Out(s, v);
}


template<>
void Out<NKikimr::NColumnShard::TLocalPathId>(IOutputStream& s, TTypeTraits<NKikimr::NColumnShard::TLocalPathId>::TFuncParam v) {
    return Out(s, v);
}

template<>
void Out<NKikimr::NColumnShard::TUnifiedPathId>(IOutputStream& s, TTypeTraits<NKikimr::NColumnShard::TUnifiedPathId>::TFuncParam v) {
    return Out(s, v);
}