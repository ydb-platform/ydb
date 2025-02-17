#include "defs.h"

template <>
void Out<NKikimr::NOlap::TOperationWriteId>(IOutputStream& os, TTypeTraits<NKikimr::NOlap::TOperationWriteId>::TFuncParam val) {
    os << (ui64)val;
}

template <>
void Out<NKikimr::NOlap::TInsertWriteId>(IOutputStream& os, TTypeTraits<NKikimr::NOlap::TInsertWriteId>::TFuncParam val) {
    os << (ui64)val;
}
