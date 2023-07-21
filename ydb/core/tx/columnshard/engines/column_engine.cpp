#include "column_engine.h"
#include <util/stream/output.h>

namespace NKikimr::NOlap {

}

template <>
void Out<NKikimr::NOlap::TColumnEngineChanges>(IOutputStream& out, TTypeTraits<NKikimr::NOlap::TColumnEngineChanges>::TFuncParam changes) {
    out << changes.DebugString();
}
