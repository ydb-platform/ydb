#include "tablet_id.h"
#include <util/stream/output.h>
#include <util/generic/typetraits.h>

namespace NKikimr::NOlap {

}

template <>
void Out<NKikimr::NOlap::TTabletId>(IOutputStream& os, TTypeTraits<NKikimr::NOlap::TTabletId>::TFuncParam val) {
    os << (ui64)val;
}
