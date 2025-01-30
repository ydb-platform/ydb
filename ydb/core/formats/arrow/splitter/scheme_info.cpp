#include "scheme_info.h"

namespace NKikimr::NArrow::NSplitter {

NAccessor::TColumnSaver ISchemaDetailInfo::GetColumnSaver(const ui32 columnId) const {
    auto saver = DoGetColumnSaver(columnId);
    if (OverrideSerializer) {
        saver.AddSerializerWithBorder(Max<ui32>(), *OverrideSerializer);
    }
    return saver;
}

}
