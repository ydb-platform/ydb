#include "dq_join_common.h"
#include <yql/essentials/minikql/mkql_node_cast.h>

namespace NKikimr::NMiniKQL {

TKeyTypes KeyTypesFromColumns(const TMKQLVector<TType*>& types, const TMKQLVector<ui32>& keyIndexes) {
    TKeyTypes kt;
    for (auto typeIndex : keyIndexes) {
        const TType* type = types[typeIndex];
        MKQL_ENSURE(type->IsData(), "exepected data type");
        kt.push_back(std::pair{*AS_TYPE(TDataType, type)->GetDataSlot(), false});
    }
    return kt;
}


} // namespace NKikimr::NMiniKQL