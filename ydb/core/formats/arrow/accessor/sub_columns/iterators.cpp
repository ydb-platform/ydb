#include "iterators.h"
#include <yql/essentials/types/binary_json/read.h>

namespace NKikimr::NArrow::NAccessor::NSubColumns {

NJson::TJsonValue TGeneralIterator::GetValue() const {
    AFL_VERIFY(IsValidFlag);
    if (RawValue.empty()) {
        return NJson::TJsonValue(NJson::JSON_UNDEFINED);
    }
    auto data = NBinaryJson::SerializeToJson(TStringBuf(RawValue.data(), RawValue.size()));
    NJson::TJsonValue res;
    AFL_VERIFY(NJson::ReadJsonTree(data, &res));
    return res;
}

}   // namespace NKikimr::NArrow::NAccessor::NSubColumns
