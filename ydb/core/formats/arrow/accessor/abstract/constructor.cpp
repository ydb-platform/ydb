#include "constructor.h"
#include <ydb/core/formats/arrow/accessor/plain/constructor.h>

namespace NKikimr::NArrow::NAccessor {

TConstructorContainer TConstructorContainer::GetDefaultConstructor() {
    static std::shared_ptr<IConstructor> result = std::make_shared<NPlain::TConstructor>();
    return result;
}

TBlobWithAdditionalAccessorData IConstructor::SerializeToBlobAndMeta(
    const std::shared_ptr<IChunkedArray>& columnData, const TChunkConstructionData& externalInfo) const {
    AFL_VERIFY(columnData);
    AFL_VERIFY(columnData->GetType() == Type)("column", columnData->GetType())("current", Type);
    AFL_VERIFY(columnData->GetDataType()->Equals(externalInfo.GetColumnType()))("column", columnData->GetDataType()->ToString())(
        "external", externalInfo.GetColumnType()->ToString());
    return DoSerializeToBlobAndMeta(columnData, externalInfo);
}

}
