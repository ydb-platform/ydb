#include "constructor.h"
#include <ydb/core/formats/arrow/accessor/plain/constructor.h>

namespace NKikimr::NArrow::NAccessor {

TConstructorContainer TConstructorContainer::GetDefaultConstructor() {
    static std::shared_ptr<IConstructor> result = std::make_shared<NPlain::TConstructor>();
    return result;
}

TString IConstructor::SerializeToString(const std::shared_ptr<IChunkedArray>& columnData, const TChunkConstructionData& externalInfo) const {
    AFL_VERIFY(columnData);
    AFL_VERIFY(columnData->GetType() == Type)("column", columnData->GetType())("current", Type);
    
    bool isDecimalConversion = (columnData->GetDataType()->id() == arrow::Type::FIXED_SIZE_BINARY && 
                               externalInfo.GetColumnType()->id() == arrow::Type::DECIMAL128) ||
                              (columnData->GetDataType()->id() == arrow::Type::DECIMAL128 && 
                               externalInfo.GetColumnType()->id() == arrow::Type::FIXED_SIZE_BINARY);
    
    if (!columnData->GetDataType()->Equals(externalInfo.GetColumnType()) && !isDecimalConversion) {
        AFL_VERIFY(false)("column", columnData->GetDataType()->ToString())("external", externalInfo.GetColumnType()->ToString());
    }
    
    return DoSerializeToString(columnData, externalInfo);
}

}
