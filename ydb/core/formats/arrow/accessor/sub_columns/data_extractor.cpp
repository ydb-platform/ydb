#include "data_extractor.h"

#include <util/string/split.h>
#include <util/string/vector.h>
#include <yql/essentials/types/binary_json/format.h>
#include <yql/essentials/types/binary_json/read.h>
#include <yql/essentials/types/binary_json/write.h>

namespace NKikimr::NArrow::NAccessor::NSubColumns {

TConclusionStatus TFirstLevelSchemaData::DoAddDataToBuilders(
    const std::shared_ptr<arrow::Array>& sourceArray, TDataBuilder& dataBuilder) const noexcept {
    if (sourceArray->type()->id() != arrow::binary()->id()) {
        return TConclusionStatus::Fail("incorrect base type for subcolumns schema usage");
    }

    auto arr = std::static_pointer_cast<arrow::StringArray>(sourceArray);
    for (ui32 i = 0; i < arr->length(); ++i) {
        const auto view = arr->GetView(i);
        if (view.size() && !arr->IsNull(i)) {
            //        NBinaryJson::TBinaryJson bJson(view.data(), view.size());
            //        auto bJson = NBinaryJson::SerializeToBinaryJson(TStringBuf(view.data(), view.size()));
            //        const NBinaryJson::TBinaryJson* bJsonParsed = std::get_if<NBinaryJson::TBinaryJson>(&bJson);
            //        AFL_VERIFY(bJsonParsed)("error", *std::get_if<TString>(&bJson))("json", TStringBuf(view.data(), view.size()));
            //        const NBinaryJson::TBinaryJson* bJsonParsed = &bJson;
            auto reader = NBinaryJson::TBinaryJsonReader::Make(TStringBuf(view.data(), view.size()));
            auto cursor = reader->GetRootCursor();
            if (cursor.GetType() == NBinaryJson::EContainerType::Object) {
                auto it = cursor.GetObjectIterator();
                while (it.HasNext()) {
                    auto [key, value] = it.Next();
                    if (key.GetType() != NBinaryJson::EEntryType::String) {
                        continue;
                    }
                    if (value.GetType() == NBinaryJson::EEntryType::String) {
                        dataBuilder.AddKV(key.GetString(), value.GetString());
                    } else if (value.GetType() == NBinaryJson::EEntryType::Number) {
                        dataBuilder.AddKVOwn(key.GetString(), ::ToString(value.GetNumber()));
                    } else if (value.GetType() == NBinaryJson::EEntryType::BoolFalse) {
                        dataBuilder.AddKVOwn(key.GetString(), "0");
                    } else if (value.GetType() == NBinaryJson::EEntryType::BoolTrue) {
                        dataBuilder.AddKVOwn(key.GetString(), "1");
                    } else {
                        continue;
                    }
                }
            } else {
            //    return TConclusionStatus::Fail("incorrect json data: " + ::ToString((int)cursor.GetType()));
            }
        }
        dataBuilder.StartNextRecord();
    }
    return TConclusionStatus::Success();
}

TConclusionStatus IDataAdapter::AddDataToBuilders(const std::shared_ptr<arrow::Array>& sourceArray, TDataBuilder& dataBuilder) const noexcept {
    return DoAddDataToBuilders(sourceArray, dataBuilder);
}

}   // namespace NKikimr::NArrow::NAccessor::NSubColumns
