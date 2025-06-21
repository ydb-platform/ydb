#include "accessor.h"
#include "direct_builder.h"
#include "signals.h"

#include <ydb/core/formats/arrow/accessor/composite_serial/accessor.h>
#include <ydb/core/formats/arrow/accessor/plain/constructor.h>
#include <ydb/core/formats/arrow/save_load/loader.h>
#include <ydb/core/formats/arrow/size_calcer.h>
#include <ydb/core/formats/arrow/splitter/simple.h>

#include <ydb/library/formats/arrow/protos/accessor.pb.h>
#include <ydb/library/formats/arrow/simple_arrays_cache.h>

#include <yql/essentials/types/binary_json/format.h>
#include <yql/essentials/types/binary_json/write.h>

namespace NKikimr::NArrow::NAccessor {

TConclusion<std::shared_ptr<TSubColumnsArray>> TSubColumnsArray::Make(
    const std::shared_ptr<IChunkedArray>& sourceArray, const NSubColumns::TSettings& settings, const std::shared_ptr<arrow::DataType>& columnType) {
    AFL_VERIFY(sourceArray);
    NSubColumns::TDataBuilder builder(columnType, settings);
    IChunkedArray::TReader reader(sourceArray);
    std::vector<std::shared_ptr<arrow::Array>> storage;
    for (ui32 i = 0; i < reader.GetRecordsCount();) {
        auto address = reader.GetReadChunk(i);
        storage.emplace_back(address.GetArray());
        auto conclusion = settings.GetDataExtractor()->AddDataToBuilders(address.GetArray(), builder);
        if (conclusion.IsFail()) {
            return conclusion;
        }
        i += address.GetArray()->length();
        AFL_VERIFY(i <= reader.GetRecordsCount());
    }
    return builder.Finish();
}

TSubColumnsArray::TSubColumnsArray(const std::shared_ptr<arrow::DataType>& type, const ui32 recordsCount, const NSubColumns::TSettings& settings)
    : TBase(recordsCount, EType::SubColumnsArray, type)
    , ColumnsData(NSubColumns::TColumnsData::BuildEmpty(recordsCount))
    , OthersData(NSubColumns::TOthersData::BuildEmpty())
    , Settings(settings) {
    AFL_VERIFY(type->id() == arrow::binary()->id())("type", type->ToString())("error", "currently supported JsonDocument only");
}

TSubColumnsArray::TSubColumnsArray(NSubColumns::TColumnsData&& columns, NSubColumns::TOthersData&& others,
    const std::shared_ptr<arrow::DataType>& type, const ui32 recordsCount, const NSubColumns::TSettings& settings)
    : TBase(recordsCount, EType::SubColumnsArray, type)
    , ColumnsData(std::move(columns))
    , OthersData(std::move(others))
    , Settings(settings) {
    AFL_VERIFY(type->id() == arrow::binary()->id())("type", type->ToString())("error", "currently supported JsonDocument only");
}

TString TSubColumnsArray::SerializeToString(const TChunkConstructionData& externalInfo) const {
    TString blobData;
    NKikimrArrowAccessorProto::TSubColumnsAccessor proto;
    std::vector<TString> blobRanges;
    if (ColumnsData.GetStats().GetColumnsCount()) {
        blobRanges.emplace_back(ColumnsData.GetStats().SerializeAsString(nullptr));
        proto.SetColumnStatsSize(blobRanges.back().size());
    } else {
        proto.SetColumnStatsSize(0);
    }

    if (OthersData.GetStats().GetColumnsCount()) {
        blobRanges.emplace_back(OthersData.GetStats().SerializeAsString(nullptr));
        proto.SetOtherStatsSize(blobRanges.back().size());
    } else {
        proto.SetOtherStatsSize(0);
    }
    ui32 columnIdx = 0;
    TMonotonic pred = TMonotonic::Now();
    for (auto&& i : ColumnsData.GetRecords()->GetColumns()) {
        TChunkConstructionData cData(GetRecordsCount(), nullptr, arrow::utf8(), externalInfo.GetDefaultSerializer());
        blobRanges.emplace_back(ColumnsData.GetStats().GetAccessorConstructor(columnIdx).SerializeToString(i, cData));
        auto* cInfo = proto.AddKeyColumns();
        cInfo->SetSize(blobRanges.back().size());
        TMonotonic next = TMonotonic::Now();
        NSubColumns::TSignals::GetColumnSignals().OnBlobSize(ColumnsData.GetStats().GetColumnSize(columnIdx), blobRanges.back().size(), next - pred);
        pred = next;
        ++columnIdx;
    }

    if (OthersData.GetRecords()->GetRecordsCount()) {
        TMonotonic pred = TMonotonic::Now();
        for (auto&& i : OthersData.GetRecords()->GetColumns()) {
            TChunkConstructionData cData(i->GetRecordsCount(), nullptr, i->GetDataType(), externalInfo.GetDefaultSerializer());
            blobRanges.emplace_back(NPlain::TConstructor().SerializeToString(i, cData));
            TMonotonic next = TMonotonic::Now();
            NSubColumns::TSignals::GetOtherSignals().OnBlobSize(i->GetRawSizeVerified(), blobRanges.back().size(), next - pred);
            pred = next;
            auto* cInfo = proto.AddOtherColumns();
            cInfo->SetSize(blobRanges.back().size());
        }
    }
    proto.SetOtherRecordsCount(OthersData.GetRecords()->GetRecordsCount());

    ui64 blobsSize = 0;
    for (auto&& i : blobRanges) {
        blobsSize += i.size();
    }

    const TString protoString = proto.SerializeAsString();
    TString result;
    TStringOutput so(result);
    so.Reserve(protoString.size() + sizeof(ui32) + blobsSize);
    const ui32 protoSize = protoString.size();
    so.Write(&protoSize, sizeof(protoSize));
    so.Write(protoString.data(), protoSize);
    for (auto&& s : blobRanges) {
        so.Write(s.data(), s.size());
    }
    so.Finish();
    return result;
}

class TJsonRestorer {
private:
    NJson::TJsonValue Result;

public:
    bool IsNull() const {
        return !Result.IsDefined();
    }

    TConclusion<NBinaryJson::TBinaryJson> Finish() {
        auto str = Result.GetStringRobust();
        auto bJson = NBinaryJson::SerializeToBinaryJson(Result.GetStringRobust());
        if (const TString* val = std::get_if<TString>(&bJson)) {
            return TConclusionStatus::Fail(*val);
        } else if (const NBinaryJson::TBinaryJson* val = std::get_if<NBinaryJson::TBinaryJson>(&bJson)) {
            return std::move(*val);
        } else {
            return TConclusionStatus::Fail("undefined case for binary json construction");
        }
    }

    void SetValueByPath(const TString& path, const TString& valueStr) {
        ui32 start = 0;
        bool enqueue = false;
        bool wasEnqueue = false;
        NJson::TJsonValue* current = &Result;
        for (ui32 i = 0; i < path.size(); ++i) {
            if (path[i] == '\\') {
                ++i;
                continue;
            }
            if (path[i] == '\'' || path[i] == '\"') {
                wasEnqueue = true;
                enqueue = !enqueue;
                continue;
            }
            if (enqueue) {
                continue;
            }
            if (path[i] == '.') {
                if (wasEnqueue) {
                    AFL_VERIFY(i > start + 2);
                    TStringBuf key(path.data() + start + 1, (i - 1) - start - 1);
                    NJson::TJsonValue* currentNext = nullptr;
                    if (current->GetValuePointer(key, &currentNext)) {
                        current = currentNext;
                    } else {
                        current = &current->InsertValue(key, NJson::JSON_MAP);
                    }
                } else {
                    AFL_VERIFY(i > start);
                    TStringBuf key(path.data() + start, i - start);
                    NJson::TJsonValue* currentNext = nullptr;
                    if (current->GetValuePointer(key, &currentNext)) {
                        current = currentNext;
                    } else {
                        ui32 keyIndex;
                        if (key.StartsWith("[") && key.EndsWith("]") && TryFromString<ui32>(key.data() + 1, key.size() - 2, keyIndex)) {
                            AFL_VERIFY(!current->IsDefined() || current->IsArray() || (current->IsMap() && current->GetMapSafe().empty()));
                            current->SetType(NJson::JSON_ARRAY);
                            if (current->GetArraySafe().size() <= keyIndex) {
                                current->GetArraySafe().resize(keyIndex + 1);
                            }
                            current = &current->GetArraySafe()[keyIndex];
                        } else {
                            AFL_VERIFY(!current->IsArray())("current_type", current->GetType())("current", current->GetStringRobust());
                            current = &current->InsertValue(key, NJson::JSON_MAP);
                        }
                    }
                }
                wasEnqueue = false;
                start = i + 1;
            }
        }
        if (wasEnqueue) {
            AFL_VERIFY(path.size() > start + 2)("path", path)("start", start);
            TStringBuf key(path.data() + start + 1, (path.size() - 1) - start - 1);
            current->InsertValue(key, valueStr);
        } else {
            AFL_VERIFY(path.size() > start);
            TStringBuf key(path.data() + start, (path.size()) - start);
            ui32 keyIndex;
            if (key.StartsWith("[") && key.EndsWith("]") && TryFromString<ui32>(key.data() + 1, key.size() - 2, keyIndex)) {
                AFL_VERIFY(!current->IsDefined() || current->IsArray() || (current->IsMap() && current->GetMapSafe().empty()));
                current->SetType(NJson::JSON_ARRAY);

                if (current->GetArraySafe().size() <= keyIndex) {
                    current->GetArraySafe().resize(keyIndex + 1);
                }
                current->GetArraySafe()[keyIndex] = valueStr;
            } else {
                AFL_VERIFY(!current->IsArray())("key", key)("current", current->GetStringRobust())("full", Result.GetStringRobust())(
                    "current_type", current->GetType());
                current->InsertValue(key, valueStr);
            }
        }
    }
};

std::shared_ptr<arrow::Array> TSubColumnsArray::BuildBJsonArray(const TColumnConstructionContext& context) const {
    auto it = BuildUnorderedIterator();
    auto builder = NArrow::MakeBuilder(GetDataType());
    const ui32 start = context.GetStartIndex().value_or(0);
    const ui32 finish = start + context.GetRecordsCount().value_or(GetRecordsCount() - start);
    std::optional<std::vector<bool>> simpleFilter;
    if (context.GetFilter()) {
        simpleFilter = context.GetFilter()->BuildSimpleFilter();
    }
    for (ui32 recordIndex = start; recordIndex < finish; ++recordIndex) {
        if (simpleFilter && !(*simpleFilter)[recordIndex]) {
            continue;
        }
        it.SkipRecordTo(recordIndex);
        TJsonRestorer value;
        auto onStartRecord = [&](const ui32 index) {
            AFL_VERIFY(recordIndex == index)("count", recordIndex)("index", index);
        };
        auto onFinishRecord = [&]() {
            if (value.IsNull()) {
                TStatusValidator::Validate(builder->AppendNull());
            } else {
                const TConclusion<NBinaryJson::TBinaryJson> bJson = value.Finish();
                NArrow::Append<arrow::BinaryType>(*builder, arrow::util::string_view(bJson->data(), bJson->size()));
            }
        };

        const auto addValueToJson = [&](const TString& path, const TString& valueStr) {
            value.SetValueByPath(path, valueStr);
        };

        auto onRecordKV = [&](const ui32 index, const std::string_view valueView, const bool isColumn) {
            if (isColumn) {
                addValueToJson(ColumnsData.GetStats().GetColumnNameString(index), TString(valueView.data(), valueView.size()));
            } else {
                addValueToJson(OthersData.GetStats().GetColumnNameString(index), TString(valueView.data(), valueView.size()));
            }
        };
        it.ReadRecord(recordIndex, onStartRecord, onRecordKV, onFinishRecord);
    }
    return NArrow::FinishBuilder(std::move(builder));
}

std::shared_ptr<arrow::ChunkedArray> TSubColumnsArray::DoGetChunkedArray(const TColumnConstructionContext& context) const {
    auto chunk = BuildBJsonArray(context);
    if (chunk->length()) {
        return std::make_shared<arrow::ChunkedArray>(chunk);
    } else {
        return std::make_shared<arrow::ChunkedArray>(arrow::ArrayVector(), GetDataType());
    }
}

IChunkedArray::TLocalDataAddress TSubColumnsArray::DoGetLocalData(
    const std::optional<TCommonChunkAddress>& /*chunkCurrent*/, const ui64 /*position*/) const {
    return TLocalDataAddress(BuildBJsonArray(TColumnConstructionContext()), 0, 0);
}

}   // namespace NKikimr::NArrow::NAccessor
