#include "accessor.h"

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

TConclusion<std::shared_ptr<TSubColumnsArray>> TSubColumnsArray::Make(const std::shared_ptr<IChunkedArray>& sourceArray,
    const std::shared_ptr<NSubColumns::IDataAdapter>& adapter, const NSubColumns::TSettings& settings) {
    AFL_VERIFY(adapter);
    AFL_VERIFY(sourceArray);
    NSubColumns::TDataBuilder builder(sourceArray->GetDataType(), settings);
    IChunkedArray::TReader reader(sourceArray);
    std::vector<std::shared_ptr<arrow::Array>> storage;
    for (ui32 i = 0; i < reader.GetRecordsCount();) {
        auto address = reader.GetReadChunk(i);
        storage.emplace_back(address.GetArray());
        auto conclusion = adapter->AddDataToBuilders(address.GetArray(), builder);
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
}

TSubColumnsArray::TSubColumnsArray(NSubColumns::TColumnsData&& columns, NSubColumns::TOthersData&& others,
    const std::shared_ptr<arrow::DataType>& type, const ui32 recordsCount, const NSubColumns::TSettings& settings)
    : TBase(recordsCount, EType::SubColumnsArray, type)
    , ColumnsData(std::move(columns))
    , OthersData(std::move(others))
    , Settings(settings) {
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
    for (auto&& i : ColumnsData.GetRecords()->GetColumns()) {
        TChunkConstructionData cData(GetRecordsCount(), nullptr, arrow::utf8(), externalInfo.GetDefaultSerializer());
        blobRanges.emplace_back(
            ColumnsData.GetStats().GetAccessorConstructor(columnIdx).SerializeToString(i, cData));
        auto* cInfo = proto.AddKeyColumns();
        cInfo->SetSize(blobRanges.back().size());
        ++columnIdx;
    }

    if (OthersData.GetRecords()->GetRecordsCount()) {
        for (auto&& i : OthersData.GetRecords()->GetColumns()) {
            TChunkConstructionData cData(i->GetRecordsCount(), nullptr, i->GetDataType(), externalInfo.GetDefaultSerializer());
            blobRanges.emplace_back(NPlain::TConstructor().SerializeToString(i, cData));
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

IChunkedArray::TLocalDataAddress TSubColumnsArray::DoGetLocalData(
    const std::optional<TCommonChunkAddress>& /*chunkCurrent*/, const ui64 /*position*/) const {
    auto it = BuildUnorderedIterator();
    auto builder = NArrow::MakeBuilder(GetDataType());
    for (ui32 recordIndex = 0; recordIndex < GetRecordsCount(); ++recordIndex) {
        NJson::TJsonValue value;
        auto onStartRecord = [&](const ui32 index) {
            AFL_VERIFY(recordIndex == index)("count", recordIndex)("index", index);
        };
        auto onFinishRecord = [&]() {
            auto str = value.GetStringRobust();
            //            NArrow::Append<arrow::BinaryType>(*builder, arrow::util::string_view(str.data(), str.size()));
            //
            auto bJson = NBinaryJson::SerializeToBinaryJson(value.GetStringRobust());
            if (const TString* val = std::get_if<TString>(&bJson)) {
                AFL_VERIFY(false)("error", *val);
            } else if (const NBinaryJson::TBinaryJson* val = std::get_if<NBinaryJson::TBinaryJson>(&bJson)) {
                if (value.IsNull() || !value.IsDefined()) {
                    TStatusValidator::Validate(builder->AppendNull());
                } else {
                    NArrow::Append<arrow::BinaryType>(*builder, arrow::util::string_view(val->data(), val->size()));
                }
            } else {
                AFL_VERIFY(false);
            }
        };
        auto onRecordKV = [&](const ui32 index, const std::string_view valueView, const bool isColumn) {
            if (isColumn) {
                value.InsertValue(ColumnsData.GetStats().GetColumnNameString(index), TString(valueView.data(), valueView.size()));
            } else {
                value.InsertValue(OthersData.GetStats().GetColumnNameString(index), TString(valueView.data(), valueView.size()));
            }
        };
        it.ReadRecord(recordIndex, onStartRecord, onRecordKV, onFinishRecord);
    }
    return TLocalDataAddress(NArrow::FinishBuilder(std::move(builder)), 0, 0);
}

}   // namespace NKikimr::NArrow::NAccessor
