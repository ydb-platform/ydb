#include "ttl.h"

#include <ydb/core/formats/arrow/accessor/dictionary/accessor.h>
#include <ydb/core/formats/arrow/accessor/sub_columns/accessor.h>
#include <ydb/core/tx/columnshard/engines/portions/data_accessor.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/util/bit_util.h>

namespace NKikimr::NOlap {

namespace {

bool NeedsDecodedSize(const NArrow::NAccessor::TColumnLoader& loader) {
    using EType = NArrow::NAccessor::IChunkedArray::EType;
    return !std::dynamic_pointer_cast<arrow::FixedWidthType>(loader.GetField()->type()) &&
           (loader.GetAccessorConstructor()->GetType() == EType::Dictionary ||
               loader.GetAccessorConstructor()->GetType() == EType::SubColumnsArray);
}

ui64 GetJsonPathMemory(const std::string_view name) {
    using namespace NBinaryJson;
    const auto path = NArrow::NAccessor::NSubColumns::ParseJsonPath(
        NArrow::NAccessor::NSubColumns::ToJsonPath(name.empty() ? TStringBuf("\"\"") : TStringBuf(name.data(), name.size())))
                          .DetachResult();
    ui64 result = 0;
    for (const auto& item : path.Items.PathItems) {
        result += sizeof(TMeta) + sizeof(TEntry) + sizeof(TKeyEntry) + sizeof(TSEntry) + item.size() + 1;
    }
    return result;
}

ui64 GetDecodedPayloadSize(const NArrow::NAccessor::IChunkedArray& data) {
    using namespace NArrow::NAccessor;
    if (const auto* dictionary = dynamic_cast<const TDictionaryArray*>(&data)) {
        ui64 result = 0;
        for (ui32 row = 0; row < data.GetRecordsCount(); ++row) {
            const auto value = std::static_pointer_cast<arrow::BaseBinaryScalar>(dictionary->GetScalar(row));
            if (value->is_valid) {
                result += value->value->size();
            }
        }
        return result;
    }
    const auto& subColumns = dynamic_cast<const TSubColumnsArray&>(data);
    using namespace NBinaryJson;
    constexpr ui64 DocumentOverhead = sizeof(THeader) + sizeof(TMeta) + sizeof(ui32);
    constexpr ui64 ValueOverhead = sizeof(TEntry) + sizeof(TSEntry) + sizeof(double) + 1;
    ui64 result = ui64(data.GetRecordsCount()) * DocumentOverhead;
    const auto& columns = subColumns.GetColumnsData();
    for (ui32 column = 0; column < columns.GetStats().GetColumnsCount(); ++column) {
        const ui64 pathMemory = GetJsonPathMemory(columns.GetStats().GetColumnName(column));
        for (auto it = columns.BuildIterator(column); it.IsValid(); it.Next()) {
            if (it.HasValue()) {
                result += pathMemory + ValueOverhead + it.GetValue().GetValueSize();
            }
        }
    }
    const auto& others = subColumns.GetOthersData();
    std::vector<ui64> pathsMemory;
    for (ui32 column = 0; column < others.GetStats().GetColumnsCount(); ++column) {
        pathsMemory.emplace_back(GetJsonPathMemory(others.GetStats().GetColumnName(column)));
    }
    for (auto it = others.BuildIterator(); it.IsValid(); it.Next()) {
        result += pathsMemory[it.GetKeyIndex()] + ValueOverhead + it.GetValue().GetValueSize();
    }
    // Shared ancestors and string-table entries are deliberately counted for every field.
    return result;
}

}   // namespace

ui64 TTTLColumnEngineChanges::TMemoryPredictor::AddPortion(const TPortionInfo::TConstPtr& portionInfo, const TPortionEvictionFeatures& features,
    const TPortionDataAccessor* accessor, const NBlobOperations::NRead::TCompositeReadBlobs* blobs) {
    SumBlobsMemory += portionInfo->GetTotalBlobBytes();
    ui64 workingMemory = portionInfo->GetTotalRawBytes();
    if (features.GetRebuildDecimalPrimaryKey()) {
        constexpr ui64 ArrayMetadataEstimateBytes = 256;
        constexpr ui64 WorkingArrayCopies = 4;
        constexpr ui64 OutputBufferCopies = 2;
        constexpr ui64 PermutationBufferCopies = 2;
        const ui64 rows = portionInfo->GetRecordsCount();
        // Stored RawBytes omits repeated defaults in sparse/absent columns.
        ui64 denseMemory = portionInfo->GetTotalRawBytes();
        THashSet<ui32> encodedColumns;
        for (const auto columnId : features.GetTargetScheme()->GetColumnIds()) {
            const auto loader = features.GetTargetScheme()->GetColumnLoaderVerified(columnId);
            const auto& type = loader->GetField()->type();
            denseMemory += arrow::BitUtil::BytesForBits(rows) + ArrayMetadataEstimateBytes;
            if (const auto fixed = std::dynamic_pointer_cast<arrow::FixedWidthType>(type)) {
                denseMemory += arrow::BitUtil::BytesForBits(rows * fixed->bit_width());
            } else {
                if (NeedsDecodedSize(*loader)) {
                    encodedColumns.emplace(columnId);
                }
                denseMemory += (rows + 1) * sizeof(ui64);   // offsets, including large binary
                if (const auto value = std::dynamic_pointer_cast<arrow::BaseBinaryScalar>(loader->GetDefaultValue()); value && value->is_valid) {
                    denseMemory += rows * value->value->size();
                }
            }
        }
        if (!encodedColumns.empty()) {
            if (accessor) {
                for (const auto& chunk : accessor->GetRecordsVerified()) {
                    if (encodedColumns.contains(chunk.GetColumnId())) {
                        AFL_VERIFY(blobs);
                        const auto& schema = *features.GetCurrentScheme();
                        const auto loader = schema.GetColumnLoaderVerified(chunk.GetColumnId());
                        const auto& blob =
                            blobs->GetBlobRangeVerified(portionInfo->GetColumnStorageId(chunk.GetColumnId(), schema.GetIndexInfo()),
                                accessor->RestoreBlobRange(chunk.GetBlobRange()));
                        const auto data = loader->ApplyVerified(
                            blob, chunk.GetMeta().GetRecordsCount(), std::nullopt, chunk.GetMeta().GetAdditionalAccessorData());
                        denseMemory += GetDecodedPayloadSize(*data);
                    }
                }
            } else {
                // Task batching precedes accessor loading, so only portion totals are available.
                denseMemory += rows * portionInfo->GetTotalRawBytes();
            }
        }
        workingMemory = WorkingArrayCopies * denseMemory + rows * (sizeof(i32) + PermutationBufferCopies * sizeof(ui64));
        SumOutputMemory += OutputBufferCopies * denseMemory;
    }
    MaxWorkingMemory = Max(MaxWorkingMemory, workingMemory);
    return SumBlobsMemory + GetConstructionMemory();
}

ui64 TTTLColumnEngineChanges::CalcMemoryForConstruction(
    const std::vector<std::shared_ptr<TPortionDataAccessor>>& accessors, const NBlobOperations::NRead::TCompositeReadBlobs* blobs) const {
    if (!HasKeyRebuild) {
        return TBase::CalcMemoryForConstruction(accessors);
    }
    THashMap<ui64, const TPortionDataAccessor*> accessorsById;
    for (const auto& accessor : accessors) {
        accessorsById.emplace(accessor->GetPortionInfo().GetPortionId(), accessor.get());
    }
    TMemoryPredictor predictor;
    for (const auto& portion : PortionsToEvict) {
        predictor.AddPortion(portion.GetPortionInfo(), portion.GetFeatures(), accessorsById.at(portion.GetPortionInfo()->GetPortionId()), blobs);
    }
    // Input blobs have a separate reservation in TAskBlobDataResourceStep.
    return predictor.GetConstructionMemory();
}

ui64 TTTLColumnEngineChanges::GetMemoryForConstructionPreparation(const std::vector<std::shared_ptr<TPortionDataAccessor>>& accessors) const {
    if (!HasKeyRebuild) {
        return 0;
    }
    THashMap<ui64, const ISnapshotSchema*> schemas;
    for (const auto& portion : PortionsToEvict) {
        schemas.emplace(portion.GetPortionInfo()->GetPortionId(), portion.GetFeatures().GetCurrentScheme().get());
    }
    ui64 maxChunkMemory = 0;
    for (const auto& accessor : accessors) {
        const auto* schema = schemas.at(accessor->GetPortionInfo().GetPortionId());
        for (const auto& chunk : accessor->GetRecordsVerified()) {
            if (NeedsDecodedSize(*schema->GetColumnLoaderVerified(chunk.GetColumnId()))) {
                maxChunkMemory = Max(maxChunkMemory, ui64(chunk.GetMeta().GetRawBytes()) + chunk.GetBlobRange().Size);
            }
        }
    }
    // Decode one compact chunk at a time, without expanding dictionary values or building JSON documents.
    constexpr ui64 DecodeBufferCopies = 8;
    return DecodeBufferCopies * maxChunkMemory;
}

}   // namespace NKikimr::NOlap
