#include "meta.h"

#include <ydb/core/formats/arrow/hash/calcer.h>
#include <ydb/core/local_indexes/bloom/const.h>
#include <ydb/core/tx/columnshard/engines/storage/chunks/data.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/bits_storage/array_power2.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/helper/case_helper.h>
#include <ydb/core/tx/program/program.h>
#include <ydb/core/tx/schemeshard/olap/schema/schema.h>

#include <ydb/library/formats/arrow/hash/xx_hash.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_primitive.h>
#include <library/cpp/deprecated/atomic/atomic.h>
#include <util/generic/bitmap.h>

#include <climits>

namespace NKikimr::NOlap::NIndexes::NBloomNGramm {

class TNGrammBuilder {
private:
    const ui32 HashesCount;
    TCaseStringNormalizer StringNormalizer;

    template <ui32 CharsRemained>
    class THashesBuilder {
    public:
        static ui64 Build(const ui8* data, const ui64 h) {
            return THashesBuilder<CharsRemained - 1>::Build(data + 1, (h ^ uint64_t(*data)) * 16777619);
        }
    };

    template <>
    class THashesBuilder<0> {
    public:
        static ui64 Build(const ui8* /*data*/, const ui64 hash) {
            return hash;
        }
    };

    template <ui32 HashIdx, ui32 CharsCount>
    class THashesCountSelector {
        static constexpr ui64 HashStart = (ui64)HashIdx * (ui64)2166136261;

    public:
        template <class TActor>
        static void BuildHashes(const ui8* data, TActor& actor) {
            actor(THashesBuilder<CharsCount>::Build(data, HashStart));
            THashesCountSelector<HashIdx - 1, CharsCount>::BuildHashes(data, actor);
        }
    };

    template <ui32 CharsCount>
    class THashesCountSelector<0, CharsCount> {
    public:
        template <class TActor>
        static void BuildHashes(const ui8* /*data*/, TActor& /*actor*/) {
        }
    };

    template <ui32 HashesCount, ui32 CharsCount>
    class THashesSelector {
    private:
        template <class TActor>
        static void BuildHashesImpl(
            const ui8* data, const ui32 dataSize, const std::optional<NRequest::TLikePart::EOperation> op, TActor& actor) {
            TBuffer fakeString;
            fakeString.Reserve(CharsCount * 2);
            if (!op || op == NRequest::TLikePart::EOperation::StartsWith || op == NRequest::TLikePart::EOperation::Equals) {
                fakeString.Clear();
                fakeString.Fill('\0', CharsCount - 1);
                fakeString.Append((const char*)data, std::min(CharsCount - 1, dataSize));
                for (ui32 c = 0; c + CharsCount <= fakeString.Size(); ++c) {
                    THashesCountSelector<HashesCount, CharsCount>::BuildHashes((const ui8*)fakeString.data(), actor);
                }
            }
            for (ui32 c = 0; c + CharsCount <= dataSize; ++c) {
                THashesCountSelector<HashesCount, CharsCount>::BuildHashes(data + c, actor);
            }
            if (!op || op == NRequest::TLikePart::EOperation::EndsWith || op == NRequest::TLikePart::EOperation::Equals) {
                fakeString.Clear();
                if (dataSize < CharsCount) {
                    fakeString.Append((const char*)data, dataSize);
                } else {
                    fakeString.Append((const char*)data + dataSize - CharsCount + 1, CharsCount - 1);
                }
                fakeString.Fill('\0', CharsCount - 1);
                for (ui32 c = 0; c + CharsCount <= fakeString.Size(); ++c) {
                    THashesCountSelector<HashesCount, CharsCount>::BuildHashes((const ui8*)fakeString.data(), actor);
                }
            }
        }

    public:
        template <class TActor>
        static void BuildHashes(const ui8* data, const ui32 dataSize, const ui32 hashesCount, const ui32 nGrammSize,
            const std::optional<NRequest::TLikePart::EOperation> op, TActor& actor) {
            if (HashesCount == hashesCount && CharsCount == nGrammSize) {
                BuildHashesImpl(data, dataSize, op, actor);
            } else if (HashesCount > hashesCount && CharsCount > nGrammSize) {
                THashesSelector<HashesCount - 1, CharsCount - 1>::BuildHashes(data, dataSize, hashesCount, nGrammSize, op, actor);
            } else if (HashesCount > hashesCount) {
                THashesSelector<HashesCount - 1, CharsCount>::BuildHashes(data, dataSize, hashesCount, nGrammSize, op, actor);
            } else if (CharsCount > nGrammSize) {
                THashesSelector<HashesCount, CharsCount - 1>::BuildHashes(data, dataSize, hashesCount, nGrammSize, op, actor);
            } else {
                AFL_VERIFY(false);
            }
        }
    };

    template <ui32 CharsCount>
    class THashesSelector<0, CharsCount> {
    public:
        template <class TActor>
        static void BuildHashes(const ui8* /*data*/, const ui32 /*dataSize*/, const ui32 /*hashesCount*/, const ui32 /*nGrammSize*/,
            const std::optional<NRequest::TLikePart::EOperation> /*op*/, TActor& /*actor*/) {
            AFL_VERIFY(false);
        }
    };

    template <ui32 HashesCount>
    class THashesSelector<HashesCount, 0> {
    public:
        template <class TActor>
        static void BuildHashes(const ui8* /*data*/, const ui32 /*dataSize*/, const ui32 /*hashesCount*/, const ui32 /*nGrammSize*/,
            const std::optional<NRequest::TLikePart::EOperation> /*op*/, TActor& /*actor*/) {
            AFL_VERIFY(false);
        }
    };

    template <>
    class THashesSelector<0, 0> {
    public:
        template <class TActor>
        static void BuildHashes(const ui8* /*data*/, const ui32 /*dataSize*/, const ui32 /*hashesCount*/, const ui32 /*nGrammSize*/,
            const std::optional<NRequest::TLikePart::EOperation> /*op*/, TActor& /*actor*/) {
            AFL_VERIFY(false);
        }
    };

public:
    TNGrammBuilder(const ui32 hashesCount, const bool caseSensitive)
        : HashesCount(hashesCount)
        , StringNormalizer(caseSensitive)
    {
    }

    template <class TAction>
    void BuildNGramms(
        const char* data, const ui32 dataSize, const std::optional<NRequest::TLikePart::EOperation> op, const ui32 nGrammSize, TAction& pred) {
        const TStringBuf normalized = StringNormalizer.Normalize(TStringBuf(data, dataSize));
        THashesSelector<TConstants::MaxHashesCount, TConstants::MaxNGrammSize>::BuildHashes(
            (const ui8*)normalized.data(), normalized.size(), HashesCount, nGrammSize, op, pred);
    }

    template <class TFiller>
    void FillNGrammHashes(const ui32 nGrammSize, const std::shared_ptr<arrow::Array>& array, TFiller& fillData) {
        AFL_VERIFY(array->type_id() == arrow::utf8()->id())("id", array->type()->ToString());
        NArrow::SwitchType(array->type_id(), [&](const auto& type) {
            using TWrap = std::decay_t<decltype(type)>;
            using T = typename TWrap::T;
            using TArray = typename arrow::TypeTraits<T>::ArrayType;
            auto& typedArray = static_cast<const TArray&>(*array);

            for (ui32 row = 0; row < array->length(); ++row) {
                if (array->IsNull(row)) {
                    continue;
                }
                if constexpr (arrow::has_string_view<T>()) {
                    auto value = typedArray.GetView(row);
                    BuildNGramms(value.data(), value.size(), {}, nGrammSize, fillData);
                } else {
                    AFL_VERIFY(false);
                }
            }
            return true;
        });
    }

    template <class TFiller>
    void FillNGrammHashes(const ui32 nGrammSize, const NRequest::TLikePart::EOperation op, const TString& userReq, TFiller& fillData) {
        const TStringBuf normalized = StringNormalizer.Normalize(userReq);
        THashesSelector<TConstants::MaxHashesCount, TConstants::MaxNGrammSize>::BuildHashes(
            (const ui8*)normalized.data(), normalized.size(), HashesCount, nGrammSize, op, fillData);
    }
};

namespace {

template <class TBuilder, class TFiller>
void VisitChunkWithBuilder(const std::shared_ptr<NArrow::NAccessor::IChunkedArray>& chunk, const TReadDataExtractorContainer& dataExtractor,
    const ui32 nGrammSize, TBuilder& builder, TFiller& filler) {
    dataExtractor->VisitAll(
        chunk,
        [&](const std::shared_ptr<arrow::Array>& arr, const ui32 /*hashBase*/) {
            builder.FillNGrammHashes(nGrammSize, arr, filler);
        },
        [&](const NArrow::NAccessor::TJsonValueView& data, const ui32 /*hashBase*/) {
            auto view = data.GetScalarOptional();
            if (!view.has_value()) {
                return;
            }

            builder.BuildNGramms(view->data(), view->size(), {}, nGrammSize, filler);
        });
}

template <class TBuilder, class TFiller>
void VisitAllChunksWithBuilder(
    TChunkedBatchReader& reader, const TReadDataExtractorContainer& dataExtractor, const ui32 nGrammSize, TBuilder& builder, TFiller& filler) {
    for (reader.Start(); reader.IsCorrect();) {
        AFL_VERIFY(reader.GetColumnsCount() == 1);
        for (auto&& r : reader) {
            VisitChunkWithBuilder(r.GetCurrentChunk(), dataExtractor, nGrammSize, builder, filler);
        }

        reader.ReadNext(reader.begin()->GetCurrentChunk()->GetRecordsCount());
    }
}

std::vector<std::pair<std::shared_ptr<NArrow::NAccessor::IChunkedArray>, ui32>> CollectChunks(TChunkedBatchReader& reader) {
    std::vector<std::pair<std::shared_ptr<NArrow::NAccessor::IChunkedArray>, ui32>> result;
    for (reader.Start(); reader.IsCorrect();) {
        AFL_VERIFY(reader.GetColumnsCount() == 1);
        auto chunk = reader.begin()->GetCurrentChunk();
        const ui32 records = chunk->GetRecordsCount();
        result.emplace_back(std::move(chunk), records);
        reader.ReadNext(records);
    }
    return result;
}

}   // namespace

std::vector<std::shared_ptr<NChunks::TPortionIndexChunk>> TIndexMeta::DoBuildIndexImpl(
    TChunkedBatchReader& reader, const ui32 recordsCount, const std::optional<ui64> chunkSizeLimit) const {
    AFL_VERIFY(reader.GetColumnsCount() == 1)("count", reader.GetColumnsCount());
    const ui32 hashesCount = Request.ResolvedHashesCount();
    const bool caseSensitive = Request.ResolvedCaseSensitive();
    const bool useOldSizing = Request.IsOldSizingMode();
    const ui32 ngramSize = Request.ResolvedNGrammSize();
    const double falsePositiveProbability = Request.ResolvedFalsePositiveProbability();
    const ui32 filterSizeBytes = Request.ResolvedFilterSizeBytes();
    const ui32 resolvedRecordsCount = Request.ResolvedRecordsCount();
    TNGrammBuilder builder(hashesCount, caseSensitive);

    static constexpr ui64 BitsPerUi64 = sizeof(ui64) * CHAR_BIT;
    static constexpr ui64 MaxBitsSize = static_cast<ui64>(TConstants::MaxFilterSizeBytes) * CHAR_BIT;

    // Splits the source chunks into consecutive groups of at most maxRecordsPerChunk records and emits one
    // index chunk per group, so every produced blob fits the storage limit. The scan applies each chunk to its
    // own record range (TIndexColumnChunked), so per-range filters are equivalent to the single one.
    const auto buildBatched = [&](const ui32 maxRecordsPerChunk, const auto& buildChunkData) {
        std::vector<std::shared_ptr<NChunks::TPortionIndexChunk>> result;
        const auto chunks = CollectChunks(reader);
        ui32 chunkIdx = 0;
        for (ui32 pos = 0; pos < chunks.size();) {
            ui32 batchRecords = chunks[pos].second;
            ui32 end = pos + 1;
            while (end < chunks.size() && batchRecords + chunks[end].second <= maxRecordsPerChunk) {
                batchRecords += chunks[end].second;
                ++end;
            }
            TString indexData = buildChunkData(chunks, pos, end, batchRecords);
            result.emplace_back(std::make_shared<NChunks::TPortionIndexChunk>(
                TChunkAddress(GetIndexId(), chunkIdx++), batchRecords, indexData.size(), indexData));
            pos = end;
        }
        return result;
    };

    if (!useOldSizing) {
        const auto foldAndSerialize = [&](TArrayPower2BitsStorage&& maxStorage) {
            const ui64 setBitsCount = maxStorage.CountSetBits();

            const double m = static_cast<double>(MaxBitsSize);
            const double k = static_cast<double>(hashesCount);
            const double ratio = static_cast<double>(setBitsCount) / m;
            const double estimatedUniqueCount = (ratio >= 1.0) ? m / k : std::max(10.0, -(m / k) * std::log(1.0 - ratio));

            const double requestedBitsSizeDouble =
                std::ceil((-k * estimatedUniqueCount) / std::log(1.0 - std::pow(falsePositiveProbability, 1.0 / k)));
            const ui64 requestedBitsSize = std::max<ui64>(BitsPerUi64, static_cast<ui64>(requestedBitsSizeDouble));
            const ui32 targetSize = std::min<ui64>(MaxBitsSize, std::bit_ceil(requestedBitsSize));

            auto foldedStorage = targetSize < MaxBitsSize ? maxStorage.Fold(MaxBitsSize / targetSize) : std::move(maxStorage);

            return GetBitsStorageConstructor()->SerializeToString(foldedStorage);
        };

        TArrayPower2BitsStorage maxStorage(MaxBitsSize);
        VisitAllChunksWithBuilder(reader, GetDataExtractor(), ngramSize, builder, maxStorage);
        TString indexData = foldAndSerialize(std::move(maxStorage));
        if (!chunkSizeLimit || indexData.size() <= *chunkSizeLimit) {
            return { std::make_shared<NChunks::TPortionIndexChunk>(TChunkAddress(GetIndexId(), 0), recordsCount, indexData.size(), indexData) };
        }

        const ui32 partsCount = (indexData.size() + *chunkSizeLimit - 1) / *chunkSizeLimit;
        const ui32 maxRecordsPerChunk = (recordsCount + partsCount - 1) / partsCount;
        return buildBatched(maxRecordsPerChunk, [&](const auto& chunks, const ui32 begin, const ui32 end, const ui32 /*batchRecords*/) {
            TArrayPower2BitsStorage batchStorage(MaxBitsSize);
            for (ui32 i = begin; i < end; ++i) {
                VisitChunkWithBuilder(chunks[i].first, GetDataExtractor(), ngramSize, builder, batchStorage);
            }
            return foldAndSerialize(std::move(batchStorage));
        });
    }

    const auto calcBitsSize = [&](const ui32 records) {
        ui32 size = filterSizeBytes * 8;
        if ((size & (size - 1)) == 0) {
            ui32 recordsCountBase = resolvedRecordsCount;
            while (recordsCountBase < records && size * 2 <= TConstants::MaxFilterSizeBytes) {
                size <<= 1;
                recordsCountBase *= 2;
            }
        } else {
            size = std::bit_ceil(size * ((records + resolvedRecordsCount - 1) / resolvedRecordsCount));
        }
        return std::max<ui32>(16, size);
    };

    if (chunkSizeLimit && calcBitsSize(recordsCount) / 8 > *chunkSizeLimit && calcBitsSize(1) / 8 <= *chunkSizeLimit) {
        ui32 maxRecordsPerChunk = 1;
        while (maxRecordsPerChunk < recordsCount && calcBitsSize(maxRecordsPerChunk * 2) / 8 <= *chunkSizeLimit) {
            maxRecordsPerChunk *= 2;
        }
        return buildBatched(maxRecordsPerChunk, [&](const auto& chunks, const ui32 begin, const ui32 end, const ui32 batchRecords) {
            TArrayPower2BitsStorage batchStorage(calcBitsSize(batchRecords));
            for (ui32 i = begin; i < end; ++i) {
                VisitChunkWithBuilder(chunks[i].first, GetDataExtractor(), ngramSize, builder, batchStorage);
            }
            return GetBitsStorageConstructor()->SerializeToString(batchStorage);
        });
    }

    TArrayPower2BitsStorage storage(calcBitsSize(recordsCount));
    VisitAllChunksWithBuilder(reader, GetDataExtractor(), ngramSize, builder, storage);

    TString indexData = GetBitsStorageConstructor()->SerializeToString(storage);
    return { std::make_shared<NChunks::TPortionIndexChunk>(TChunkAddress(GetIndexId(), 0), recordsCount, indexData.size(), indexData) };
}

bool TIndexMeta::DoCheckValueImpl(const IBitsStorageViewer& data, const std::optional<ui64> category,
    const std::shared_ptr<arrow::Scalar>& value, const NArrow::NSSA::TIndexCheckOperation& op, const TIndexInfo&) const {
    const ui32 hashesCount = Request.ResolvedHashesCount();
    const bool caseSensitive = Request.ResolvedCaseSensitive();
    const ui32 ngramSize = Request.ResolvedNGrammSize();
    AFL_VERIFY(!category);
    AFL_VERIFY(value->type->id() == arrow::utf8()->id() || value->type->id() == arrow::binary()->id())("id", value->type->ToString());
    bool result = true;
    const ui32 bitsCount = data.GetBitsCount();
    const auto predSet = [&](const ui64 hashSecondary) {
        if (!data.Get(hashSecondary % bitsCount)) {
            result = false;
        }
    };

    TNGrammBuilder builder(hashesCount, caseSensitive);
    AFL_VERIFY(!caseSensitive || op.GetCaseSensitive());

    NRequest::TLikePart::EOperation opLike;
    switch (op.GetOperation()) {
        case TSkipIndex::EOperation::Equals:
            opLike = NRequest::TLikePart::EOperation::Equals;
            break;
        case TSkipIndex::EOperation::Contains:
            opLike = NRequest::TLikePart::EOperation::Contains;
            break;
        case TSkipIndex::EOperation::StartsWith:
            opLike = NRequest::TLikePart::EOperation::StartsWith;
            break;
        case TSkipIndex::EOperation::EndsWith:
            opLike = NRequest::TLikePart::EOperation::EndsWith;
            break;
        default:
            AFL_VERIFY(false);
    }
    auto strVal = std::static_pointer_cast<arrow::BinaryScalar>(value);
    const TString valString((const char*)strVal->value->data(), strVal->value->size());
    builder.FillNGrammHashes(ngramSize, opLike, valString, predSet);
    return result;
}

}   // namespace NKikimr::NOlap::NIndexes::NBloomNGramm
