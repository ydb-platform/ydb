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
        AFL_VERIFY(array->type_id() == arrow::utf8()->id() || array->type_id() == arrow::binary()->id())("id", array->type()->ToString());
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

constexpr ui64 BitsPerUi64 = sizeof(ui64) * CHAR_BIT;
constexpr ui64 MaxBitsSize = static_cast<ui64>(TConstants::MaxFilterSizeBytes) * CHAR_BIT;

}   // namespace

std::vector<std::shared_ptr<NChunks::TPortionIndexChunk>> TIndexMeta::DoBuildIndexImpl(
    TChunkedBatchReader& reader, const ui32 recordsCount, const std::optional<ui64> chunkSizeLimit) const {
    AFL_VERIFY(reader.GetColumnsCount() == 1)("count", reader.GetColumnsCount());

    // The largest power-of-2 filter strictly below the storage blob limit: filters above it are either split
    // by record subranges or folded down to it (a smaller bloom filter stays correct, only its FPR grows).
    // The -1 stays strictly below limits that are itself a power of 2: serialization adds a small header, so
    // a filter of exactly limit * CHAR_BIT bits would overflow the blob.
    const ui64 clampBits =
        (chunkSizeLimit && *chunkSizeLimit > 0) ? std::max<ui64>(BitsPerUi64, std::bit_floor(*chunkSizeLimit * CHAR_BIT - 1)) : MaxBitsSize;

    if (Request.IsOldSizingMode()) {
        return BuildIndexOldSizing(reader, recordsCount, chunkSizeLimit, clampBits);
    }
    return BuildIndexNewSizing(reader, recordsCount, chunkSizeLimit, clampBits);
}

std::vector<std::shared_ptr<NChunks::TPortionIndexChunk>> TIndexMeta::BuildIndexNewSizing(
    TChunkedBatchReader& reader, const ui32 recordsCount, const std::optional<ui64> chunkSizeLimit, const ui64 clampBits) const {
    const ui32 hashesCount = Request.ResolvedHashesCount();
    const ui32 ngramSize = Request.ResolvedNGrammSize();
    const double falsePositiveProbability = Request.ResolvedFalsePositiveProbability();
    TNGrammBuilder builder(hashesCount, Request.ResolvedCaseSensitive());

    const auto foldAndSerialize = [&](TArrayPower2BitsStorage&& maxStorage, const ui64 maxTargetBits) {
        const ui64 setBitsCount = maxStorage.CountSetBits();

        const double m = static_cast<double>(MaxBitsSize);
        const double k = static_cast<double>(hashesCount);
        const double ratio = static_cast<double>(setBitsCount) / m;
        const double estimatedUniqueCount = (ratio >= 1.0) ? m / k : std::max(10.0, -(m / k) * std::log(1.0 - ratio));

        const double requestedBitsSizeDouble =
            std::ceil((-k * estimatedUniqueCount) / std::log(1.0 - std::pow(falsePositiveProbability, 1.0 / k)));
        const ui64 requestedBitsSize = std::max<ui64>(BitsPerUi64, static_cast<ui64>(requestedBitsSizeDouble));
        const ui64 targetSize = std::min<ui64>({ MaxBitsSize, maxTargetBits, std::bit_ceil(requestedBitsSize) });

        auto foldedStorage = targetSize < MaxBitsSize ? maxStorage.Fold(MaxBitsSize / targetSize) : std::move(maxStorage);

        return GetBitsStorageConstructor()->SerializeToString(foldedStorage);
    };

    // No size budget => splitting is impossible: hash everything into a single filter, skipping the
    // per-chunk storages below that only pay off when a split may follow.
    if (!chunkSizeLimit) {
        TArrayPower2BitsStorage maxStorage(MaxBitsSize);
        VisitAllChunksWithBuilder(reader, GetDataExtractor(), ngramSize, builder, maxStorage);
        TString indexData = foldAndSerialize(std::move(maxStorage), MaxBitsSize);
        return { std::make_shared<NChunks::TPortionIndexChunk>(TChunkAddress(GetIndexId(), 0), recordsCount, indexData.size(), indexData) };
    }

    // Hash every source chunk exactly once into its own max-size filter: the full-portion filter is the
    // bitwise union of the per-chunk filters, so deciding to split does not re-hash the data.
    const auto chunks = CollectChunks(reader);
    std::vector<TArrayPower2BitsStorage> chunkStorages;
    chunkStorages.reserve(chunks.size());
    for (const auto& chunk : chunks) {
        auto& storage = chunkStorages.emplace_back(MaxBitsSize);
        VisitChunkWithBuilder(chunk.first, GetDataExtractor(), ngramSize, builder, storage);
    }
    TArrayPower2BitsStorage maxStorage(MaxBitsSize);
    for (const auto& storage : chunkStorages) {
        maxStorage |= storage;
    }
    TString indexData = foldAndSerialize(std::move(maxStorage), MaxBitsSize);
    if (indexData.size() <= *chunkSizeLimit) {
        return { std::make_shared<NChunks::TPortionIndexChunk>(TChunkAddress(GetIndexId(), 0), recordsCount, indexData.size(), indexData) };
    }

    const ui32 partsCount = (indexData.size() + *chunkSizeLimit - 1) / *chunkSizeLimit;
    const ui32 maxRecordsPerChunk = (recordsCount + partsCount - 1) / partsCount;
    return BuildIndexChunksBatched(
        GetIndexId(), chunks, maxRecordsPerChunk, [&](const auto& /*chunks*/, const ui32 begin, const ui32 end, const ui32 /*batchRecords*/) {
            TArrayPower2BitsStorage batchStorage(MaxBitsSize);
            for (ui32 i = begin; i < end; ++i) {
                batchStorage |= chunkStorages[i];
            }
            return foldAndSerialize(std::move(batchStorage), clampBits);
        });
}

std::vector<std::shared_ptr<NChunks::TPortionIndexChunk>> TIndexMeta::BuildIndexOldSizing(
    TChunkedBatchReader& reader, const ui32 recordsCount, const std::optional<ui64> chunkSizeLimit, const ui64 clampBits) const {
    const ui32 ngramSize = Request.ResolvedNGrammSize();
    const ui32 filterSizeBytes = Request.ResolvedFilterSizeBytes();
    const ui32 resolvedRecordsCount = Request.ResolvedRecordsCount();
    TNGrammBuilder builder(Request.ResolvedHashesCount(), Request.ResolvedCaseSensitive());

    const auto calcBitsSize = [&](const ui32 records) {
        ui32 size = filterSizeBytes * CHAR_BIT;
        if ((size & (size - 1)) == 0) {
            ui32 recordsCountBase = resolvedRecordsCount;
            // TODO: the guard compares bits against MaxFilterSizeBytes (bytes) — pre-existing behaviour kept
            // as-is to keep this PR strictly flag-gated.
            while (recordsCountBase < records && size * 2 <= TConstants::MaxFilterSizeBytes) {
                size <<= 1;
                recordsCountBase *= 2;
            }
        } else {
            size = std::bit_ceil(size * ((records + resolvedRecordsCount - 1) / resolvedRecordsCount));
        }
        return std::max<ui32>(16, size);
    };

    if (chunkSizeLimit && calcBitsSize(recordsCount) > clampBits) {
        ui32 maxRecordsPerChunk = 1;
        while (maxRecordsPerChunk <= recordsCount / 2 && calcBitsSize(maxRecordsPerChunk * 2) <= clampBits) {
            maxRecordsPerChunk *= 2;
        }
        return BuildIndexChunksBatched(GetIndexId(), CollectChunks(reader), maxRecordsPerChunk,
            [&](const auto& chunks, const ui32 begin, const ui32 end, const ui32 batchRecords) {
                TArrayPower2BitsStorage batchStorage(std::min<ui64>(calcBitsSize(batchRecords), clampBits));
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
