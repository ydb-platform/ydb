#include "const.h"
#include "meta.h"

#include <ydb/core/formats/arrow/hash/calcer.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/bloom/bits_storage.h>
#include <ydb/core/tx/program/program.h>
#include <ydb/core/tx/schemeshard/olap/schema/schema.h>

#include <ydb/library/formats/arrow/hash/xx_hash.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_primitive.h>
#include <library/cpp/deprecated/atomic/atomic.h>
#include <util/generic/bitmap.h>

namespace NKikimr::NOlap::NIndexes::NBloomNGramm {

class TNGrammBuilder {
private:
    const ui32 HashesCount;

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
            if (!op || op == NRequest::TLikePart::EOperation::StartsWith || op == NRequest::TLikePart::EOperation::Equals) {
                for (ui32 c = 1; c <= CharsCount; ++c) {
                    fakeString.Clear();
                    fakeString.Fill('\0', CharsCount - c);
                    fakeString.Append((const char*)data, std::min((ui32)c, dataSize));
                    if (fakeString.size() < CharsCount) {
                        fakeString.Fill('\0', CharsCount - fakeString.size());
                    }
                    THashesCountSelector<HashesCount, CharsCount>::BuildHashes((const ui8*)fakeString.data(), actor);
                }
            }
            ui32 c = 0;
            for (; c + CharsCount <= dataSize; ++c) {
                THashesCountSelector<HashesCount, CharsCount>::BuildHashes(data + c, actor);
            }
            if (!op || op == NRequest::TLikePart::EOperation::EndsWith || op == NRequest::TLikePart::EOperation::Equals) {
                for (; c < dataSize; ++c) {
                    fakeString.Clear();
                    fakeString.Append((const char*)data + c, dataSize - c);
                    fakeString.Fill('\0', CharsCount - fakeString.size());
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
    TNGrammBuilder(const ui32 hashesCount)
        : HashesCount(hashesCount) {
    }

    template <class TAction>
    void BuildNGramms(
        const char* data, const ui32 dataSize, const std::optional<NRequest::TLikePart::EOperation> op, const ui32 nGrammSize, TAction& pred) {
        THashesSelector<TConstants::MaxHashesCount, TConstants::MaxNGrammSize>::BuildHashes(
            (const ui8*)data, dataSize, HashesCount, nGrammSize, op, pred);
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
        BuildNGramms(userReq.data(), userReq.size(), op, nGrammSize, fillData);
    }
};

class TVectorInserter {
private:
    TDynBitMap& Values;
    const ui32 Size;

public:
    TVectorInserter(TDynBitMap& values)
        : Values(values)
        , Size(values.Size()) {
        AFL_VERIFY(values.Size());
    }

    void operator()(const ui64 hash) {
        Values.Set(hash % Size);
    }
};

class TVectorInserterPower2 {
private:
    TDynBitMap& Values;
    const ui32 SizeMask;

public:
    TVectorInserterPower2(TDynBitMap& values)
        : Values(values)
        , SizeMask(values.Size() - 1) {
        AFL_VERIFY(values.Size());
    }

    void operator()(const ui64 hash) {
        Values.Set(hash & SizeMask);
    }
};

TString TIndexMeta::DoBuildIndexImpl(TChunkedBatchReader& reader, const ui32 recordsCount) const {
    AFL_VERIFY(reader.GetColumnsCount() == 1)("count", reader.GetColumnsCount());
    TNGrammBuilder builder(HashesCount);

    TDynBitMap bitMap;
    ui32 size = FilterSizeBytes * 8;
    if ((size & (size - 1)) == 0) {
        ui32 recordsCountBase = RecordsCount;
        while (recordsCountBase < recordsCount && size * 2 <= TConstants::MaxFilterSizeBytes) {
            size <<= 1;
            recordsCountBase *= 2;
        }
    } else {
        size *= ((recordsCount <= RecordsCount) ? 1.0 : (1.0 * recordsCount / RecordsCount));
    }
    bitMap.Reserve(size * 8);

    const auto doFillFilter = [&](auto& inserter) {
        for (reader.Start(); reader.IsCorrect();) {
            AFL_VERIFY(reader.GetColumnsCount() == 1);
            for (auto&& r : reader) {
                GetDataExtractor()->VisitAll(
                    r.GetCurrentChunk(),
                    [&](const std::shared_ptr<arrow::Array>& arr, const ui32 /*hashBase*/) {
                        builder.FillNGrammHashes(NGrammSize, arr, inserter);
                    },
                    [&](const std::string_view data, const ui32 /*hashBase*/) {
                        builder.BuildNGramms(data.data(), data.size(), {}, NGrammSize, inserter);
                    });
            }
            reader.ReadNext(reader.begin()->GetCurrentChunk()->GetRecordsCount());
        }
    };

    if ((size & (size - 1)) == 0) {
        TVectorInserterPower2 inserter(bitMap);
        doFillFilter(inserter);
    } else {
        TVectorInserter inserter(bitMap);
        doFillFilter(inserter);
    }
    return TFixStringBitsStorage(bitMap).GetData();
}

bool TIndexMeta::DoCheckValue(
    const TString& data, const std::optional<ui64> category, const std::shared_ptr<arrow::Scalar>& value, const EOperation op) const {
    TFixStringBitsStorage bits(data);
    AFL_VERIFY(!category);
    AFL_VERIFY(value->type->id() == arrow::utf8()->id() || value->type->id() == arrow::binary()->id())("id", value->type->ToString());
    bool result = true;
    const auto predSet = [&](const ui64 hashSecondary) {
        if (!bits.Get(hashSecondary % bits.GetSizeBits())) {
            result = false;
        }
    };
    TNGrammBuilder builder(HashesCount);

    NRequest::TLikePart::EOperation opLike;
    switch (op) {
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
    }
    auto strVal = std::static_pointer_cast<arrow::BinaryScalar>(value);
    const TString valString((const char*)strVal->value->data(), strVal->value->size());
    builder.FillNGrammHashes(NGrammSize, opLike, valString, predSet);
    return result;
}

}   // namespace NKikimr::NOlap::NIndexes::NBloomNGramm
