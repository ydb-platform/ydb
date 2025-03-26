#include "const.h"
#include "meta.h"

#include <ydb/core/formats/arrow/hash/calcer.h>
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
    TDynBitMap Values;
    const ui32 Size;

public:
    TDynBitMap ExtractBits() {
        return std::move(Values);
    }

    TVectorInserter(const ui32 bitsSize)
        : Size(bitsSize) {
        AFL_VERIFY(bitsSize);
        Values.Reserve(bitsSize);
    }

    void operator()(const ui64 hash) {
        Values.Set(hash % Size);
    }
};

template <ui64 BitsSize>
class TVectorInserterPower2 {
private:
    TBitMapOps<TFixedBitMapTraits<BitsSize, ui64>> Values;
    static constexpr ui32 SizeMask = BitsSize - 1;
    static_assert(((BitsSize - 1) & BitsSize) == 0);

public:
    TBitMapOps<TFixedBitMapTraits<BitsSize, ui64>> ExtractBits() {
        return std::move(Values);
    }

    void operator()(const ui64 hash) {
        Values.Set(hash & SizeMask);
    }
};

namespace {

template <ui64 Size>
class TBitmapDetector {
private:
    const TSkipBitmapIndex* Meta;
    const ui32 ExtSize;
    static constexpr ui64 NextSize = Size >> 1;

public:
    TBitmapDetector(const TSkipBitmapIndex* meta, const ui32 size)
        : Meta(meta)
        , ExtSize(size) {
        AFL_VERIFY(ExtSize <= Size);
    }

    template <class TFiller>
    TString Detector(const TFiller& filler) const {
        if (ExtSize == Size) {
            TVectorInserterPower2<Size> inserter;
            filler(inserter);
            return Meta->GetBitsStorageConstructor()->Build(inserter.ExtractBits())->SerializeToString();
        } else {
            return TBitmapDetector<NextSize>(Meta, ExtSize).Detector(filler);
        }
    }
};

template <>
class TBitmapDetector<0> {
public:
    TBitmapDetector(const TSkipBitmapIndex* /*meta*/, const ui32 /*size*/) {
        AFL_VERIFY(false);
    }

    template <class TFiller>
    static TString Detector(const TFiller& /*filler*/) {
        AFL_VERIFY(false);
        return "";
    }
};
}   // namespace

TString TIndexMeta::DoBuildIndexImpl(TChunkedBatchReader& reader, const ui32 recordsCount) const {
    AFL_VERIFY(reader.GetColumnsCount() == 1)("count", reader.GetColumnsCount());
    TNGrammBuilder builder(HashesCount);

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
    size = std::max<ui32>(16, size);
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
        if (size == 1024) {
            return TBitmapDetector<1024>(this, 1024).Detector(doFillFilter);
        } else if (size == 2048) {
            return TBitmapDetector<2048>(this, 2048).Detector(doFillFilter);
        } else if (size == 4096) {
            return TBitmapDetector<4096>(this, 4096).Detector(doFillFilter);
        } else if (size == 4096 * 2) {
            return TBitmapDetector<4096 * 2>(this, 4096 * 2).Detector(doFillFilter);
        } else if (size == 4096 * 4) {
            return TBitmapDetector<4096 * 4>(this, 4096 * 4).Detector(doFillFilter);
        } else if (size == 4096 * 8) {
            return TBitmapDetector<4096 * 8>(this, 4096 * 8).Detector(doFillFilter);
        } else if (size == 4096 * 16) {
            return TBitmapDetector<4096 * 16>(this, 4096 * 16).Detector(doFillFilter);
        }
    }
    TVectorInserter inserter(size);
    doFillFilter(inserter);
    return GetBitsStorageConstructor()->Build(inserter.ExtractBits())->SerializeToString();
}

bool TIndexMeta::DoCheckValueImpl(
    const IBitsStorage& data, const std::optional<ui64> category, const std::shared_ptr<arrow::Scalar>& value, const EOperation op) const {
    AFL_VERIFY(!category);
    AFL_VERIFY(value->type->id() == arrow::utf8()->id() || value->type->id() == arrow::binary()->id())("id", value->type->ToString());
    bool result = true;
    const ui32 bitsCount = data.GetBitsCount();
    const auto predSet = [&](const ui64 hashSecondary) {
        if (!data.Get(hashSecondary % bitsCount)) {
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
