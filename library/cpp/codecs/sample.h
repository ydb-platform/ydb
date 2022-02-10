#pragma once

#include <library/cpp/deprecated/accessors/accessors.h>

#include <util/generic/buffer.h>
#include <util/generic/vector.h>
#include <util/random/fast.h>
#include <util/random/shuffle.h>

#include <functional>
#include <type_traits>

namespace NCodecs {
    class ISequenceReader {
    public:
        virtual bool NextRegion(TStringBuf& s) = 0;

        virtual ~ISequenceReader() = default;
    };

    template <class TValue>
    TStringBuf ValueToStringBuf(TValue&& t) {
        return TStringBuf{NAccessors::Begin(t), NAccessors::End(t)};
    }

    template <class TIter>
    TStringBuf IterToStringBuf(TIter iter) {
        return ValueToStringBuf(*iter);
    }

    template <class TItem>
    class TSimpleSequenceReader: public ISequenceReader {
        const TVector<TItem>& Items;
        size_t Idx = 0;

    public:
        TSimpleSequenceReader(const TVector<TItem>& items)
            : Items(items)
        {
        }

        bool NextRegion(TStringBuf& s) override {
            if (Idx >= Items.size()) {
                return false;
            }

            s = ValueToStringBuf(Items[Idx++]);
            return true;
        }
    };

    template <class TIter, class TGetter>
    size_t GetInputSize(TIter begin, TIter end, TGetter getter) {
        size_t totalBytes = 0;
        for (TIter iter = begin; iter != end; ++iter) {
            totalBytes += getter(iter).size();
        }
        return totalBytes;
    }

    template <class TIter>
    size_t GetInputSize(TIter begin, TIter end) {
        return GetInputSize(begin, end, IterToStringBuf<TIter>);
    }

    template <class TIter, class TGetter>
    TVector<TBuffer> GetSample(TIter begin, TIter end, size_t sampleSizeBytes, TGetter getter) {
        TFastRng64 rng{0x1ce1f2e507541a05, 0x07d45659, 0x7b8771030dd9917e, 0x2d6636ce};

        size_t totalBytes = GetInputSize(begin, end, getter);
        double sampleProb = (double)sampleSizeBytes / Max<size_t>(1, totalBytes);

        TVector<TBuffer> result;
        for (TIter iter = begin; iter != end; ++iter) {
            if (sampleProb >= 1 || rng.GenRandReal1() < sampleProb) {
                TStringBuf reg = getter(iter);
                result.emplace_back(reg.data(), reg.size());
            }
        }
        Shuffle(result.begin(), result.end(), rng);
        return result;
    }

    template <class TIter>
    TVector<TBuffer> GetSample(TIter begin, TIter end, size_t sampleSizeBytes) {
        return GetSample(begin, end, sampleSizeBytes, IterToStringBuf<TIter>);
    }

}
