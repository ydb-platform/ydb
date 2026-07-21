#include <library/cpp/containers/bitseq/bitvector.h>
#include <library/cpp/containers/bitseq/readonly_bitvector.h>

#include <util/generic/yexception.h>
#include <util/memory/blob.h>
#include <util/stream/buffer.h>
#include <util/stream/str.h>
#include <util/system/types.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <algorithm>
#include <vector>

namespace {

class TBitVectorModel {
public:
    explicit TBitVectorModel(size_t maxBits)
        : MaxBits_(maxBits)
    {
    }

    size_t Size() const {
        return Bits_.size();
    }

    bool GetBit(size_t pos) const {
        return pos < Bits_.size() && Bits_[pos];
    }

    void Clear() {
        Bits_.clear();
    }

    void Resize(size_t size) {
        Bits_.resize(std::min(size, MaxBits_));
    }

    void SetBit(size_t pos) {
        if (pos < Bits_.size()) {
            Bits_[pos] = true;
        }
    }

    void ResetBit(size_t pos) {
        if (pos < Bits_.size()) {
            Bits_[pos] = false;
        }
    }

    template <class TWord>
    TWord Get(size_t pos, ui8 width) const {
        TWord result = 0;
        for (ui8 bit = 0; bit < width && pos + bit < Bits_.size(); ++bit) {
            if (Bits_[pos + bit]) {
                result |= TWord(1) << bit;
            }
        }
        return result;
    }

    template <class TWord>
    void Set(size_t pos, TWord value, ui8 width) {
        for (ui8 bit = 0; bit < width && pos + bit < Bits_.size(); ++bit) {
            Bits_[pos + bit] = (value >> bit) & TWord(1);
        }
    }

    template <class TWord>
    void Append(TWord value, ui8 width) {
        const size_t oldSize = Bits_.size();
        Bits_.resize(std::min(MaxBits_, oldSize + size_t(width)), false);
        Set(oldSize, value, static_cast<ui8>(Bits_.size() - oldSize));
    }

    size_t Count() const {
        size_t count = 0;
        for (bool bit : Bits_) {
            count += bit;
        }
        return count;
    }

private:
    const size_t MaxBits_;
    std::vector<bool> Bits_;
};

template <class TWord>
void CheckReadonly(const TBitVector<TWord>& vector, const TBitVectorModel& model) {
    TBufferStream out;
    TReadonlyBitVector<TWord>::SaveForReadonlyAccess(&out, vector);
    const TBlob blob = TBlob::FromBuffer(out.Buffer());

    TReadonlyBitVector<TWord> readonly;
    const TBlob rest = readonly.LoadFromBlob(blob);
    Y_ABORT_UNLESS(rest.Size() == 0);
    Y_ABORT_UNLESS(readonly.Size() == model.Size());

    for (size_t pos = 0; pos < model.Size(); ++pos) {
        Y_ABORT_UNLESS(readonly.Test(pos) == model.GetBit(pos));
    }
    if (model.Size() > 0) {
        const ui8 maxWidth = std::min<ui8>(TBitSeqTraits<TWord>::NumBits, 16);
        const ui8 width = std::min<ui8>(maxWidth, static_cast<ui8>(model.Size()));
        const size_t pos = model.Size() - width;
        Y_ABORT_UNLESS(readonly.Get(pos, width) == model.template Get<TWord>(pos, width));
    }
}

template <class TWord>
void CheckVector(const TBitVector<TWord>& vector, const TBitVectorModel& model) {
    Y_ABORT_UNLESS(vector.Size() == model.Size());
    Y_ABORT_UNLESS(vector.Words() == TBitSeqTraits<TWord>::NumOfWords(model.Size()));
    Y_ABORT_UNLESS(vector.Count() == model.Count());

    for (size_t pos = 0; pos < model.Size(); ++pos) {
        Y_ABORT_UNLESS(vector.Test(pos) == model.GetBit(pos));
    }

    TStringStream stream;
    vector.Save(&stream);
    TBitVector<TWord> loaded;
    loaded.Load(&stream);
    Y_ABORT_UNLESS(loaded.Size() == vector.Size());
    Y_ABORT_UNLESS(loaded.Words() == vector.Words());
    Y_ABORT_UNLESS(loaded.Count() == vector.Count());
    for (size_t pos = 0; pos < model.Size(); ++pos) {
        Y_ABORT_UNLESS(loaded.Test(pos) == model.GetBit(pos));
    }

    CheckReadonly(vector, model);
}

template <class TWord>
void FuzzBitVectorType(const ui8* data, size_t size) {
    constexpr size_t MaxBits = 512;
    FuzzedDataProvider fdp(data, size);
    TBitVector<TWord> vector;
    TBitVectorModel model(MaxBits);

    for (size_t step = 0; step < 192 && fdp.remaining_bytes() > 0; ++step) {
        switch (fdp.ConsumeIntegralInRange<ui8>(0, 9)) {
            case 0: {
                const size_t newSize = fdp.ConsumeIntegralInRange<size_t>(0, MaxBits);
                vector.Resize(newSize);
                model.Resize(newSize);
                break;
            }
            case 1:
                vector.Clear();
                model.Clear();
                break;
            case 2:
                if (model.Size() > 0) {
                    const size_t pos = fdp.ConsumeIntegralInRange<size_t>(0, model.Size() - 1);
                    vector.Set(pos);
                    model.SetBit(pos);
                }
                break;
            case 3:
                if (model.Size() > 0) {
                    const size_t pos = fdp.ConsumeIntegralInRange<size_t>(0, model.Size() - 1);
                    vector.Reset(pos);
                    model.ResetBit(pos);
                }
                break;
            case 4: {
                const ui8 width = fdp.ConsumeIntegralInRange<ui8>(0, TBitSeqTraits<TWord>::NumBits);
                if (model.Size() + width <= MaxBits) {
                    const TWord value = fdp.ConsumeIntegral<TWord>();
                    vector.Append(value, width);
                    model.Append(value, width);
                }
                break;
            }
            case 5:
                if (model.Size() > 0) {
                    const ui8 width = fdp.ConsumeIntegralInRange<ui8>(0, TBitSeqTraits<TWord>::NumBits);
                    if (width <= model.Size()) {
                        const size_t pos = fdp.ConsumeIntegralInRange<size_t>(0, model.Size() - width);
                        const TWord value = fdp.ConsumeIntegral<TWord>();
                        vector.Set(pos, value, width);
                        model.Set(pos, value, width);
                    }
                }
                break;
            case 6:
                if (model.Size() > 0) {
                    const size_t pos = fdp.ConsumeIntegralInRange<size_t>(0, model.Size() - 1);
                    Y_ABORT_UNLESS(vector.Test(pos) == model.GetBit(pos));
                }
                break;
            case 7:
                if (model.Size() > 0) {
                    const ui8 width = fdp.ConsumeIntegralInRange<ui8>(0, TBitSeqTraits<TWord>::NumBits);
                    if (width <= model.Size()) {
                        const size_t pos = fdp.ConsumeIntegralInRange<size_t>(0, model.Size() - width);
                        Y_ABORT_UNLESS(vector.Get(pos, width) == model.template Get<TWord>(pos, width));
                    }
                }
                break;
            case 8: {
                TBitVector<TWord> other(vector);
                other.Swap(vector);
                other.Swap(vector);
                break;
            }
            case 9:
                CheckVector(vector, model);
                break;
        }

        CheckVector(vector, model);
    }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const ui8* data, size_t size) {
    FuzzBitVectorType<ui8>(data, size);
    FuzzBitVectorType<ui32>(data, size);
    FuzzBitVectorType<ui64>(data, size);
    return 0;
}
