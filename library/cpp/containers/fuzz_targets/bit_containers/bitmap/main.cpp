#include <util/generic/bitmap.h>
#include <util/generic/yexception.h>
#include <util/stream/str.h>
#include <util/system/types.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <algorithm>
#include <vector>

namespace {

class TBitModel {
public:
    explicit TBitModel(size_t size)
        : Bits_(size)
    {
    }

    size_t Size() const {
        return Bits_.size();
    }

    bool Get(size_t pos) const {
        return pos < Bits_.size() && Bits_[pos];
    }

    void Set(size_t pos, bool value = true) {
        if (pos < Bits_.size()) {
            Bits_[pos] = value;
        }
    }

    void Reset(size_t pos) {
        Set(pos, false);
    }

    void Flip(size_t pos) {
        if (pos < Bits_.size()) {
            Bits_[pos] = !Bits_[pos];
        }
    }

    void SetRange(size_t start, size_t end) {
        for (size_t i = start; i < end && i < Bits_.size(); ++i) {
            Bits_[i] = true;
        }
    }

    void ResetRange(size_t start, size_t end) {
        for (size_t i = start; i < end && i < Bits_.size(); ++i) {
            Bits_[i] = false;
        }
    }

    void Clear() {
        std::fill(Bits_.begin(), Bits_.end(), false);
    }

    void Flip() {
        for (size_t i = 0; i < Bits_.size(); ++i) {
            Bits_[i] = !Bits_[i];
        }
    }

    void And(const TBitModel& other) {
        for (size_t i = 0; i < Bits_.size(); ++i) {
            Bits_[i] = Bits_[i] && other.Get(i);
        }
    }

    void Or(const TBitModel& other) {
        for (size_t i = 0; i < Bits_.size(); ++i) {
            Bits_[i] = Bits_[i] || other.Get(i);
        }
    }

    void Xor(const TBitModel& other) {
        for (size_t i = 0; i < Bits_.size(); ++i) {
            Bits_[i] = Bits_[i] != other.Get(i);
        }
    }

    void SetDifference(const TBitModel& other) {
        for (size_t i = 0; i < Bits_.size(); ++i) {
            Bits_[i] = Bits_[i] && !other.Get(i);
        }
    }

    void OrShifted(const TBitModel& other, size_t offset) {
        for (size_t i = 0; i + offset < Bits_.size(); ++i) {
            if (other.Get(i)) {
                Bits_[i + offset] = true;
            }
        }
    }

    void LShift(size_t shift) {
        auto old = Bits_;
        for (size_t i = Bits_.size(); i > 0; --i) {
            const size_t pos = i - 1;
            Bits_[pos] = pos >= shift && old[pos - shift];
        }
    }

    void RShift(size_t shift) {
        auto old = Bits_;
        for (size_t i = 0; i < Bits_.size(); ++i) {
            Bits_[i] = i + shift < old.size() && old[i + shift];
        }
    }

    bool Pop() {
        const bool value = Get(0);
        RShift(1);
        return value;
    }

    void Push(bool value) {
        LShift(1);
        Set(0, value);
    }

    size_t Count() const {
        size_t result = 0;
        for (bool bit : Bits_) {
            result += bit;
        }
        return result;
    }

    bool Empty() const {
        return Count() == 0;
    }

    size_t First() const {
        for (size_t i = 0; i < Bits_.size(); ++i) {
            if (Bits_[i]) {
                return i;
            }
        }
        return Bits_.size();
    }

    size_t Next(size_t pos) const {
        for (size_t i = pos + 1; i < Bits_.size(); ++i) {
            if (Bits_[i]) {
                return i;
            }
        }
        return Bits_.size();
    }

    size_t HighestValueBitCount() const {
        for (size_t i = Bits_.size(); i > 0; --i) {
            if (Bits_[i - 1]) {
                return i;
            }
        }
        return 0;
    }

    ui64 Export64(size_t pos) const {
        ui64 result = 0;
        for (size_t bit = 0; bit < 64 && pos + bit < Bits_.size(); ++bit) {
            if (Bits_[pos + bit]) {
                result |= ui64(1) << bit;
            }
        }
        return result;
    }

private:
    std::vector<bool> Bits_;
};

template <class TBitmap>
void CheckBitmap(const TBitmap& bitmap, const TBitModel& model) {
    Y_ABORT_UNLESS(bitmap.Count() == model.Count());
    Y_ABORT_UNLESS(bitmap.Empty() == model.Empty());
    Y_ABORT_UNLESS(bitmap.FirstNonZeroBit() == (model.Empty() ? bitmap.Size() : model.First()));

    for (size_t i = 0; i < bitmap.Size(); ++i) {
        Y_ABORT_UNLESS(bitmap.Get(i) == model.Get(i));
        Y_ABORT_UNLESS(bitmap.Test(i) == model.Get(i));
    }
    for (size_t i = bitmap.Size(); i < model.Size(); ++i) {
        Y_ABORT_UNLESS(!model.Get(i));
    }

    size_t iterated = 0;
    for (size_t pos = bitmap.FirstNonZeroBit(); pos != bitmap.Size(); pos = bitmap.NextNonZeroBit(pos)) {
        Y_ABORT_UNLESS(pos < model.Size());
        Y_ABORT_UNLESS(model.Get(pos));
        ++iterated;
    }
    Y_ABORT_UNLESS(iterated == model.Count());

    const size_t exportPos = model.Size() > 1 ? model.First() % model.Size() : 0;
    ui64 exported = 0;
    bitmap.Export(exportPos, exported);
    Y_ABORT_UNLESS(exported == model.Export64(exportPos));

    TStringStream stream;
    bitmap.Save(&stream);
    TBitmap loaded;
    loaded.Load(&stream);
    Y_ABORT_UNLESS(loaded == bitmap);
    Y_ABORT_UNLESS(loaded.Hash() == bitmap.Hash());
}

template <class TBitmap>
void MutateBit(TBitmap& bitmap, TBitModel& model, FuzzedDataProvider& fdp, size_t maxBit) {
    const size_t pos = fdp.ConsumeIntegralInRange<size_t>(0, maxBit - 1);
    switch (fdp.ConsumeIntegralInRange<ui8>(0, 5)) {
        case 0:
            bitmap.Set(pos);
            model.Set(pos);
            break;
        case 1:
            bitmap.Reset(pos);
            model.Reset(pos);
            break;
        case 2:
            bitmap.Flip(pos);
            model.Flip(pos);
            break;
        case 3: {
            const bool value = fdp.ConsumeBool();
            bitmap[pos] = value;
            model.Set(pos, value);
            break;
        }
        case 4:
            Y_ABORT_UNLESS(bitmap.Get(pos) == model.Get(pos));
            break;
        case 5:
            Y_ABORT_UNLESS(bitmap.Test(pos) == model.Get(pos));
            break;
    }
}

template <class TBitmap>
void MutateRange(TBitmap& bitmap, TBitModel& model, FuzzedDataProvider& fdp, size_t maxBit, bool set) {
    size_t start = fdp.ConsumeIntegralInRange<size_t>(0, maxBit);
    size_t end = fdp.ConsumeIntegralInRange<size_t>(0, maxBit);
    if (start > end) {
        DoSwap(start, end);
    }
    if (set) {
        bitmap.Set(start, end);
        model.SetRange(start, end);
    } else {
        bitmap.Reset(start, end);
        model.ResetRange(start, end);
    }
}

template <class TBitmap>
void FuzzBitmapType(const ui8* data, size_t size, size_t modelBits, size_t addressableBits, size_t maxShift) {
    FuzzedDataProvider fdp(data, size);
    TBitmap left;
    TBitmap right;
    TBitModel leftModel(modelBits);
    TBitModel rightModel(modelBits);

    for (size_t step = 0; step < 192 && fdp.remaining_bytes() > 0; ++step) {
        TBitmap& bitmap = fdp.ConsumeBool() ? left : right;
        TBitModel& model = (&bitmap == &left) ? leftModel : rightModel;
        TBitmap& other = (&bitmap == &left) ? right : left;
        TBitModel& otherModel = (&bitmap == &left) ? rightModel : leftModel;

        switch (fdp.ConsumeIntegralInRange<ui8>(0, 17)) {
            case 0:
                MutateBit(bitmap, model, fdp, addressableBits);
                break;
            case 1:
                MutateRange(bitmap, model, fdp, addressableBits, true);
                break;
            case 2:
                MutateRange(bitmap, model, fdp, addressableBits, false);
                break;
            case 3:
                bitmap.Clear();
                model.Clear();
                break;
            case 4:
                bitmap.Flip();
                model.Flip();
                break;
            case 5:
                bitmap.And(other);
                model.And(otherModel);
                break;
            case 6:
                bitmap.Or(other);
                model.Or(otherModel);
                break;
            case 7:
                bitmap.Xor(other);
                model.Xor(otherModel);
                break;
            case 8:
                bitmap.SetDifference(other);
                model.SetDifference(otherModel);
                break;
            case 9: {
                const size_t shift = fdp.ConsumeIntegralInRange<size_t>(0, maxShift);
                bitmap.LShift(shift);
                model.LShift(shift);
                break;
            }
            case 10: {
                const size_t shift = fdp.ConsumeIntegralInRange<size_t>(0, maxShift);
                bitmap.RShift(shift);
                model.RShift(shift);
                break;
            }
            case 11: {
                const size_t offset = fdp.ConsumeIntegralInRange<size_t>(0, maxShift);
                if (otherModel.HighestValueBitCount() + offset <= model.Size()) {
                    bitmap.Or(other, offset);
                    model.OrShifted(otherModel, offset);
                }
                break;
            }
            case 12: {
                const bool expected = model.Pop();
                Y_ABORT_UNLESS(bitmap.Pop() == expected);
                break;
            }
            case 13: {
                const bool value = fdp.ConsumeBool();
                bitmap.Push(value);
                model.Push(value);
                break;
            }
            case 14:
                Y_ABORT_UNLESS(bitmap.HasAny(other) == [&] {
                    for (size_t i = 0; i < model.Size(); ++i) {
                        if (model.Get(i) && otherModel.Get(i)) {
                            return true;
                        }
                    }
                    return false;
                }());
                break;
            case 15:
                Y_ABORT_UNLESS(bitmap.HasAll(other) == [&] {
                    for (size_t i = 0; i < otherModel.Size(); ++i) {
                        if (otherModel.Get(i) && !model.Get(i)) {
                            return false;
                        }
                    }
                    return true;
                }());
                break;
            case 16: {
                TBitmap copy(bitmap);
                TBitmap assigned;
                assigned = bitmap;
                Y_ABORT_UNLESS(copy == bitmap);
                Y_ABORT_UNLESS(assigned.Compare(bitmap) == 0);
                break;
            }
            case 17:
                CheckBitmap(bitmap, model);
                break;
        }

        CheckBitmap(left, leftModel);
        CheckBitmap(right, rightModel);
    }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const ui8* data, size_t size) {
    FuzzBitmapType<TBitMap<257, ui8>>(data, size, 257, 257, 320);
    FuzzBitmapType<TBitMap<257, ui16>>(data, size, 257, 257, 320);
    FuzzBitmapType<TBitMap<257, ui32>>(data, size, 257, 257, 320);
    FuzzBitmapType<TBitMap<257, ui64>>(data, size, 257, 257, 320);
    FuzzBitmapType<TDynBitMap>(data, size, 8192, 384, 32);
    return 0;
}
