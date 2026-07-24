#include <library/cpp/enumbitset/enumbitset.h>

#include <util/generic/string.h>
#include <util/generic/yexception.h>
#include <util/stream/str.h>
#include <util/system/types.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <algorithm>
#include <vector>

namespace {

enum EBitContainerFuzzEnum {
    EB_0 = 0,
    EB_1 = 1,
    EB_2 = 2,
    EB_17 = 17,
    EB_31 = 31,
    EB_32 = 32,
    EB_63 = 63,
    EB_64 = 64,
    EB_95 = 95,
    EB_END = 96,
    EB_OVERFLOW = 120,
};

using TSet = TEnumBitSet<EBitContainerFuzzEnum, EB_0, EB_END>;
using TSfSet = TSfEnumBitSet<EBitContainerFuzzEnum, EB_0, EB_END>;

class TEnumModel {
public:
    TEnumModel()
        : Bits_(TSet::BitsetSize)
    {
    }

    static bool IsValid(int value) {
        return value >= TSet::BeginIndex && value < TSet::EndIndex;
    }

    bool Get(int value) const {
        return IsValid(value) && Bits_[value - TSet::BeginIndex];
    }

    void Set(int value, bool bit = true) {
        if (IsValid(value)) {
            Bits_[value - TSet::BeginIndex] = bit;
        }
    }

    void Reset(int value) {
        Set(value, false);
    }

    void Flip(int value) {
        if (IsValid(value)) {
            Bits_[value - TSet::BeginIndex] = !Bits_[value - TSet::BeginIndex];
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

    void And(const TEnumModel& other) {
        for (size_t i = 0; i < Bits_.size(); ++i) {
            Bits_[i] = Bits_[i] && other.Bits_[i];
        }
    }

    void Or(const TEnumModel& other) {
        for (size_t i = 0; i < Bits_.size(); ++i) {
            Bits_[i] = Bits_[i] || other.Bits_[i];
        }
    }

    void Xor(const TEnumModel& other) {
        for (size_t i = 0; i < Bits_.size(); ++i) {
            Bits_[i] = Bits_[i] != other.Bits_[i];
        }
    }

    size_t Count() const {
        size_t count = 0;
        for (bool bit : Bits_) {
            count += bit;
        }
        return count;
    }

    bool Empty() const {
        return Count() == 0;
    }

    ui64 Low() const {
        ui64 low = 0;
        for (size_t i = 0; i < std::min<size_t>(64, Bits_.size()); ++i) {
            if (Bits_[i]) {
                low |= ui64(1) << i;
            }
        }
        return low;
    }

private:
    std::vector<bool> Bits_;
};

EBitContainerFuzzEnum ConsumeEnum(FuzzedDataProvider& fdp) {
    static constexpr int MinEnum = -8;
    static constexpr int MaxEnum = int(EB_OVERFLOW) + 8;
    return static_cast<EBitContainerFuzzEnum>(fdp.ConsumeIntegralInRange<int>(MinEnum, MaxEnum));
}

EBitContainerFuzzEnum ConsumeValidEnum(FuzzedDataProvider& fdp) {
    return static_cast<EBitContainerFuzzEnum>(fdp.ConsumeIntegralInRange<int>(TSet::BeginIndex, TSet::EndIndex - 1));
}

TString ConsumeSmallString(FuzzedDataProvider& fdp) {
    static constexpr char Alphabet[] = "0123456789abcdefABCDEFxyz-+";
    TString result;
    const size_t len = fdp.ConsumeIntegralInRange<size_t>(0, 40);
    for (size_t i = 0; i < len; ++i) {
        result.push_back(Alphabet[fdp.ConsumeIntegralInRange<size_t>(0, sizeof(Alphabet) - 2)]);
    }
    return result;
}

void CheckSet(const TSet& set, const TEnumModel& model) {
    Y_ABORT_UNLESS(set.Count() == model.Count());
    Y_ABORT_UNLESS(set.Empty() == model.Empty());
    Y_ABORT_UNLESS(bool(set) == !model.Empty());
    Y_ABORT_UNLESS(set.Low() == model.Low());

    size_t iterated = 0;
    TSet rebuilt;
    for (auto value : set) {
        Y_ABORT_UNLESS(TSet::IsValid(value));
        Y_ABORT_UNLESS(model.Get(value));
        rebuilt.Set(value);
        ++iterated;
    }
    Y_ABORT_UNLESS(iterated == model.Count());
    Y_ABORT_UNLESS(rebuilt == set);

    for (int value = TSet::BeginIndex; value < TSet::EndIndex; ++value) {
        const auto e = static_cast<EBitContainerFuzzEnum>(value);
        Y_ABORT_UNLESS(set.Test(e) == model.Get(value));
        Y_ABORT_UNLESS(set.SafeTest(e) == model.Get(value));
        Y_ABORT_UNLESS(set[e] == model.Get(value));
    }
    Y_ABORT_UNLESS(!set.SafeTest(EB_OVERFLOW));

    TStringStream stream;
    set.Save(&stream);
    TSet loaded;
    loaded.Load(&stream);
    Y_ABORT_UNLESS(loaded == set);

    const TString encoded = set.ToString();
    TSet decoded;
    decoded.FromString(encoded);
    Y_ABORT_UNLESS(decoded == set);
    Y_ABORT_UNLESS(TSfSet::GetFromString(encoded) == TSfSet(set));
}

void FuzzEnumBitSet(const ui8* data, size_t size) {
    FuzzedDataProvider fdp(data, size);
    TSet left;
    TSet right;
    TEnumModel leftModel;
    TEnumModel rightModel;

    for (size_t step = 0; step < 192 && fdp.remaining_bytes() > 0; ++step) {
        TSet& set = fdp.ConsumeBool() ? left : right;
        TEnumModel& model = (&set == &left) ? leftModel : rightModel;
        TSet& other = (&set == &left) ? right : left;
        TEnumModel& otherModel = (&set == &left) ? rightModel : leftModel;

        switch (fdp.ConsumeIntegralInRange<ui8>(0, 15)) {
            case 0: {
                const auto e = ConsumeValidEnum(fdp);
                set.Set(e);
                model.Set(e);
                break;
            }
            case 1: {
                const auto e = ConsumeValidEnum(fdp);
                set.Reset(e);
                model.Reset(e);
                break;
            }
            case 2: {
                const auto e = ConsumeValidEnum(fdp);
                set.Flip(e);
                model.Flip(e);
                break;
            }
            case 3: {
                const auto e = ConsumeEnum(fdp);
                set.SafeSet(e);
                model.Set(e);
                break;
            }
            case 4: {
                const auto e = ConsumeEnum(fdp);
                set.SafeReset(e);
                model.Reset(e);
                break;
            }
            case 5: {
                const auto e = ConsumeEnum(fdp);
                set.SafeFlip(e);
                model.Flip(e);
                break;
            }
            case 6: {
                const auto e = ConsumeValidEnum(fdp);
                const bool bit = fdp.ConsumeBool();
                set.Set(e, bit);
                model.Set(e, bit);
                break;
            }
            case 7:
                set.Reset();
                model.Clear();
                break;
            case 8:
                set.Flip();
                model.Flip();
                break;
            case 9:
                set &= other;
                model.And(otherModel);
                break;
            case 10:
                set |= other;
                model.Or(otherModel);
                break;
            case 11:
                set ^= other;
                model.Xor(otherModel);
                break;
            case 12: {
                const auto e = ConsumeValidEnum(fdp);
                const bool bit = fdp.ConsumeBool();
                set[e] = bit;
                model.Set(e, bit);
                break;
            }
            case 13: {
                const auto e = ConsumeEnum(fdp);
                const TSet singleton = TSet::SafeConstruct(e);
                Y_ABORT_UNLESS(singleton.SafeTest(e) == TSet::IsValid(e));
                break;
            }
            case 14: {
                TSet parsed;
                (void)parsed.TryFromString(ConsumeSmallString(fdp));
                break;
            }
            case 15:
                Y_ABORT_UNLESS((set | other) == TSet(set).operator|=(other));
                Y_ABORT_UNLESS((set & other) == TSet(set).operator&=(other));
                Y_ABORT_UNLESS((set ^ other) == TSet(set).operator^=(other));
                break;
        }

        CheckSet(left, leftModel);
        CheckSet(right, rightModel);
    }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const ui8* data, size_t size) {
    FuzzEnumBitSet(data, size);
    return 0;
}
