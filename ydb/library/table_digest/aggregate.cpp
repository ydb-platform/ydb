#include "aggregate.h"

#include <util/generic/yexception.h>
#include <util/stream/mem.h>
#include <util/stream/str.h>
#include <util/system/byteorder.h>
#include <util/system/unaligned_mem.h>
#include <util/ysaveload.h>

#include <limits>

namespace NYql::NDigest {
namespace {

constexpr TStringBuf StateMagic = "YQLDG1T";

struct TAlgorithmInfo {
    EAlgorithm Algorithm;
    TStringBuf Name;
    size_t WordCount;
};

constexpr TAlgorithmInfo Algorithms[] = {
    {EAlgorithm::CityHash64, "CityHash64", 1},
    {EAlgorithm::CityHash128, "CityHash128", 2},
    {EAlgorithm::Sha256, "Sha256", 4},
};

const TAlgorithmInfo& GetAlgorithmInfo(EAlgorithm algorithm) {
    for (const auto& info : Algorithms) {
        if (info.Algorithm == algorithm) {
            return info;
        }
    }
    ythrow yexception() << "Invalid Digest algorithm identifier";
}

template <typename T>
T ReadInteger(TMemoryInput& input) {
    Y_ENSURE(input.Avail() >= sizeof(T), "Truncated Digest state");
    T value;
    Load(&input, value);
    return LittleToHost(value);
}

void AddWords(TWords& destination, const TWords& source, size_t wordCount) {
    ui64 carry = 0;
    for (size_t i = 0; i < wordCount; ++i) {
        const ui64 sum = destination[i] + source[i];
        const ui64 overflow = sum < destination[i];
        const ui64 result = sum + carry;
        carry = overflow | (result < sum);
        destination[i] = result;
    }
}

} // namespace

EAlgorithm ParseAlgorithm(TStringBuf name) {
    for (const auto& info : Algorithms) {
        if (info.Name == name) {
            return info.Algorithm;
        }
    }
    ythrow yexception() << "Unsupported Digest algorithm: " << name;
}

TStringBuf GetAlgorithmName(EAlgorithm algorithm) {
    return GetAlgorithmInfo(algorithm).Name;
}

size_t GetWordCount(EAlgorithm algorithm) {
    return GetAlgorithmInfo(algorithm).WordCount;
}

TState::TState(EAlgorithm algorithm)
    : Algorithm_(algorithm)
{
    GetWordCount(algorithm);
}

void TState::AddHash(const TWords& hash) {
    const size_t wordCount = GetWordCount(Algorithm_);
    for (size_t i = wordCount; i < hash.size(); ++i) {
        Y_ENSURE(!hash[i], "Digest hash exceeds algorithm width");
    }
    Y_ENSURE(Count_ < std::numeric_limits<ui64>::max(), "Digest row count overflow");
    AddWords(Hash_, hash, wordCount);
    ++Count_;
}

void TState::AddHash(TStringBuf bytes) {
    const size_t wordCount = GetWordCount(Algorithm_);
    Y_ENSURE(bytes.size() == wordCount * sizeof(ui64), "Invalid Digest hash byte length");
    TWords hash{};
    for (size_t i = 0; i < wordCount; ++i) {
        hash[i] = InetToHost(ReadUnaligned<ui64>(bytes.data() + (wordCount - 1 - i) * sizeof(ui64)));
    }
    AddHash(hash);
}

void TState::Merge(const TState& other) {
    Y_ENSURE(Algorithm_ == other.Algorithm_, "Digest algorithm mismatch");
    Y_ENSURE(other.Count_ <= std::numeric_limits<ui64>::max() - Count_, "Digest row count overflow");
    AddWords(Hash_, other.Hash_, GetWordCount(Algorithm_));
    Count_ += other.Count_;
}

TString TState::GetHashBytes() const {
    const size_t wordCount = GetWordCount(Algorithm_);
    TString result(wordCount * sizeof(ui64), '\0');
    char* bytes = result.Detach();
    for (size_t i = 0; i < wordCount; ++i) {
        WriteUnaligned<ui64>(bytes + (wordCount - 1 - i) * sizeof(ui64), HostToInet(Hash_[i]));
    }
    return result;
}

TString TState::Serialize() const {
    TString bytes(StateMagic);
    TStringOutput output(bytes);
    Save(&output, HostToLittle(Version));
    Save(&output, static_cast<ui8>(Algorithm_));
    Save(&output, HostToLittle(Count_));
    for (size_t i = 0; i < GetWordCount(Algorithm_); ++i) {
        Save(&output, HostToLittle(Hash_[i]));
    }
    return bytes;
}

TState TState::Deserialize(TStringBuf bytes) {
    Y_ENSURE(bytes.SkipPrefix(StateMagic), "Invalid Digest state signature");
    TMemoryInput input(bytes);
    Y_ENSURE(ReadInteger<ui32>(input) == Version, "Unsupported Digest state version");
    TState result(static_cast<EAlgorithm>(ReadInteger<ui8>(input)));
    result.Count_ = ReadInteger<ui64>(input);
    for (size_t i = 0; i < GetWordCount(result.Algorithm_); ++i) {
        result.Hash_[i] = ReadInteger<ui64>(input);
        Y_ENSURE(result.Count_ || !result.Hash_[i], "Nonzero Digest hash for empty state");
    }
    Y_ENSURE(!input.Avail(), "Trailing Digest state bytes");
    return result;
}

} // namespace NYql::NDigest
