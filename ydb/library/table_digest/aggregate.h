#pragma once

#include <util/generic/strbuf.h>
#include <util/generic/string.h>
#include <util/system/types.h>

#include <array>

namespace NYql::NDigest {

enum class EAlgorithm: ui8 {
    CityHash64 = 1,
    CityHash128 = 2,
    Sha256 = 3,
};

EAlgorithm ParseAlgorithm(TStringBuf name);
TStringBuf GetAlgorithmName(EAlgorithm algorithm);
size_t GetWordCount(EAlgorithm algorithm);

// Unsigned words, least significant first; unused words must be zero.
using TWords = std::array<ui64, 4>;

class TState {
public:
    static constexpr ui32 Version = 1;

    explicit TState(EAlgorithm algorithm);

    void AddHash(const TWords& hash);
    // Fixed-width big-endian representation, as returned by Digest::Sha256.
    void AddHash(TStringBuf bytes);
    void Merge(const TState& other);
    TString Serialize() const;
    static TState Deserialize(TStringBuf bytes);

    EAlgorithm GetAlgorithm() const {
        return Algorithm_;
    }
    ui64 GetCount() const {
        return Count_;
    }
    const TWords& GetHash() const {
        return Hash_;
    }
    TString GetHashBytes() const;

private:
    EAlgorithm Algorithm_;
    ui64 Count_ = 0;
    TWords Hash_{};
};

} // namespace NYql::NDigest
