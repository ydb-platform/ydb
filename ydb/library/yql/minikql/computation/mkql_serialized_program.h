#pragma once

#include <util/generic/string.h>
#include <util/generic/hash.h>

namespace NKikimr::NMiniKQL {

/// Serialized program with precomputed hash
class TSerializedProgram
{
public:
    TSerializedProgram(TString data)
        : Data(std::move(data))
        , Hash(THash<TString>()(data))
    {}

    const TString & GetData() const
    {
        return Data;
    }

    uint64_t GetHash() const
    {
        return Hash;
    }

    friend bool operator==(const TSerializedProgram & lhs, const TSerializedProgram & rhs)
    {
        return lhs.Hash == rhs.Hash && lhs.Data == rhs.Data;
    }

    friend bool operator!=(const TSerializedProgram & lhs, const TSerializedProgram & rhs)
    {
        return !(lhs == rhs);
    }

private:
    TString Data;
    ui64 Hash;
};

}

template<>
struct THash<NKikimr::NMiniKQL::TSerializedProgram> {
    inline ui64 operator()(const NKikimr::NMiniKQL::TSerializedProgram& serializedProgram) const noexcept {
        return serializedProgram.GetHash();
    }
};
