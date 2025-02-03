#pragma once

#include <util/generic/string.h>

#include <memory>
#include <vector>
#include <cstddef>

namespace NKikimr::NPQ {

struct TBlobKeyToken {
    TString Key;
};

using TBlobKeyTokenPtr = std::shared_ptr<TBlobKeyToken>;

struct TBlobKeyTokens {
    void Append(TBlobKeyTokenPtr token) { Tokens.push_back(std::move(token)); }
    size_t Size() const { return Tokens.size(); }

    std::vector<TBlobKeyTokenPtr> Tokens;
};

}
