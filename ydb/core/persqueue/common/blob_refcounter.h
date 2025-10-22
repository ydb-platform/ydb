#pragma once

#include <util/generic/string.h>

#include <memory>
#include <vector>
#include <cstddef>

namespace NKikimr::NPQ {

// Stores the block key. `std::shared_ptr` is used for reference counting.  To create a token, the constructor
// method in `TPartition' is used. It creates a special destructor that places the key in the deletion queue
// before deleting the token.
struct TBlobKeyToken {
    TString Key;
    bool NeedDelete = true;
};

using TBlobKeyTokenPtr = std::shared_ptr<TBlobKeyToken>;
using TBlobKeyTokenCreator = std::function<TBlobKeyTokenPtr (const TString&)>;

// It is used for synchronization between deleting and reading blocks. You cannot delete the blocks that the client
// reads from. The keys are placed in the collection for the duration of the reading.
struct TBlobKeyTokens {
    void Append(TBlobKeyTokenPtr token) { Tokens.push_back(std::move(token)); }
    size_t Size() const { return Tokens.size(); }

    std::vector<TBlobKeyTokenPtr> Tokens;
};

}
