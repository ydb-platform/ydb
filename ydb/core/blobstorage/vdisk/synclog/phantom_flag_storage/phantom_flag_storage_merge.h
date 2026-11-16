#pragma once

#include <ydb/core/base/logoblob.h>

#include <optional>
#include <span>
#include <vector>

namespace NKikimr::NSyncLog {

// Sort in place and return the prefix of distinct keys.
std::span<TLogoBlobID> SortPhantomFlagKeys(std::span<TLogoBlobID> keys);

class TPhantomFlagRunMerger {
public:
    struct TProgress {
        size_t Written = 0;
        std::optional<size_t> InputToRefill;
        bool Finished = false;
    };

    explicit TPhantomFlagRunMerger(size_t inputCount);

    // Supply one block for the input requested by Merge. Blocks in each input
    // must form a nondecreasing stream. An empty block with eof ends the input.
    void SetInputBlock(size_t inputIndex, std::vector<TLogoBlobID> keys, bool eof);

    // Fill the output prefix with distinct keys. A missing input blocks further
    // output until its next block is available. Pass a fresh output span to resume.
    TProgress Merge(std::span<TLogoBlobID> output);

private:
    struct TInputBlock {
        std::vector<TLogoBlobID> Keys;
        size_t Offset = 0;
        bool Eof = false;
    };

    struct TInputHead {
        TLogoBlobID Key;
        size_t InputIndex;

        bool operator>(const TInputHead& other) const;
    };

    std::optional<size_t> GetInputToRefill() const;

    std::vector<TInputBlock> Inputs;
    std::vector<TInputHead> Heap;
    size_t NextInitialInput = 0;
    std::optional<size_t> RefillInput;
    std::optional<TLogoBlobID> LastEmittedKey;
};

} // namespace NKikimr::NSyncLog
