#pragma once

#include <arrow/datum.h>
#include <vector>
#include <deque>

#include "util.h"

namespace NYql::NUdf {

class TArgsDechunker {
public:
    explicit TArgsDechunker(std::vector<arrow20::Datum>&& args);
    bool Next(std::vector<arrow20::Datum>& chunk);
    // chunkLen will be zero if no arrays are present in chunk
    bool Next(std::vector<arrow20::Datum>& chunk, ui64& chunkLen);

private:
    const std::vector<arrow20::Datum> Args_;
    std::vector<std::deque<std::shared_ptr<arrow20::ArrayData>>> Arrays_;
    bool Finish_ = false;
};

} // namespace NYql::NUdf
