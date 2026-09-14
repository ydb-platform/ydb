#include "args_dechunker.h"

#include <util/generic/yexception.h>
#include <util/generic/ylimits.h>

namespace NYql::NUdf {

TArgsDechunker::TArgsDechunker(std::vector<arrow::Datum>&& args)
    : Args_(std::move(args))
    , Arrays_(Args_.size())
{
    for (size_t i = 0; i < Args_.size(); ++i) {
        if (Args_[i].is_arraylike()) {
            ForEachArrayData(Args_[i], [&](const auto& data) {
                Arrays_[i].push_back(data);
            });
        }
    }
}

bool TArgsDechunker::Next(std::vector<arrow::Datum>& chunk) {
    ui64 chunkLen;
    return Next(chunk, chunkLen);
}

bool TArgsDechunker::Next(std::vector<arrow::Datum>& chunk, ui64& chunkLen) {
    chunkLen = 0;
    if (Finish_) {
        return false;
    }

    size_t minSize = Max<size_t>();
    bool haveData = false;
    chunk.resize(Args_.size());
    for (size_t i = 0; i < Args_.size(); ++i) {
        if (Args_[i].is_scalar()) {
            chunk[i] = Args_[i];
            continue;
        }
        while (!Arrays_[i].empty() && Arrays_[i].front()->length == 0) {
            Arrays_[i].pop_front();
        }
        if (!Arrays_[i].empty()) {
            haveData = true;
            minSize = std::min<size_t>(minSize, Arrays_[i].front()->length);
        } else {
            minSize = 0;
        }
    }

    Y_ENSURE(!haveData || minSize > 0, "Block length mismatch");
    if (!haveData) {
        Finish_ = true;
        return false;
    }

    for (size_t i = 0; i < Args_.size(); ++i) {
        if (!Args_[i].is_scalar()) {
            Y_ENSURE(!Arrays_[i].empty(), "Block length mismatch");
            chunk[i] = arrow::Datum(Chop(Arrays_[i].front(), minSize));
        }
    }
    chunkLen = minSize;
    return true;
}

} // namespace NYql::NUdf
