#include "args_dechunker.h"

#include <util/generic/yexception.h>
#include <util/generic/ylimits.h>

namespace NYql {
namespace NUdf {

TArgsDechunker::TArgsDechunker(std::vector<arrow::Datum>&& args)
    : Args(std::move(args))
    , Arrays(Args.size())
{
    for (size_t i = 0; i < Args.size(); ++i) {
        if (Args[i].is_arraylike()) {
            ForEachArrayData(Args[i], [&](const auto& data) {
                Arrays[i].push_back(data);
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
    if (Finish) {
        return false;
    }

    size_t minSize = Max<size_t>();
    bool haveData = false;
    chunk.resize(Args.size());
    for (size_t i = 0; i < Args.size(); ++i) {
        if (Args[i].is_scalar()) {
            chunk[i] = Args[i];
            continue;
        }
        while (!Arrays[i].empty() && Arrays[i].front()->length == 0) {
            Arrays[i].pop_front();
        }
        if (!Arrays[i].empty()) {
            haveData = true;
            minSize = std::min<size_t>(minSize, Arrays[i].front()->length);
        } else {
            minSize = 0;
        }
    }

    Y_ENSURE(!haveData || minSize > 0, "Block length mismatch");
    if (!haveData) {
        Finish = true;
        return false;
    }

    for (size_t i = 0; i < Args.size(); ++i) {
        if (!Args[i].is_scalar()) {
            Y_ENSURE(!Arrays[i].empty(), "Block length mismatch");
            chunk[i] = arrow::Datum(Chop(Arrays[i].front(), minSize));
        }
    }
    chunkLen = minSize;
    return true;
}


}
}
