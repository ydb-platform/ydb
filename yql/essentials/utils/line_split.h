#pragma once

#include <util/stream/input.h>
#include <util/generic/string.h>

class TLineSplitter final {
public:
    explicit TLineSplitter(IInputStream& stream);

    size_t Next(TString& st);

private:
    IInputStream& Stream_;
    bool HasPendingLineChar_ = false;
    char PendingLineChar_ = 0;
};
