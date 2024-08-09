#pragma once

#include <random>

#include <util/generic/yexception.h>

namespace NYql {

template <typename T>
class TRangeWalker {
private:
    const T Start_;
    const T Finish_;
    T Current_;

public:
    TRangeWalker(T start, T finish)
        : Start_(start)
        , Finish_(finish)
        , Current_(start)
    {
        if (Start_ > Finish_) {
            ythrow yexception() << "Invalid range for walker";
        }
    }

    T GetStart() const {
        return Start_;
    }

    T GetFinish() const {
        return Finish_;
    }

    T GetRangeSize() const {
        return Finish_ - Start_ + 1;
    }

    T MoveToNext() {
        T result = Current_++;

        if (Current_ > Finish_) {
            Current_ = Start_;
        }

        return result;
    }
};
}
