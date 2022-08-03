#pragma once

#include "output_queue.h"
#include <util/generic/size_literals.h>
#include <ydb/library/yql/utils/yql_panic.h>

#include <numeric>
#include <deque>

namespace NYql {

template <size_t MinItemSize = 5_MB, size_t MaxItemSize = 0ULL>
class TOutputQueue : public IOutputQueue {
public:
    TOutputQueue() = default;

    void Push(TString&& item) override {
        YQL_ENSURE(!Sealed, "Queue is sealed.");
        if (!Queue.empty() && Queue.back().size() < MinItemSize)
            if constexpr (MaxItemSize > 0ULL)
                if (Queue.back().size() + item.size() > MaxItemSize) {
                    Queue.back().append(item.substr(0U, MaxItemSize - Queue.back().size()));
                    Queue.emplace_back(item.substr(item.size() +  Queue.back().size() - MaxItemSize));
                } else
                    Queue.back().append(std::move(item));
            else
                Queue.back().append(std::move(item));
        else
            Queue.emplace_back(std::move(item));
    }

    TString Pop() override {
        if (Queue.empty() || !Sealed && Queue.front().size() < MinItemSize)
            return {};

        auto out = std::move(Queue.front());
        Queue.pop_front();
        return out;
    }

    void Seal() override {
        Sealed = true;
    }

    size_t Size() const override {
        return Queue.size();
    }

    bool Empty() const override {
        return Queue.empty();
    }

    bool IsSealed() const override {
        return Sealed;
    }

    size_t Volume() const override {
        return std::accumulate(Queue.cbegin(), Queue.cend(), 0ULL, [](size_t sum, const TString& item) { return sum += item.size(); });
    }
private:
    std::deque<TString> Queue;
    bool Sealed = false;
};

}
