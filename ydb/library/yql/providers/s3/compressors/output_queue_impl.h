#pragma once

#include "output_queue.h"
#include <util/generic/size_literals.h>
#include <ydb/library/yql/utils/yql_panic.h>

#include <numeric>
#include <deque>

namespace NYql {

// https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html
constexpr size_t S3PartUploadMinSize = 5_MB;

template <size_t MinItemSize = S3PartUploadMinSize, size_t MaxItemSize = 0ULL>
class TOutputQueue : public IOutputQueue {
public:
    TOutputQueue() = default;

    void Push(TString&& item) override {
        YQL_ENSURE(!Sealed, "Queue is sealed.");
        if constexpr (MinItemSize > 0ULL) {
            if (!Queue.empty() && Queue.back().size() < MinItemSize) {
                if constexpr (MaxItemSize > 0ULL) {
                    if (Queue.back().size() + item.size() > MaxItemSize) {
                        Queue.back().append(item.substr(0U, MaxItemSize - Queue.back().size()));
                        item = item.substr(item.size() + Queue.back().size() - MaxItemSize);
                    } else {
                        Queue.back().append(std::move(item));
                        item.clear();
                    }
                } else {
                    Queue.back().append(std::move(item));
                    item.clear();
                }
            }
        }

        if (!item.empty()) {
            if constexpr (MaxItemSize > 0ULL) {
                while (item.size() > MaxItemSize) {
                    Queue.emplace_back(item.substr(0U, MaxItemSize));
                    item = item.substr(MaxItemSize);
                }
            }

            Queue.emplace_back(std::move(item));
        }
    }

    TString Pop() override {
        if (Queue.empty() || 1U == Queue.size() && !Sealed)
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
