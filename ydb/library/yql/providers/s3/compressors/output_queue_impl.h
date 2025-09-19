#pragma once

#include "output_queue.h"

#include <yql/essentials/utils/yql_panic.h>

#include <util/generic/size_literals.h>

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
                        const auto sizeToAdd = MaxItemSize - Queue.back().size();
                        Queue.back().append(item.substr(0U, sizeToAdd));
                        item = item.substr(sizeToAdd);
                    } else {
                        Queue.back().append(item);
                        item.clear();
                    }
                } else {
                    Queue.back().append(item);
                    item.clear();
                }
            }
        }

        if (item) {
            if constexpr (MaxItemSize > 0ULL) {
                ui64 i = 0;
                for (; i + MaxItemSize < item.size(); i += MaxItemSize) {
                    Queue.emplace_back(item.substr(i, MaxItemSize));
                }
                item = item.substr(i);
            }

            Queue.emplace_back(std::move(item));
        }
    }

    TString Pop() override {
        if (Queue.empty() || (1U == Queue.size() && !Sealed)) {
            return {};
        }

        auto out = std::move(Queue.front());
        Queue.pop_front();
        return out;
    }

    void Seal() override {
        Sealed = true;

        if constexpr (MinItemSize > 0ULL) {
            if (Queue.size() <= 1U || Queue.back().size() >= MinItemSize) {
                return;
            }

            TString lastItem = std::move(Queue.back());
            Queue.pop_back();

            if constexpr (MaxItemSize > 0ULL) {
                const auto endSize = Queue.back().size();
                if (endSize + lastItem.size() <= MaxItemSize) {
                    Queue.back().append(lastItem);
                    return;
                }

                // In case then last item is smaller than MinItemSize and item before it + Last item bigger than MaxItemSize
                // we can not guarantee that all requirements are met if 2 * MinItemSize > MaxItemSize
                if (endSize > MinItemSize) {
                    const auto appendSize = endSize - std::min(endSize - MinItemSize, MinItemSize - lastItem.size());
                    lastItem = Queue.back().substr(appendSize) + lastItem;
                    Queue.back().resize(appendSize);
                }

                Queue.push_back(std::move(lastItem));
            } else {
                Queue.back().append(lastItem);
            }
        }
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

} // namespace NYql
