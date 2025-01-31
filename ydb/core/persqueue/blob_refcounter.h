#pragma once

#include <memory>
#include <vector>
#include <cstddef>

namespace NKikimr::NPQ {

class TBlobRefCounter {
public:
    TBlobRefCounter() = default;
    TBlobRefCounter(size_t count);
    TBlobRefCounter(const TBlobRefCounter& rhs);
    TBlobRefCounter(TBlobRefCounter&& rhs) = default;
    ~TBlobRefCounter();

    TBlobRefCounter& operator=(const TBlobRefCounter& rhs);
    TBlobRefCounter& operator=(TBlobRefCounter&& rhs) = default;

    size_t GetUseCount() const;

    void Inc() const;
    void Dec() const;

private:
    std::shared_ptr<size_t> Counter;
};

struct TBlobRefCounters {
    void Append(const TBlobRefCounter& counter);
    size_t Size() const { return Counters.size(); }

    std::vector<TBlobRefCounter> Counters;
};

}
