/**
 * Non-metric Space Library
 *
 * Main developers: Bilegsaikhan Naidan, Leonid Boytsov, Yury Malkov, Ben Frederickson, David Novak
 *
 * For the complete list of contributors and further details see:
 * https://github.com/nmslib/nmslib
 *
 * Copyright (c) 2013-2018
 *
 * This code is released under the
 * Apache License Version 2.0 http://www.apache.org/licenses/.
 *
 */
#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <utility>
#include <vector>

namespace similarity {

using vl_type = unsigned char;

struct VisitedListPoolPolicy {
    size_t MaxIdleLists = 4;
    // A successful reservation must remain alive until the allocation is freed.
    std::function<std::shared_ptr<void>(size_t)> Reserve;
    std::function<bool()> CanRetain;
};

struct VisitedListPoolStats {
    uint64_t Allocations = 0;
    uint64_t Reuses = 0;
    uint64_t Frees = 0;
    uint64_t AllocationFailures = 0;
    uint64_t ActiveBuffers = 0;
    uint64_t IdleBuffers = 0;
    uint64_t RetainedBytes = 0;
    uint64_t AllocatedBytes = 0;
};

class VisitedListMemoryLimitExceeded : public std::exception {
public:
    const char* what() const noexcept override { return "HNSW visited-list memory limit exceeded"; }
};

class VisitedList {
public:
    VisitedList(size_t elements, std::shared_ptr<const VisitedListPoolPolicy> policy = {},
            std::shared_ptr<void> reservation = {})
        : Policy(std::move(policy))
        , Reservation(std::move(reservation))
        , mass(new vl_type[elements + 1]) // Sentinel for prefetch.
        , numelements(elements)
    {}

    VisitedList(const VisitedList&) = delete;
    VisitedList& operator=(const VisitedList&) = delete;

    void reset() noexcept {
        if (++curV == 0) {
            std::memset(mass, 0, numelements * sizeof(vl_type));
            ++curV;
        }
    }

    ~VisitedList() { delete[] mass; } // Release the reservation after the buffer.

    std::shared_ptr<const VisitedListPoolPolicy> Policy;
    std::shared_ptr<void> Reservation;
    vl_type curV = static_cast<vl_type>(-1);
    vl_type* mass;
    size_t numelements;
};

class VisitedListPool {
    struct Deleter {
        VisitedListPool* Pool;
        void operator()(VisitedList* list) const noexcept { Pool->releaseVisitedList(list); }
    };

public:
    using Lease = std::unique_ptr<VisitedList, Deleter>;

    VisitedListPool(size_t initialLists, size_t elements, size_t maxIdleLists = 4)
        : Elements(elements)
        , Policy(std::make_shared<VisitedListPoolPolicy>())
    {
        Policy->MaxIdleLists = maxIdleLists;
        Idle.reserve(initialLists > Policy->MaxIdleLists ? initialLists : Policy->MaxIdleLists);
        for (size_t i = 0; i < initialLists; ++i) {
            Idle.emplace_back(std::make_unique<VisitedList>(Elements, Policy));
            ++Stats.Allocations;
            Stats.AllocatedBytes += BufferMemoryBytes();
        }
    }

    // Install before publishing a graph. Discard construction/loading scratch
    // buffers so all subsequent allocations follow the same memory policy.
    void Configure(VisitedListPoolPolicy policy) {
        auto nextPolicy = std::make_shared<VisitedListPoolPolicy>(std::move(policy));
        std::shared_ptr<VisitedListPoolPolicy> oldPolicy;
        std::vector<std::unique_ptr<VisitedList>> old;
        old.reserve(nextPolicy->MaxIdleLists);
        {
            std::lock_guard guard(Mutex);
            if (Stats.ActiveBuffers)
                throw std::logic_error("Cannot configure an active visited-list pool");
            old.swap(Idle);
            Stats.Frees += old.size();
            oldPolicy = std::move(Policy);
            Policy = std::move(nextPolicy);
        }
    }

    size_t BufferMemoryBytes() const noexcept {
        return sizeof(VisitedList) + (Elements + 1) * sizeof(vl_type);
    }

    Lease acquire() { return Lease(getFreeVisitedList(), Deleter{this}); }

    VisitedList* getFreeVisitedList() {
        std::unique_ptr<VisitedList> list;
        std::shared_ptr<const VisitedListPoolPolicy> policy;
        {
            std::lock_guard guard(Mutex);
            policy = Policy;
            if (!Idle.empty()) {
                list = std::move(Idle.back());
                Idle.pop_back();
                ++Stats.Reuses;
            }
            ++Stats.ActiveBuffers;
        }
        if (!list) {
            try {
                std::shared_ptr<void> reservation;
                if (policy->Reserve) {
                    reservation = policy->Reserve(BufferMemoryBytes());
                    if (!reservation)
                        throw VisitedListMemoryLimitExceeded();
                }
                list = std::make_unique<VisitedList>(Elements, policy, std::move(reservation));
                std::lock_guard guard(Mutex);
                ++Stats.Allocations;
                Stats.AllocatedBytes += BufferMemoryBytes();
            } catch (...) {
                std::lock_guard guard(Mutex);
                --Stats.ActiveBuffers;
                ++Stats.AllocationFailures;
                throw;
            }
        }
        list->reset();
        return list.release();
    }

    void releaseVisitedList(VisitedList* ptr) noexcept {
        std::unique_ptr<VisitedList> list(ptr);
        bool retain = true;
        try {
            if (list->Policy && list->Policy->CanRetain)
                retain = list->Policy->CanRetain();
        } catch (...) {
            retain = false;
        }
        {
            std::lock_guard guard(Mutex);
            --Stats.ActiveBuffers;
            if (retain && Idle.size() < Policy->MaxIdleLists) {
                // Capacity is reserved by Configure; returning a lease cannot allocate.
                Idle.push_back(std::move(list));
            } else {
                ++Stats.Frees;
            }
        }
        // Destruction and memory-controller callbacks run outside Mutex.
    }

    void TrimIdle() {
        std::vector<std::unique_ptr<VisitedList>> old;
        // Keep the bounded bookkeeping capacity available to returning leases.
        size_t capacity;
        {
            std::lock_guard guard(Mutex);
            capacity = Policy->MaxIdleLists;
        }
        old.reserve(capacity);
        {
            std::lock_guard guard(Mutex);
            old.swap(Idle);
            Stats.Frees += old.size();
        }
    }

    VisitedListPoolStats GetStats() const {
        std::lock_guard guard(Mutex);
        auto result = Stats;
        result.IdleBuffers = Idle.size();
        result.RetainedBytes = Idle.size() * BufferMemoryBytes();
        return result;
    }

private:
    const size_t Elements;
    mutable std::mutex Mutex;
    std::vector<std::unique_ptr<VisitedList>> Idle;
    std::shared_ptr<VisitedListPoolPolicy> Policy;
    VisitedListPoolStats Stats;
};

} // namespace similarity
