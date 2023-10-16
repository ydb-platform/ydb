#pragma once

#include <util/generic/ptr.h>
#include <util/generic/vector.h>

#include <atomic>

namespace NMonitoring {
    class TAtomicsArray {
    public:
        explicit TAtomicsArray(size_t size)
            : Values_(new std::atomic<ui64>[size])
            , Size_(size)
        {
            for (size_t i = 0; i < Size_; i++) {
                Values_[i].store(0, std::memory_order_relaxed);
            }
        }

        ui64 operator[](size_t index) const noexcept {
            Y_DEBUG_ABORT_UNLESS(index < Size_);
            return Values_[index].load(std::memory_order_relaxed);
        }

        size_t Size() const noexcept {
            return Size_;
        }

        void Add(size_t index, ui64 count) noexcept {
            Y_DEBUG_ABORT_UNLESS(index < Size_);
            Values_[index].fetch_add(count, std::memory_order_relaxed);
        }

        void Reset() noexcept {
            for (size_t i = 0; i < Size_; i++) {
                Values_[i].store(0, std::memory_order_relaxed);
            }
        }

        TVector<ui64> Copy() const {
            TVector<ui64> copy(Reserve(Size_));
            for (size_t i = 0; i < Size_; i++) {
                copy.push_back(Values_[i].load(std::memory_order_relaxed));
            }
            return copy;
        }

    private:
        TArrayHolder<std::atomic<ui64>> Values_;
        size_t Size_;
    };
}
