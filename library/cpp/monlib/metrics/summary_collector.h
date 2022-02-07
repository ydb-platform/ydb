#pragma once

#include "summary_snapshot.h"

#include <atomic>
#include <limits>
#include <cmath>

namespace NMonitoring {

    class ISummaryDoubleCollector {
    public:
        virtual ~ISummaryDoubleCollector() = default;

        virtual void Collect(double value) = 0;

        virtual ISummaryDoubleSnapshotPtr Snapshot() const = 0;

        virtual size_t SizeBytes() const = 0;
    };

    using ISummaryDoubleCollectorPtr = THolder<ISummaryDoubleCollector>;

    class TSummaryDoubleCollector final: public ISummaryDoubleCollector {
    public:
        TSummaryDoubleCollector() {
            Sum_.store(0, std::memory_order_relaxed);
            Min_.store(std::numeric_limits<double>::max(), std::memory_order_relaxed);
            Max_.store(std::numeric_limits<double>::lowest(), std::memory_order_relaxed);
            Count_.store(0, std::memory_order_relaxed);
        }

        void Collect(double value) noexcept override {
            if (std::isnan(value)) {
                return;
            }
            UpdateSum(value);
            UpdateMin(value);
            UpdateMax(value);
            Last_.store(value, std::memory_order_relaxed);
            Count_.fetch_add(1ul, std::memory_order_relaxed);
        }

        ISummaryDoubleSnapshotPtr Snapshot() const override {
            return new TSummaryDoubleSnapshot(
                    Sum_.load(std::memory_order_relaxed),
                    Min_.load(std::memory_order_relaxed),
                    Max_.load(std::memory_order_relaxed),
                    Last_.load(std::memory_order_relaxed),
                    Count_.load(std::memory_order_relaxed));
        }

        size_t SizeBytes() const override {
            return sizeof(*this);
        }

    private:
        std::atomic<double> Sum_;
        std::atomic<double> Min_;
        std::atomic<double> Max_;
        std::atomic<double> Last_;
        std::atomic_uint64_t Count_;

        void UpdateSum(double add) noexcept {
            double newValue;
            double oldValue = Sum_.load(std::memory_order_relaxed);
            do {
                newValue = oldValue + add;
            } while (!Sum_.compare_exchange_weak(
                    oldValue,
                    newValue,
                    std::memory_order_release,
                    std::memory_order_consume));
        }

        void UpdateMin(double candidate) noexcept {
            double oldValue = Min_.load(std::memory_order_relaxed);
            do {
                if (oldValue <= candidate) {
                    break;
                }
            } while (!Min_.compare_exchange_weak(
                    oldValue,
                    candidate,
                    std::memory_order_release,
                    std::memory_order_consume));
        }

        void UpdateMax(double candidate) noexcept {
            double oldValue = Max_.load(std::memory_order_relaxed);
            do {
                if (oldValue >= candidate) {
                    break;
                }
            } while (!Max_.compare_exchange_weak(
                    oldValue,
                    candidate,
                    std::memory_order_release,
                    std::memory_order_consume));
        }

    };

}
