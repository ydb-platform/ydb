#pragma once
#include <util/system/types.h>
#include <limits>

namespace NKikimr {
namespace NMiniKQL {

// -------------------------------------------------------------------
/**
 * This interface is used to customize BlockGraceJoin's behavior from the outside.
 * Usually this functionality is used in tests and benchmarks.
 * Otherwise the default policy is used. It is defined in .cpp file.
 * Not all join behavior can be changed to not break the join.
 * 
 * Default join behavior in general:
 * 1. Fetch some data from left and right streams to decide what algorithm to pick. [Maximum fetched data size can be changed]
 * 2. If only one stream has finished after step 1. then use HashJoin or GraceHashJoin with spilling. [The choice of algorithm depends on the policy]
 *      - If tuples are wide then external payload optimization can be applied. [Usage depends on the policy]
 * 3. If no stream has finished after step 1. then use GraceHashJoin with spilling. [This CAN NOT be changed]
 * 4. If both streams have finished after step 1. then use HashJoin or InMemoryGraceJoin. [The choice of algorithm depends on the policy]
 *      - If tuples are wide then external payload optimization can be applied. [Usage depends on the policy]
 */
class IBlockGraceJoinPolicy {
public:
    static constexpr size_t STREAM_NOT_FETCHED = std::numeric_limits<size_t>::max();

public:
    enum class EJoinAlgo {
        HashJoin,
        InMemoryGraceJoin,
        SpillingGraceJoin,
    };

public:
    virtual ~IBlockGraceJoinPolicy() = default;

    IBlockGraceJoinPolicy&
    SetMaximumInitiallyFetchedData(size_t maximumInitiallyFetchedData) {
        MaximumInitiallyFetchedData_ = maximumInitiallyFetchedData;
        return *this;
    }

    IBlockGraceJoinPolicy& SetMaximumData(size_t size) {
        MaximumData_ = size;
        return *this;
    }

    size_t GetMaximumInitiallyFetchedData() const {
        return MaximumInitiallyFetchedData_;
    }

    size_t GetMaximumData() const {
        return MaximumData_;
    }

    virtual bool UseExternalPayload(EJoinAlgo type, size_t payloadSize, ui64 cardinality) const = 0;

    virtual EJoinAlgo PickAlgorithm(size_t leftFetchedTuples, size_t rightFetchedTuples) const = 0;

private:
    /// arent those just same parameters?
    size_t MaximumInitiallyFetchedData_{0}; // in bytes
    size_t MaximumData_{0}; // in bytes
};

// -------------------------------------------------------------------
// Default block grace join policy
class TDefaultBlockGraceJoinPolicy : public IBlockGraceJoinPolicy {
private:
    static constexpr size_t KB{1024};
    static constexpr size_t MB{KB * KB};
    static constexpr size_t L1_CACHE_SIZE{32 * KB};
    static constexpr size_t L2_CACHE_SIZE{256 * KB};
    static constexpr size_t L3_CACHE_SIZE{16 * MB};

public:
    TDefaultBlockGraceJoinPolicy() {
        SetMaximumInitiallyFetchedData(200 * MB);
        SetMaximumData(200 * MB);
    }

    // ratio arg is the ratio of the expected size of the large stream to the output size
    bool UseExternalPayload(EJoinAlgo type, size_t payloadSize, ui64 ratio) const override {
        // If payload size of tuple bigger than treshold bytes,
        // then external payloads should be more efficient than pure TLayout.
        // But if cardinality is too big, we would waste a lot of time
        // to restore result from external payload storage.
        switch (type) {
        case EJoinAlgo::HashJoin: {
            if (ratio > HashJoinRatioThreshold_) {
                return payloadSize > HashJoinPayloadThreshold_;
            } else {
                return false;
            }
        }
        case EJoinAlgo::InMemoryGraceJoin: {
            if (ratio > InMemoryGraceJoinRatioThreshold_) {
                return payloadSize > InMemoryGraceJoinPayloadThreshold_;
            } else {
                return false;
            }
        }
        default:
            Y_UNREACHABLE();
        }
    }

    EJoinAlgo PickAlgorithm(size_t leftFetchedTuples, size_t rightFetchedTuples) const override {
        // if no stream is finished
        if (leftFetchedTuples == STREAM_NOT_FETCHED && rightFetchedTuples == STREAM_NOT_FETCHED) {
            return EJoinAlgo::SpillingGraceJoin;
        }

        // If one stream is small enough then use Hash Join
        if (leftFetchedTuples <= TuplesCountTreshold_ || rightFetchedTuples <= TuplesCountTreshold_) {
            return EJoinAlgo::HashJoin;
        } else {
            return EJoinAlgo::InMemoryGraceJoin;
        }
    }

private:
    size_t InMemoryGraceJoinPayloadThreshold_{32}; // bytes
    size_t InMemoryGraceJoinRatioThreshold_{7}; // 1/7 means output size ~= 15% of larger input size. Also take into account that cardinality estimation often less than truth cardinality

    size_t HashJoinPayloadThreshold_{16};
    size_t HashJoinRatioThreshold_{7};

    size_t TuplesCountTreshold_{100'000}; // treshold to decide what algoritm to choose
};

} // NKikimr
} // NMiniKQL
