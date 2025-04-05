#pragma once
#include <util/system/types.h>

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
 * 2. If only one stream has finished after step 1. then use HashJoin algorithm. [This CAN NOT be changed]
 *      - If tuples are wide then external payload optimization can be applied. [Usage depends on the policy]
 * 3. If no stream has finished after step 1. then use GraceHashJoin with spilling. [This CAN NOT be changed]
 * 4. If both streams have finished after step 1. then use HashJoin or InMemoryGraceJoin. [The choice of algorithm depends on the policy]
 *      - If tuples are wide then external payload optimization can be applied. [Usage depends on the policy]
 */
class IBlockGraceJoinPolicy {
public:
    enum class EJoinAlgo {
        HashJoin,
        InMemoryGraceJoin,
    };

public:
    virtual ~IBlockGraceJoinPolicy() = default;

    IBlockGraceJoinPolicy&
    SetMaximumInitiallyFetchedData(size_t maximumInitiallyFetchedData) {
        MaximumInitiallyFetchedData_ = maximumInitiallyFetchedData;
        return *this;
    }

    size_t GetMaximumInitiallyFetchedData() const {
        return MaximumInitiallyFetchedData_;
    }

    virtual bool UseExternalPayload(EJoinAlgo type, size_t payloadSize) const = 0;

    virtual EJoinAlgo PickAlgorithm(size_t leftFetchedSize, size_t rightFetchedSize) const = 0;

private:
    size_t MaximumInitiallyFetchedData_{0}; // in bytes
};

// -------------------------------------------------------------------
// Default block grace join policy
class TDefaultBlockGraceJoinPolicy : public IBlockGraceJoinPolicy {
private:
    static constexpr size_t KB{1024};
    static constexpr size_t MB{KB * KB};
    static constexpr size_t L1_CACHE_SIZE{256 * KB};
    static constexpr size_t L2_CACHE_SIZE{  2 * MB};
    static constexpr size_t L3_CACHE_SIZE{ 16 * MB};

public:
    TDefaultBlockGraceJoinPolicy() {
        SetMaximumInitiallyFetchedData(L3_CACHE_SIZE / 2);
    }

    bool UseExternalPayload(EJoinAlgo type, size_t payloadSize) const override {
        // If payload size of tuple bigger than MaximumPayloadSizeThreshold_ bytes
        // then external payloads should be more efficient than pure TLayout
        switch (type) {
        case EJoinAlgo::HashJoin: {
            return payloadSize > HashJoinPayloadThreshold_;
        }
        case EJoinAlgo::InMemoryGraceJoin: {
            return payloadSize > InMemoryGraceJoinPayloadThreshold_;
        }
        default:
            Y_UNREACHABLE();
        }
    }

    EJoinAlgo PickAlgorithm(size_t leftFetchedSize, size_t rightFetchedSize) const override {
        // If one stream is small enough then use Hash Join
        if (leftFetchedSize <= L2_CACHE_SIZE || rightFetchedSize <= L2_CACHE_SIZE) {
            return EJoinAlgo::HashJoin;
        } else {
            return EJoinAlgo::InMemoryGraceJoin;
        }
    }

private:
    size_t InMemoryGraceJoinPayloadThreshold_{8}; // for in mem. grace join it is almost always beneficial to use external payload due to bucket splitting
    size_t HashJoinPayloadThreshold_{32};
};

} // NKikimr
} // NMiniKQL
