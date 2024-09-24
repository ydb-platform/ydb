#include "range_ops.h"

#include <ydb/core/tx/datashard/datashard_user_table.h>
#include <ydb/core/tablet_flat/flat_scan_lead.h>

#include <library/cpp/dot_product/dot_product.h>
#include <library/cpp/l1_distance/l1_distance.h>
#include <library/cpp/l2_distance/l2_distance.h>

#include <span>

#define LOG_T(stream) LOG_TRACE_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, stream)
#define LOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, stream)
#define LOG_N(stream) LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, stream)
#define LOG_E(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, stream)

namespace NKikimr::NDataShard::NKMeans {

// TODO(mbkkt) move it to cpp

template <typename TRes>
Y_PURE_FUNCTION TTriWayDotProduct<TRes> CosineImpl(const float* lhs, const float* rhs, size_t length) noexcept {
    auto r = TriWayDotProduct(lhs, rhs, length);
    return {static_cast<TRes>(r.LL), static_cast<TRes>(r.LR), static_cast<TRes>(r.RR)};
}

template <typename TRes>
Y_PURE_FUNCTION TTriWayDotProduct<TRes> CosineImpl(const i8* lhs, const i8* rhs, size_t length) noexcept {
    const auto ll = DotProduct(lhs, lhs, length);
    const auto lr = DotProduct(lhs, rhs, length);
    const auto rr = DotProduct(rhs, rhs, length);
    return {static_cast<TRes>(ll), static_cast<TRes>(lr), static_cast<TRes>(rr)};
}

template <typename TRes>
Y_PURE_FUNCTION TTriWayDotProduct<TRes> CosineImpl(const ui8* lhs, const ui8* rhs, size_t length) noexcept {
    const auto ll = DotProduct(lhs, lhs, length);
    const auto lr = DotProduct(lhs, rhs, length);
    const auto rr = DotProduct(rhs, rhs, length);
    return {static_cast<TRes>(ll), static_cast<TRes>(lr), static_cast<TRes>(rr)};
}

inline TTableRange CreateRangeFrom(const TUserTable& table, ui32 parent, TCell& from, TCell& to) {
    if (parent == 0) {
        return table.GetTableRange();
    }
    from = TCell::Make(parent - 1);
    to = TCell::Make(parent);
    TTableRange range{{&from, 1}, false, {&to, 1}, true};
    return Intersect(table.KeyColumnTypes, range, table.GetTableRange());
}

inline NTable::TLead CreateLeadFrom(const TTableRange& range) {
    NTable::TLead lead;
    if (range.From) {
        lead.To(range.From, range.InclusiveFrom ? NTable::ESeek::Lower : NTable::ESeek::Upper);
    } else {
        lead.To({}, NTable::ESeek::Lower);
    }
    if (range.To) {
        lead.Until(range.To, range.InclusiveTo);
    }
    return lead;
}

// TODO(mbkkt) separate implementation for bit
template <typename T>
struct TMetric {
    using TCoord = T;
    // TODO(mbkkt) maybe compute floating sum in double? Needs benchmark
    using TSum = std::conditional_t<std::is_floating_point_v<T>, T, int64_t>;

    ui32 Dimensions = 0;

    bool IsExpectedSize(TArrayRef<const char> data) const noexcept {
        return data.size() == 1 + sizeof(TCoord) * Dimensions;
    }

    auto GetCoords(const char* coords) {
        return std::span{reinterpret_cast<const TCoord*>(coords), Dimensions};
    }

    auto GetData(char* data) {
        return std::span{reinterpret_cast<TCoord*>(data), Dimensions};
    }

    void Fill(TString& d, TSum* embedding, ui64& c) {
        const auto count = static_cast<TSum>(std::exchange(c, 0));
        auto data = GetData(d.MutRef().data());
        for (auto& coord : data) {
            coord = *embedding / count;
            *embedding++ = 0;
        }
    }
};

template <typename T>
struct TCosineSimilarity: TMetric<T> {
    using TCoord = typename TMetric<T>::TCoord;
    using TSum = typename TMetric<T>::TSum;
    // double used to avoid precision issues
    using TRes = double;

    static TRes Init() {
        return std::numeric_limits<TRes>::max();
    }

    auto Distance(const char* cluster, const char* embedding) const noexcept {
        const auto r = CosineImpl<TRes>(reinterpret_cast<const TCoord*>(cluster), reinterpret_cast<const TCoord*>(embedding), this->Dimensions);
        // sqrt(ll) * sqrt(rr) computed instead of sqrt(ll * rr) to avoid precision issues
        const auto norm = std::sqrt(r.LL) * std::sqrt(r.RR);
        const TRes similarity = norm != 0 ? static_cast<TRes>(r.LR) / static_cast<TRes>(norm) : 0;
        return -similarity;
    }
};

template <typename T>
struct TL1Distance: TMetric<T> {
    using TCoord = typename TMetric<T>::TCoord;
    using TSum = typename TMetric<T>::TSum;
    using TRes = std::conditional_t<std::is_floating_point_v<T>, T, ui64>;

    static TRes Init() {
        return std::numeric_limits<TRes>::max();
    }

    auto Distance(const char* cluster, const char* embedding) const noexcept {
        const auto distance = L1Distance(reinterpret_cast<const TCoord*>(cluster), reinterpret_cast<const TCoord*>(embedding), this->Dimensions);
        return distance;
    }
};

template <typename T>
struct TL2Distance: TMetric<T> {
    using TCoord = typename TMetric<T>::TCoord;
    using TSum = typename TMetric<T>::TSum;
    using TRes = std::conditional_t<std::is_floating_point_v<T>, T, ui64>;

    static TRes Init() {
        return std::numeric_limits<TRes>::max();
    }

    auto Distance(const char* cluster, const char* embedding) const noexcept {
        const auto distance = L2SqrDistance(reinterpret_cast<const TCoord*>(cluster), reinterpret_cast<const TCoord*>(embedding), this->Dimensions);
        return distance;
    }
};

template <typename T>
struct TMaxInnerProductSimilarity: TMetric<T> {
    using TCoord = typename TMetric<T>::TCoord;
    using TSum = typename TMetric<T>::TSum;
    using TRes = std::conditional_t<std::is_floating_point_v<T>, T, i64>;

    static TRes Init() {
        return std::numeric_limits<TRes>::max();
    }

    auto Distance(const char* cluster, const char* embedding) const noexcept {
        const TRes similarity = DotProduct(reinterpret_cast<const TCoord*>(cluster), reinterpret_cast<const TCoord*>(embedding), this->Dimensions);
        return -similarity;
    }
};

template <typename TMetric>
struct TCalculation: TMetric {
    ui32 FindClosest(std::span<const TString> clusters, const char* embedding) {
        auto min = this->Init();
        ui32 closest = std::numeric_limits<ui32>::max();
        for (size_t i = 0; const auto& cluster : clusters) {
            auto distance = this->Distance(cluster.data(), embedding);
            if (distance < min) {
                min = distance;
                closest = i;
            }
            ++i;
        }
        return closest;
    }
};

}
