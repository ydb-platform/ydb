#pragma once

#include <ydb/core/tx/datashard/buffer_data.h>
#include <ydb/core/tx/datashard/datashard_user_table.h>
#include <ydb/core/tx/datashard/range_ops.h>
#include <ydb/core/tx/datashard/scan_common.h>
#include <ydb/core/tablet_flat/flat_scan_lead.h>
#include <ydb/core/protos/tx_datashard.pb.h>

#include <ydb/public/api/protos/ydb_table.pb.h>

#include <library/cpp/dot_product/dot_product.h>
#include <library/cpp/l1_distance/l1_distance.h>
#include <library/cpp/l2_distance/l2_distance.h>

#include <span>

// TODO(mbkkt) BUILD_INDEX_DATASHARD
#define LOG_T(stream) LOG_TRACE_S (*TlsActivationContext, NKikimrServices::BUILD_INDEX, stream)
#define LOG_D(stream) LOG_DEBUG_S (*TlsActivationContext, NKikimrServices::BUILD_INDEX, stream)
#define LOG_N(stream) LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::BUILD_INDEX, stream)
#define LOG_E(stream) LOG_ERROR_S (*TlsActivationContext, NKikimrServices::BUILD_INDEX, stream)

namespace NKikimr::NDataShard::NKMeans {

template <typename TRes>
Y_PURE_FUNCTION TTriWayDotProduct<TRes> CosineImpl(const float* lhs, const float* rhs, size_t length) noexcept
{
    auto r = TriWayDotProduct(lhs, rhs, length);
    return {static_cast<TRes>(r.LL), static_cast<TRes>(r.LR), static_cast<TRes>(r.RR)};
}

template <typename TRes>
Y_PURE_FUNCTION TTriWayDotProduct<TRes> CosineImpl(const i8* lhs, const i8* rhs, size_t length) noexcept
{
    const auto ll = DotProduct(lhs, lhs, length);
    const auto lr = DotProduct(lhs, rhs, length);
    const auto rr = DotProduct(rhs, rhs, length);
    return {static_cast<TRes>(ll), static_cast<TRes>(lr), static_cast<TRes>(rr)};
}

template <typename TRes>
Y_PURE_FUNCTION TTriWayDotProduct<TRes> CosineImpl(const ui8* lhs, const ui8* rhs, size_t length) noexcept
{
    const auto ll = DotProduct(lhs, lhs, length);
    const auto lr = DotProduct(lhs, rhs, length);
    const auto rr = DotProduct(rhs, rhs, length);
    return {static_cast<TRes>(ll), static_cast<TRes>(lr), static_cast<TRes>(rr)};
}

TTableRange CreateRangeFrom(const TUserTable& table, ui32 parent, TCell& from, TCell& to);

NTable::TLead CreateLeadFrom(const TTableRange& range);

// TODO(mbkkt) separate implementation for bit
template <typename T>
struct TMetric {
    using TCoord = T;
    // TODO(mbkkt) maybe compute floating sum in double? Needs benchmark
    using TSum = std::conditional_t<std::is_floating_point_v<T>, T, i64>;

    ui32 Dimensions = 0;

    bool IsExpectedSize(TArrayRef<const char> data) const noexcept
    {
        return data.size() == 1 + sizeof(TCoord) * Dimensions;
    }

    auto GetCoords(const char* coords)
    {
        return std::span{reinterpret_cast<const TCoord*>(coords), Dimensions};
    }

    auto GetData(char* data)
    {
        return std::span{reinterpret_cast<TCoord*>(data), Dimensions};
    }

    void Fill(TString& d, TSum* embedding, ui64& c)
    {
        Y_ASSERT(c > 0);
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

    static TRes Init()
    {
        return std::numeric_limits<TRes>::max();
    }

    auto Distance(const char* cluster, const char* embedding) const noexcept
    {
        const auto r = CosineImpl<TRes>(reinterpret_cast<const TCoord*>(cluster),
                                        reinterpret_cast<const TCoord*>(embedding), this->Dimensions);
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

    static TRes Init()
    {
        return std::numeric_limits<TRes>::max();
    }

    auto Distance(const char* cluster, const char* embedding) const noexcept
    {
        const auto distance = L1Distance(reinterpret_cast<const TCoord*>(cluster),
                                         reinterpret_cast<const TCoord*>(embedding), this->Dimensions);
        return distance;
    }
};

template <typename T>
struct TL2Distance: TMetric<T> {
    using TCoord = typename TMetric<T>::TCoord;
    using TSum = typename TMetric<T>::TSum;
    using TRes = std::conditional_t<std::is_floating_point_v<T>, T, ui64>;

    static TRes Init()
    {
        return std::numeric_limits<TRes>::max();
    }

    auto Distance(const char* cluster, const char* embedding) const noexcept
    {
        const auto distance = L2SqrDistance(reinterpret_cast<const TCoord*>(cluster),
                                            reinterpret_cast<const TCoord*>(embedding), this->Dimensions);
        return distance;
    }
};

template <typename T>
struct TMaxInnerProductSimilarity: TMetric<T> {
    using TCoord = typename TMetric<T>::TCoord;
    using TSum = typename TMetric<T>::TSum;
    using TRes = std::conditional_t<std::is_floating_point_v<T>, T, i64>;

    static TRes Init()
    {
        return std::numeric_limits<TRes>::max();
    }

    auto Distance(const char* cluster, const char* embedding) const noexcept
    {
        const TRes similarity = DotProduct(reinterpret_cast<const TCoord*>(cluster),
                                           reinterpret_cast<const TCoord*>(embedding), this->Dimensions);
        return -similarity;
    }
};

template <typename TMetric>
struct TCalculation: TMetric {
    ui32 FindClosest(std::span<const TString> clusters, TArrayRef<const char> embedding) const
    {
        Y_DEBUG_ABORT_UNLESS(this->IsExpectedSize(embedding));
        auto min = this->Init();
        ui32 closest = std::numeric_limits<ui32>::max();
        for (size_t i = 0; const auto& cluster : clusters) {
            Y_DEBUG_ABORT_UNLESS(this->IsExpectedSize(cluster));
            auto distance = this->Distance(cluster.data(), embedding.data());
            if (distance < min) {
                min = distance;
                closest = i;
            }
            ++i;
        }
        return closest;
    }
};

template <typename TMetric>
ui32 FeedEmbedding(const TCalculation<TMetric>& calculation, std::span<const TString> clusters,
                   const NTable::TRowState& row, NTable::TPos embeddingPos)
{
    Y_ASSERT(embeddingPos < row.Size());
    const auto embedding = row.Get(embeddingPos).AsRef();
    if (!calculation.IsExpectedSize(embedding)) {
        return std::numeric_limits<ui32>::max();
    }
    return calculation.FindClosest(clusters, embedding);
}

void AddRowMain2Build(TBufferData& buffer, ui32 parent, TArrayRef<const TCell> key, const NTable::TRowState& row);

void AddRowMain2Posting(TBufferData& buffer, ui32 parent, TArrayRef<const TCell> key, const NTable::TRowState& row,
                        ui32 dataPos);

void AddRowBuild2Build(TBufferData& buffer, ui32 parent, TArrayRef<const TCell> key, const NTable::TRowState& row);

void AddRowBuild2Posting(TBufferData& buffer, ui32 parent, TArrayRef<const TCell> key, const NTable::TRowState& row,
                         ui32 dataPos);

TTags MakeUploadTags(const TUserTable& table, const TProtoStringType& embedding,
                     const google::protobuf::RepeatedPtrField<TProtoStringType>& data, ui32& embeddingPos,
                     ui32& dataPos, NTable::TTag& embeddingTag);

std::shared_ptr<NTxProxy::TUploadTypes>
MakeUploadTypes(const TUserTable& table, NKikimrTxDataShard::TEvLocalKMeansRequest::EState uploadState,
                const TProtoStringType& embedding, const google::protobuf::RepeatedPtrField<TProtoStringType>& data);

void MakeScan(auto& record, const auto& createScan, const auto& badRequest)
{
    if (!record.HasEmbeddingColumn()) {
        badRequest("Should be specified embedding column");
        return;
    }

    const auto& settings = record.GetSettings();
    if (settings.vector_dimension() < 1) {
        badRequest("Dimension of vector should be at least one");
        return;
    }

    auto handleType = [&]<template <typename...> typename T>() {
        switch (settings.vector_type()) {
            case Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT:
                return createScan.template operator()<T<float>>();
            case Ydb::Table::VectorIndexSettings::VECTOR_TYPE_UINT8:
                return createScan.template operator()<T<ui8>>();
            case Ydb::Table::VectorIndexSettings::VECTOR_TYPE_INT8:
                return createScan.template operator()<T<i8>>();
            case Ydb::Table::VectorIndexSettings::VECTOR_TYPE_BIT:
                return badRequest("TODO(mbkkt) bit vector type is not supported");
            default:
                return badRequest("Wrong vector type");
        }
    };

    // TODO(mbkkt) unify distance and similarity to single field in proto
    switch (settings.metric()) {
        case Ydb::Table::VectorIndexSettings::SIMILARITY_INNER_PRODUCT:
            handleType.template operator()<TMaxInnerProductSimilarity>();
            break;
        case Ydb::Table::VectorIndexSettings::SIMILARITY_COSINE:
        case Ydb::Table::VectorIndexSettings::DISTANCE_COSINE:
            // We don't need to have separate implementation for distance,
            // because clusters will be same as for similarity
            handleType.template operator()<TCosineSimilarity>();
            break;
        case Ydb::Table::VectorIndexSettings::DISTANCE_MANHATTAN:
            handleType.template operator()<TL1Distance>();
            break;
        case Ydb::Table::VectorIndexSettings::DISTANCE_EUCLIDEAN:
            handleType.template operator()<TL2Distance>();
            break;
        default:
            badRequest("Wrong similarity");
            return;
    }
}

}
