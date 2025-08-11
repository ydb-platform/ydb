#pragma once

#include "common_helper.h"
#include <ydb/core/base/kmeans_clusters.h>
#include <ydb/core/base/table_index.h>
#include <ydb/core/tx/datashard/buffer_data.h>
#include <ydb/core/tx/datashard/datashard_user_table.h>
#include <ydb/core/tx/datashard/range_ops.h>
#include <ydb/core/tx/datashard/scan_common.h>
#include <ydb/core/tablet_flat/flat_scan_lead.h>
#include <ydb/core/protos/tx_datashard.pb.h>

#include <ydb/public/api/protos/ydb_table.pb.h>

namespace NKikimr::NDataShard::NKMeans {

using NKikimr::NKMeans::IClusters;

TTableRange CreateRangeFrom(const TUserTable& table, TClusterId parent, TCell& from, TCell& to);

NTable::TLead CreateLeadFrom(const TTableRange& range);

void AddRowToLevel(TBufferData& buffer, TClusterId parent, TClusterId child, const TString& embedding, bool isPostingLevel);

void AddRowToData(TBufferData& buffer, TClusterId parent, TArrayRef<const TCell> sourcePk,
    TArrayRef<const TCell> dataColumns, TArrayRef<const TCell> origKey, bool isPostingLevel);

TTags MakeScanTags(const TUserTable& table, const TProtoStringType& embedding,
    const google::protobuf::RepeatedPtrField<TProtoStringType>& data, ui32& embeddingPos,
    ui32& dataPos, NTable::TTag& embeddingTag);

std::shared_ptr<NTxProxy::TUploadTypes> MakeOutputTypes(const TUserTable& table, NKikimrTxDataShard::EKMeansState uploadState,
    const TProtoStringType& embedding, const google::protobuf::RepeatedPtrField<TProtoStringType>& data,
    const google::protobuf::RepeatedPtrField<TProtoStringType>& pkColumns = {});

class TSampler {
    struct TProbability {
        ui64 P = 0;
        ui64 I = 0;

        auto operator<=>(const TProbability&) const = default;
    };

    ui32 K = 0;
    TReallyFastRng32 Rng;
    ui64 MaxProbability = 0;

    // We are using binary heap, because we don't want to do batch processing here,
    // serialization is more expensive than compare
    TVector<TProbability> MaxRows;
    TVector<TString> DataRows;

public:

    TSampler(ui32 k, ui64 seed,  ui64 maxProbability = Max<ui64>())
        : K(k)
        , Rng(seed)
        , MaxProbability(maxProbability)

    {}

    void Add(const auto& getValue) {
        const auto probability = GetProbability();
        if (probability > MaxProbability) {
            return;
        }

        if (DataRows.size() < K) {
            MaxRows.push_back({probability, DataRows.size()});
            DataRows.emplace_back(getValue());
            if (DataRows.size() == K) {
                std::make_heap(MaxRows.begin(), MaxRows.end());
                MaxProbability = MaxRows.front().P;
            }
        } else {
            // TODO(mbkkt) use tournament tree to make less compare and swaps
            std::pop_heap(MaxRows.begin(), MaxRows.end());
            DataRows[MaxRows.back().I] = getValue();
            MaxRows.back().P = probability;
            std::push_heap(MaxRows.begin(), MaxRows.end());
            MaxProbability = MaxRows.front().P;
        }
    }

    std::pair<TVector<TProbability>, TVector<TString>> Finish() {
        MaxProbability = Max<ui64>();
        return {
            std::exchange(MaxRows, {}),
            std::exchange(DataRows, {})
        };
    }

    ui64 GetMaxProbability() const {
        return MaxProbability;
    }

    TString Debug() const {
        return TStringBuilder() << "Sample: " << DataRows.size();
    }

private:
    ui64 GetProbability() {
        while (true) {
            auto p = Rng.GenRand64();
            // We exclude max ui64 from generated probabilities, so we can use this value as initial max
            if (Y_LIKELY(p != std::numeric_limits<ui64>::max())) {
                return p;
            }
        }
    }
};

}
