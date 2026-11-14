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

#include <util/random/fast.h>
#include <algorithm>

namespace NKikimr::NDataShard::NKMeans {

using NKikimr::NKMeans::IClusters;
using namespace NTableIndex::NKMeans;

TTableRange CreateRangeFrom(const TUserTable& table, TClusterId parent, TCell& from, TCell& to);

NTable::TLead CreateLeadFrom(const TTableRange& range);

inline bool PrepareLocalKMeansLead(const TUserTable& table,
    const NKikimrTxDataShard::TEvLocalKMeansRequest& request, NTable::TLead* lead, TString& error)
{
    const auto fromParent = request.GetParentFrom();
    const auto toParent = request.GetParentTo();
    TCell from = TCell::Make(fromParent - 1);
    TCell to = TCell::Make(toParent);
    TTableRange parent{{&from, 1}, false, {&to, 1}, true};
    if (!fromParent) {
        if (request.GetUpload() == NKikimrTxDataShard::UPLOAD_BUILD_TO_BUILD ||
            request.GetUpload() == NKikimrTxDataShard::UPLOAD_BUILD_TO_POSTING) {
            error = "Wrong upload for zero parent";
            return false;
        }
        parent = table.Range.ToTableRange();
    } else {
        if (fromParent > toParent) {
            error = TStringBuilder() << "Parent from " << fromParent << " should be less or equal to parent to " << toParent;
            return false;
        }
        if (request.GetUpload() == NKikimrTxDataShard::UPLOAD_MAIN_TO_BUILD ||
            request.GetUpload() == NKikimrTxDataShard::UPLOAD_MAIN_TO_POSTING) {
            error = "Wrong upload for non-zero parent";
            return false;
        }
        const auto range = Intersect(table.KeyColumnTypes, parent, table.Range.ToTableRange());
        if (range.IsEmptyRange(table.KeyColumnTypes)) {
            error = TStringBuilder() << "Requested range doesn't intersect with table range:"
                << " requestedRange: " << DebugPrintRange(table.KeyColumnTypes, parent, *AppData()->TypeRegistry)
                << " tableRange: " << DebugPrintRange(table.KeyColumnTypes, table.Range.ToTableRange(), *AppData()->TypeRegistry)
                << " scanRange: " << DebugPrintRange(table.KeyColumnTypes, range, *AppData()->TypeRegistry);
            return false;
        }
    }
    if (request.HasKeyRange()) {
        TSerializedTableRange resume;
        resume.Load(request.GetKeyRange());
        const auto range = Intersect(table.KeyColumnTypes, resume.ToTableRange(),
            Intersect(table.KeyColumnTypes, parent, table.Range.ToTableRange()));
        if (lead) {
            *lead = CreateLeadFrom(range);
        }
    } else if (lead) {
        if (!fromParent) {
            lead->To({}, NTable::ESeek::Lower);
        } else {
            lead->To(parent.From, NTable::ESeek::Upper);
            lead->Until(parent.To, true);
        }
    }
    return true;
}

template<class TRequest>
bool PrepareParentKMeansLead(const TUserTable& table, const TRequest& request, NTable::TLead* lead, TString& error) {
    if constexpr (requires { request.GetUpload(); }) {
        const bool main = request.GetUpload() == NKikimrTxDataShard::UPLOAD_MAIN_TO_BUILD ||
            request.GetUpload() == NKikimrTxDataShard::UPLOAD_MAIN_TO_POSTING;
        const bool build = request.GetUpload() == NKikimrTxDataShard::UPLOAD_BUILD_TO_BUILD ||
            request.GetUpload() == NKikimrTxDataShard::UPLOAD_BUILD_TO_POSTING;
        if ((main && request.GetParent()) || (build && !request.GetParent())) {
            error = main ? "Wrong upload for non-zero parent" : "Wrong upload for zero parent";
            return false;
        }
    }
    TCell from, to;
    const auto parent = CreateRangeFrom(table, request.GetParent(), from, to);
    if (request.GetParent() && parent.IsEmptyRange(table.KeyColumnTypes)) {
        error = " requested range doesn't intersect with table range";
        return false;
    }
    if constexpr (requires { request.HasKeyRange(); }) {
        if (request.HasKeyRange()) {
            TSerializedTableRange resume;
            resume.Load(request.GetKeyRange());
            const auto range = Intersect(table.KeyColumnTypes, resume.ToTableRange(), parent);
            if (lead) {
                *lead = CreateLeadFrom(range);
            }
            return true;
        }
    }
    if (lead) {
        if (!request.GetParent()) {
            lead->To({}, NTable::ESeek::Lower);
        } else {
            *lead = CreateLeadFrom(parent);
        }
    }
    return true;
}

template<class TRequest>
std::unique_ptr<IClusters> PrepareKMeansClusters(const TRequest& request,
    NKikimr::NKMeans::EClustersMode mode, bool load, TString& error)
{
    ui32 rounds = 0;
    if constexpr (requires { request.GetNeedsRounds(); }) {
        rounds = request.GetNeedsRounds();
    }
    auto clusters = NKikimr::NKMeans::CreateClusters(request.GetSettings(), rounds, error, mode);
    if constexpr (requires { request.ClustersSize(); }) {
        if (clusters) {
            if (request.ClustersSize() < 1) {
                error = "Should be requested for at least one cluster";
                return {};
            }
            if (!std::all_of(request.GetClusters().begin(), request.GetClusters().end(), [&](const auto& value) {
                return clusters->IsExpectedFormat({value.data(), value.size()});
            })) {
                error = "Clusters have invalid format";
                return {};
            }
            if (load && !clusters->SetClusters(TVector<TString>{request.GetClusters().begin(), request.GetClusters().end()})) {
                error = "Invalid clusters";
                return {};
            }
        }
    }
    return clusters;
}

void AddRowToLevel(TBufferData& buffer, TClusterId parent, TClusterId child, const TString& embedding, bool isPostingLevel);

void AddRowToData(TBufferData& buffer, TClusterId parent, TArrayRef<const TCell> sourcePk,
    TArrayRef<const TCell> dataColumns, TArrayRef<const TCell> origKey, bool isPostingLevel);

void AddRowToDataWithForeign(TBufferData& buffer, TClusterId parent, TArrayRef<const TCell> sourcePk,
    TArrayRef<const TCell> dataColumns, TArrayRef<const TCell> origKey, bool isForeign, double distance, bool isPostingLevel);

TTags MakeScanTags(const TUserTable& table, const TProtoStringType& embedding,
    const google::protobuf::RepeatedPtrField<TProtoStringType>& data, bool forBuild, NTable::TPos& embeddingPos,
    NTable::TPos& dataPos, NTable::TPos* isForeignPos = nullptr);

std::shared_ptr<NTxProxy::TUploadTypes> MakeOutputTypes(const TUserTable& table, NKikimrTxDataShard::EKMeansState uploadState,
    const TProtoStringType& embedding, const google::protobuf::RepeatedPtrField<TProtoStringType>& data,
    const google::protobuf::RepeatedPtrField<TProtoStringType>& pkColumns = {}, bool withForeignFlag = false);

class TSampler {
    struct TProbability {
        ui64 P = 0;
        ui64 I = 0;

        auto operator<=>(const TProbability&) const = default;
    };

    ui32 K = 0;
    TReallyFastRng32 Rng;
    ui64 MaxProbability = 0;
    ui64 TotalSeen = 0;

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
        ++TotalSeen;
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
        TotalSeen = 0;
        return {
            std::exchange(MaxRows, {}),
            std::exchange(DataRows, {})
        };
    }

    ui64 GetMaxProbability() const {
        return MaxProbability;
    }

    ui64 GetTotalSeen() const {
        return TotalSeen;
    }

    TString Debug() const {
        return TStringBuilder() << "Sample: " << DataRows.size() << " TotalSeen: " << TotalSeen;
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
