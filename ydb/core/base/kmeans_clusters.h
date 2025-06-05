#pragma once

#include <ydb/core/scheme/scheme_tablecell.h>

#include <ydb/public/api/protos/ydb_table.pb.h>

namespace NKikimr::NKMeans {

class IClusters {
public:
    virtual ~IClusters() = default;

    virtual void Init(ui32 k, ui32 maxRounds) = 0;

    virtual void SetRound(ui32 round) = 0;

    virtual ui32 GetK() const = 0;

    virtual TString Debug() const = 0;

    virtual const TVector<TString>& GetClusters() const = 0;

    virtual const TVector<ui64>& GetClusterSizes() const = 0;

    virtual const TVector<ui64>& GetNewClusterSizes() const = 0;

    virtual void SetOldClusterSize(ui32 num, ui64 size) = 0;

    virtual void Clear() = 0;

    virtual bool SetClusters(TVector<TString> && newClusters) = 0;

    virtual void InitAggregatedClusters() = 0;

    virtual void ResetAggregatedClusters() = 0;

    virtual void RecomputeNoClear() = 0;

    virtual bool RecomputeClusters() = 0;

    virtual void RemoveEmptyClusters() = 0;

    virtual std::optional<ui32> FindCluster(TArrayRef<const TCell> row, ui32 embeddingPos) = 0;

    virtual void AggregateToCluster(ui32 pos, const char* embedding) = 0;

    virtual void AddAggregatedCluster(ui32 pos, const TString& embedding, ui64 size) = 0;

    virtual bool IsExpectedSize(TArrayRef<const char> data) = 0;
};

std::unique_ptr<IClusters> CreateClusters(const Ydb::Table::VectorIndexSettings& settings, TString& error);

}
