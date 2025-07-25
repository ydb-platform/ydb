#pragma once

#include <ydb/core/scheme/scheme_tablecell.h>

#include <ydb/public/api/protos/ydb_table.pb.h>

namespace NKikimr::NKMeans {

class IClusters {
public:
    virtual ~IClusters() = default;

    virtual void SetRound(ui32 round) = 0;

    virtual TString Debug() const = 0;

    virtual const TVector<TString>& GetClusters() const = 0;

    virtual const TVector<ui64>& GetClusterSizes() const = 0;

    virtual const TVector<ui64>& GetNextClusterSizes() const = 0;

    virtual void SetClusterSize(ui32 num, ui64 size) = 0;

    virtual void Clear() = 0;

    virtual bool SetClusters(TVector<TString> && newClusters) = 0;

    virtual bool RecomputeClusters() = 0;

    virtual bool NextRound() = 0;

    virtual void RemoveEmptyClusters() = 0;

    virtual std::optional<ui32> FindCluster(TArrayRef<const char> embedding) = 0;

    virtual std::optional<ui32> FindCluster(TArrayRef<const TCell> row, ui32 embeddingPos) = 0;

    virtual void AggregateToCluster(ui32 pos, const TArrayRef<const char>& embedding, ui64 weight = 1) = 0;

    virtual bool IsExpectedSize(const TArrayRef<const char>& data) = 0;
};

std::unique_ptr<IClusters> CreateClusters(const Ydb::Table::VectorIndexSettings& settings, ui32 maxRounds, TString& error);

}
