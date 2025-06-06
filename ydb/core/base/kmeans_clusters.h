#pragma once

#include <ydb/core/base/table_index.h>
#include <ydb/core/tablet_flat/flat_cxx_database.h>

#include <ydb/public/api/protos/ydb_table.pb.h>

namespace NKikimr::NDataShard::NKMeans {

class TClusters {
public:
    static std::unique_ptr<TClusters> Create(const Ydb::Table::VectorIndexSettings& settings, TString& error);

    virtual ~TClusters() = default;

    virtual void Init(ui32 k, ui32 maxRounds) = 0;

    virtual ui32 GetK() const = 0;

    virtual TString Debug() const = 0;

    virtual const TVector<TString>& GetClusters() const = 0;

    virtual void Clear() = 0;

    virtual bool SetClusters(TVector<TString> && newClusters) = 0;

    virtual void InitAggregatedClusters() = 0;

    virtual bool RecomputeClusters() = 0;

    virtual std::optional<ui32> FindCluster(TArrayRef<const TCell> row, ui32 embeddingPos) = 0;

    virtual void AggregateToCluster(ui32 pos, const char* embedding) = 0;

    virtual bool IsExpectedSize(TArrayRef<const char> data) = 0;
};

}
