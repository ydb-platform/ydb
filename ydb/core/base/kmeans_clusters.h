#pragma once

#include <ydb/core/scheme/scheme_tablecell.h>

namespace Ydb::Table {
    class VectorIndexSettings;
    class KMeansTreeSettings;
}

namespace NKikimr::NTableIndex::NKMeans {
    using TClusterId = ui64;
}

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

    virtual void FindClusters(const TStringBuf embedding, std::vector<std::pair<ui32, double>>& clusters, size_t n, double skipRatio) = 0;

    virtual std::optional<ui32> FindCluster(const TStringBuf embedding) = 0;

    virtual std::optional<ui32> FindCluster(TArrayRef<const TCell> row, ui32 embeddingPos) = 0;

    virtual double CalcDistance(const TStringBuf a, const TStringBuf b) = 0;

    virtual void AggregateToCluster(ui32 pos, const TArrayRef<const char>& embedding, ui64 weight = 1) = 0;

    virtual bool IsExpectedFormat(const TArrayRef<const char>& data) = 0;

    virtual TString GetEmptyRow() const = 0;
};

std::unique_ptr<IClusters> CreateClusters(const Ydb::Table::VectorIndexSettings& settings, ui32 maxRounds, TString& error);

// Auto-detect vector type and dimension from target vector when settings have dimension=0
std::unique_ptr<IClusters> CreateClustersAutoDetect(Ydb::Table::VectorIndexSettings settings, const TStringBuf& targetVector, ui32 maxRounds, TString& error);

bool ValidateSettings(const Ydb::Table::VectorIndexSettings& settings, TString& error);
bool ValidateSettings(const Ydb::Table::KMeansTreeSettings& settings, TString& error);
bool FillSetting(Ydb::Table::KMeansTreeSettings& settings, const TString& name, const TString& value, TString& error);
void FilterOverlapRows(TVector<TSerializedCellVec>& rows, size_t distancePos, ui32 overlapClusters, double overlapRatio);
void FilterOverlapRows(TVector<std::pair<NTableIndex::NKMeans::TClusterId, double>>& rowClusters, ui32 overlapClusters, double overlapRatio);

}
