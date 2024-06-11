#include <util/generic/yexception.h>
#include "yql_statistics.h"

#include <library/cpp/json/json_reader.h>

using namespace NYql;

static TString ConvertToStatisticsTypeString(EStatisticsType type) {
    switch (type) {
        case EStatisticsType::BaseTable:
            return "BaseTable";
        case EStatisticsType::FilteredFactTable:
            return "FilteredFactTable";
        case EStatisticsType::ManyManyJoin:
            return "ManyManyJoin";
        default:
            Y_ENSURE(false,"Unknown EStatisticsType");
    }
    return "";
}

std::ostream& NYql::operator<<(std::ostream& os, const TOptimizerStatistics& s) {
    os << "Type: " << ConvertToStatisticsTypeString(s.Type) << ", Nrows: " << s.Nrows
        << ", Ncols: " << s.Ncols << ", ByteSize: " << s.ByteSize << ", Cost: " << s.Cost;
    if (s.KeyColumns) {
        for (const auto& c : s.KeyColumns->Data) {
            os << ", " << c;
        }
    }
    return os;
}

bool TOptimizerStatistics::Empty() const {
    return ! (Nrows || Ncols || Cost);
}

TOptimizerStatistics::TOptimizerStatistics(
    EStatisticsType type,
    double nrows,
    int ncols,
    double byteSize,
    double cost,
    TIntrusivePtr<TKeyColumns> keyColumns,
    TIntrusivePtr<TColumnStatMap> columnMap,
    std::unique_ptr<IProviderStatistics> specific)
    : Type(type)
    , Nrows(nrows)
    , Ncols(ncols)
    , ByteSize(byteSize)
    , Cost(cost)
    , KeyColumns(keyColumns)
    , ColumnStatistics(columnMap)
    , Specific(std::move(specific))
{
}

TOptimizerStatistics& TOptimizerStatistics::operator+=(const TOptimizerStatistics& other) {
    Nrows += other.Nrows;
    Ncols += other.Ncols;
    ByteSize += other.ByteSize;
    Cost += other.Cost;
    return *this;
}

std::shared_ptr<TOptimizerStatistics> NYql::OverrideStatistics(const NYql::TOptimizerStatistics& s, const TStringBuf& tablePath, const TString& statHints) {
    auto res = std::make_shared<TOptimizerStatistics>(s.Type, s.Nrows, s.Ncols, s.ByteSize, s.Cost, s.KeyColumns, s.ColumnStatistics);

    NJson::TJsonValue root;
    NJson::ReadJsonTree(statHints, &root, true);
    auto dbStats = root.GetMapSafe();

    if (!dbStats.contains(tablePath)){
        return res;
    }

    auto tableStats = dbStats.at(tablePath).GetMapSafe();

    if (tableStats.contains("key_columns")) {
        TVector<TString> cols;
        for (auto c : tableStats.at("key_columns").GetArraySafe()) {
            cols.push_back(c.GetStringSafe());
        }
        res->KeyColumns = TIntrusivePtr<TOptimizerStatistics::TKeyColumns>(new TOptimizerStatistics::TKeyColumns(cols));
    }

    if (tableStats.contains("n_rows")) {
        res->Nrows = tableStats.at("n_rows").GetDoubleSafe();
    }
    if (tableStats.contains("byte_size")) {
        res->ByteSize = tableStats.at("byte_size").GetDoubleSafe();
    }
    if (tableStats.contains("n_attrs")) {
        res->Ncols = tableStats.at("n_attrs").GetIntegerSafe();
    }

    if (tableStats.contains("columns")) {
        if (!res->ColumnStatistics) {
            res->ColumnStatistics = TIntrusivePtr<TOptimizerStatistics::TColumnStatMap>(new TOptimizerStatistics::TColumnStatMap());
        }

        for (auto col : tableStats.at("columns").GetArraySafe()) {
            auto colMap = col.GetMapSafe();

            TColumnStatistics cStat;

            auto column_name = colMap.at("name").GetStringSafe();

            if (colMap.contains("n_unique_vals")) {
                cStat.NuniqueVals = colMap.at("n_unique_vals").GetDoubleSafe();
            }
            if (colMap.contains("hyperloglog")) {
                cStat.HyperLogLog = colMap.at("hyperloglog").GetDoubleSafe();
            }

            res->ColumnStatistics->Data[column_name] = cStat;
        }
    }

    return res;
}
