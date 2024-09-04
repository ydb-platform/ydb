#include <util/generic/yexception.h>
#include "yql_statistics.h"

#include <library/cpp/json/json_reader.h>
#include <library/cpp/string_utils/base64/base64.h>

#include <sstream>

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

static TString ConvertToStatisticsTypeString(EStorageType storageType) {
    switch (storageType) {
        case EStorageType::NA:
            return "NA";
        case EStorageType::RowStorage:
            return "RowStorage";
        case EStorageType::ColumnStorage:
            return "ColumnStorage";
        default:
            Y_ENSURE(false,"Unknown Storage type");
    }
    return "";
}

TString TOptimizerStatistics::ToString() const {
    std::stringstream ss;
    ss << *this;
    return ss.str();
}

std::ostream& NYql::operator<<(std::ostream& os, const TOptimizerStatistics& s) {
    os << "Type: " << ConvertToStatisticsTypeString(s.Type) << ", Nrows: " << s.Nrows
        << ", Ncols: " << s.Ncols << ", ByteSize: " << s.ByteSize << ", Cost: " << s.Cost;
    if (s.KeyColumns) {
        for (const auto& c : s.KeyColumns->Data) {
            os << ", " << c;
        }
    }
    os << ", Storage: " << ConvertToStatisticsTypeString(s.StorageType);
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
    EStorageType storageType,
    std::unique_ptr<IProviderStatistics> specific)
    : Type(type)
    , Nrows(nrows)
    , Ncols(ncols)
    , ByteSize(byteSize)
    , Cost(cost)
    , KeyColumns(keyColumns)
    , ColumnStatistics(columnMap)
    , StorageType(storageType)
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

std::shared_ptr<TOptimizerStatistics> NYql::OverrideStatistics(const NYql::TOptimizerStatistics& s, const TStringBuf& tablePath, const std::shared_ptr<NJson::TJsonValue>& stats) {
    auto res = std::make_shared<TOptimizerStatistics>(s.Type, s.Nrows, s.Ncols, s.ByteSize, s.Cost, s.KeyColumns, s.ColumnStatistics);

    auto dbStats = stats->GetMapSafe();

    if (!dbStats.contains(tablePath)){
        return res;
    }

    auto tableStats = dbStats.at(tablePath).GetMapSafe();

    if (auto keyCols = tableStats.find("key_columns"); keyCols != tableStats.end()) {
        TVector<TString> cols;
        for (auto c : keyCols->second.GetArraySafe()) {
            cols.push_back(c.GetStringSafe());
        }
        res->KeyColumns = TIntrusivePtr<TOptimizerStatistics::TKeyColumns>(new TOptimizerStatistics::TKeyColumns(cols));
    }

    if (auto nrows = tableStats.find("n_rows"); nrows != tableStats.end()) {
        res->Nrows = nrows->second.GetDoubleSafe();
    }
    if (auto byteSize = tableStats.find("byte_size"); byteSize != tableStats.end()) {
        res->ByteSize = byteSize->second.GetDoubleSafe();
    }
    if (auto nattrs = tableStats.find("n_attrs"); nattrs != tableStats.end()) {
        res->Ncols = nattrs->second.GetIntegerSafe();
    }

    if (auto columns = tableStats.find("columns"); columns != tableStats.end()) {
        if (!res->ColumnStatistics) {
            res->ColumnStatistics = TIntrusivePtr<TOptimizerStatistics::TColumnStatMap>(new TOptimizerStatistics::TColumnStatMap());
        }

        for (auto col : columns->second.GetArraySafe()) {
            auto colMap = col.GetMapSafe();

            TColumnStatistics cStat;

            auto columnName = colMap.at("name").GetStringSafe();

            if (auto numUniqueVals = colMap.find("n_unique_vals"); numUniqueVals != colMap.end()) {
                cStat.NumUniqueVals = numUniqueVals->second.IsNull()? 0.0: numUniqueVals->second.GetDoubleSafe();
            }
            if (auto hll = colMap.find("hyperloglog"); hll != colMap.end()) {
                cStat.HyperLogLog = hll->second.IsNull()? 0.0: hll->second.GetDoubleSafe();
            }
            if (auto countMinSketch = colMap.find("count-min"); countMinSketch != colMap.end()) {
                TString countMinBase64 = countMinSketch->second.GetStringSafe();

                TString countMinRaw{};
                Base64StrictDecode(countMinBase64, countMinRaw);
                
                cStat.CountMinSketch.reset(NKikimr::TCountMinSketch::FromString(countMinRaw.Data(), countMinRaw.Size()));
            }

            res->ColumnStatistics->Data[columnName] = cStat;
        }
    }

    return res;
}
