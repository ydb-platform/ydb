#include <util/generic/yexception.h>
#include "yql_statistics.h"

#include <library/cpp/json/json_reader.h>
#include <library/cpp/string_utils/base64/base64.h>
#include <util/string/join.h>

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

TString TShufflingOrderingsByJoinLabels::ToString() const
{
        if (ShufflingOrderingsByJoinLabels_.empty()) {
            return "TShufflingOrderingsByJoinLabels{empty}";
        }

        TStringBuilder result;
        result << "TShufflingOrderingsByJoinLabels{" << ShufflingOrderingsByJoinLabels_.size() << " entries: ";

        for (size_t i = 0; i < ShufflingOrderingsByJoinLabels_.size(); ++i) {
            if (i > 0) result << "; ";

            const auto& [joinLabels, shufflings] = ShufflingOrderingsByJoinLabels_[i];
            result << "{" << JoinSeq(", ", joinLabels) << ":" << shufflings.GetState() << "}";
        }

        result << "}";
        return result;
}

std::ostream& NYql::operator<<(std::ostream& os, const TOptimizerStatistics& s) {
    os << "Type: " << ConvertToStatisticsTypeString(s.Type) << ", Nrows: " << s.Nrows
        << ", Ncols: " << s.Ncols << ", ByteSize: " << s.ByteSize << ", Cost: " << s.Cost;

        os << ", Upper aliases: " << "[";
    if (s.Aliases) {

        std::string tmp;
        for (const auto& c: *s.Aliases) {
            tmp.append(c).append(", ");
        }

        if (!tmp.empty()) {
            tmp.pop_back();
            tmp.pop_back();
        }
        os << tmp;
    }
    os << "]";


    if (s.KeyColumns) {
        os << ", keys: ";

        std::string tmp;
        for (const auto& c: s.KeyColumns->Data) {
            tmp.append(c).append(", ");
        }

        if (!tmp.empty()) {
            tmp.pop_back();
            tmp.pop_back();
        }
        os << "[" << tmp << "]";
    }

    if (s.ShuffledByColumns) {
        os << ", shuffled by: ";

        std::string tmp;
        for (const auto& c: s.ShuffledByColumns->Data) {
            tmp.append(c.RelName).append(".").append(c.AttributeName).append(", ");
        }

        if (!tmp.empty()) {
            tmp.pop_back();
            tmp.pop_back();
        }
        os << "[" << tmp << "]";
    }
    os << ", LogicalOrderings (Shufflings) state: " << s.LogicalOrderings.GetState();
    os << ", Init Shuffling: " << s.LogicalOrderings.GetInitOrderingIdx();
    os << ", SortingOrderings (Sortings) state: "   << s.SortingOrderings.GetState();
    os << ", Init Sorting: " << s.SortingOrderings.GetInitOrderingIdx();

    if (s.ReversedSortingOrderings.HasState()) {
        os << ", ReversedSortingOrderings (Sortings) state: "   << s.ReversedSortingOrderings.GetState();
    }

    if (s.SortingOrderingIdx >= 0) {
        os << ", SortingOrderingIdx: " << s.SortingOrderingIdx;
    }

    if (s.ShufflingOrderingIdx >= 0) {
        os << ", ShufflingOrderingIdx: " << s.ShufflingOrderingIdx;
    }

    os << ", Sel: " << s.Selectivity;
    os << ", Storage: " << ConvertToStatisticsTypeString(s.StorageType);
    if (s.SortColumns) {
        os << ", sorted: ";

        std::string tmp;
        for (size_t i = 0; i < s.SortColumns->Columns.size() && i < s.SortColumns->Aliases.size(); i++) {
            auto c = s.SortColumns->Columns[i];
            auto a = s.SortColumns->Aliases[i];
            if (!a.empty()) {
                tmp.append(a).append(".");
            }

            tmp.append(c).append(", ");
        }

        if (!tmp.empty()) {
            tmp.pop_back();
            tmp.pop_back();
        }

        os << tmp;
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
    EStorageType storageType,
    std::shared_ptr<IProviderStatistics> specific)
    : Type(type)
    , Nrows(nrows)
    , Ncols(ncols)
    , ByteSize(byteSize)
    , Cost(cost)
    , Selectivity(1.0)
    , KeyColumns(keyColumns)
    , ColumnStatistics(columnMap)
    , ShuffledByColumns(nullptr)
    , SortColumns(nullptr)
    , StorageType(storageType)
    , Specific(std::move(specific))
    , Labels(nullptr)
    , LogicalOrderings()
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
    auto res = std::make_shared<TOptimizerStatistics>(s.Type, s.Nrows, s.Ncols, s.ByteSize, s.Cost, s.KeyColumns, s.ColumnStatistics, s.StorageType, s.Specific);
    res->SortColumns = s.SortColumns;

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
                cStat.CountMinSketch.reset(NKikimr::TCountMinSketch::FromString(countMinRaw.data(), countMinRaw.size()));
            }
            if (auto eqWidthHistogram = colMap.find("histogram"); eqWidthHistogram != colMap.end()) {
              TString histogramBase64 = eqWidthHistogram->second.GetStringSafe();

              TString histogramBinary{};
              Base64StrictDecode(histogramBase64, histogramBinary);
              auto histogram = std::make_shared<NKikimr::TEqWidthHistogram>(
                  histogramBinary.data(), histogramBinary.size());
              cStat.EqWidthHistogramEstimator =
                  std::make_shared<NKikimr::TEqWidthHistogramEstimator>(histogram);
            }

            res->ColumnStatistics->Data[columnName] = cStat;
        }
    }

    return res;
}
