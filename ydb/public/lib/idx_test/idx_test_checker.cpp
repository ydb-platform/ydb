#include "idx_test.h"
#include "idx_test_common.h"

#include <ydb/public/lib/yson_value/ydb_yson_value.h>

#include <util/generic/hash.h>

using namespace NYdb;
using namespace NYdb::NTable;

namespace NIdxTest {

class TChecker : public IChecker {
public:
    TChecker(NYdb::NTable::TTableClient&& client, IProgressTracker::TPtr&& progressTracker)
        : Client_(std::move(client))
        , ProgressTracker_(std::move(progressTracker))
    {}

    void Run(const TString& tableName) {
        DescribeTable(tableName);

        if (!TableDescription_) {
            throw yexception() << "Unable to load table description";
        }
        if (!TableDescription_.GetRef().GetIndexDescriptions()) {
            throw yexception() << "Index metadata was not found";
        }
        CheckIndexes(tableName);
    }

private:
    void CheckIndexData(const TString& tableName,
                        const TIndexDescription& indexDesc,
                        const THashMap<TString, size_t>& indexedColumnsMap,
                        const THashMap<TString, TVector<TValue>>& mainTable) {
        TMaybe<TTablePartIterator> tableIterator;

        auto settings = TReadTableSettings();

        // map indexTableColumn id -> dataTableColumnId
        TVector<size_t> checkColumnsMap;
        checkColumnsMap.resize(indexDesc.GetIndexColumns().size() + indexDesc.GetDataColumns().size());

        THashMap<TString, size_t> indexColumns;

        {
            size_t j = 0;
            for (size_t i = 0; i < indexDesc.GetIndexColumns().size(); i++, j++) {
                const auto& col = indexDesc.GetIndexColumns()[i];
                auto it = indexedColumnsMap.find(col);
                Y_ABORT_UNLESS(it != indexedColumnsMap.end());
                settings.AppendColumns(col);
                checkColumnsMap[j] = it->second;
                indexColumns[col] = i;
            }

            for (size_t i = 0; i < indexDesc.GetDataColumns().size(); i++, j++) {
                const auto& col = indexDesc.GetDataColumns()[i];
                auto it = indexedColumnsMap.find(col);
                Y_ABORT_UNLESS(it != indexedColumnsMap.end());
                settings.AppendColumns(col);
                checkColumnsMap[j] = it->second;
            }
        }


        TVector<size_t> pkColumnIdx;
        size_t includedColumns = 0; // count of PK columns included in the index (before i-th PK column)
        for (size_t i = 0; i < TableDescription_.GetRef().GetPrimaryKeyColumns().size(); i++) {
            const auto& col = TableDescription_.GetRef().GetPrimaryKeyColumns()[i];
            auto it = indexColumns.find(col);
            if (it != indexColumns.end()) {
                // PK column is included in the secondary index
                pkColumnIdx.push_back(it->second);
                ++includedColumns;
            } else {
                settings.AppendColumns(col);
                pkColumnIdx.push_back(checkColumnsMap.size() + i - includedColumns);
            }
        }


        const TString indexTableName = tableName + "/" + indexDesc.GetIndexName() + "/indexImplTable";

        if (ProgressTracker_) {
            ProgressTracker_->Start("rows read", "Reading indexTable table " + indexTableName + "...");
        }
        ThrowOnError(Client_.RetryOperationSync([indexTableName, settings, &tableIterator](TSession session) {
            auto result = session.ReadTable(indexTableName, settings).GetValueSync();

            if (result.IsSuccess()) {
                tableIterator = result;
            }

            return result;
        }));

        size_t totalRows = 0;
        for (;;) {
            auto tablePart = tableIterator->ReadNext().GetValueSync();
            if (!tablePart.IsSuccess()) {
                if (tablePart.EOS()) {
                    break;
                }

                ThrowOnError(tablePart);
            }

            auto rsParser = TResultSetParser(tablePart.ExtractPart());
            while (rsParser.TryNextRow()) {
                if (ProgressTracker_) {
                    ProgressTracker_->Update(totalRows);
                }
                totalRows++;
                TString key;

                for (const auto& id : pkColumnIdx) {
                    auto value = rsParser.GetValue(id);
                    key += NYdb::FormatValueYson(value);
                }

                auto mainTableRow = mainTable.find(key);
                if (mainTableRow == mainTable.end()) {
                    throw yexception() << "index table has unknown key: " << key;
                }

                for (size_t id = 0; id < checkColumnsMap.size(); id++) {
                    auto value = rsParser.GetValue(id);
                    const auto mainTableColumnId = checkColumnsMap[id];

                    const auto& valueFromMain = mainTableRow->second[mainTableColumnId];

                    if (NYdb::FormatValueYson(value) != NYdb::FormatValueYson(valueFromMain)) {
                        throw yexception() << " value missmatch for row with key: " << key;
                    }
                }
            }
        }

        if (totalRows != mainTable.size()) {
            throw yexception() << "rows count missmatch, index table "
                             << indexTableName << " has " << totalRows
                             << " rows, but main table has " << mainTable.size() << " rows.";
        }
        if (ProgressTracker_) {
            ProgressTracker_->Finish("Done.");
        }
    }

    void ReadMainTable(
        const TString& tableName,
        const TVector<TString>& columns,
        const THashMap<TString, size_t>& columnMap,
        THashMap<TString, TVector<TValue>>& buf)
    {
        TMaybe<TTablePartIterator> tableIterator;

        auto settings = TReadTableSettings();

        for (const auto& col : columns) {
            settings.AppendColumns(col);
        }

        if (ProgressTracker_) {
            ProgressTracker_->Start("rows read", "Reading main table " + tableName + "...");
        }

        // columnsMap.size() columns will be readed, but probably there are no pk here
        // so append pk column in this case
        size_t pkPos = columnMap.size();
        TVector<size_t> pkMap;

        for (const auto& col : TableDescription_.GetRef().GetPrimaryKeyColumns()) {
            const auto it = columnMap.find(col);
            if (it == columnMap.end()) {
                settings.AppendColumns(col);
                pkMap.push_back(pkPos);
                pkPos++;
            } else {
                pkMap.push_back(it->second);
            }
        }

        ThrowOnError(Client_.RetryOperationSync([tableName, settings, &tableIterator](TSession session) {
            auto result = session.ReadTable(tableName, settings).GetValueSync();

            if (result.IsSuccess()) {
                tableIterator = result;
            }

            return result;
        }));

        size_t rows = 0;
        for (;;) {
            auto tablePart = tableIterator->ReadNext().GetValueSync();
            if (!tablePart.IsSuccess()) {
                if (tablePart.EOS()) {
                    break;
                }

                ThrowOnError(tablePart);
            }

            auto rsParser = TResultSetParser(tablePart.ExtractPart());

            while (rsParser.TryNextRow()) {
                TString key;
                TVector<TValue> values;

                for (size_t i = 0; i < columns.size(); i++) {
                    values.push_back(rsParser.GetValue(i));
                }

                for (auto i : pkMap) {
                    auto value = rsParser.GetValue(i);
                    key += NYdb::FormatValueYson(value);
                }
                Y_ABORT_UNLESS(buf.insert(std::make_pair(key, values)).second);
                if (ProgressTracker_) {
                    ProgressTracker_->Update(rows++);
                }
            }
        }
        if (ProgressTracker_) {
            ProgressTracker_->Finish("Done.");
        }
    }

    void CheckIndexes(const TString& tableName) {

        // Some magic for case of using same columns for multiple indexes
        // or pk and indexes

        // Columns need to check index
        TVector<TString> indexedColumns;
        // Map column name -> position in result row in data table
        THashMap<TString, size_t> indexedColumnsMap;

        size_t id = 0;
        for (const auto& index : TableDescription_.GetRef().GetIndexDescriptions()) {
            for (const auto& col : index.GetIndexColumns()) {
                auto it = indexedColumnsMap.insert({col, id});
                if (it.second) {
                    indexedColumns.push_back(col);
                    id++;
                }
            }

            for (const auto& col : index.GetDataColumns()) {
                auto it = indexedColumnsMap.insert({col, id});
                if (it.second) {
                    indexedColumns.push_back(col);
                    id++;
                }
            }
        }

        // Data table
        THashMap<TString, TVector<TValue>> mainTable;

        ReadMainTable(tableName, indexedColumns, indexedColumnsMap, mainTable);
        for (const auto& index : TableDescription_.GetRef().GetIndexDescriptions()) {
            CheckIndexData(tableName, index, indexedColumnsMap, mainTable);
        }
    }

    void DescribeTable(const TString& tableName) {
        TableDescription_ = ::NIdxTest::DescribeTable(tableName, Client_);
    }

    TTableClient Client_;
    IProgressTracker::TPtr ProgressTracker_;
    TMaybe<TTableDescription> TableDescription_;
};

IChecker::TPtr CreateChecker(NYdb::TDriver& driver, IProgressTracker::TPtr&& progressTracker) {
    return std::make_unique<TChecker>(TTableClient(driver), std::move(progressTracker));
}

} // namespace NIdxTest
