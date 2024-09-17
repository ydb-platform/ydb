#pragma once

#include <ydb/services/metadata/ds_table/accessor_snapshot_simple.h>
#include <ydb/services/metadata/manager/abstract.h>

#include <util/string/escape.h>

namespace NKikimr::NMetadata::NContainer {

class TDSAccessorHistoryInstant: public NProvider::TDSAccessorSimple {
protected:
    TString GetSelectQuery(const IClassBehaviour::TPtr& manager) const override {
        AFL_VERIFY(manager);
        AFL_VERIFY(manager->GetOperationsManager());
        auto schema = manager->GetOperationsManager()->GetSchema();

        TStringBuilder selectColumns;
        {
            const auto& columns = schema.GetYDBColumns();
            for (auto it = columns.begin(); it != columns.end(); ++it) {
                if (it != columns.begin()) {
                    selectColumns << ", ";
                }
                selectColumns << "SOME(o." << it->name() << ") AS " << it->name();
            }
        }

        TStringBuilder joinCondition;
        {
            const auto& pkColumns = schema.GetPKColumnIds();
            for (auto it = pkColumns.begin(); it != pkColumns.end(); ++it) {
                if (it != pkColumns.begin()) {
                    selectColumns << " AND ";
                }
                selectColumns << "o." << *it << " == h." << *it;
            }
        }

        TStringBuilder groupByColumns;
        {
            const auto& columns = schema.GetYDBColumns();
            for (auto it = columns.begin(); it != columns.end(); ++it) {
                if (it != columns.begin()) {
                    groupByColumns << ", ";
                }
                groupByColumns << "o." << it->name();
            }
        }

        TStringBuilder sb;
        sb << "SELECT " << selectColumns << ", MAX(h.historyInstant) AS historyInstant\n";
        sb << "    FROM `" << EscapeC(manager->GetStorageTablePath()) << "` o\n";
        sb << "    LEFT JOIN `" << EscapeC(manager->GetStorageHistoryTablePath()) << "` h\n";
        sb << "        ON " << joinCondition << "\n";
        sb << "    GROUP BY " << groupByColumns << ";";
        return sb;
    }

public:
    using NProvider::TDSAccessorSimple::TDSAccessorSimple;
};

}   // namespace NKikimr::NMetadata::NContainer
