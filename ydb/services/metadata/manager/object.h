#pragma once

#include "common.h"

#include <ydb/core/base/appdata.h>

#include <ydb/public/api/protos/ydb_table.pb.h>

namespace NKikimr::NMetadata::NModifications {

class TBaseObject : public IRecordsMerger {
protected:
    virtual IColumnValuesMerger::TPtr BuildMerger(const TString& columnName) const override;

public:
    static Ydb::Table::CreateTableRequest AddHistoryTableScheme(const Ydb::Table::CreateTableRequest& baseScheme, const TString& tableName);
    virtual TConclusionStatus MergeRecords(NInternal::TTableRecord& value, const NInternal::TTableRecord& patch) const override;
};

template <class TDerived>
class TObject: public TBaseObject {
public:
    static Ydb::Table::CreateTableRequest AddHistoryTableScheme(const Ydb::Table::CreateTableRequest& baseScheme) {
        return TBaseObject::AddHistoryTableScheme(baseScheme, TDerived::GetBehaviour()->GetStorageHistoryTablePath());
    }
};

template <typename T>
concept MetadataObject = requires {
    { T::GetTypeId() } -> std::same_as<TString>;
} && std::derived_from<T, NMetadata::NModifications::TObject<T>>;

}
