#pragma once
#include <ydb/core/base/appdata.h>

#include <ydb/public/api/protos/ydb_table.pb.h>
#include <ydb/services/metadata/service.h>

namespace NKikimr::NMetadata::NModifications {

class TBaseObject {
public:
    static Ydb::Table::CreateTableRequest AddHistoryTableScheme(const Ydb::Table::CreateTableRequest& baseScheme, const TString& tableName);
    static NColumnMerger::TMerger MergerFactory(const TString& columnName);

private:
    static bool DefaultColumnMerger(Ydb::Value& self, const Ydb::Value& other);
};

template <class TDerived>
class TObject: public TBaseObject {
public:
    static Ydb::Table::CreateTableRequest AddHistoryTableScheme(const Ydb::Table::CreateTableRequest& baseScheme) {
        return TBaseObject::AddHistoryTableScheme(baseScheme, TDerived::GetBehaviour()->GetStorageHistoryTablePath());
    }
};

}
