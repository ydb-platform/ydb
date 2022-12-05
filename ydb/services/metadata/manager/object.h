#pragma once
#include <ydb/public/api/protos/ydb_table.pb.h>
#include <ydb/core/base/appdata.h>

namespace NKikimr::NMetadataManager {

class TBaseObject {
public:
    static Ydb::Table::CreateTableRequest AddHistoryTableScheme(const Ydb::Table::CreateTableRequest& baseScheme, const TString& tableName);

};

template <class TDerived>
class TObject: public TBaseObject {
public:
    static TString GetStorageHistoryTablePath() {
        return TDerived::GetStorageTablePath() + "_history";
    }

    static TString GetStorageTablePath() {
        return "/" + AppData()->TenantName + "/.metadata/" + TDerived::GetInternalStorageTablePath();
    }

    static Ydb::Table::CreateTableRequest AddHistoryTableScheme(const Ydb::Table::CreateTableRequest& baseScheme) {
        return TBaseObject::AddHistoryTableScheme(baseScheme, TDerived::GetStorageHistoryTablePath());
    }
};

}
