#pragma once
#include <ydb/core/base/appdata.h>

#include <ydb/public/api/protos/ydb_table.pb.h>
#include <ydb/services/metadata/service.h>

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
        return "/" + AppData()->TenantName + "/" + NMetadataProvider::TServiceOperator::GetPath() + "/" + TDerived::GetInternalStorageTablePath();
    }

    static Ydb::Table::CreateTableRequest AddHistoryTableScheme(const Ydb::Table::CreateTableRequest& baseScheme) {
        return TBaseObject::AddHistoryTableScheme(baseScheme, TDerived::GetStorageHistoryTablePath());
    }
};

}
