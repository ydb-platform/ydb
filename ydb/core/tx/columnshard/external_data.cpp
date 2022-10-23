#include "external_data.h"

namespace NKikimr::NColumnShard {

std::optional<TString> TCSKVSnapshot::GetValue(const TString& key) const {
    auto it = Data.find(key);
    if (it == Data.end()) {
        return {};
    } else {
        return it->second;
    }
}

bool TCSKVSnapshot::DoDeserializeFromResultSet(const Ydb::ResultSet& rawData) {
    if (rawData.columns().size() != 2) {
        Cerr << "incorrect proto columns info" << Endl;
        return false;
    }
    i32 keyIdx = -1;
    i32 valueIdx = -1;
    ui32 idx = 0;
    for (auto&& i : rawData.columns()) {
        if (i.name() == "key") {
            keyIdx = idx;
        } else if (i.name() == "value") {
            valueIdx = idx;
        }
        ++idx;
    }
    if (keyIdx < 0 || valueIdx < 0) {
        Cerr << "incorrect table columns";
        return false;
    }
    for (auto&& r : rawData.rows()) {
        TString key(r.items()[keyIdx].bytes_value());
        TString value(r.items()[valueIdx].bytes_value());
        if (!Data.emplace(key, value).second) {
            Cerr << "keys duplication: " << key;
            return false;
        }
    }
    return true;
}

Ydb::Table::CreateTableRequest TSnapshotConstructor::DoGetTableSchema() const {
    Ydb::Table::CreateTableRequest request;
    request.set_session_id("");
    request.set_path(TablePath);
    request.add_primary_key("key");
    {
        auto& column = *request.add_columns();
        column.set_name("key");
        column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::STRING);
    }
    {
        auto& column = *request.add_columns();
        column.set_name("value");
        column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::STRING);
    }
    return request;
}

}
