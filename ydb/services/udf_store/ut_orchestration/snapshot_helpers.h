#pragma once

#include <ydb/services/udf_store/metadata_subscription/snapshot.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NUdfStore {

//! One row of the `modules` snapshot as a test wants to see it. The real
//! snapshot only exists as a parsed result set, so the tests go through the
//! same deserialization the metadata provider would use.
struct TModuleDesc {
    TString Name;
    TString Uid;
    EUdfType Type = EUdfType::WASM;
    TString Manifest;
};

inline TString MakeManifest(const TString& moduleName, const TVector<TString>& requiredLibraries = {}) {
    TStringBuilder sb;
    sb << "{\"module_name\":\"" << moduleName << "\"";
    if (!requiredLibraries.empty()) {
        sb << ",\"required_libraries\":[";
        for (size_t i = 0; i < requiredLibraries.size(); ++i) {
            if (i) {
                sb << ',';
            }
            sb << '"' << requiredLibraries[i] << '"';
        }
        sb << ']';
    }
    // A manifest without callable declarations is rejected outright, so even a
    // scheduling test has to name one function.
    sb << ",\"functions\":[{\"name\":\"f\",\"argument_types\":[],"
       << "\"result_type\":{\"value\":\"int64\",\"tag\":\"concrete_type\"}}]}";
    return sb;
}

inline std::shared_ptr<TSnapshot> MakeSnapshot(const TVector<TModuleDesc>& modules) {
    Ydb::Table::ExecuteQueryResult raw;
    auto& resultSet = *raw.add_result_sets();

    const auto addColumn = [&](const TString& name, Ydb::Type::PrimitiveTypeId type) {
        auto& column = *resultSet.add_columns();
        column.set_name(name);
        column.mutable_type()->set_type_id(type);
    };
    addColumn(TUdfModule::NameColName, Ydb::Type::UTF8);
    addColumn(TUdfModule::UidColName, Ydb::Type::UTF8);
    addColumn(TUdfModule::Md5ColName, Ydb::Type::UTF8);
    addColumn(TUdfModule::SizeColName, Ydb::Type::UINT64);
    addColumn(TUdfModule::TypeColName, Ydb::Type::UTF8);
    addColumn(TUdfModule::ManifestColName, Ydb::Type::JSON);

    for (const auto& module : modules) {
        auto& row = *resultSet.add_rows();
        row.add_items()->set_text_value(module.Name);
        row.add_items()->set_text_value(module.Uid);
        row.add_items()->set_text_value("md5-" + module.Uid);
        row.add_items()->set_uint64_value(1);
        row.add_items()->set_text_value(TUdfModule::TypeToString(module.Type));
        row.add_items()->set_text_value(
            module.Manifest ? module.Manifest : MakeManifest(module.Name));
    }

    auto snapshot = std::make_shared<TSnapshot>(TInstant::Now());
    UNIT_ASSERT(snapshot->DeserializeFromResultSet(raw));
    return snapshot;
}

} // namespace NKikimr::NUdfStore
