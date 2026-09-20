#include "snapshot.h"

namespace NKikimr::NUdfStore {

bool TSnapshot::DoDeserializeFromResultSet(const Ydb::Table::ExecuteQueryResult& rawDataResult) {
    if (rawDataResult.result_sets().size() != 1) {
        return false;
    }
    ParseSnapshotObjects<TUdfModule>(rawDataResult.result_sets()[0], [this](TUdfModule&& module) {
        switch (module.GetType()) {
            case EUdfType::LIBRARY:
                Libraries.emplace(module.GetName(), std::move(module));
                break;
            case EUdfType::WASM:
            case EUdfType::NATIVE_UNSAFE:
                Udfs.emplace(module.GetName(), std::move(module));
                break;
        }
    });
    return true;
}

TString TSnapshot::DoSerializeToString() const {
    TStringBuilder sb;
    sb << "UDFS:";
    for (auto&& [name, udf] : Udfs) {
        sb << udf.SerializeToString();
    }
    sb << " LIBRARIES:";
    for (auto&& [name, library] : Libraries) {
        sb << library.SerializeToString();
    }
    return sb;
}

const TUdfModule* TSnapshot::GetUdfByName(const TString& name) const {
    auto it = Udfs.find(name);
    if (it == Udfs.end()) {
        return nullptr;
    }
    return &it->second;
}

std::vector<TString> TSnapshot::GetUdfNames() const {
    std::vector<TString> result;
    result.reserve(Udfs.size());
    for (auto&& [name, _] : Udfs) {
        result.emplace_back(name);
    }
    return result;
}

const TUdfModule* TSnapshot::GetLibraryByName(const TString& name) const {
    auto it = Libraries.find(name);
    if (it == Libraries.end()) {
        return nullptr;
    }
    return &it->second;
}

std::vector<TString> TSnapshot::GetLibraryNames() const {
    std::vector<TString> result;
    result.reserve(Libraries.size());
    for (auto&& [name, _] : Libraries) {
        result.emplace_back(name);
    }
    return result;
}

} // namespace NKikimr::NUdfStore
