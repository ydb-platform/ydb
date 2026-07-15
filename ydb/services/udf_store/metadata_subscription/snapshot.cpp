#include "snapshot.h"

namespace NKikimr::NUdfStore {

bool TSnapshot::DoDeserializeFromResultSet(const Ydb::Table::ExecuteQueryResult& rawDataResult) {
    Y_ABORT_UNLESS(rawDataResult.result_sets().size() == 2);
    ParseSnapshotObjects<TUdfMeta>(rawDataResult.result_sets()[0], [this](TUdfMeta&& u) {
        Udfs.emplace(u.GetMd5(), std::move(u));
    });
    ParseSnapshotObjects<TUdfLibrarySource>(rawDataResult.result_sets()[1], [this](TUdfLibrarySource&& library) {
        Libraries.emplace(library.GetName(), std::move(library));
    });
    return true;
}

TString TSnapshot::DoSerializeToString() const {
    TStringBuilder sb;
    sb << "UDFS:";
    for (auto&& [md5, udf] : Udfs) {
        sb << udf.SerializeToString();
    }
    sb << " LIBRARIES:";
    for (auto&& [name, library] : Libraries) {
        sb << library.SerializeToString();
    }
    return sb;
}

const TUdfMeta* TSnapshot::GetUdfByMd5(const TString& name) const {
    auto it = Udfs.find(name);
    if (it == Udfs.end()) {
        return nullptr;
    }
    return &it->second;
}

std::vector<TString> TSnapshot::GetUdfMd5s() const {
    std::vector<TString> result;
    result.reserve(Udfs.size());
    for (auto&& [md5, _] : Udfs) {
        result.emplace_back(md5);
    }
    return result;
}

const TUdfLibrarySource* TSnapshot::GetLibraryByName(const TString& name) const {
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
