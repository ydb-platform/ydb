#include "snapshot.h"

namespace NKikimr::NUdfStore {

bool TSnapshot::DoDeserializeFromResultSet(const Ydb::Table::ExecuteQueryResult& rawDataResult) {
    Y_ABORT_UNLESS(rawDataResult.result_sets().size() == 1);
    ParseSnapshotObjects<TUdfMeta>(rawDataResult.result_sets()[0], [this](TUdfMeta&& u) {
        Udfs.emplace(u.GetMd5(), std::move(u));
    });
    return true;
}

TString TSnapshot::DoSerializeToString() const {
    TStringBuilder sb;
    sb << "UDFS:";
    for (auto&& [md5, udf] : Udfs) {
        sb << udf.SerializeToString();
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

} // namespace NKikimr::NUdfStore
