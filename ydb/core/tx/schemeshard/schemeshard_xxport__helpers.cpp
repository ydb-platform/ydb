#include <ydb/public/api/protos/ydb_operation.pb.h>

namespace NKikimr::NSchemeShard {

TString GetUid(const Ydb::Operations::OperationParams& operationParams) {
    const auto& labels = operationParams.labels();
    auto it = labels.find("uid");
    if (it != labels.end()) {
        return it->second;
    }
    return {};
}

}  // NKikimr::NSchemeShard
