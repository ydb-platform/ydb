#include "yql_yt_table_data_service_proto_helpers.h"

namespace NYql::NFmr {

NProto::TTableDataServiceGroupDeletionRequest TTableDataServiceGroupDeletionRequestToProto(const std::vector<TString>& groups) {
    NProto::TTableDataServiceGroupDeletionRequest protoGroups;
    for (auto& group: groups) {
        protoGroups.AddGroups(group);
    }
    return protoGroups;
}

std::vector<TString> TTableDataServiceGroupDeletionRequestFromProto(const NProto::TTableDataServiceGroupDeletionRequest& protoGroups) {
    std::vector<TString> groups;
    for (auto& protoGroup: protoGroups.GetGroups()) {
        groups.emplace_back(protoGroup);
    }
    return groups;
}

} // namespace NYql::NFmr
