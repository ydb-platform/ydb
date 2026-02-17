#include "yql_yt_table_data_service_proto_helpers.h"

namespace NYql::NFmr {

NProto::TTableDataServicePutResponse TableDataServicePutResponseToProto(bool putResponse) {
    NProto::TTableDataServicePutResponse protoPutResponse;
    protoPutResponse.SetSuccess(putResponse);
    return protoPutResponse;
}

bool TableDataServicePutResponseFromProto(const NProto::TTableDataServicePutResponse& protoPutResponse) {
    return protoPutResponse.GetSuccess();
}

NProto::TTableDataServiceGetResponse TableDataServiceGetResponseToProto(const TMaybe<TString>& getResponse) {
    NProto::TTableDataServiceGetResponse protoGetResponse;
    if (getResponse.Defined()) {
        protoGetResponse.SetData(*getResponse);
    }
    return protoGetResponse;
}

TMaybe<TString> TableDataServiceGetResponseFromProto(const NProto::TTableDataServiceGetResponse& protoGetResponse) {
    TMaybe<TString> getResponse;
    if (protoGetResponse.HasData()) {
        getResponse = protoGetResponse.GetData();
    }
    return getResponse;
}

NProto::TTableDataServiceGroupDeletionRequest TableDataServiceGroupDeletionRequestToProto(const std::vector<TString>& groups) {
    NProto::TTableDataServiceGroupDeletionRequest protoGroups;
    for (auto& group: groups) {
        protoGroups.AddGroups(group);
    }
    return protoGroups;
}

std::vector<TString> TableDataServiceGroupDeletionRequestFromProto(const NProto::TTableDataServiceGroupDeletionRequest& protoGroups) {
    std::vector<TString> groups;
    for (auto& protoGroup: protoGroups.GetGroups()) {
        groups.emplace_back(protoGroup);
    }
    return groups;
}

} // namespace NYql::NFmr
