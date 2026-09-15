#include <yt/yql/providers/yt/fmr/table_data_service/local/interface/yql_yt_table_data_service_local_interface.h>

#include <yt/yql/providers/yt/fmr/proto/table_data_service.pb.h>

namespace NYql::NFmr {

NProto::TTableDataServicePutResponse TableDataServicePutResponseToProto(bool putResponse);

bool TableDataServicePutResponseFromProto(const NProto::TTableDataServicePutResponse& protoPutResponse);

NProto::TTableDataServiceGetResponse TableDataServiceGetResponseToProto(const TMaybe<TString>& getResponse);

TMaybe<TString> TableDataServiceGetResponseFromProto(const NProto::TTableDataServiceGetResponse& protoGetResponse);

NProto::TTableDataServiceGroupDeletionRequest TableDataServiceGroupDeletionRequestToProto(const std::vector<TString>& groups);

std::vector<TString> TableDataServiceGroupDeletionRequestFromProto(const NProto::TTableDataServiceGroupDeletionRequest& protoRequest);

} // namespace NYql::NFmr
