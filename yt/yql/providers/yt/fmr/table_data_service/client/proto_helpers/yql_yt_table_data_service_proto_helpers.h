#include <yt/yql/providers/yt/fmr/table_data_service/local/interface/yql_yt_table_data_service_local_interface.h>

#include <yt/yql/providers/yt/fmr/proto/table_data_service.pb.h>

namespace NYql::NFmr {

NProto::TTableDataServiceGroupDeletionRequest TTableDataServiceGroupDeletionRequestToProto(const std::vector<TString>& groups);

std::vector<TString> TTableDataServiceGroupDeletionRequestFromProto(const NProto::TTableDataServiceGroupDeletionRequest& protoRequest);

} // namespace NYql::NFmr
