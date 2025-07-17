#pragma once

#include <yt/yql/providers/yt/fmr/coordinator/interface/yql_yt_coordinator.h>
#include <yt/yql/providers/yt/fmr/proto/coordinator.pb.h>
#include <yt/yql/providers/yt/fmr/request_options/proto_helpers/yql_yt_request_proto_helpers.h>

namespace NYql::NFmr {

NProto::THeartbeatRequest HeartbeatRequestToProto(const THeartbeatRequest& heartbeatRequest);

THeartbeatRequest HeartbeatRequestFromProto(const NProto::THeartbeatRequest protoHeartbeatRequest);

NProto::THeartbeatResponse HeartbeatResponseToProto(const THeartbeatResponse& heartbeatResponse);

THeartbeatResponse HeartbeatResponseFromProto(const NProto::THeartbeatResponse& protoHeartbeatResponse);

NProto::TStartOperationRequest StartOperationRequestToProto(const TStartOperationRequest& startOperationRequest);

TStartOperationRequest StartOperationRequestFromProto(const NProto::TStartOperationRequest& protoStartOperationRequest);

NProto::TStartOperationResponse StartOperationResponseToProto(const TStartOperationResponse& startOperationResponse);

TStartOperationResponse StartOperationResponseFromProto(const NProto::TStartOperationResponse& protoStartOperationResponse);

NProto::TGetOperationResponse GetOperationResponseToProto(const TGetOperationResponse& getOperationResponse);

TGetOperationResponse GetOperationResponseFromProto(const NProto::TGetOperationResponse protoGetOperationReponse);

NProto::TDeleteOperationResponse DeleteOperationResponseToProto(const TDeleteOperationResponse& deleteOperationResponse);

TDeleteOperationResponse DeleteOperationResponseFromProto(const NProto::TDeleteOperationResponse& protoDeleteOperationResponse);

NProto::TGetFmrTableInfoRequest GetFmrTableInfoRequestToProto(const TGetFmrTableInfoRequest& getFmrTableInfoRequest);

TGetFmrTableInfoRequest GetFmrTableInfoRequestFromProto(const NProto::TGetFmrTableInfoRequest& protoGetFmrTableInfoRequest);

NProto::TGetFmrTableInfoResponse GetFmrTableInfoResponseToProto(const TGetFmrTableInfoResponse& getFmrTableInfoResponse);

TGetFmrTableInfoResponse GetFmrTableInfoResponseFromProto(const NProto::TGetFmrTableInfoResponse& protoGetFmrTableInfoResponse);

NProto::TClearSessionRequest ClearSessionRequestToProto(const TClearSessionRequest& request);

TClearSessionRequest ClearSessionRequestFromProto(const NProto::TClearSessionRequest& protoRequest);

} // namespace NYql::NFmr
