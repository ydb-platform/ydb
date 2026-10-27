#pragma once

#include <ydb/core/grpc_services/base/base.h>
#include <ydb/public/api/protos/ydb_udf.pb.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actorid.h>

#include <memory>

namespace NKikimr {
namespace NGRpcService {

class IRequestOpCtx;
class IFacilityProvider;

void DoDeleteModuleRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoListModulesRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoDescribeModuleRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);

template <>
void FillYdbStatus(Ydb::Udf::UploadModuleResponse& response,
    const NYql::TIssues& issues, Ydb::StatusIds::StatusCode status);

// UploadModule uses runtime dispatch through the same request proxy as unary RPCs.
class TEvUploadModuleRequest final
    : public TGRpcRequestBiStreamWrapper<TRpcServices::EvGrpcRuntimeRequest,
        Ydb::Udf::UploadModuleChunk, Ydb::Udf::UploadModuleResponse>
{
public:
    using TGRpcRequestBiStreamWrapper::TGRpcRequestBiStreamWrapper;

    void Pass(const IFacilityProvider& facility) override;
};

}
}
