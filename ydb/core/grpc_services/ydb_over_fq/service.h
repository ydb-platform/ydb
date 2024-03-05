#pragma once

#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/service_fq.h>
#include <ydb/core/fq/libs/events/event_subspace.h>
#include <memory>

namespace NKikimr::NGRpcService {

class IRequestOpCtx;
class IRequestNoOpCtx;
class IFacilityProvider;

namespace NYdbOverFq {

template <typename TMsg, ui32 EMsgType>
class TEvent : public TEventLocal<TEvent<TMsg, EMsgType>, EMsgType> {
public:
    TEvent() = default;

    TEvent(TMsg message)
        : Message{std::move(message)}
    {}

    TMsg Message;
};

enum EEventTypes {
    EvCreateQueryRequest = NFq::YqEventSubspaceBegin(NFq::TYqEventSubspace::TableOverFq),
    EvCreateQueryResponse,
    EvGetQueryStatusRequest,
    EvGetQueryStatusResponse,
    EvDescribeQueryRequest,
    EvDescribeQueryResponse,
    EvGetResultDataRequest,
    EvGetResultDataResponse,
};

#define DEFINE_EVENT(Event) \
using TEv##Event = TEvent<FederatedQuery::Event, Ev##Event>;

#define DEFINE_REQ_RESP(Name) \
DEFINE_EVENT(Name##Request)  \
DEFINE_EVENT(Name##Response) \

DEFINE_REQ_RESP(CreateQuery)
DEFINE_REQ_RESP(GetQueryStatus)
DEFINE_REQ_RESP(DescribeQuery)
DEFINE_REQ_RESP(GetResultData)

#undef DEFINE_REQ_RESP
#undef DEFINE_EVENT

template <typename TReq, typename TResp>
class TGrpcYdbOverFqOpCall
    : public TGrpcRequestOperationCall<TReq, TResp>
    , public TFqPermissionsBase<TReq> {
public:
    using TBase = TGrpcRequestOperationCall<TReq, TResp>;
    using TPermissionsBase = TFqPermissionsBase<TReq>;
    using TPermissionsFunc = typename TPermissionsBase::TPermissionsFunc;
    using TCallback = void(std::unique_ptr<IRequestOpCtx>, const IFacilityProvider&);

    TGrpcYdbOverFqOpCall(NYdbGrpc::IRequestContextBase* ctx, TCallback* cb, NFederatedQuery::TPermissionsVec&& permissions)
        : TBase{ctx, cb, {}}, TPermissionsBase{AsConst(std::move(permissions))}
    {}

    bool TryCustomAttributeProcess(const TSchemeBoardEvents::TDescribeSchemeResult& , ICheckerIface* iface) override {
        TString scope = "yandexcloud:/" + TBase::GetDatabaseName().GetOrElse("/");
        TVector entries = TPermissionsBase::FillSids(scope, *TBase::GetProtoRequest());
        if (entries.empty()) {
            return false;
        }

        iface->SetEntries(entries);
        return true;
    }

private:
    static TPermissionsFunc AsConst(NFederatedQuery::TPermissionsVec&& permissions) {
        return [permissions = std::move(permissions)](const TReq&) {
            return permissions;
        };
    }
};

NFederatedQuery::TPermissionsVec GetCreateSessionPermissions();
NFederatedQuery::TPermissionsVec GetExecuteDataQueryPermissions();

// table
void DoCreateSessionRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoKeepAliveRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoDescribeTableRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
void DoExplainDataQueryRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoExecuteDataQueryRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoListDirectoryRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);

} // namespace NYdbOverFq

} // namespace NKikimr::NGRpcService
