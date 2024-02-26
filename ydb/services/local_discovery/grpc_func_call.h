#include <ydb/core/grpc_services/base/base.h>

#include <util/system/hostname.h>

namespace NKikimr {
namespace NGRpcService {

using TFuncCallback = std::function<void(std::unique_ptr<IRequestOpCtx>, const IFacilityProvider&)>;

template <typename TReq, typename TResp>
class TGrpcRequestFunctionCall
    : public std::conditional_t<TProtoHasValidate<TReq>::Value,
            TGRpcRequestValidationWrapperImpl<
                TRpcServices::EvGrpcRuntimeRequest, TReq, TResp, true, TGrpcRequestFunctionCall<TReq, TResp>>,
            TGRpcRequestWrapperImpl<
                TRpcServices::EvGrpcRuntimeRequest, TReq, TResp, true, TGrpcRequestFunctionCall<TReq, TResp>>>
    {
public:
    static constexpr bool IsOp = true;
    static IActor* CreateRpcActor(IRequestOpCtx* msg);
    using TBase = std::conditional_t<TProtoHasValidate<TReq>::Value,
            TGRpcRequestValidationWrapperImpl<
                TRpcServices::EvGrpcRuntimeRequest, TReq, TResp, true, TGrpcRequestFunctionCall<TReq, TResp>>,
            TGRpcRequestWrapperImpl<
                TRpcServices::EvGrpcRuntimeRequest, TReq, TResp, true, TGrpcRequestFunctionCall<TReq, TResp>>>;

    TGrpcRequestFunctionCall(NYdbGrpc::IRequestContextBase* ctx,
        TFuncCallback cb, TRequestAuxSettings auxSettings = {})
        : TBase(ctx)
        , PassMethod(cb)
        , AuxSettings(std::move(auxSettings))
    { }

    void Pass(const IFacilityProvider& facility) override {
        PassMethod(std::move(std::unique_ptr<IRequestOpCtx>(this)), facility);
    }

    TRateLimiterMode GetRlMode() const override {
        return AuxSettings.RlMode;
    }

    bool TryCustomAttributeProcess(const TSchemeBoardEvents::TDescribeSchemeResult& schemeData,
        ICheckerIface* iface) override
    {
        if (!AuxSettings.CustomAttributeProcessor) {
            return false;
        } else {
            AuxSettings.CustomAttributeProcessor(schemeData, iface);
            return true;
        }
    }

    NJaegerTracing::TRequestDiscriminator GetRequestDiscriminator() const override {
        return {
            .RequestType = AuxSettings.RequestType,
            .Database = TBase::GetDatabaseName(),
        };
    }

private:
    TFuncCallback PassMethod;
    const TRequestAuxSettings AuxSettings;
};

} // namespace NGRpcService
} // namespace NKikimr
