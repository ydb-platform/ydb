#include "service.h"

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

void IServiceContext::SetRequestInfo()
{
    SetRawRequestInfo(TString(), false);
}

void IServiceContext::SetResponseInfo()
{
    SetRawResponseInfo(TString(), false);
}

void IServiceContext::ReplyFrom(TFuture<TSharedRefArray> asyncMessage)
{
    asyncMessage.Subscribe(BIND([this, this_ = MakeStrong(this)] (const TErrorOr<TSharedRefArray>& result) {
        if (result.IsOK()) {
            Reply(result.Value());
        } else {
            Reply(TError(result));
        }
    }));
    SubscribeCanceled(BIND([asyncMessage = std::move(asyncMessage)] (const TError& error) {
        asyncMessage.Cancel(error);
    }));
}

void IServiceContext::ReplyFrom(TFuture<void> asyncError)
{
    asyncError.Subscribe(BIND([this, this_ = MakeStrong(this)] (const TError& error) {
        Reply(error);
    }));
    SubscribeCanceled(BIND([asyncError = std::move(asyncError)] (const TError& error) {
        asyncError.Cancel(error);
    }));
}

void IServiceContext::ReplyFrom(TFuture<void> asyncError, const IInvokerPtr& invoker)
{
    asyncError.Subscribe(BIND([this, this_ = MakeStrong(this)] (const TError& error) {
        Reply(error);
    })
        .Via(invoker));
    SubscribeCanceled(BIND([asyncError = std::move(asyncError)] (const TError& error) {
        asyncError.Cancel(error);
    }));
}

namespace NDetail {

bool IsClientFeatureSupported(const IServiceContext* context, int featureId)
{
    const auto& header = context->RequestHeader();
    return
        std::find(header.declared_client_feature_ids().begin(), header.declared_client_feature_ids().end(), featureId) !=
        header.declared_client_feature_ids().end();
}

void ThrowUnsupportedClientFeature(int featureId, TStringBuf featureName)
{
    THROW_ERROR_EXCEPTION(
        NRpc::EErrorCode::UnsupportedClientFeature,
        "Client does not support the feature requested by server")
        << TErrorAttribute("feature_id", featureId)
        << TErrorAttribute("feature_name", featureName);
}

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

TServiceId::TServiceId(std::string serviceName, TRealmId realmId)
    : ServiceName(std::move(serviceName))
    , RealmId(realmId)
{ }

void FormatValue(TStringBuilderBase* builder, const TServiceId& id, TStringBuf /*spec*/)
{
    builder->AppendFormat(
        "%v%v",
        id.ServiceName,
        MakeFormatterWrapper([&] (TStringBuilderBase* builder) {
            if (!id.RealmId.IsEmpty()) {
                builder->AppendFormat(":%v", id.RealmId);
            }
        }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
