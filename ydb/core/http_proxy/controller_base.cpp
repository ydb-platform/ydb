#include "controller_base.h"

namespace NKikimr::NHttpProxy {

    bool TBaseHttpController::Execute(
        THttpRequestContext&& context,
        THolder<NKikimr::NSQS::TAwsRequestSignV4> signature
    ) const {
        const auto& ctx = TlsActivationContext->AsActorContext();

        const auto name = context.MethodName;
        const auto contentType = context.ContentType;
        const auto apiVersion = context.ApiVersion;

        auto proc = GetProcessor(name, context);
        if (proc.has_value()) {
            proc.value()->Execute(std::move(context), std::move(signature), ctx);
            return true;
        } else {
            switch (proc.error()) {
                case IHttpController::EError::MethodNotFound:
                    context.DoReply(MakeError(contentType, NYdb::EStatus::UNSUPPORTED,
                        TStringBuilder() << "Unknown method name " << name.Quote(),
                        static_cast<size_t>(name.empty() ? NYds::EErrorCodes::MISSING_ACTION : NYds::EErrorCodes::ERROR)));
                    return false;
                case IHttpController::EError::ServiceDisabled:
                    context.DoReply(MakeError(contentType, NYdb::EStatus::BAD_REQUEST,
                        TStringBuilder() << apiVersion << " is disabled", static_cast<size_t>(NYds::EErrorCodes::NOT_FOUND)));
                    return false;
            }
        }

        return false;
    }

    std::expected<IHttpRequestProcessor*, IHttpController::EError> TBaseHttpController::GetProcessor(
        const TString& name,
        const THttpRequestContext& context
    ) const  {
        if (!IsEnabled(context.ServiceConfig.GetHttpConfig())) {
            return std::unexpected(IHttpController::EError::ServiceDisabled);
        }

        if (auto proc = Name2Processor.find(name); proc != Name2Processor.end()) {
            return proc->second.get();
        }

        return std::unexpected(IHttpController::EError::MethodNotFound);
    }

} // namespace NKikimr::NHttpProxy