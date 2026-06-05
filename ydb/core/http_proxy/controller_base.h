#pragma once

#include "http_req.h"

namespace NKikimr::NHttpProxy {

    class TBaseHttpController: public IHttpController {
    public:
        bool Execute(
            THttpRequestContext&& context,
            THolder<NKikimr::NSQS::TAwsRequestSignV4> signature
        ) const override;

        virtual bool IsEnabled(const NKikimrConfig::THttpProxyConfig&) const = 0;

    protected:
        std::expected<IHttpRequestProcessor*, IHttpController::EError> GetProcessor(
            const TString& name,
            const THttpRequestContext& context
        ) const;

    protected:
        absl::flat_hash_map<TString, std::unique_ptr<IHttpRequestProcessor>> Name2Processor;

    };

} // namespace NKikimr::NHttpProxy
