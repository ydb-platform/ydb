#include "datastreams.h"
#include "http_req.h"
#include "sqs.h"
#include "ymq.h"

#include <ydb/core/protos/config.pb.h>
#include <ydb/core/protos/serverless_proxy_config.pb.h>

#include <util/generic/algorithm.h>

namespace NKikimr::NHttpProxy {

    namespace {

        class TSqsControllerProxy: public IHttpController {
        public:
            THttpResponseData MakeError(MimeTypes contentType, NYdb::EStatus Status, const TStringBuf message, size_t issueCode) const override {
                return GetSqsHttpController()->MakeError(contentType, Status, message, issueCode);
            }

            bool IsPossible(const TStringBuf apiVersion, const NKikimrConfig::TServerlessProxyConfig& config) const override {
                return GetController(config)->IsPossible(apiVersion, config);
            }

            bool Execute(
                THttpRequestContext&& context,
                THolder<NKikimr::NSQS::TAwsRequestSignV4> signature
            ) const override {
                return GetController(context.ServiceConfig)->Execute(std::move(context), std::move(signature));
            }

        private:
            const IHttpController* GetController(const NKikimrConfig::TServerlessProxyConfig& config) const {
                return config.GetHttpConfig().GetYmqEnabled() ? GetYmqHttpController() : GetSqsHttpController();
            }
        };

        TSqsControllerProxy SqsControllerProxyInstance;

        const std::array<const IHttpController*, 2> Controllers = {
            &SqsControllerProxyInstance,
            GetDataStreamsHttpController()
        };

        THttpControllerRegistry Instance;

    } // namespace

    const THttpControllerRegistry& GetHttpControllerRegistry() {
        return Instance;
    }

    const IHttpController* THttpControllerRegistry::GetController(const TStringBuf apiVersion, const NKikimrConfig::TServerlessProxyConfig& config) const {
        for (const auto& controller : Controllers) {
            if (controller->IsPossible(apiVersion, config)) {
                return controller;
            }
        }
        return nullptr;
    }

} // namespace NKikimr::NHttpProxy
