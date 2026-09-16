#include "yql_pq_deferred_publish_client.h"

namespace NYql {

namespace {

using namespace NYdb;
using namespace NYdb::NTopic;

class TNativeDeferredPublishClient final : public IDeferredPublishClient {
public:
    TNativeDeferredPublishClient(const TDriver& driver, const TCommonClientSettings& settings)
        : Client(driver, settings)
    {}

    TAsyncBeginPublicationResult BeginPublication(const TString& extPublicationId, const TBeginPublicationSettings& settings) final {
        return Client.BeginPublication(extPublicationId, settings);
    }

    TAsyncPublishResult Publish(const TDeferredPublication& publication, const TPublishSettings& settings) final {
        return Client.Publish(publication, settings);
    }

private:
    TDeferredPublishClient Client;
};

} // anonymous namespace

IDeferredPublishClient::TPtr CreateExternalDeferredPublishClient(const TDriver& driver, const TCommonClientSettings& settings) {
    return MakeIntrusive<TNativeDeferredPublishClient>(driver, settings);
}

} // namespace NYql
