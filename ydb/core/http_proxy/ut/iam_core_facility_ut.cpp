#include <ydb/core/http_proxy/http_service.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/ptr.h>
#include <util/generic/yexception.h>

#include <memory>

namespace {

// Stand-in for the IAM provider: it stores only a weak_ptr, same as
// TGrpcIamCredentialsProvider. GetAuthInfo fails once the last shared_ptr is gone.
class TStubCoreFacility final : public NYdb::ICoreFacility {
public:
    void AddPeriodicTask(NYdb::TPeriodicCb&&, NYdb::TDeadline::Duration) override {
    }

    void PostToResponseQueue(NYdb::TPostTaskCb&&) override {
    }
};

class TWeakCoreFacilityCredentials final : public NYdb::ICredentialsProvider {
public:
    TWeakCoreFacilityCredentials(std::weak_ptr<NYdb::ICoreFacility> facility, bool* stoppedWhileFacilityAlive)
        : Facility_(std::move(facility))
        , StoppedWhileFacilityAlive_(stoppedWhileFacilityAlive)
    {}

    ~TWeakCoreFacilityCredentials() override {
        if (StoppedWhileFacilityAlive_ != nullptr) {
            *StoppedWhileFacilityAlive_ = !Facility_.expired();
        }
    }

    std::string GetAuthInfo() const override {
        if (Facility_.expired()) {
            ythrow yexception() << "IAM-token provider response facility is not available";
        }
        return "token";
    }

    bool IsValid() const override {
        return !Facility_.expired();
    }

private:
    std::weak_ptr<NYdb::ICoreFacility> Facility_;
    bool* StoppedWhileFacilityAlive_;
};

} // namespace

using namespace NActors;
using namespace NKikimr::NHttpProxy;

Y_UNIT_TEST_SUITE(HttpProxyIamCoreFacility) {
    // TIamAuthFactory::Initialize drops its shared_ptr when it returns. The proxy actor
    // must still own it. Dropping the actor must destroy the provider first, while the
    // facility is alive — that is why CoreFacility is declared before CredentialsProvider.
    Y_UNIT_TEST(ActorKeepsCoreFacilityAfterCallerDropsIt) {
        auto facility = std::make_shared<TStubCoreFacility>();
        bool stoppedWhileFacilityAlive = false;
        auto provider = std::make_shared<TWeakCoreFacilityCredentials>(facility, &stoppedWhileFacilityAlive);

        THolder<IActor> actor;
        {
            THttpProxyConfig cfg;
            cfg.CoreFacility = facility;
            cfg.CredentialsProvider = provider;
            actor.Reset(CreateHttpProxy(cfg));
        }

        facility.reset();
        UNIT_ASSERT_VALUES_EQUAL(provider->GetAuthInfo(), "token");

        provider.reset();
        actor.Reset();
        UNIT_ASSERT(stoppedWhileFacilityAlive);
    }
}
