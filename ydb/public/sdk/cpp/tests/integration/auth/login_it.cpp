#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>

#include <ydb/public/sdk/cpp/src/client/common_client/impl/client.h>

#include <library/cpp/testing/gtest/gtest.h>

#include <util/datetime/base.h>

#include <jwt-cpp/jwt.h>

using namespace NYdb;
using namespace NYdb::NQuery;

// copy-pasted from <robot/library/utils/time_convert.h>
template<typename Rep, typename Period>
constexpr ui64 ToMicroseconds(std::chrono::duration<Rep, Period> value) {
    return std::chrono::duration_cast<std::chrono::microseconds>(value).count();
}

template<typename Clock, typename Duration>
constexpr TInstant ToInstant(std::chrono::time_point<Clock, Duration> value) {
    return TInstant::MicroSeconds(ToMicroseconds(value.time_since_epoch()));
}

std::chrono::system_clock::time_point GetTokenExpiresAt(const std::string& token) {
    try {
        jwt::decoded_jwt decoded_token = jwt::decode(token);
        return decoded_token.get_expires_at();
    }
    catch (...) {
    }
    return {};
}

class TDummyClient {
    class TImpl;
public:
    TDummyClient(NYdb::TDriver& driver);

    std::shared_ptr<NYdb::ICoreFacility> GetCoreFacility();
private:
    std::shared_ptr<TImpl> Impl_;
};

class TDummyClient::TImpl : public TClientImplCommon<TDummyClient::TImpl> {
public:
    TImpl(std::shared_ptr<TGRpcConnectionsImpl>&& connections, const TCommonClientSettings& settings)
        : TClientImplCommon(std::move(connections), settings)
    {}

    std::shared_ptr<NYdb::ICoreFacility> GetCoreFacility() {
        return DbDriverState_;
    }
};

TDummyClient::TDummyClient(NYdb::TDriver& driver)
    : Impl_(new TImpl(CreateInternalInterface(driver), TCommonClientSettings()))
{}

std::shared_ptr<NYdb::ICoreFacility> TDummyClient::GetCoreFacility() {
    return Impl_->GetCoreFacility();
}

TEST(Login, TokenIsNotExpired) {
    std::string endpoint = std::getenv("YDB_ENDPOINT");
    std::string database = std::getenv("YDB_DATABASE");

    std::cout << "endpoint: " << endpoint << std::endl;
    std::cout << "database: " << database << std::endl;

    TDriver driver(TDriverConfig()
        .SetEndpoint(endpoint)
        .SetDatabase(database));

    TQueryClient client(driver);

    auto status = client.ExecuteQuery("CREATE USER `test` PASSWORD '12345678'", TTxControl::NoTx()).GetValueSync();
    if (!status.IsSuccess()) {
        FAIL() << "Create user failed with status: " << ToString(static_cast<TStatus>(status)) << std::endl;
    }

    status = client.ExecuteQuery(std::format("GRANT ALL ON `/{}` TO `test`", database), TTxControl::NoTx()).GetValueSync();
    if (!status.IsSuccess()) {
        FAIL() << "Grant all failed with status: " << ToString(static_cast<TStatus>(status)) << std::endl;
    }

    TDummyClient dummyClient(driver);

    auto token = CreateLoginCredentialsProviderFactory(TLoginCredentialsParams{
        .User = "test",
        .Password = "12345678"
    })->CreateProvider(dummyClient.GetCoreFacility())->GetAuthInfo();

    ASSERT_LT(TInstant::Now(), ToInstant(GetTokenExpiresAt(token)));
}
