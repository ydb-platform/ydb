#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/credentials/credentials.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/iam/iam.h>

#include <iostream>

using namespace NYdb;
using namespace NYdb::NQuery;

// Mirrors the CLI's TSingleProviderFactory (command.cpp:125-144).
// Forces the driver to use the provider obtained via the parameterless
// CreateProvider(), exactly as the CLI does.
class TForceParameterlessFactory : public ICredentialsProviderFactory {
public:
    explicit TForceParameterlessFactory(TCredentialsProviderFactoryPtr inner)
        : Inner_(std::move(inner))
    {}

    // The parameterless call — delegates to the inner factory's parameterless method.
    TCredentialsProviderPtr CreateProvider() const override {
        if (!Cached_) {
            std::cerr << "  -> calling Inner_->CreateProvider() [parameterless]" << std::endl;
            Cached_ = Inner_->CreateProvider();
        }
        return Cached_;
    }

    // The driver calls this overload (state.cpp:230).  We redirect it to the
    // parameterless version so the provider that actually talks to YDB was
    // created via the parameterless path.
    TCredentialsProviderPtr CreateProvider(std::weak_ptr<ICoreFacility>) const override {
        return CreateProvider();
    }

private:
    TCredentialsProviderFactoryPtr Inner_;
    mutable TCredentialsProviderPtr Cached_;
};

static void PrintUsage(const char* argv0) {
    std::cerr << "Usage: " << argv0 << " <connection-string> <sa-key-file>\n"
              << "\n"
              << "  connection-string  e.g. grpcs://ydb.serverless.yandexcloud.net:2135/?database=/ru-central1/.../...\n"
              << "  sa-key-file        path to Yandex Cloud service account authorized key JSON\n"
              << "                     (create with: yc iam key create --service-account-id <id> -o key.json)\n"
              << "\n"
              << "Example:\n"
              << "  " << argv0
              << " 'grpcs://ydb.serverless.yandexcloud.net:2135/?database=/ru-central1/b1g8skpblkos03malf3s/etnlrmdturk4cs99omn9'"
              << " /path/to/sa-key.json\n";
}

int main(int argc, char** argv) {
    if (argc != 3) {
        PrintUsage(argv[0]);
        return 1;
    }

    const std::string connectionString = argv[1];
    const std::string saKeyFile = argv[2];

    std::cerr << "Connection string: " << connectionString << "\n";
    std::cerr << "SA key file:       " << saKeyFile << "\n";

    // 1. Build the IAM JWT credentials factory from the service account key file.
    //    The key file must be a JSON with fields: id, service_account_id, public_key, private_key.
    //    Create it with: yc iam key create --service-account-id <id> -o key.json
    auto inner = CreateIamJwtFileCredentialsProviderFactory(TIamJwtFilename{.JwtFilename = saKeyFile});

    // 2. Wrap it so the driver is forced through the parameterless CreateProvider().
    auto factory = std::make_shared<TForceParameterlessFactory>(inner);

    // 3. Plug it into the driver.
    //    The connection string constructor handles grpcs:// (TLS) automatically.
    auto config = TDriverConfig(connectionString)
        .SetCredentialsProviderFactory(factory);

    TDriver driver(config);
    TQueryClient client(driver);

    // 4. Run a trivial query — this proves the parameterless-created
    //    provider is actually used for gRPC auth end-to-end.
    std::cerr << "Running SELECT 1...\n";
    auto result = client.ExecuteQuery("SELECT 1", TTxControl::NoTx()).GetValueSync();
    std::cerr << "Query status: " << static_cast<int>(result.GetStatus()) << std::endl;

    driver.Stop(true);

    if (result.IsSuccess()) {
        std::cerr << "PASS" << std::endl;
        return 0;
    }
    std::cerr << "FAIL: " << result.GetIssues().ToString() << std::endl;
    return 1;
}
