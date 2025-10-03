#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/iam/iam.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/iam_private/iam.h>

#include <library/cpp/getopt/last_getopt.h>


int main(int argc, char** argv) {
    std::string endpoint;
    std::string database;
    std::string serviceId;
    std::string microserviceId;
    std::string targetServiceAccountId;
    std::string resourceId;
    std::string resourceType;
    std::string iamEndpoint;
    bool useSsl = false;
    NLastGetopt::TOpts opts = NLastGetopt::TOpts::Default();

    opts.AddLongOption('e', "endpoint", "YDB endpoint").Required().RequiredArgument("HOST:PORT").StoreResult(&endpoint);
    opts.AddLongOption('d', "database", "YDB database").Required().RequiredArgument("PATH").StoreResult(&database);

    opts.AddLongOption("ssl", "Use SSL").NoArgument().SetFlag(&useSsl);

    opts.AddLongOption("target-service-account-id", "Target service account id")
        .Required()
        .RequiredArgument("ID")
        .StoreResult(&targetServiceAccountId);

    opts.AddLongOption("service-id", "Service id")
        .RequiredArgument("ID")
        .DefaultValue("ydb")
        .StoreResult(&serviceId);

    opts.AddLongOption("microservice-id", "Microservice id")
        .RequiredArgument("ID")
        .DefaultValue("data-plane")
        .StoreResult(&microserviceId);

    opts.AddLongOption("resource-id", "Resource id")
        .Required()
        .RequiredArgument("ID")
        .StoreResult(&resourceId);

    opts.AddLongOption("iam-endpoint", "IAM endpoint")
        .RequiredArgument("HOST")
        .DefaultValue("iam.api.cloud.yandex.net")
        .StoreResult(&iamEndpoint);

    opts.AddLongOption("resource-type", "Resource type")
        .RequiredArgument("STRING")
        .DefaultValue("resource-manager.cloud")
        .StoreResult(&resourceType);

    opts.SetFreeArgsMin(0);

    NLastGetopt::TOptsParseResult optsResult(&opts, argc, argv);

    NYdb::TIamServiceParams iamParams{
        .SystemServiceAccountCredentials = NYdb::CreateIamCredentialsProviderFactory(),
        .ServiceId = serviceId,
        .MicroserviceId = microserviceId,
        .ResourceId = resourceId,
        .ResourceType = resourceType,
        .TargetServiceAccountId = targetServiceAccountId,
    };

    iamParams.Endpoint = iamEndpoint;

    auto config = NYdb::TDriverConfig()
        .SetEndpoint(endpoint)
        .SetDatabase(database)
        .SetCredentialsProviderFactory(NYdb::CreateIamServiceCredentialsProviderFactory(iamParams));

    if (useSsl) {
        config.UseSecureConnection();
    }

    NYdb::TDriver driver(config);
    NYdb::NQuery::TQueryClient client(driver);

    auto result = client.ExecuteQuery("SELECT 1", NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
    if (!result.IsSuccess()) {
        std::cerr << ToString(static_cast<NYdb::TStatus>(result)) << std::endl;
        return 1;
    }

    auto parser = result.GetResultSetParser(0);
    while (parser.TryNextRow()) {
        std::cout << parser.ColumnParser(0).GetInt32() << std::endl;
    }

    return 0;
}
