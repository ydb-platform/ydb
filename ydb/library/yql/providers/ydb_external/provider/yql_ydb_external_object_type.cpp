#include "yql_ydb_external_object_type.h"

#include <ydb/core/base/path.h>
#include <ydb/public/sdk/cpp/adapters/issue/issue.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/scheme/scheme.h>
#include <util/string/builder.h>

namespace NYql::NYdbExternal {
namespace {

NThreading::TFuture<NFq::TExternalObjectKindResult> DescribeObjectType(
    const std::shared_ptr<NYdb::TDriver>& driver,
    const TString& endpoint,
    const TString& database,
    bool useTls,
    const std::shared_ptr<NYdb::ICredentialsProviderFactory>& credentials,
    const TString& path,
    bool requireMessageStream,
    const TDescribePathErrorHandler& onDescribePathError,
    bool addRoot) {
    NYdb::TCommonClientSettings settings;
    settings
        .DiscoveryEndpoint(endpoint)
        .Database(addRoot ? "/Root" + database : database)
        .SslCredentials(NYdb::TSslCredentials(useTls))
        .DiscoveryMode(NYdb::EDiscoveryMode::Async)
        .CredentialsProviderFactory(credentials);
    auto client = std::make_shared<NYdb::NScheme::TSchemeClient>(*driver, settings);

    return client->DescribePath(addRoot ? "/Root" + path : path)
        .Apply([driver, client, endpoint, database, useTls, credentials, path, requireMessageStream, onDescribePathError, addRoot]
            (const NThreading::TFuture<NYdb::NScheme::TDescribePathResult>& future) {
            const auto response = future.GetValue();
            NFq::TExternalObjectKindResult result;
            if (!response.IsSuccess()) {
                if (response.GetStatus() == NYdb::EStatus::CLIENT_UNAUTHENTICATED && !addRoot) {
                    return DescribeObjectType(driver, endpoint, database, useTls, credentials, path,
                        requireMessageStream, onDescribePathError, true);
                }
                const TString message = TStringBuilder() << "Describe path '" << path
                    << "' in external YDB database '" << database << "' with endpoint '" << endpoint << "' failed.";
                auto issue = TIssue(message);
                if (onDescribePathError) {
                    onDescribePathError(message, response.GetIssues().ToString());
                }
                for (const auto& sdkIssue : response.GetIssues()) {
                    issue.AddSubIssue(MakeIntrusive<TIssue>(NYdb::NAdapters::ToYqlIssue(sdkIssue)));
                }
                TIssue rootIssue("Couldn't determine external YDB entity type");
                rootIssue.AddSubIssue(MakeIntrusive<TIssue>(issue));
                result.Issues.AddIssue(rootIssue);
            } else if (response.GetEntry().Type == NYdb::NScheme::ESchemeEntryType::Topic) {
                result.Kind = NFq::EExternalObjectKind::MessageStream;
            } else if (requireMessageStream) {
                result.Issues.AddIssue(TIssue(
                    "External YDB entity is not a topic, and its database is not configured for connector table access"));
            } else {
                result.Kind = NFq::EExternalObjectKind::Table;
            }
            return NThreading::MakeFuture(std::move(result));
        });
}

} // anonymous namespace

NThreading::TFuture<NFq::TExternalObjectKindResult> GetYdbObjectType(
    const std::shared_ptr<NYdb::TDriver>& driver,
    IStructuredTokenCredentialsFactory::TPtr credentialsFactory,
    const TString& endpoint,
    const TString& database,
    bool useTls,
    const TString& structuredToken,
    const TString& path,
    bool requireMessageStream,
    TDescribePathErrorHandler onDescribePathError) {
    NFq::TExternalObjectKindResult result;
    if (!driver || !endpoint || !database) {
        if (requireMessageStream) {
            result.Issues.AddIssue(TIssue(
                "External YDB entity is not a topic, and its database is not configured for connector table access"));
        } else {
            result.Kind = NFq::EExternalObjectKind::Table;
        }
        return NThreading::MakeFuture(std::move(result));
    }
    try {
        Y_ENSURE(credentialsFactory, "External YDB credentials factory is unavailable");
        return DescribeObjectType(driver, endpoint, NKikimr::CanonizePath(database), useTls,
            credentialsFactory->Create(structuredToken), path, requireMessageStream, onDescribePathError, false);
    } catch (const std::exception& error) {
        TIssue rootIssue("Couldn't determine external YDB entity type");
        rootIssue.AddSubIssue(MakeIntrusive<TIssue>(TString(TStringBuilder() << "Failed to get scheme entry type: " << error.what())));
        result.Issues.AddIssue(rootIssue);
        return NThreading::MakeFuture(std::move(result));
    }
}

} // namespace NYql::NYdbExternal
