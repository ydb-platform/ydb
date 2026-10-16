#include "object_storage.h"

#include <ydb/library/yql/providers/common/http_gateway/yql_http_gateway.h>
#include <library/cpp/testing/unittest/registar.h>
#include <util/string/builder.h>

namespace NKikimr::NExternalSource {
namespace {

class TListingGateway : public NYql::IHTTPGateway {
public:
    TVector<TString> Responses;
    TVector<TString> Requests;
    TVector<THeaders> RequestHeaders;
    long ResponseCode = 200;
    NYql::TIssues Issues;

    void Download(TString url, THeaders headers, size_t, size_t, TOnResult callback, TString,
                  TRetryPolicy::TPtr, NYql::IHttpRequestContext::TPtr) override {
        Requests.push_back(std::move(url));
        RequestHeaders.push_back(std::move(headers));
        if (Issues) {
            callback(TResult(CURLE_COULDNT_CONNECT, Issues));
            return;
        }
        UNIT_ASSERT_LE(Requests.size(), Responses.size());
        callback(TResult(TContent(Responses[Requests.size() - 1], ResponseCode)));
    }

    void Upload(TString, THeaders, TString, TOnResult, bool, TRetryPolicy::TPtr, NYql::IHttpRequestContext::TPtr) override {
        UNIT_FAIL("Unexpected upload");
    }

    void Delete(TString, THeaders, TOnResult, TRetryPolicy::TPtr, NYql::IHttpRequestContext::TPtr) override {
        UNIT_FAIL("Unexpected delete");
    }

    TCancelHook Download(TString, THeaders, size_t, size_t, TOnDownloadStart, TOnNewDataPart, TOnDownloadFinish,
                        const NMonitoring::TDynamicCounters::TCounterPtr&, NYql::IHttpRequestContext::TPtr) override {
        UNIT_FAIL("Unexpected streaming download");
        return {};
    }

    ui64 GetBuffersSizePerStream() override { return 0; }
    void UpdatePoolCaps(THashMap<NYql::NDq::TWorkScope, size_t>) override {}
};

TString Listing(const TVector<TString>& keys = {}, bool truncated = false, const TVector<TString>& prefixes = {}) {
    TStringBuilder xml;
    xml << "<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"
        << "<IsTruncated>" << (truncated ? "true" : "false") << "</IsTruncated>"
        << "<MaxKeys>1000</MaxKeys><KeyCount>" << keys.size() + prefixes.size() << "</KeyCount>";
    if (truncated) {
        xml << "<NextContinuationToken>next-page</NextContinuationToken>";
    }
    for (const auto& key : keys) {
        xml << "<Contents><Key>" << key << "</Key><Size>0</Size></Contents>";
    }
    for (const auto& prefix : prefixes) {
        xml << "<CommonPrefixes><Prefix>" << prefix << "</Prefix></CommonPrefixes>";
    }
    return xml << "</ListBucketResult>";
}

NThreading::TFuture<void> Validate(const TString& location, const std::shared_ptr<TListingGateway>& gateway,
                                 TAuth auth = NAuth::MakeNone(), const std::vector<TRegExMatch>& hostnamePatterns = {}) {
    auto source = CreateObjectStorageExternalSource(hostnamePatterns, nullptr, 1000,
        NYql::CreateStructuredTokenCredentialsFactory(), false, false, gateway);
    TMetadata metadata;
    metadata.DataSourceLocation = "https://storage.example/bucket/";
    metadata.TableLocation = location;
    metadata.Auth = std::move(auth);
    return source->ValidateExternalTable(metadata);
}

} // namespace

Y_UNIT_TEST_SUITE(ObjectStorageLocationTest) {
    Y_UNIT_TEST(ExactFileIncludingEmptyFile) {
        auto gateway = std::make_shared<TListingGateway>();
        gateway->Responses = {Listing({"dir/file.csv"})};
        Validate("/dir/file.csv", gateway).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(gateway->Requests.size(), 1);
        UNIT_ASSERT_STRING_CONTAINS(gateway->Requests[0], "prefix=dir%2Ffile.csv");
    }

    Y_UNIT_TEST(FilePrefixIsNotAnExactMatch) {
        auto gateway = std::make_shared<TListingGateway>();
        gateway->Responses = {Listing({"dir/file.csv.backup"})};
        UNIT_ASSERT_EXCEPTION_CONTAINS(Validate("dir/file.csv", gateway).GetValueSync(),
            TExternalSourceException, "Location does not exist");
    }

    Y_UNIT_TEST(WildcardMatchOnLaterPage) {
        auto gateway = std::make_shared<TListingGateway>();
        gateway->Responses = {Listing({"dir/a.txt"}, true), Listing({"dir/b.csv"}, true)};
        Validate("dir/*.{csv,json}", gateway).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(gateway->Requests.size(), 2);
        UNIT_ASSERT_STRING_CONTAINS(gateway->Requests[1], "continuation-token=next-page");
    }

    Y_UNIT_TEST(WildcardWithoutMatches) {
        auto gateway = std::make_shared<TListingGateway>();
        gateway->Responses = {Listing({"dir/a.txt"}, true), Listing({"dir/b.txt"})};
        UNIT_ASSERT_EXCEPTION_CONTAINS(Validate("dir/*.csv", gateway).GetValueSync(),
            TExternalSourceException, "no objects match the location pattern");
        UNIT_ASSERT_VALUES_EQUAL(gateway->Requests.size(), 2);
    }

    Y_UNIT_TEST(DirectoryWithFilesOrSubdirectoriesOrMarker) {
        for (const auto& response : {Listing({"dir/file"}), Listing({}, false, {"dir/subdir/"}), Listing({"dir/"})}) {
            auto gateway = std::make_shared<TListingGateway>();
            gateway->Responses = {response};
            Validate("/dir/", gateway).GetValueSync();
            UNIT_ASSERT_STRING_CONTAINS(gateway->Requests[0], "delimiter=%2F");
            UNIT_ASSERT_STRING_CONTAINS(gateway->Requests[0], "prefix=dir%2F");
        }
    }

    Y_UNIT_TEST(DirectoryMarkerDoesNotMatchFilePattern) {
        auto gateway = std::make_shared<TListingGateway>();
        gateway->Responses = {Listing({"dir/"})};
        UNIT_ASSERT_EXCEPTION_CONTAINS(Validate("dir/*", gateway).GetValueSync(),
            TExternalSourceException, "Location does not exist");
    }

    Y_UNIT_TEST(EmptyBucketExists) {
        auto gateway = std::make_shared<TListingGateway>();
        gateway->Responses = {Listing()};
        Validate("/", gateway).GetValueSync();
    }

    Y_UNIT_TEST(MissingDirectoryNamesLocationAndBucket) {
        auto gateway = std::make_shared<TListingGateway>();
        gateway->Responses = {Listing()};
        UNIT_ASSERT_EXCEPTION_CONTAINS(Validate("/missing/", gateway).GetValueSync(), TExternalSourceException,
            "Failed to validate LOCATION '/missing/' in 'https://storage.example/bucket/': Location does not exist");
    }

    Y_UNIT_TEST(S3ErrorsArePreserved) {
        for (const TString code : {"AccessDenied", "InvalidAccessKeyId", "NoSuchBucket"}) {
            auto gateway = std::make_shared<TListingGateway>();
            gateway->ResponseCode = code == "NoSuchBucket" ? 404 : 403;
            gateway->Responses = {TStringBuilder() << "<Error><Code>" << code
                << "</Code><Message>Cannot list bucket</Message></Error>"};
            UNIT_ASSERT_EXCEPTION_CONTAINS(Validate("/", gateway).GetValueSync(), TExternalSourceException, code);
        }
    }

    Y_UNIT_TEST(TransportErrorsArePreserved) {
        auto gateway = std::make_shared<TListingGateway>();
        gateway->Issues.AddIssue("Connection failed");
        UNIT_ASSERT_EXCEPTION_CONTAINS(Validate("/", gateway).GetValueSync(), TExternalSourceException, "Connection failed");
    }

    Y_UNIT_TEST(InvalidLocationDoesNotSendRequests) {
        for (const TString location : {"", "dir/{csv"}) {
            auto gateway = std::make_shared<TListingGateway>();
            UNIT_ASSERT_EXCEPTION(Validate(location, gateway).GetValueSync(), TExternalSourceException);
            UNIT_ASSERT(gateway->Requests.empty());
        }
    }

    Y_UNIT_TEST(AwsCredentialsAreUsedForListing) {
        auto gateway = std::make_shared<TListingGateway>();
        gateway->Responses = {Listing()};
        Validate("/", gateway, NAuth::MakeAws("test-access-key", "test-secret-key", "test-region")).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(gateway->RequestHeaders.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(gateway->RequestHeaders[0].Options.UserPwd, "test-access-key:test-secret-key");
        UNIT_ASSERT_VALUES_EQUAL(gateway->RequestHeaders[0].Options.AwsSigV4, "aws:amz:test-region:s3");
    }

    Y_UNIT_TEST(DisallowedHostnameDoesNotSendRequests) {
        auto gateway = std::make_shared<TListingGateway>();
        std::vector<TRegExMatch> patterns;
        patterns.emplace_back("^allowed[.]example$");
        UNIT_ASSERT_EXCEPTION_CONTAINS(Validate("/", gateway, NAuth::MakeNone(), patterns).GetValueSync(),
            TExternalSourceException, "It is not allowed to access hostname 'storage.example'");
        UNIT_ASSERT(gateway->Requests.empty());
    }
}

} // namespace NKikimr::NExternalSource
