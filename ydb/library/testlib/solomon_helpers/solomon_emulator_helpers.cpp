#include "solomon_emulator_helpers.h"

#include <library/cpp/http/simple/http_client.h>
#include <library/cpp/retry/retry.h>

#include <util/string/builder.h>

namespace NTestUtils {

namespace {

TSimpleHttpClient CreateHttpClient() {
    return TSimpleHttpClient("localhost", std::stoi(getenv("SOLOMON_HTTP_PORT")));
}

TString BuildLocationUrl(const TSolomonLocation& location, const TString& url) {
    auto builder = TStringBuilder() << url;

    if (location.IsCloud) {
        builder << "?folderId=" << location.FolderId << "&service=" << location.Service;
    } else {
        builder << "?project=" << location.ProjectId << "&cluster=" << location.FolderId << "&service=" << location.Service;
    }

    return builder;
}

} // anonymous namespace

void CleanupSolomon(const TSolomonLocation& location) {
    DoWithRetry([url = BuildLocationUrl(location, "/cleanup"), client = CreateHttpClient()]() {
        TStringStream str;
        client.DoPost(url, "", &str);
    }, TRetryOptions(3, TDuration::Seconds(1)), true);
}

TString GetSolomonMetrics(const TSolomonLocation& location) {
    TStringStream str;
    CreateHttpClient().DoGet(BuildLocationUrl(location, "/metrics/get"), &str);
    return str.Str();
}

}  // namespace NTestUtils
