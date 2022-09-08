#include "yql_s3_list.h"
#include "yql_s3_path.h"

#include <ydb/library/yql/providers/common/http_gateway/yql_http_default_retry_policy.h>
#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/utils/url_builder.h>
#include <ydb/library/yql/utils/yql_panic.h>

#include <contrib/libs/re2/re2/re2.h>

#ifdef THROW
#undef THROW
#endif
#include <library/cpp/xml/document/xml-document.h>
#include <util/string/builder.h>

namespace NYql {

namespace {

TString RegexFromWildcards(const std::string_view& pattern) {
    const auto& escaped = NS3::EscapeRegex(ToString(pattern));
    TStringBuilder result;
    result << "(?s)";
    bool slash = false;
    bool group = false;

    for (const char& c : escaped) {
        switch (c) {
            case '{':
                result << '(';
                group = true;
                slash = false;
                break;
            case '}':
                result << ')';
                group = false;
                slash = false;
                break;
            case ',':
                if (group)
                    result << '|';
                else
                    result << "\\,";
                slash = false;
                break;
            case '\\':
                if (slash)
                    result << "\\\\";
                slash = !slash;
                break;
            case '*':
                result << "(.*)";
                slash = false;
                break;
            case '?':
                result << "(.)";
                slash = false;
                break;
            default:
                if (slash)
                    result << '\\';
                result << c;
                slash = false;
                break;
        }
    }
    return result;
}

using namespace NThreading;

class TS3Lister : public IS3Lister {
public:
    explicit TS3Lister(const IHTTPGateway::TPtr& httpGateway, ui64 maxFilesPerQuery)
        : Gateway(httpGateway)
        , MaxFilesPerQuery(maxFilesPerQuery)
    {}
private:
    using TResultFilter = std::function<bool (const TString& path, TVector<TString>& matchedGlobs)>;

    static TResultFilter MakeFilter(const TString& pattern, const TMaybe<TString>& regexPatternPrefix, TString& prefix) {
        const bool isRegex = regexPatternPrefix.Defined();
        prefix = isRegex ? *regexPatternPrefix : pattern.substr(0, NS3::GetFirstWildcardPos(pattern));
        if (!isRegex && prefix == pattern) {
            // just match for equality
            return [pattern](const TString& path, TVector<TString>& matchedGlobs) {
                matchedGlobs.clear();
                return path == pattern;
            };
        }

        const auto regex = isRegex ? pattern : RegexFromWildcards(pattern);
        auto re = std::make_shared<RE2>(re2::StringPiece(regex), RE2::Options());
        YQL_ENSURE(re->ok());
        YQL_ENSURE(re->NumberOfCapturingGroups() > 0);

        const size_t numGroups = re->NumberOfCapturingGroups();
        YQL_CLOG(INFO, ProviderS3) << "Got prefix: '" << prefix << "', regex: '" << regex
            << "' with " << numGroups << " capture groups from original pattern '" << pattern << "'";

        auto groups = std::make_shared<std::vector<std::string>>(numGroups);
        auto reArgs = std::make_shared<std::vector<re2::RE2::Arg>>(numGroups);
        auto reArgsPtr = std::make_shared<std::vector<re2::RE2::Arg*>>(numGroups);

        for (size_t i = 0; i < size_t(numGroups); ++i) {
            (*reArgs)[i] = &(*groups)[i];
            (*reArgsPtr)[i] = &(*reArgs)[i];
        }

        return [groups, reArgs, reArgsPtr, re](const TString& path, TVector<TString>& matchedGlobs) {
            matchedGlobs.clear();
            bool matched = re2::RE2::FullMatchN(path, *re, reArgsPtr->data(), reArgsPtr->size());
            if (matched) {
                matchedGlobs.reserve(groups->size());
                for (auto& group : *groups) {
                    matchedGlobs.push_back(ToString(group));
                }
            }
            return matched;
        };
    }

    static void OnDiscovery(
        const IHTTPGateway::TWeakPtr& gatewayWeak,
        IHTTPGateway::TResult&& result,
        NThreading::TPromise<IS3Lister::TListResult> promise,
        const std::shared_ptr<IS3Lister::TListEntries>& output,
        const IRetryPolicy<long>::TPtr& retryPolicy,
        const TResultFilter& filter,
        const TString& token,
        const TString& urlStr,
        const TString& prefix,
        ui64 maxDiscoveryFilesPerQuery)
    try {
        auto gateway = gatewayWeak.lock();
        if (!gateway) {
            ythrow yexception() << "Gateway disappeared";
        }
        switch (result.index()) {
            case 0U: {
                const NXml::TDocument xml(std::get<IHTTPGateway::TContent>(std::move(result)).Extract(), NXml::TDocument::String);
                if (const auto& root = xml.Root(); root.Name() == "Error") {
                    const auto& code = root.Node("Code", true).Value<TString>();
                    const auto& message = root.Node("Message", true).Value<TString>();
                    ythrow yexception() << message << ", error: code: " << code << " [" << urlStr << prefix << "]";
                } else if (root.Name() != "ListBucketResult") {
                    ythrow yexception() << "Unexpected response '" << root.Name() << "' on discovery.";
                } else if (
                    const NXml::TNamespacesForXPath nss(1U, {"s3", "http://s3.amazonaws.com/doc/2006-03-01/"});
                    root.Node("s3:KeyCount", false, nss).Value<unsigned>() > 0U)
                {
                    const auto& contents = root.XPath("s3:Contents", false, nss);
                    YQL_CLOG(INFO, ProviderS3) << "Listing of " << urlStr << prefix << ": have " << output->size() << " entries, got another " << contents.size() << " entries";
                    if (maxDiscoveryFilesPerQuery && output->size() + contents.size() > maxDiscoveryFilesPerQuery) {
                        ythrow yexception() << "Over " << maxDiscoveryFilesPerQuery << " files discovered in '" << urlStr << prefix << "'";
                    }

                    for (const auto& content : contents) {
                        TString path = content.Node("s3:Key", false, nss).Value<TString>();
                        if (path.EndsWith('/')) {
                            // skip 'directories'
                            continue;
                        }
                        TVector<TString> matchedGlobs;
                        if (filter(path, matchedGlobs)) {
                            output->emplace_back();
                            output->back().Path = path;
                            output->back().Size = content.Node("s3:Size", false, nss).Value<ui64>();
                            output->back().MatchedGlobs.swap(matchedGlobs);
                        }
                    }

                    if (root.Node("s3:IsTruncated", false, nss).Value<bool>()) {
                        YQL_CLOG(INFO, ProviderS3) << "Listing of " << urlStr << prefix << ": got truncated flag, will continue";
                        const auto& next = root.Node("s3:NextContinuationToken", false, nss).Value<TString>();
                        const auto& maxKeys = root.Node("s3:MaxKeys", false, nss).Value<TString>();

                        IHTTPGateway::THeaders headers;
                        if (!token.empty()) {
                            headers.emplace_back("X-YaCloud-SubjectToken:" + token);
                        }

                        TUrlBuilder urlBuilder(urlStr);
                        auto url = urlBuilder.AddUrlParam("list-type", "2")
                            .AddUrlParam("prefix", prefix)
                            .AddUrlParam("continuation-token", next)
                            .AddUrlParam("max-keys", maxKeys)
                            .Build();

                        return gateway->Download(
                            url,
                            std::move(headers),
                            0U,
                            std::bind(&OnDiscovery,
                                      IHTTPGateway::TWeakPtr(gateway),
                                      std::placeholders::_1,
                                      promise,
                                      output,
                                      retryPolicy,
                                      filter,
                                      token,
                                      urlStr,
                                      prefix,
                                      maxDiscoveryFilesPerQuery),
                            /*data=*/"",
                            retryPolicy);
                    }
                }
                promise.SetValue(std::move(*output));
                break;
            }
            case 1U: {
                auto issues = std::get<TIssues>(std::move(result));
                YQL_CLOG(INFO, ProviderS3) << "Listing of " << urlStr << prefix << ": got error from http gateway: " << issues.ToString(true);
                promise.SetValue(std::move(issues));
                break;
            }
            default:
                ythrow yexception() << "Undefined variant index: " << result.index();
        }
    } catch (const std::exception& ex) {
        YQL_CLOG(INFO, ProviderS3) << "Listing of " << urlStr << prefix << " : got exception: " << ex.what();
        promise.SetException(std::current_exception());
    }


    TFuture<TListResult> DoList(const TString& token, const TString& urlStr, const TString& pattern, const TMaybe<TString>& pathPrefix) {
        TString prefix;
        TResultFilter filter = MakeFilter(pattern, pathPrefix, prefix);

        const auto retryPolicy = GetHTTPDefaultRetryPolicy();
        TUrlBuilder urlBuilder(urlStr);
        const auto url = urlBuilder
            .AddUrlParam("list-type", "2")
            .AddUrlParam("prefix", prefix)
            .Build();

        IHTTPGateway::THeaders headers;
        if (!token.empty()) {
            headers.emplace_back("X-YaCloud-SubjectToken:" + token);
        }

        auto promise = NewPromise<IS3Lister::TListResult>();
        auto future = promise.GetFuture();

        Gateway->Download(
            url,
            std::move(headers),
            0U,
            std::bind(&OnDiscovery,
                      IHTTPGateway::TWeakPtr(Gateway),
                      std::placeholders::_1,
                      promise,
                      std::make_shared<IS3Lister::TListEntries>(),
                      retryPolicy,
                      filter,
                      token,
                      urlStr,
                      prefix,
                      MaxFilesPerQuery),
            /*data=*/"",
            retryPolicy);
        return future;
    }

    NThreading::TFuture<TListResult> List(const TString& token, const TString& url, const TString& pattern) override {
        YQL_CLOG(INFO, ProviderS3) << "Enumerating items using glob pattern " << url << pattern;
        return DoList(token, url, pattern, {});
    }

    NThreading::TFuture<TListResult> ListRegex(const TString& token, const TString& url, const TString& pattern, const TString& pathPrefix) override {
        YQL_CLOG(INFO, ProviderS3) << "Enumerating items using RE2 pattern " << url << pattern;
        return DoList(token, url, pattern, pathPrefix);
    }

    const IHTTPGateway::TPtr Gateway;
    const ui64 MaxFilesPerQuery;
};


}

IS3Lister::TPtr IS3Lister::Make(const IHTTPGateway::TPtr& httpGateway, ui64 maxFilesPerQuery) {
    return IS3Lister::TPtr(new TS3Lister(httpGateway, maxFilesPerQuery));
}

}
