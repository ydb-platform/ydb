#pragma once

#include <ydb/library/yql/providers/common/http_gateway/yql_http_gateway.h>

#include <library/cpp/threading/future/future.h>

#include <variant>
#include <vector>
#include <memory>

namespace NYql {

class IS3Lister {
public:
    using TPtr = std::shared_ptr<IS3Lister>;

    struct TListEntry {
        TString Path;
        ui64 Size = 0;
        std::vector<TString> MatchedGlobs;
    };

    using TListEntries = std::vector<TListEntry>;
    using TListResult = std::variant<TListEntries, TIssues>;

    virtual ~IS3Lister() = default;
    // List all S3 objects matching wildcard pattern.
    // Pattern may include following wildcard expressions:
    // * - any (possibly empty) sequence of characters
    // ? - single character
    // {variant1, variant2} - list of alternatives
    virtual NThreading::TFuture<TListResult> List(const TString& token, const TString& url, const TString& pattern) = 0;

    // pattern should be valid RE2 regex
    // pathPrefix is a "constant" path prefix
    virtual NThreading::TFuture<TListResult> ListRegex(const TString& token, const TString& url, const TString& pattern, const TString& pathPrefix) = 0;

    static TPtr Make(const IHTTPGateway::TPtr& httpGateway, ui64 maxFilesPerQuery, ui64 maxInflightListsPerQuery, bool allowLocalFiles);
};

}