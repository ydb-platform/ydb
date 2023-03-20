#pragma once

#include <library/cpp/threading/future/future.h>
#include <ydb/library/yql/providers/common/http_gateway/yql_http_gateway.h>

#include <memory>
#include <variant>
#include <vector>

namespace NYql {

template<typename T>
class TIterator {
public:
    virtual T Next() = 0;

    virtual bool HasNext() = 0;

    virtual ~TIterator() = default;
};

namespace NS3Lister {

enum class ES3PatternVariant { FilePattern, PathPattern };

enum class ES3PatternType {
    /**
     * Pattern may include following wildcard expressions:
     * * - any (possibly empty) sequence of characters
     * ? - single character
     * {variant1, variant2} - list of alternatives
     */
    Wildcard,
    /**
     * Pattern should be valid RE2 regex
     */
    Regexp
};

struct TObjectListEntry {
    TString Path;
    ui64 Size = 0;
    std::vector<TString> MatchedGlobs;
};

struct TDirectoryListEntry {
    TString Path;
    bool MatchedRegexp = false;
    std::vector<TString> MatchedGlobs;
};

class TListEntries {
public:
    [[nodiscard]] size_t Size() const { return Objects.size() + Directories.size(); }
    [[nodiscard]] bool Empty() const { return Objects.empty() && Directories.empty(); }

    TListEntries& operator+=(TListEntries&& other) {
        Objects.insert(
            Objects.end(),
            std::make_move_iterator(other.Objects.begin()),
            std::make_move_iterator(other.Objects.end()));
        Directories.insert(
            Directories.end(),
            std::make_move_iterator(other.Directories.begin()),
            std::make_move_iterator(other.Directories.end()));
        return *this;
    }

public:
    std::vector<TObjectListEntry> Objects;
    std::vector<TDirectoryListEntry> Directories;
};

using TListResult = std::variant<TListEntries, TIssues>;

struct TListingRequest {
    TString Url;
    TString Token;
    TString Pattern;
    ES3PatternType PatternType = ES3PatternType::Wildcard;
    TString Prefix;
};

IOutputStream& operator<<(IOutputStream& stream, const TListingRequest& request);

class IS3Lister : public TIterator<NThreading::TFuture<TListResult>> {
public:
    using TPtr = std::shared_ptr<IS3Lister>;
};

IS3Lister::TPtr MakeS3Lister(
    const IHTTPGateway::TPtr& httpGateway,
    const TListingRequest& listingRequest,
    const TMaybe<TString>& delimiter,
    bool allowLocalFiles);

} // namespace NS3Lister
} // namespace NYql
