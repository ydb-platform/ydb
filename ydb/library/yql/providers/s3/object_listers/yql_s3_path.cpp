#include "yql_s3_path.h"

#include <ydb/library/yql/providers/s3/object_listers/yql_s3_list.h>
#include <ydb/library/yql/utils/yql_panic.h>

#include <contrib/libs/re2/re2/re2.h>

namespace NYql::NS3 {

TString NormalizePath(const TString& path, char slash) {
    if (path.empty()) {
        throw yexception() << "Path should not be empty";
    }
    TString result;
    bool start = true;
    for (char c : path) {
        if (start && c == slash) {
            continue;
        }
        start = false;
        if (c != slash || result.back() != slash) {
            result.push_back(c);
        }
    }

    if (result.empty()) {
        YQL_ENSURE(start);
        result.push_back(slash);
    }

    return result;
}

size_t GetFirstWildcardPos(const TString& path) {
    return path.find_first_of("*?{");
}

TString EscapeRegex(const std::string_view& str) {
    return RE2::QuoteMeta(re2::StringPiece(str));

}

TString EscapeRegex(const TString& str) {
    return EscapeRegex(static_cast<std::string_view>(str));
}

TString RegexFromWildcards(const std::string_view& pattern) {
    const auto& escaped = EscapeRegex(pattern);
    TStringBuilder result;
    bool slash = false;
    bool group = false;

    for (const char& c : escaped) {
        switch (c) {
            case '{':
                if (group) {
                    result << "\\{";
                } else {
                    result << "(?:";
                    group = true;
                }
                slash = false;
                break;
            case '}':
                if (group) {
                    result << ')';
                    group = false;
                } else {
                    result << "\\}";
                }
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
                result << ".*";
                slash = false;
                break;
            case '?':
                result << ".";
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
    Y_ENSURE(!group, "Found unterminated group");
    Y_ENSURE(!slash, "Expected symbol after slash");
    return result;
}

TMaybe<TString> BuildS3FilePattern(
    const TString& path,
    const TString& filePattern,
    const TVector<TString>& partitionedBy,
    NYql::NS3Lister::TListingRequest& req) {

    TString effectiveFilePattern = filePattern ? filePattern : "*";

    if (partitionedBy.empty()) {
        if (path.Empty()) {
            return "Can not read from empty path";
        }

        if (path.EndsWith('/')) {
            req.Pattern = path + effectiveFilePattern;
        } else {
            if (filePattern) {
                return "Path pattern cannot be used with file_pattern";
            }
            req.Pattern = path;
        }

        req.Pattern = NormalizePath(req.Pattern);
        req.PatternType = NYql::NS3Lister::ES3PatternType::Wildcard;
        req.Prefix = req.Pattern.substr(0, GetFirstWildcardPos(req.Pattern));
    } else {
        if (HasWildcards(path)) {
            return TStringBuilder() << "Path prefix: '" << path << "' contains wildcards";
        }
                
        req.Prefix = path;
        if (!path.empty()) {
            req.Prefix = NS3::NormalizePath(TStringBuilder() << path << "/");
            if (req.Prefix == "/") {
                req.Prefix = "";
            }
        }
        TString pp = req.Prefix;
        if (!pp.empty() && pp.back() == '/') {
            pp.pop_back();
        }

        TStringBuilder generated;
        generated << EscapeRegex(pp);
        for (auto& col : partitionedBy) {
            if (!generated.empty()) {
                generated << "/";
            }
            generated << EscapeRegex(col) << "=(.*?)";
        }
        generated << '/' << RegexFromWildcards(effectiveFilePattern);
        req.Pattern = generated;
        req.PatternType = NS3Lister::ES3PatternType::Regexp;
    }
    return {};
}

TString ValidateWildcards(const std::string_view& pattern) {
    std::optional<size_t> groupStart;
    for (size_t i = 0; i < pattern.size(); ++i) {
        if (pattern[i] == '{' && !groupStart) {
            groupStart = i;
        } else if (pattern[i] == '}') {
            groupStart = std::nullopt;
        }
    }
    if (groupStart) {
        return TStringBuilder() << "found unterminated group start at position " << *groupStart;
    }
    return {};
}

}
