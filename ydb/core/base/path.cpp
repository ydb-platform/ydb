#include "path.h"

#include <util/string/builder.h>
#include <util/string/printf.h>

#include <locale>

namespace NKikimr {

TVector<TString> SplitPath(TString path) {
    TVector<TString> res;
    if (path.empty())
        return res;
// xenoxeno: don't do it unless you want YQL to complain about paths...
//    if (*path.begin() != '/')
//        return res;
    size_t prevpos = 0;
    size_t pos = 0;
    size_t len = path.size();
    while (pos < len) {
        if (path[pos] == '/') {
            if (pos != prevpos)
                res.emplace_back(path.substr(prevpos, pos-prevpos));
            ++pos;
            prevpos = pos;
        } else {
            ++pos;
        }
    }
    if (pos != prevpos)
        res.emplace_back(path.substr(prevpos, pos-prevpos));
    return res;
}

TString JoinPath(const TVector<TString>& path) {
    TString result;
    size_t size = 0;
    for (const TString& s : path) {
        if (size != 0)
            ++size;
        size += s.size();
    }
    result.reserve(size);
    for (const TString& s : path) {
        if (!result.empty())
            result += '/';
        result += s;
    }
    return result;
}

/**
 * Returns the first position that matches "//" or the end of string
 */
static const char* FindDoubleSlash(const char* p, const char* end) {
    if (p == end) {
        return end;
    }
    const char* last = end - 1;
    while (p != last) {
        if (p[0] == '/' && p[1] == '/') {
            return p;
        }
        ++p;
    }
    return end;
}

/**
 * Returns the first position that does not match '/'
 */
static const char* SkipSlashes(const char* p, const char* end) {
    while (p != end && *p == '/') {
        ++p;
    }
    return p;
}

TString CanonizePath(const TString& in) {
    if (in.empty()) {
        // Special handling for an empty string
        return in;
    }

    const char* p = in.c_str();
    const char* end = p + in.size();
    const char* s = FindDoubleSlash(p, end);

    // Check if there is no '//' anywhere
    // Note when true we cannot match an empty string
    if (s == end) {
        if (p[0] == '/' && s[-1] != '/') {
            // Current string is fully canonized
            return in;
        }

        // Strip the last '/' if present
        if (s[-1] == '/') {
            --s;
        }

        // Check if all we had to do was strip the trailing slash
        if (p[0] == '/') {
            return TString(p, s);
        }

        // Otherwise we must append the leading slash
        TString result;
        result.reserve((s - p) + 1);
        result.push_back('/');
        result.append(p, s);
        return result;
    }

    TString result;

    // There's at least one "//" so in.size() is enough even when leading slash is missing
    result.reserve(in.size());

    // The first segment may need to add a leading slash
    if (p != s) {
        if (p[0] != '/') {
            result.push_back('/');
        }
        result.append(p, s);
    }

    // Note: s matches "//" at the beginning of each iteration
    while (true) {
        p = SkipSlashes(s + 2, end);
        if (p == end) {
            // Original string ended with multiple "//"
            break;
        }

        s = FindDoubleSlash(p + 1, end);
        if (s == end) {
            // No more double slashes, handle the last '/' and append
            if (s[-1] == '/') {
                --s;
            }
            result.append(p - 1, s);
            break;
        }

        // Since p is the first non-'/' character, p-1 is always '/'
        result.append(p - 1, s);
    }

    return result;
}

TString CanonizePath(const TVector<TString>& path) {
    if (path.empty())
        return TString();

    return TString("/") + JoinPath(path);
}

ui32 CanonizedPathLen(const TVector<TString>& path) {
    ui32 ret = path.size();
    for (auto &x : path)
        ret += x.size();
    return ret;
}

TStringBuf ExtractDomain(const TString& path) noexcept {
    auto domain = TStringBuf(path);

    return ExtractDomain(domain);
}

TStringBuf ExtractDomain(TStringBuf path) noexcept {
    //  coherence with SplitPath and JoinPath that allow no / leading path
    path.SkipPrefix(TStringBuf("/"));

    return path.Before('/');
}

bool IsEqualPaths(const TString& l, const TString& r) noexcept {
    auto left = TStringBuf(l);
    // coherence with SplitPath and JoinPath that allow no / leading path
    left.SkipPrefix(TStringBuf("/"));
    // also do not accaunt / at the end
    left.ChopSuffix(TStringBuf("/"));

    auto right = TStringBuf(r);
    right.SkipPrefix(TStringBuf("/"));
    right.ChopSuffix(TStringBuf("/"));

    return left == right;
}

bool IsStartWithSlash(const TString &l) {
    return TStringBuf(l).StartsWith(TStringBuf("/"));
}

bool IsPathPartContainsOnlyDots(const TString &part) {
    for (auto it = part.begin(); it != part.end(); ++it) {
        if (*it != '.') {
            return false;
        }
    }

    return true;
}

TString::const_iterator PathPartBrokenAt(const TString &part, const TStringBuf extraSymbols) {
    static constexpr TStringBuf basicSymbols = "-_.";
    for (auto it = part.begin(); it != part.end(); ++it) {
        if (!std::isalnum(*it, std::locale::classic())
                && !basicSymbols.Contains(*it)
                && !extraSymbols.Contains(*it)) {
            return it;
        }
    }

    return part.end();
}

bool CheckDbPath(const TString &path, const TString &domain, TString &error) {
    auto parts = SplitPath(path);

    if (parts.empty()) {
        error = "Database path cannot be empty or root";
        return false;
    }

    if (parts.front() != domain) {
        error = Sprintf("Database path should be in domain /%s", domain.data());
        return false;
    }

    for (auto &part : parts) {
        if (!part) {
            error = "Database names and path parts shouldn't be empty";
            return false;
        }

        if (IsPathPartContainsOnlyDots(part)) {
            error = Sprintf("Is not allowed in database names and path parts contains only dots");
            return false;
        }

        auto brokenAt = PathPartBrokenAt(part);
        if (brokenAt != part.end()) {
            error = Sprintf("Symbol '%c' is not allowed in database names and path parts ", *brokenAt);
            return false;
        }
    }

    return true;
}

TStringBuf ExtractParent(const TString &path) noexcept {
    TStringBuf parent = TStringBuf(path);

    //  coherence with SplitPath and JoinPath that allow no / leading path
    parent.ChopSuffix(TStringBuf("/"));

    return parent.RBefore('/');
}

TStringBuf ExtractBase(const TString &path) noexcept {
    TStringBuf parent = TStringBuf(path);

    //  coherence with SplitPath and JoinPath that allow no / leading path
    parent.ChopSuffix(TStringBuf("/"));

    return parent.RAfter('/');
}

bool TrySplitPathByDb(const TString& path, const TString& database,
    std::pair<TString, TString>& result, TString& error)
{
    auto makeWrongDbError = [&]() {
        return TStringBuilder() << "Table path not in database, path: " << path << ", database: " << database;
    };

    auto pathParts = SplitPath(path);
    auto databaseParts = SplitPath(database);

    if (pathParts.size() <= databaseParts.size()) {
        error = makeWrongDbError();
        return false;
    }

    if (databaseParts.empty()) {
        if (pathParts.size() < 2) {
            error = TStringBuilder() << "Bad table path: " << path;
            return false;
        }

        result = std::make_pair(
            CombinePath(pathParts.begin(), pathParts.begin() + 1),
            CombinePath(pathParts.begin() + 1, pathParts.end(), false));

        return true;
    }

    for (ui32 i = 0; i < databaseParts.size(); ++i) {
        if (pathParts[i] != databaseParts[i]) {
            error = makeWrongDbError();
            return false;
        }
    }

    result = std::make_pair(
        CombinePath(databaseParts.begin(), databaseParts.end()),
        CombinePath(pathParts.begin() + databaseParts.size(), pathParts.end(), false)
    );

    return true;
}

}
