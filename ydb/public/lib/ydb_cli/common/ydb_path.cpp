#include "ydb_path.h"

namespace NYdb::NConsoleClient {

TString JoinYdbPath(const TVector<TString>& path) {
    TString result;
    size_t size = 0;
    for (const TString& s : path) {
        if (size != 0) {
            ++size;
        }
        size += s.size();
    }
    result.reserve(size);
    for (const TString& s : path) {
        if (!result.empty()) {
            result += '/';
        }
        result += s;
    }
    return result;
}

namespace {

// Returns the first position that matches "//" or the end of string
const char* FindDoubleSlash(const char* p, const char* end) {
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

// Returns the first position that does not match '/'
const char* SkipSlashes(const char* p, const char* end) {
    while (p != end && *p == '/') {
        ++p;
    }
    return p;
}

} // namespace

TString CanonizeYdbPath(const TString& in) {
    if (in.empty()) {
        return in;
    }

    const char* p = in.c_str();
    const char* end = p + in.size();
    const char* s = FindDoubleSlash(p, end);

    // Check if there is no '//' anywhere
    if (s == end) {
        if (p[0] == '/' && s[-1] != '/') {
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
    result.reserve(in.size());

    // The first segment may need to add a leading slash
    if (p != s) {
        if (p[0] != '/') {
            result.push_back('/');
        }
        result.append(p, s);
    }

    while (true) {
        p = SkipSlashes(s + 2, end);
        if (p == end) {
            break;
        }

        s = FindDoubleSlash(p + 1, end);
        if (s == end) {
            if (s[-1] == '/') {
                --s;
            }
            result.append(p - 1, s);
            break;
        }

        result.append(p - 1, s);
    }

    return result;
}

}

