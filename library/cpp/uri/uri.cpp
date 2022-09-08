#include "uri.h"
#include "parse.h"

#include <util/string/cast.h>
#include <util/string/util.h>
#include <util/system/yassert.h>

namespace NUri {
    TState::EParsed TUri::CheckHost(const TStringBuf& host) {
        if (host.empty())
            return ParsedOK;

        unsigned domainLevel = 0;
        unsigned domainLevelOfUnderscore = 0;

        bool isAlnum = false;
        bool startLabel = true;
        for (size_t i = 0; i != host.length(); ++i) {
            const char ch = host[i];

            if ('.' == ch) {                // label separator
                if (!isAlnum || startLabel) // previous label must end in alnum
                    return ParsedBadHost;
                startLabel = true;
                continue;
            }

            isAlnum = isalnum((const unsigned char)ch);

            if (startLabel) {              // label is starting
                if (!isAlnum && '_' != ch) // new label must start with alnum or '_'
                    return ParsedBadHost;
                startLabel = false;
                ++domainLevel;
                if (ch == '_')
                    domainLevelOfUnderscore = domainLevel;
                continue;
            }

            if (isAlnum || '-' == ch)
                continue;

            if (ch == '_') { // non-standard case we allow for certain hosts
                domainLevelOfUnderscore = domainLevel;
                continue;
            }

            return ParsedBadHost;
        }

        if (0 < domainLevelOfUnderscore && domainLevel < 2 + domainLevelOfUnderscore)
            return ParsedBadHost;

        return ParsedOK;
    }

    /********************************************************/
    TUri::TUri(const TStringBuf& host, ui16 port, const TStringBuf& path, const TStringBuf& query, const TStringBuf& scheme, unsigned defaultPort, const TStringBuf& hashbang)
        : FieldsSet(0)
        , Port(port)
        , DefaultPort(0)
        , Scheme(SchemeEmpty)
        , FieldsDirty(0)
    {
        if (!scheme.empty()) {
            if (SetSchemeImpl(TSchemeInfo::Get(scheme)).Str.empty())
                FldSet(FieldScheme, scheme);
        }

        if (0 < defaultPort) // override the scheme's default port
            DefaultPort = static_cast<ui16>(defaultPort);

        char sport[6]; // enough for ui16
        if (0 != port) {
            const size_t len = ToString(port, sport, sizeof(sport));
            FldSet(FieldPort, TStringBuf(sport, len));
        }

        FldTrySet(FieldHost, host);
        FldTrySet(FieldPath, path);
        FldTrySet(FieldQuery, query);
        FldTrySet(FieldHashBang, hashbang);

        Rewrite();
    }

    /********************************************************/
    bool TUri::FldSetImpl(
        EField field, TStringBuf value, bool strconst, bool nocopy) {
        if (!FldIsValid(field))
            return false;

        switch (field) {
            case FieldScheme:
                if (!SetScheme(TSchemeInfo::Get(value)).Str.empty())
                    return false;
                break;

            case FieldPort:
                Port = value.empty() ? 0 : FromString<ui16>(value);
                break;

            default:
                break;
        }

        if (!value.IsInited()) {
            FldClr(field);
            return false;
        }

        if (strconst) { // string constants don't need to be saved in the buffer
            FldMarkClean(field);
            FldSetNoDirty(field, value);
            return false;
        }

        if (nocopy) {
            FldSet(field, value);
            return true;
        }

        return FldTryCpy(field, value);
    }

    /********************************************************/
    bool TUri::FldTryCpy(EField field, const TStringBuf& value) {
        if (!FldIsDirty(field)) {
            do {
                if (!FldIsSet(field))
                    break;

                TStringBuf& fld = Fields[field];
                if (fld.length() < value.length())
                    break;

                char* oldV = (char*)fld.data();
                if (!IsInBuffer(oldV))
                    break;

                memcpy(oldV, value.data(), value.length());
                oldV[value.length()] = 0;
                fld.Trunc(value.length());
                return false;
            } while (false);

            FldMarkDirty(field);
        }

        FldSetNoDirty(field, value);
        return true;
    }

    /********************************************************/
    void TUri::RewriteImpl() {
        size_t len = 0;
        for (int i = 0; i < FieldAllMAX; ++i) {
            const EField fld = EField(i);
            if (FldIsSet(fld))
                len += 1 + Fields[fld].length();
        }

        if (!len)
            Buffer.Clear();
        else {
            TBuffer newbuf;
            newbuf.Resize(len);
            TMemoryWriteBuffer out(newbuf.data(), newbuf.size());
            for (int i = 0; i < FieldAllMAX; ++i) {
                const EField fld = EField(i);
                if (!FldIsSet(fld))
                    continue;

                const char* beg = out.Buf();
                const TStringBuf& val = Fields[fld];
                out << val;
                FldSetNoDirty(fld, TStringBuf(beg, val.length()));
                out << '\0';
            }
            Buffer = std::move(newbuf);
        }

        CheckMissingFields();

        FieldsDirty = 0;
    }

    void TUri::CheckMissingFields() {
        // if host is set but path is not...
        if (FldSetCmp(FlagPath | FlagHost, FlagHost))
            // ... and the scheme requires a path...
            if (GetSchemeInfo().FldReq & FlagPath)
                // ... set path
                FldSetNoDirty(FieldPath, TStringBuf("/"));
    }

    /********************************************************/
    void TUri::Merge(const TUri& base, int correctAbs) {
        if (base.Scheme == SchemeUnknown)
            return;

        if (!base.IsValidGlobal())
            return;

        const TStringBuf& selfscheme = GetField(FieldScheme);
        // basescheme is present since IsValidGlobal() succeeded
        const TStringBuf& basescheme = base.GetField(FieldScheme);
        const bool noscheme = !selfscheme.IsInited();
        if (!noscheme && !EqualNoCase(selfscheme, basescheme))
            return;

        const ui32 cleanFields = ~FieldsDirty;
        do {
            static constexpr TStringBuf rootPath = "/";

            if (noscheme) {
                if (!basescheme.empty()) {
                    FldSetNoDirty(FieldScheme, basescheme);
                    // check if it is canonical
                    if (basescheme.data() != base.GetSchemeInfo().Str.data())
                        FldMarkDirty(FieldScheme);
                }
                Scheme = base.Scheme;
                DefaultPort = base.DefaultPort;
            }

            if (!IsNull(FlagHost))
                break; // no merge

            FldTrySet(FieldHost, base);
            FldChkSet(FieldPort, base);
            Port = base.Port;

            if (noscheme && IsNull(FlagQuery) && IsNull(FlagPath))
                FldTrySet(FieldQuery, base);

           if (noscheme && IsNull(FlagHashBang) && IsNull(FlagPath))
                FldTrySet(FieldHashBang, base);

            if (IsNull(FlagAuth) && !base.IsNull(FlagAuth)) {
                FldChkSet(FieldUser, base);
                FldChkSet(FieldPass, base);
            }

            if (IsValidAbs())
                break;

            TStringBuf p0 = base.GetField(FieldPath);
            if (!p0.IsInited())
                p0 = rootPath;

            TStringBuf p1 = GetField(FieldPath);
            if (!p1.IsInited()) {
                if (p0.data() != rootPath.data())
                    FldSet(FieldPath, p0);
                else
                    FldSetNoDirty(FieldPath, rootPath);
                break;
            }
            if (p1 && '/' == p1[0])
                p1.Skip(1); // p0 will have one

            bool pathop = true;

            TTempBufOutput out(p0.length() + p1.length() + 4);
            out << p0;
            if ('/' != p0.back())
                out << "/../";
            else if (p1.empty() || '.' != p1[0])
                pathop = false;
            out << p1;

            char* beg = out.Data();
            char* end = beg + out.Filled();
            if (pathop && !PathOperation(beg, end, correctAbs)) {
                Clear();
                break;
            }

            // Needs immediate forced rewrite because of TTempBuf
            FldSetNoDirty(FieldPath, TStringBuf(beg, end));
            RewriteImpl();
        } while (false);

        CheckMissingFields();

        // rewrite only if borrowed fields from base
        if (cleanFields & FieldsDirty)
            RewriteImpl();
    }

    /********************************************************/
    TUri::TLinkType TUri::Normalize(const TUri& base,
                                    const TStringBuf& link, const TStringBuf& codebase, ui64 careFlags, ECharset enc) {
        // parse URL
        if (ParsedOK != ParseImpl(link, careFlags, 0, SchemeEmpty, enc))
            return LinkIsBad;

        const TStringBuf& host = GetHost();

        // merge with base URL
        // taken either from _BASE_ property or from optional argument
        if (!codebase.empty()) {
            // if optional code base given -- parse it
            TUri codebaseUrl;
            if (codebaseUrl.ParseImpl(codebase, careFlags, 0, SchemeEmpty, enc) != ParsedOK || !codebaseUrl.IsValidAbs())
                return LinkIsBad;
            Merge(codebaseUrl);
        } else {
            // Base is already in this variable
            // see SetProperty() for details
            Merge(base);
        }

        // check result: must be correct absolute URL
        if (!IsValidAbs())
            return LinkBadAbs;

        if (!host.empty()) {
            //  - we don't care about different ports for the same server
            //  - we don't care about win|www|koi|etc. preffixes for the same server
            if (GetPort() != base.GetPort() || !EqualNoCase(host, base.GetHost()))
                return LinkIsGlobal;
        }

        // find out if it is link to itself then ignore it
        if (!Compare(base, FlagPath | FlagQuery | FlagHashBang))
            return LinkIsFragment;

        return LinkIsLocal;
    }

    /********************************************************/

    size_t TUri::PrintSize(ui32 flags) const {
        size_t len = 10;
        flags &= FieldsSet; // can't output what we don't have
        if (flags & FlagHostAscii)
            flags &= ~FlagHost; // don't want to print both of them
        ui32 opt = 1;
        for (int fld = 0; opt <= flags && fld < FieldAllMAX; ++fld, opt <<= 1) {
            if (opt & flags) {
                const TStringBuf& v = Fields[fld];
                if (v.IsInited()) {
                    if (opt & FlagAuth)
                        len += 3 * v.length() + 1;
                    else
                        len += v.length() + 1;
                }
            }
        }

        return len;
    }

    IOutputStream& TUri::PrintImpl(IOutputStream& out, int flags) const {
        TStringBuf v;

        const int wantFlags = flags; // save the original
        flags &= FieldsSet;          // can't print what we don't have
        if (flags & FlagHostAscii)
            flags |= FlagHost; // to make host checks simpler below

        if (flags & FlagScheme) {
            v = Fields[FieldScheme];
            if (!v.empty())
                out << v << ':';
        }

        TStringBuf host;
        if (flags & FlagHost) {
            const EField fldhost =
                flags & FlagHostAscii ? FieldHostAscii : FieldHost;
            host = Fields[fldhost];
        }

        TStringBuf port;
        if ((flags & FlagPort) && 0 != Port && Port != DefaultPort)
            port = Fields[FieldPort];

        if (host) {
            if (wantFlags & FlagScheme)
                out << "//";

            if (flags & FlagAuth) {
                if (flags & FlagUser) {
                    v = Fields[FieldUser];
                    if (!v.empty())
                        TEncoder::EncodeNotAlnum(out, v);
                }

                if (flags & FlagPass) {
                    v = Fields[FieldPass];
                    if (v.IsInited()) {
                        out << ':';
                        TEncoder::EncodeAll(out, v);
                    }
                }

                out << '@';
            }

            out << host;

            if (port)
                out << ':';
        }
        if (port)
            out << port;

        if (flags & FlagPath) {
            v = Fields[FieldPath];
            // for relative, empty path is not the same as missing
            if (v.empty() && 0 == (flags & FlagHost))
                v = TStringBuf(".");
            out << v;
        }

        if (flags & FlagQuery) {
            v = Fields[FieldQuery];
            if (v.IsInited())
                out << '?' << v;
        }

        if (flags & FlagFrag) {
            v = Fields[FieldFrag];
            if (v.IsInited())
                out << '#' << v;
        }

        if (flags & FlagHashBang) {
            v = Fields[FieldHashBang];
            if (v.IsInited())
                out << '#' << '!' << v;
        }

        return out;
    }

    /********************************************************/
    int TUri::CompareField(EField fld, const TUri& url) const {
        const TStringBuf& v0 = GetField(fld);
        const TStringBuf& v1 = url.GetField(fld);
        switch (fld) {
            case FieldScheme:
            case FieldHost:
                return CompareNoCase(v0, v1);
            default:
                return v0.compare(v1);
        }
    }

    /********************************************************/
    int TUri::Compare(const TUri& url, int flags) const {
        // first compare fields with default values
        if (flags & FlagPort) {
            const int ret = GetPort() - url.GetPort();
            if (ret)
                return ret;
            flags &= ~FlagPort;
        }

        // compare remaining sets of available fields
        const int rtflags = flags & url.FieldsSet;
        flags &= FieldsSet;
        const int fldcmp = flags - rtflags;
        if (fldcmp)
            return fldcmp;

        // field sets are the same, compare the fields themselves
        for (int i = 0; i < FieldAllMAX; ++i) {
            const EField fld = EField(i);
            if (flags & FldFlag(fld)) {
                const int ret = CompareField(fld, url);
                if (ret)
                    return ret;
            }
        }

        return 0;
    }

    /********************************************************/
    bool TUri::PathOperation(char*& pathPtr, char*& pathEnd, int correctAbs) {
        if (!pathPtr)
            return false;
        if (pathPtr == pathEnd)
            return true;

        if ((pathEnd - pathPtr) >= 2 && *(pathEnd - 2) == '/' && *(pathEnd - 1) == '.') {
            --pathEnd;
        }

        char* p_wr = pathEnd;
        int upCount = 0;

        char* p_prev = pathEnd;
        Y_ASSERT(p_prev > pathPtr);
        while (p_prev > pathPtr && *(p_prev - 1) == '/')
            p_prev--;

        for (char* p_rd = p_prev; p_rd; p_rd = p_prev) {
            Y_ASSERT(p_rd == pathEnd || p_rd[0] == '/');
            p_prev = nullptr;

            char* p = p_rd;

            if (p > pathPtr) {
                for (p--; *p != '/'; p--) {
                    if (p == pathPtr)
                        break;
                }
                if (*p == '/') {
                    p_prev = p++;
                    if ((p_prev - pathPtr >= 6 && !strnicmp(p_prev - 6, "http://", 7)) ||
                        (p_prev - pathPtr >= 7 && !strnicmp(p_prev - 7, "https://", 8))) {
                        --p_prev;
                        --p;
                    } else {
                        //skip multiple from head '/'
                        while (p_prev > pathPtr && *(p_prev - 1) == '/')
                            p_prev--;
                    }
                }
            }

            Y_ASSERT(p_prev == nullptr || p_prev[0] == '/');
            //and the first symbol !='/' after p_prev is p

            if (p == p_rd) {
                //empty block:
                if (p_prev) { //either tail:
                    Y_ASSERT(p_rd == p_wr && *(p - 1) == '/');
                    --p_wr;
                    continue;
                } else { //or head of abs path
                    *(--p_wr) = '/';
                    break;
                }
            }

            if (p[0] == '.') {
                if (p + 1 == p_rd) {
                    if (correctAbs || p_prev > pathPtr || pathPtr[0] != '/')
                        // ignore "./"
                        continue;
                } else {
                    if ((p[1] == '.') && (p + 2 == p_rd)) {
                        // register "../" but not print
                        upCount++;
                        continue;
                    }
                }
            }

            if (upCount) {
                //unregister "../" and not print
                upCount--;
                continue;
            }

            // print
            Y_ASSERT(p < p_rd);
            Y_ASSERT(!p_prev || *(p - 1) == '/');
            if (p_wr == p_rd) { //just skip
                p_wr = p;
            } else { //copy
                int l = p_rd - p + 1;
                p_wr -= l;
                memmove(p_wr, p, l);
            }
        }

        if (upCount) {
            if (*pathPtr != '/') {
                if (pathEnd == p_wr && *(p_wr - 1) == '.') {
                    Y_ASSERT(*(p_wr - 2) == '.');
                    p_wr -= 2;
                    upCount--;
                }
                for (; upCount > 0; upCount--) {
                    *(--p_wr) = '/';
                    *(--p_wr) = '.';
                    *(--p_wr) = '.';
                }
            } else {
                if (correctAbs > 0)
                    return false;
                if (correctAbs == 0) {
                    //Bad path but present in RFC:
                    // "Similarly, parsers must avoid treating "." and ".."
                    // as special when they are not complete components of
                    // a relative path. "
                    for (; upCount > 0; upCount--) {
                        *(--p_wr) = '.';
                        *(--p_wr) = '.';
                        *(--p_wr) = '/';
                    }
                } else {
                    upCount = false;
                }
            }
        }

        Y_ASSERT(p_wr >= pathPtr);

        if (upCount)
            return false;
        pathPtr = p_wr;
        return true;
    }

    /********************************************************/
    const char* LinkTypeToString(const TUri::TLinkType& t) {
        switch (t) {
            case TUri::LinkIsBad:
                return "LinkIsBad";
            case TUri::LinkBadAbs:
                return "LinkBadAbs";
            case TUri::LinkIsFragment:
                return "LinkIsFragment";
            case TUri::LinkIsLocal:
                return "LinkIsLocal";
            case TUri::LinkIsGlobal:
                return "LinkIsGlobal";
        }
        Y_ASSERT(0);
        return "";
    }

}
