#pragma once

#include "common.h"
#include "encode.h"

#include <library/cpp/charset/doccodes.h>
#include <util/generic/buffer.h>
#include <util/generic/ptr.h>
#include <util/generic/singleton.h>
#include <util/generic/string.h>
#include <util/memory/alloc.h>
#include <util/stream/mem.h>
#include <util/stream/output.h>
#include <util/stream/str.h>
#include <util/system/yassert.h>

#include <cstdlib>

namespace NUri {
    /********************************************************/
    class TUri
        : public TFeature,
          public TField,
          public TScheme,
          public TState {
    public:
        enum TLinkType {
            LinkIsBad,
            LinkBadAbs,
            LinkIsFragment,
            LinkIsLocal,
            LinkIsGlobal
        };

    private:
        TBuffer Buffer;
        TStringBuf Fields[FieldAllMAX];
        ui32 FieldsSet;
        ui16 Port;
        ui16 DefaultPort;
        TScheme::EKind Scheme;
        /// contains fields out of buffer (and possibly not null-terminated)
        ui32 FieldsDirty;

    private:
        void Alloc(size_t len) {
            Dealloc(); // to prevent copy below
            Buffer.Resize(len);
        }
        void Dealloc() {
            Buffer.Clear();
        }

        void ClearImpl() {
            Port = 0;
            FieldsSet = 0;
            Scheme = SchemeEmpty;
            FieldsDirty = 0;
        }

        void CopyData(const TUri& url) {
            FieldsSet = url.FieldsSet;
            Port = url.Port;
            DefaultPort = url.DefaultPort;
            Scheme = url.Scheme;
            FieldsDirty = url.FieldsDirty;
        }

        void CopyImpl(const TUri& url) {
            for (int i = 0; i < FieldAllMAX; ++i)
                Fields[i] = url.Fields[i];

            RewriteImpl();
        }

    private:
        static ui32 FldFlag(EField fld) {
            return 1 << fld;
        }

    public:
        static bool FldIsValid(EField fld) {
            return 0 <= fld && FieldAllMAX > fld;
        }

        bool FldSetCmp(ui32 chk, ui32 exp) const {
            return (FieldsSet & chk) == exp;
        }

        bool FldSetCmp(ui32 chk) const {
            return FldSetCmp(chk, chk);
        }

        bool FldIsSet(EField fld) const {
            return !FldSetCmp(FldFlag(fld), 0);
        }

    private:
        void FldMarkSet(EField fld) {
            FieldsSet |= FldFlag(fld);
        }

        void FldMarkUnset(EField fld) {
            FieldsSet &= ~FldFlag(fld);
        }

        // use when we know the field is dirty or RewriteImpl will be called
        void FldSetNoDirty(EField fld, const TStringBuf& value) {
            Fields[fld] = value;
            FldMarkSet(fld);
        }

        void FldSet(EField fld, const TStringBuf& value) {
            FldSetNoDirty(fld, value);
            FldMarkDirty(fld);
        }

        const TStringBuf& FldGet(EField fld) const {
            return Fields[fld];
        }

    private:
        /// depending on value, clears or sets it
        void FldChkSet(EField fld, const TStringBuf& value) {
            if (value.IsInited())
                FldSet(fld, value);
            else
                FldClr(fld);
        }
        void FldChkSet(EField fld, const TUri& other) {
            FldChkSet(fld, other.GetField(fld));
        }

        /// set only if initialized
        bool FldTrySet(EField fld, const TStringBuf& value) {
            const bool ok = value.IsInited();
            if (ok)
                FldSet(fld, value);
            return ok;
        }
        bool FldTrySet(EField fld, const TUri& other) {
            return FldTrySet(fld, other.GetField(fld));
        }

    private:
        /// copies the value if it fits
        bool FldTryCpy(EField fld, const TStringBuf& value);

        // main method: sets the field value, possibly copies, etc.
        bool FldSetImpl(EField fld, TStringBuf value, bool strconst = false, bool nocopy = false);

    public: // clear a field
        void FldClr(EField fld) {
            Fields[fld].Clear();
            FldMarkUnset(fld);
            FldMarkClean(fld);
        }

        bool FldTryClr(EField field) {
            const bool ok = FldIsSet(field);
            if (ok)
                FldClr(field);
            return ok;
        }

    public: // set a field value: might leave state dirty and require a Rewrite()
        // copies if fits and not dirty, sets and marks dirty otherwise
        bool FldMemCpy(EField field, const TStringBuf& value) {
            return FldSetImpl(field, value, false);
        }

        // uses directly, marks dirty
        /// @note client MUST guarantee value will be alive until Rewrite is called
        bool FldMemSet(EField field, const TStringBuf& value) {
            return FldSetImpl(field, value, false, true);
        }

        // uses directly, doesn't mark dirty (value scope exceeds "this")
        bool FldMemUse(EField field, const TStringBuf& value) {
            return FldSetImpl(field, value, true);
        }

        // uses directly, doesn't mark dirty
        template <size_t size>
        bool FldMemSet(EField field, const char (&value)[size]) {
            static_assert(size > 0);
            return FldSetImpl(field, TStringBuf(value, size - 1), true);
        }

        // duplicate one field to another
        bool FldDup(EField src, EField dst) {
            if (!FldIsSet(src) || !FldIsValid(dst))
                return false;
            FldSetNoDirty(dst, FldGet(src));
            if (FldIsDirty(src))
                FldMarkDirty(dst);
            else
                FldMarkClean(dst);
            return true;
        }

        // move one field to another
        bool FldMov(EField src, EField dst) {
            if (!FldDup(src, dst))
                return false;
            FldClr(src);
            return true;
        }

    private:
        bool IsInBuffer(const char* buf) const {
            return buf >= Buffer.data() && buf < Buffer.data() + Buffer.size();
        }

    public:
        bool FldIsDirty() const {
            return 0 != FieldsDirty;
        }

        bool FldIsDirty(EField fld) const {
            return 0 != (FieldsDirty & FldFlag(fld));
        }

    private:
        void FldMarkDirty(EField fld) {
            FieldsDirty |= FldFlag(fld);
        }

        void FldMarkClean(EField fld) {
            FieldsDirty &= ~FldFlag(fld);
        }

        void RewriteImpl();

    public:
        static TState::EParsed CheckHost(const TStringBuf& host);

        // convert a [potential] IDN to ascii
        static TMallocPtr<char> IDNToAscii(const wchar32* idna);
        static TMallocPtr<char> IDNToAscii(const TStringBuf& host, ECharset enc = CODES_UTF8);

        // convert hosts with percent-encoded or extended chars

        // returns non-empty string if host can be converted to ASCII with given parameters
        static TStringBuf HostToAscii(TStringBuf host, TMallocPtr<char>& buf, bool hasExtended, bool allowIDN, ECharset enc = CODES_UTF8);

        // returns host if already ascii, or non-empty if it can be converted
        static TStringBuf HostToAscii(const TStringBuf& host, TMallocPtr<char>& buf, bool allowIDN, ECharset enc = CODES_UTF8);

    public:
        explicit TUri(unsigned defaultPort = 0)
            : FieldsSet(0)
            , Port(0)
            , DefaultPort(static_cast<ui16>(defaultPort))
            , Scheme(SchemeEmpty)
            , FieldsDirty(0)
        {
        }

        TUri(const TStringBuf& host, ui16 port, const TStringBuf& path, const TStringBuf& query = TStringBuf(), const TStringBuf& scheme = "http", unsigned defaultPort = 0, const TStringBuf& hashbang = TStringBuf());

        TUri(const TUri& url)
            : FieldsSet(url.FieldsSet)
            , Port(url.Port)
            , DefaultPort(url.DefaultPort)
            , Scheme(url.Scheme)
            , FieldsDirty(url.FieldsDirty)
        {
            CopyImpl(url);
        }

        ~TUri() {
            Clear();
        }

        void Copy(const TUri& url) {
            if (&url != this) {
                CopyData(url);
                CopyImpl(url);
            }
        }

        void Clear() {
            Dealloc();
            ClearImpl();
        }

        ui32 GetFieldMask() const {
            return FieldsSet;
        }

        ui32 GetUrlFieldMask() const {
            return GetFieldMask() & FlagUrlFields;
        }

        ui32 GetDirtyMask() const {
            return FieldsDirty;
        }

        void CheckMissingFields();

        // Process methods

        void Rewrite() {
            if (FldIsDirty())
                RewriteImpl();
        }

    private:
        TState::EParsed AssignImpl(const TParser& parser, TScheme::EKind defscheme = SchemeEmpty);

        TState::EParsed ParseImpl(const TStringBuf& url, const TParseFlags& flags = FeaturesDefault, ui32 maxlen = 0, TScheme::EKind defscheme = SchemeEmpty, ECharset enc = CODES_UTF8);

    public:
        TState::EParsed Assign(const TParser& parser, TScheme::EKind defscheme = SchemeEmpty) {
            const TState::EParsed ret = AssignImpl(parser, defscheme);
            if (ParsedOK == ret)
                Rewrite();
            return ret;
        }

        TState::EParsed ParseUri(const TStringBuf& url, const TParseFlags& flags = FeaturesDefault, ui32 maxlen = 0, ECharset enc = CODES_UTF8) {
            const TState::EParsed ret = ParseImpl(url, flags, maxlen, SchemeEmpty, enc);
            if (ParsedOK == ret)
                Rewrite();
            return ret;
        }

        // parses absolute URIs
        // prepends default scheme (unless unknown) if URI has none
        TState::EParsed ParseAbsUri(const TStringBuf& url, const TParseFlags& flags = FeaturesDefault, ui32 maxlen = 0, TScheme::EKind defscheme = SchemeUnknown, ECharset enc = CODES_UTF8);

        TState::EParsed ParseAbsOrHttpUri(const TStringBuf& url, const TParseFlags& flags = FeaturesDefault, ui32 maxlen = 0, ECharset enc = CODES_UTF8) {
            return ParseAbsUri(url, flags, maxlen, SchemeHTTP, enc);
        }

        TState::EParsed Parse(const TStringBuf& url, const TUri& base, const TParseFlags& flags = FeaturesDefault, ui32 maxlen = 0, ECharset enc = CODES_UTF8);

        TState::EParsed Parse(const TStringBuf& url, const TParseFlags& flags = FeaturesDefault) {
            return ParseUri(url, flags);
        }

        TState::EParsed Parse(const TStringBuf& url, const TParseFlags& flags, const TStringBuf& base_url, ui32 maxlen = 0, ECharset enc = CODES_UTF8);

        TState::EParsed ParseAbs(const TStringBuf& url, const TParseFlags& flags = FeaturesDefault, const TStringBuf& base_url = TStringBuf(), ui32 maxlen = 0, ECharset enc = CODES_UTF8) {
            const TState::EParsed result = Parse(url, flags, base_url, maxlen, enc);
            return ParsedOK != result || IsValidGlobal() ? result : ParsedBadFormat;
        }

        // correctAbs works with head "/.." portions:
        //  1 - reject URL
        //  0 - keep portions
        // -1 - ignore portions

        void Merge(const TUri& base, int correctAbs = -1);

        TLinkType Normalize(const TUri& base, const TStringBuf& link, const TStringBuf& codebase = TStringBuf(), ui64 careFlags = FeaturesDefault, ECharset enc = CODES_UTF8);

    private:
        int PrintFlags(int flags) const {
            if (0 == (FlagUrlFields & flags))
                flags |= FlagUrlFields;
            return flags;
        }

    protected:
        size_t PrintSize(ui32 flags) const;

        // Output method, prints to stream
        IOutputStream& PrintImpl(IOutputStream& out, int flags) const;

        char* PrintImpl(char* str, size_t size, int flags) const {
            TMemoryOutput out(str, size);
            PrintImpl(out, flags) << '\0';
            return str;
        }

        static bool IsAbsPath(const TStringBuf& path) {
            return 1 <= path.length() && path[0] == '/';
        }

        bool IsAbsPathImpl() const {
            return IsAbsPath(GetField(FieldPath));
        }

    public:
        // Output method, prints to stream
        IOutputStream& Print(IOutputStream& out, int flags = FlagUrlFields) const {
            return PrintImpl(out, PrintFlags(flags));
        }

        // Output method, print to str, allocate memory if str is NULL
        // Should be deprecated
        char* Print(char* str, size_t size, int flags = FlagUrlFields) const {
            return nullptr == str ? Serialize(flags) : Serialize(str, size, flags);
        }

        char* Serialize(char* str, size_t size, int flags = FlagUrlFields) const {
            Y_ASSERT(str);
            flags = PrintFlags(flags);
            const size_t printSize = PrintSize(flags) + 1;
            return printSize > size ? nullptr : PrintImpl(str, size, flags);
        }

        char* Serialize(int flags = FlagUrlFields) const {
            flags = PrintFlags(flags);
            const size_t size = PrintSize(flags) + 1;
            return PrintImpl(static_cast<char*>(malloc(size)), size, flags);
        }

        // Output method to str
        void Print(TString& str, int flags = FlagUrlFields) const {
            flags = PrintFlags(flags);
            str.reserve(str.length() + PrintSize(flags));
            TStringOutput out(str);
            PrintImpl(out, flags);
        }

        TString PrintS(int flags = FlagUrlFields) const {
            TString str;
            Print(str, flags);
            return str;
        }

        // Only non-default scheme and port are printed
        char* PrintHost(char* str, size_t size) const {
            return Print(str, size, (Scheme != SchemeHTTP ? FlagScheme : 0) | FlagHostPort);
        }
        TString PrintHostS() const {
            return PrintS((Scheme != SchemeHTTP ? FlagScheme : 0) | FlagHostPort);
        }

        // Info methods
        int Compare(const TUri& A, int flags = FlagUrlFields) const;

        int CompareField(EField fld, const TUri& url) const;

        const TStringBuf& GetField(EField fld) const {
            return FldIsValid(fld) && FldIsSet(fld) ? FldGet(fld) : Default<TStringBuf>();
        }

        ui16 GetPort() const {
            return 0 == Port ? DefaultPort : Port;
        }

        const TStringBuf& GetHost() const {
            if (GetFieldMask() & FlagHostAscii)
                return FldGet(FieldHostAscii);
            if (GetFieldMask() & FlagHost)
                return FldGet(FieldHost);
            return Default<TStringBuf>();
        }

        bool UseHostAscii() {
            return FldMov(FieldHostAscii, FieldHost);
        }

        TScheme::EKind GetScheme() const {
            return Scheme;
        }
        const TSchemeInfo& GetSchemeInfo() const {
            return TSchemeInfo::Get(Scheme);
        }

        bool IsNull(ui32 flags = FlagScheme | FlagHost | FlagPath) const {
            return !FldSetCmp(flags);
        }

        bool IsNull(EField fld) const {
            return !FldIsSet(fld);
        }

        bool IsValidAbs() const {
            if (IsNull(FlagScheme | FlagHost | FlagPath))
                return false;
            return IsAbsPathImpl();
        }

        bool IsValidGlobal() const {
            if (IsNull(FlagScheme | FlagHost))
                return false;
            if (IsNull(FlagPath))
                return true;
            return IsAbsPathImpl();
        }

        bool IsRootless() const {
            return FldSetCmp(FlagScheme | FlagHost | FlagPath, FlagScheme | FlagPath) && !IsAbsPathImpl();
        }

        // for RFC 2396 compatibility
        bool IsOpaque() const {
            return IsRootless();
        }

        // Inline helpers
        TUri& operator=(const TUri& u) {
            Copy(u);
            return *this;
        }

        bool operator!() const {
            return IsNull();
        }

        bool Equal(const TUri& A, int flags = FlagUrlFields) const {
            return (Compare(A, flags) == 0);
        }

        bool Less(const TUri& A, int flags = FlagUrlFields) const {
            return (Compare(A, flags) < 0);
        }

        bool operator==(const TUri& A) const {
            return Equal(A, FlagNoFrag);
        }

        bool operator!=(const TUri& A) const {
            return !Equal(A, FlagNoFrag);
        }

        bool operator<(const TUri& A) const {
            return Less(A, FlagNoFrag);
        }

        bool IsSameDocument(const TUri& other) const {
            // pre: both *this and 'other' should be normalized to valid abs
            Y_ASSERT(IsValidAbs());
            return Equal(other, FlagNoFrag);
        }

        bool IsLocal(const TUri& other) const {
            // pre: both *this and 'other' should be normalized to valid abs
            Y_ASSERT(IsValidAbs() && other.IsValidAbs());
            return Equal(other, FlagScheme | FlagHostPort);
        }

        TLinkType Locality(const TUri& other) const {
            if (IsSameDocument(other))
                return LinkIsFragment;
            else if (IsLocal(other))
                return LinkIsLocal;
            return LinkIsGlobal;
        }

        static IOutputStream& ReEncodeField(IOutputStream& out, const TStringBuf& val, EField fld, ui64 flags = FeaturesEncodeDecode) {
            return NEncode::TEncoder::ReEncode(out, val, NEncode::TEncodeMapper(flags, fld));
        }

        static IOutputStream& ReEncodeToField(IOutputStream& out, const TStringBuf& val, EField srcfld, ui64 srcflags, EField dstfld, ui64 dstflags) {
            return NEncode::TEncoder::ReEncodeTo(out, val, NEncode::TEncodeMapper(srcflags, srcfld), NEncode::TEncodeToMapper(dstflags, dstfld));
        }

        static IOutputStream& ReEncode(IOutputStream& out, const TStringBuf& val, ui64 flags = FeaturesEncodeDecode) {
            return ReEncodeField(out, val, FieldAllMAX, flags);
        }

        static int PathOperationFlag(const TParseFlags& flags) {
            return flags & FeaturePathDenyRootParent ? 1
                                                     : flags & FeaturePathStripRootParent ? -1 : 0;
        }

        static bool PathOperation(char*& pathBeg, char*& pathEnd, int correctAbs);

    private:
        const TSchemeInfo& SetSchemeImpl(const TSchemeInfo& info) {
            Scheme = info.Kind;
            DefaultPort = info.Port;
            if (!info.Str.empty())
                FldSetNoDirty(FieldScheme, info.Str);
            return info;
        }
        const TSchemeInfo& SetSchemeImpl(TScheme::EKind scheme) {
            return SetSchemeImpl(TSchemeInfo::Get(scheme));
        }

    public:
        const TSchemeInfo& SetScheme(const TSchemeInfo& info) {
            SetSchemeImpl(info);
            if (!info.Str.empty())
                FldMarkClean(FieldScheme);
            return info;
        }
        const TSchemeInfo& SetScheme(TScheme::EKind scheme) {
            return SetScheme(TSchemeInfo::Get(scheme));
        }
    };

    class TUriUpdate {
        TUri& Uri_;

    public:
        TUriUpdate(TUri& uri)
            : Uri_(uri)
        {
        }
        ~TUriUpdate() {
            Uri_.Rewrite();
        }

    public:
        bool Set(TField::EField field, const TStringBuf& value) {
            return Uri_.FldMemSet(field, value);
        }

        template <size_t size>
        bool Set(TField::EField field, const char (&value)[size]) {
            return Uri_.FldMemSet(field, value);
        }

        void Clr(TField::EField field) {
            Uri_.FldClr(field);
        }
    };

    const char* LinkTypeToString(const TUri::TLinkType& t);

}

Y_DECLARE_OUT_SPEC(inline, NUri::TUri, out, url) {
    url.Print(out);
}

Y_DECLARE_OUT_SPEC(inline, NUri::TUri::TLinkType, out, t) {
    out << NUri::LinkTypeToString(t);
}
