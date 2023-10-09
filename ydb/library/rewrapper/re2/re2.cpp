#include <ydb/library/rewrapper/re.h>
#include <ydb/library/rewrapper/registrator.h>
#include <ydb/library/rewrapper/proto/serialization.pb.h>
#include <contrib/libs/re2/re2/re2.h>
#include <util/charset/utf8.h>

namespace NReWrapper {

using namespace re2;

namespace NRe2 {

namespace {

RE2::Options CreateOptions(const TStringBuf& regex, unsigned int flags) {
    RE2::Options options;
    bool needUtf8 = (UTF8Detect(regex) == UTF8);
    options.set_encoding(
        needUtf8
            ? RE2::Options::Encoding::EncodingUTF8
            : RE2::Options::Encoding::EncodingLatin1
    );
    options.set_case_sensitive(!(flags & FLAGS_CASELESS));
    return options;
}

class TRe2 : public IRe {
public:
    TRe2(const TStringBuf& regex, unsigned int flags)
        : Regexp(StringPiece(regex.data(), regex.size()), CreateOptions(regex, flags))
    {
        auto re2 = RawRegexp.MutableRe2();
        re2->set_regexp(TString(regex));
        re2->set_flags(flags);
    }

    TRe2(const TSerialization& proto)
        : Regexp(StringPiece(proto.GetRe2().GetRegexp().data(), proto.GetRe2().GetRegexp().size()),
            CreateOptions(proto.GetRe2().GetRegexp(), proto.GetRe2().GetFlags()))
        , RawRegexp(proto)
    { }

    bool Matches(const TStringBuf& text) const override {
        const StringPiece piece(text.data(), text.size());
        RE2::Anchor anchor = RE2::UNANCHORED;

        return Regexp.Match(piece, 0, text.size(), anchor, nullptr, 0);
    }

    TString Serialize() const override {
        TString data;
        auto res = RawRegexp.SerializeToString(&data);
        Y_ABORT_UNLESS(res);
        return data;
    }

    bool Ok(TString* error) const {
        if (Regexp.ok()) {
            return true;
        } else {
            *error = Regexp.error();
            return false;
        }
    }
private:
    RE2 Regexp;
    TSerialization RawRegexp;
};

}

IRePtr Compile(const TStringBuf& regex, unsigned int flags) {
    auto ptr = std::make_unique<TRe2>(regex, flags);
    TString error;
    if (!ptr->Ok(&error)) {
        ythrow TCompileException() << error;
    }
    return ptr;
}

IRePtr Deserialize(const TSerialization& p) {
    return std::make_unique<TRe2>(p);
}

REGISTER_RE_LIB(TSerialization::kRe2, Compile, Deserialize)

}

}
