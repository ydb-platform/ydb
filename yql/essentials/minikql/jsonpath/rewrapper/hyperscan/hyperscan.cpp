#include <yql/essentials/minikql/jsonpath/rewrapper/re.h>
#include <yql/essentials/minikql/jsonpath/rewrapper/registrator.h>
#include <yql/essentials/minikql/jsonpath/rewrapper/proto/serialization.pb.h>
#include <library/cpp/regex/hyperscan/hyperscan.h>
#include <util/charset/utf8.h>

namespace NReWrapper::NHyperscan {

namespace {

class THyperscan: public IRe {
public:
    explicit THyperscan(::NHyperscan::TDatabase&& db)
        : Database_(std::move(db))
    {
    }

    bool Matches(const TStringBuf& text) const override {
        if (!Scratch_) {
            Scratch_ = ::NHyperscan::MakeScratch(Database_);
        }
        return ::NHyperscan::Matches(Database_, Scratch_, text);
    }

    TString Serialize() const override {
        // Compatibility with old versions
        return ::NHyperscan::Serialize(Database_);
        /*
         *       TSerialization proto;
         *       proto.SetHyperscan(::NHyperscan::Serialize(Database));
         *       TString data;
         *       auto res = proto.SerializeToString(&data);
         *       Y_ABORT_UNLESS(res);
         *       return data;
         */
    }

private:
    ::NHyperscan::TDatabase Database_;
    mutable ::NHyperscan::TScratch Scratch_;
};

} // namespace

IRePtr Compile(const TStringBuf& regex, unsigned int flags) {
    unsigned int hyperscanFlags = 0;
    try {
        if (UTF8Detect(regex)) {
            hyperscanFlags |= HS_FLAG_UTF8;
        }
        if (NX86::HaveAVX2()) {
            hyperscanFlags |= HS_CPU_FEATURES_AVX2;
        }
        if (flags & FLAGS_CASELESS) {
            hyperscanFlags |= HS_FLAG_CASELESS;
        }
        return std::make_unique<THyperscan>(::NHyperscan::Compile(regex, hyperscanFlags));
    } catch (const ::NHyperscan::TCompileException& ex) {
        ythrow TCompileException() << ex.what();
    }
}

IRePtr Deserialize(const TSerialization& proto) {
    return std::make_unique<THyperscan>(::NHyperscan::Deserialize(proto.GetHyperscan()));
}

REGISTER_RE_LIB(TSerialization::kHyperscan, Compile, Deserialize)

} // namespace NReWrapper::NHyperscan
