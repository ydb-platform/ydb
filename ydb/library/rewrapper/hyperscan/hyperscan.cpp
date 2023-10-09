#include <ydb/library/rewrapper/re.h>
#include <ydb/library/rewrapper/registrator.h>
#include <ydb/library/rewrapper/proto/serialization.pb.h>
#include <library/cpp/regex/hyperscan/hyperscan.h>
#include <util/charset/utf8.h>

namespace NReWrapper {
namespace NHyperscan {

namespace {

class THyperscan : public IRe {
public:
    THyperscan(::NHyperscan::TDatabase&& db)
        : Database(std::move(db))
    { }

    bool Matches(const TStringBuf& text) const override {
        if (!Scratch) {
            Scratch = ::NHyperscan::MakeScratch(Database);
        }
        return ::NHyperscan::Matches(Database, Scratch, text);
    }

    TString Serialize() const override {
        // Compatibility with old versions
        return ::NHyperscan::Serialize(Database);
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
    ::NHyperscan::TDatabase Database;
    mutable ::NHyperscan::TScratch Scratch;
};

}

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

}
}
