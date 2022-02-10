#include <library/cpp/resource/resource.h>

#include <util/string/strip.h>
 
namespace {
    class TBuiltinVersion {
    public:
        TBuiltinVersion() {
            Version_ = NResource::Find("/builtin/version");
            StripInPlace(Version_);
        }

        TStringBuf Get() const {
            return Version_;
        }

    private:
        TString Version_;
    };
}

namespace NTvmAuth {
    TStringBuf LibVersion() { 
        return Singleton<TBuiltinVersion>()->Get();
    } 
}
