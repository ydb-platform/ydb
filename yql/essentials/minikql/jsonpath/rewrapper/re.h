#pragma once

#include <memory>

#include <util/generic/fwd.h>
#include <util/generic/yexception.h>

namespace NReWrapper {

class TCompileException : public yexception {
};

enum EFlags {
    FLAGS_CASELESS = 1,
};

class IRe {
public:
    virtual ~IRe() = default;
    virtual bool Matches(const TStringBuf& text) const = 0;
    virtual TString Serialize() const = 0;
};

using IRePtr = std::unique_ptr<IRe>;

namespace NDispatcher {
    IRePtr Compile(const TStringBuf& regex, unsigned int flags, ui32 id);
    IRePtr Deserialize(const TStringBuf& serializedRegex);
}

}
