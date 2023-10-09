#include "registrator.h"
#include "re.h"

#include <util/generic/fwd.h>
#include <util/generic/vector.h>
#include <util/generic/singleton.h>
#include <util/generic/yexception.h>

#include <ydb/library/rewrapper/proto/serialization.pb.h>

namespace NReWrapper {

namespace NRegistrator {

struct TLib {
    ui64 Id;
    TCompiler Compiler;
    TDeserializer Deserializer;
};

using TModules = TVector<TLib>;

TModules* GetModules() {
    return Singleton<TModules>();
}

void AddLibrary(ui32 id, TCompiler compiler, TDeserializer deserializer) {
    Y_ABORT_UNLESS(id > 0);
    if (GetModules()->size() < id) {
        GetModules()->resize(id);
    }
    GetModules()->at(id - 1) = TLib{id, compiler, deserializer};
}

}

namespace NDispatcher {

void ThrowOnOutOfRange(ui32 id) {
    if (NRegistrator::GetModules()->size() < id || id == 0) {
        ythrow yexception()
            << "Libs with id: " << id
            << " was not found. Total added libs: " << NRegistrator::GetModules()->size();
    }
}

IRePtr Deserialize(const TStringBuf& serializedRegex) {
    TSerialization proto;
    TString str(serializedRegex);
    auto res = proto.ParseFromString(str);
    if (!res) {
        proto.SetHyperscan(str);
    }

    ui64 id = (ui64)proto.GetDataCase();;
    ThrowOnOutOfRange(id);
    return NRegistrator::GetModules()->at(id - 1).Deserializer(proto);
}

IRePtr Compile(const TStringBuf& regex, unsigned int flags, ui32 id) {
    ThrowOnOutOfRange(id);
    return NRegistrator::GetModules()->at(id - 1).Compiler(regex, flags);
}

}

}
