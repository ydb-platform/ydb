#pragma once

#include <util/generic/fwd.h>

#define REGISTER_RE_LIB(...) \
    namespace { \
        struct TReWrapperStaticRegistrator { \
            inline TReWrapperStaticRegistrator() { \
               NRegistrator::AddLibrary(__VA_ARGS__); \
            } \
        } RE_REGISTRATOR; \
    }

namespace NReWrapper {

class IRe;
class TSerialization;
using IRePtr = std::unique_ptr<IRe>;

namespace NRegistrator {

using TCompiler = IRePtr(*)(const TStringBuf&, unsigned int);
using TDeserializer = IRePtr(*)(const TSerialization&);

void AddLibrary(ui32 id, TCompiler compiler, TDeserializer deserializer);

}
}
