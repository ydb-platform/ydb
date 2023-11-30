#include "actorid.h"
#include <util/string/builder.h>
#include <util/string/cast.h>

namespace NActors {
    void TActorId::Out(IOutputStream& o) const {
        o << "[" << NodeId() << ":" << LocalId() << ":" << Hint() << "]";
    }

    TString TActorId::ToString() const {
        TString x;
        TStringOutput o(x);
        Out(o);
        return x;
    }

    bool TActorId::Parse(const char* buf, ui32 sz) {
        if (sz < 4 || buf[0] != '[' || buf[sz - 1] != ']')
            return false;

        size_t semicolons[2];
        TStringBuf str(buf, sz);
        semicolons[0] = str.find(':', 1);
        if (semicolons[0] == TStringBuf::npos)
            return false;
        semicolons[1] = str.find(':', semicolons[0] + 1);
        if (semicolons[1] == TStringBuf::npos)
            return false;

        bool success = TryFromString(buf + 1, semicolons[0] - 1, Raw.N.NodeId) && TryFromString(buf + semicolons[0] + 1, semicolons[1] - semicolons[0] - 1, Raw.N.LocalId) && TryFromString(buf + semicolons[1] + 1, sz - semicolons[1] - 2, Raw.N.Hint);

        return success;
    }
}
