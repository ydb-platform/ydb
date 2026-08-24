#include "pbuffer_key.h"

#include <util/string/builder.h>

namespace NYdb::NBS::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

TString TPBufferKey::Print() const
{
    return TStringBuilder() << Generation << ":" << Lsn;
}

IOutputStream& operator<<(IOutputStream& out, const TPBufferKey& rhs)
{
    out << rhs.Print();
    return out;
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
