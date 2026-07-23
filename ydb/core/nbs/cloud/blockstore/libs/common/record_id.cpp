#include "record_id.h"

#include <util/string/builder.h>

namespace NYdb::NBS::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

TString TRecordId::Print() const
{
    return TStringBuilder() << Generation << ":" << Lsn;
}

IOutputStream& operator<<(IOutputStream& out, const TRecordId& rhs)
{
    out << rhs.Print();
    return out;
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
