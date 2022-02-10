#include "defs.h"

#include <util/digest/multi.h>

namespace NYq {

////////////////////////////////////////////////////////////////////////////////

TString TCoordinatorId::ToString() const {
    TStringStream ss;
    PrintTo(ss);
    return ss.Str();
}

void TCoordinatorId::PrintTo(IOutputStream& out) const
{
    out << GraphId << "." << Generation;
}

size_t TCheckpointIdHash::operator ()(const TCheckpointId& checkpointId)
{
    return MultiHash(checkpointId.CoordinatorGeneration, checkpointId.SeqNo);
}

} // namespace NYq

////////////////////////////////////////////////////////////////////////////////

template<>
void Out<NYq::TCoordinatorId>(
    IOutputStream& out,
    const NYq::TCoordinatorId& coordinatorId)
{
    coordinatorId.PrintTo(out);
}

template<>
void Out<NYq::TCheckpointId>(
    IOutputStream& out,
    const NYq::TCheckpointId& checkpointId)
{
    out << checkpointId.CoordinatorGeneration << ":" << checkpointId.SeqNo;
}
