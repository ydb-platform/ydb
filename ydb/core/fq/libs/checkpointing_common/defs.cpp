#include "defs.h"

#include <util/digest/multi.h>

namespace NFq {

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

} // namespace NFq

////////////////////////////////////////////////////////////////////////////////

template<>
void Out<NFq::TCoordinatorId>(
    IOutputStream& out,
    const NFq::TCoordinatorId& coordinatorId)
{
    coordinatorId.PrintTo(out);
}

template<>
void Out<NFq::TCheckpointId>(
    IOutputStream& out,
    const NFq::TCheckpointId& checkpointId)
{
    out << checkpointId.CoordinatorGeneration << ":" << checkpointId.SeqNo;
}
