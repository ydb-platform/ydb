#include "id_generator.h"
#include "serialize.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

ui64 TIdGenerator::Next()
{
    return Current_++;
}

void TIdGenerator::Reset()
{
    Current_ = 0;
}

void TIdGenerator::Save(TStreamSaveContext& context) const
{
    NYT::Save(context, Current_.load());
}

void TIdGenerator::Load(TStreamLoadContext& context)
{
    Current_ = NYT::Load<ui64>(context);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

