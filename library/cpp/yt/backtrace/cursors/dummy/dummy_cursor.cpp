#include "dummy_cursor.h"

namespace NYT::NBacktrace {

////////////////////////////////////////////////////////////////////////////////

bool TDummyCursor::IsFinished() const
{
    return true;
}

const void* TDummyCursor::GetCurrentIP() const
{
    return nullptr;
}

void TDummyCursor::MoveNext()
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBacktrace
