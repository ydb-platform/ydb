#pragma once

#include <library/cpp/yt/global/access.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

// NB(arkady-e1ppa): This tag name conflicts with the one in module2.
// However, if module2_defs is not linked at the same time as
// module3_defs, you are guaranteed to read global from
// module3_defs.
// If you happen to mistakenly link module2_defs
// instead of module3_defs, you will actually
// read erased varibale from module_defs.
// Be careful!
inline constexpr NGlobal::TVariableTag TestTag3 = {};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
