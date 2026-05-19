#include <ydb/core/control/lib/immediate_control_board_wrapper.h>

namespace NKikimr::NKqp {

bool IsEnabledReadsMerge();

// The calls below are for unittests only
void SetMaxTaskSize(ui64 size);
ui64 GetMaxTaskSize();

} // namespace NKikimr::NKqp
