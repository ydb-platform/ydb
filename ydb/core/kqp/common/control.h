#include <ydb/core/control/lib/immediate_control_board_wrapper.h>
#include <util/generic/size_literals.h>

namespace NKikimr::NKqp {

bool IsEnabledReadsMerge();
void SetMaxTaskSize(ui64 size);
ui64 GetMaxTaskSize();

} // namespace NKikimr::NKqp
