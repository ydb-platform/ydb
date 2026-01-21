#include "kqp_rbo.h"
#include "kqp_rbo_physical_convertion_utils.h"
#include <yql/essentials/core/yql_opt_utils.h>
#include <yql/essentials/utils/log/log.h>

using namespace NYql::NNodes;
using namespace NKikimr;
using namespace NKikimr::NKqp;

namespace NKikimr::NKqp::NPhysicalConvertionUtils {
TString GetFullName(const TString& name) {
    return name;
}
TString GetFullName(const TInfoUnit& name) {
    return name.GetFullName();
}
} // namespace NKikimr::NKqp::NPhysicalConvertionUtils
