#include <yt/yql/providers/yt/fmr/coordinator/yt_coordinator_service/interface/yql_yt_coordinator_service_interface.h>

namespace NYql::NFmr {

IYtCoordinatorService::TPtr MakeFileYtCoordinatorService();

} // namespace NYql::NFmr
