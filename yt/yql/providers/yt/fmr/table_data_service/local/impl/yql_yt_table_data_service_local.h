#include <library/cpp/threading/future/future.h>
#include <yt/yql/providers/yt/fmr/table_data_service/local/interface/yql_yt_table_data_service_local_interface.h>

namespace NYql::NFmr {

ILocalTableDataService::TPtr MakeLocalTableDataService();

} // namespace NYql::NFmr
