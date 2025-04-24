#include <library/cpp/threading/future/future.h>
#include <yt/yql/providers/yt/fmr/table_data_service/interface/yql_yt_table_data_service.h>

namespace NYql::NFmr {

struct TLocalTableDataServiceSettings {
    ui32 NumParts;
};

ITableDataService::TPtr MakeLocalTableDataService(const TLocalTableDataServiceSettings& settings);

} // namespace NYql::NFmr
