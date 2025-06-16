#pragma once

#include <yt/yql/providers/yt/fmr/table_data_service/interface/yql_yt_table_data_service.h>

namespace NYql::NFmr {

struct TTableDataServiceStats {
    ui64 KeysNum = 0;
    ui64 DataWeight = 0; // Sum of len of all values
};

class ILocalTableDataService: public ITableDataService {
public:
    using TPtr = TIntrusivePtr<ILocalTableDataService>;

    virtual ~ILocalTableDataService() = default;

    virtual NThreading::TFuture<TTableDataServiceStats> GetStatistics() const = 0;
};

} // namespace NYql::NFmr
