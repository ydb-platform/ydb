#pragma once

#include <yt/yql/providers/yt/fmr/table_data_service/interface/yql_yt_table_data_service.h>

namespace NYql::NFmr {

class IFmrGcService: public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<IFmrGcService>;

    virtual ~IFmrGcService() = default;

    // clears specified groups from data service
    virtual NThreading::TFuture<void> ClearGarbage(const std::vector<TString>& groupsToDelete) = 0;

    // totally clears data service
    virtual NThreading::TFuture<void> ClearAll() = 0;
};

} // namespace NYql::NFmr
