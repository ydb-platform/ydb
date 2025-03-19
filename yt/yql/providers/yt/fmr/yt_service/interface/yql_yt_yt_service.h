#pragma once

#include <util/system/tempfile.h>
#include <yt/cpp/mapreduce/interface/io.h>
#include <yt/yql/providers/yt/fmr/request_options/yql_yt_request_options.h>

namespace NYql::NFmr {

struct TYtReaderSettings {
    bool WithAttributes = false; // Enable RowIndex and RangeIndex
};

struct TYtWriterSettings {
    bool AppendMode = true;
};

class IYtService: public TThrRefBase {
public:
    virtual ~IYtService() = default;

    using TPtr = TIntrusivePtr<IYtService>;

    virtual NYT::TRawTableReaderPtr MakeReader(
        const TYtTableRef& ytTable,
        const TClusterConnection& clusterConnection,
        const TYtReaderSettings& settings = TYtReaderSettings()
    ) = 0;

    virtual NYT::TRawTableWriterPtr MakeWriter(
        const TYtTableRef& ytTable,
        const TClusterConnection& clusterConnection,
        const TYtWriterSettings& settings = TYtWriterSettings()
    ) = 0;
};

} // namespace NYql::NFmr
