#pragma once

#include <util/system/tempfile.h>
#include <yt/cpp/mapreduce/interface/io.h>
#include <yt/yql/providers/yt/fmr/request_options/yql_yt_request_options.h>

namespace NYql::NFmr {

struct TYtReaderSettings {
    bool WithAttributes = false; // Enable RowIndex and RangeIndex, for now only mode = false is supported.
};

struct TYtWriterSettings {
    TMaybe<ui64> MaxRowWeight = Nothing();
};

class IYtJobService: public TThrRefBase {
public:
    virtual ~IYtJobService() = default;

    using TPtr = TIntrusivePtr<IYtJobService>;

    // Either RichPath to actual Yt table or filepath is passed depending on type of underlying gateway.
    virtual NYT::TRawTableReaderPtr MakeReader(
        const std::variant<NYT::TRichYPath, TString>& inputTableRef,
        const TClusterConnection& clusterConnection = TClusterConnection(),
        const TYtReaderSettings& settings = TYtReaderSettings()
    ) = 0;

    virtual NYT::TRawTableWriterPtr MakeWriter(
        const TYtTableRef& ytTable,
        const TClusterConnection& clusterConnection,
        const TYtWriterSettings& settings = TYtWriterSettings()
    ) = 0;
};

} // namespace NYql::NFmr
