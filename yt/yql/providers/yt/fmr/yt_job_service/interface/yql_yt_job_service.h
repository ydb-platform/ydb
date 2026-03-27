#pragma once

#include <util/system/tempfile.h>
#include <yt/cpp/mapreduce/interface/io.h>
#include <yt/cpp/mapreduce/interface/raw_client.h>
#include <yt/yql/providers/yt/fmr/request_options/yql_yt_request_options.h>
#include <yt/cpp/mapreduce/interface/distributed_session.h>
#include <yt/yql/providers/yt/fmr/utils/yql_yt_client.h>
#include <yt/yql/providers/yt/fmr/yt_job_service/interface/yql_yt_write_distributed_session.h>

namespace NYql::NFmr {

class IYtJobService: public TThrRefBase {
public:
    virtual ~IYtJobService() = default;

    using TPtr = TIntrusivePtr<IYtJobService>;

    virtual NYT::TRawTableReaderPtr MakeReader(
        const TYtTableRef& ytTablePart,
        const TClusterConnection& clusterConnection = TClusterConnection(),
        const TYtReaderSettings& settings = TYtReaderSettings()
    ) = 0;

    virtual NYT::TRawTableWriterPtr MakeWriter(
        const TYtTableRef& ytTable,
        const TClusterConnection& clusterConnection,
        const TYtWriterSettings& settings = TYtWriterSettings()
    ) = 0;


    virtual IWriteDistributedSession::TPtr StartDistributedWriteSession(
        const TYtTableRef& ytTable,
        ui64 cookieCount,
        const TClusterConnection& clusterConnection,
        const TStartDistributedWriteOptions& options
    ) = 0;

    virtual std::unique_ptr<NYT::IOutputStreamWithResponse> GetDistributedWriter(
        const TString& cookieYson,
        const TClusterConnection& clusterConnection
    ) = 0;

    virtual void Create(
        const TYtTableRef& ytTable,
        const TClusterConnection& clusterConnection,
        const NYT::TNode& attributes
    ) = 0;
};

} // namespace NYql::NFmr
