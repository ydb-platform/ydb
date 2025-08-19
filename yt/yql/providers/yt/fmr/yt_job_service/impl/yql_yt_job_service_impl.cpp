#include <library/cpp/yt/error/error.h>
#include <yt/cpp/mapreduce/common/helpers.h>
#include <yt/cpp/mapreduce/interface/client.h>
#include <yt/yql/providers/yt/fmr/utils/yql_yt_client.h>
#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/utils/yql_panic.h>

#include "yql_yt_job_service_impl.h"

namespace NYql::NFmr {

namespace {

class TFmrYtJobService: public IYtJobService {
public:
    NYT::TRawTableReaderPtr MakeReader(
        const TYtTableRef& ytTablePart,
        const TClusterConnection& clusterConnection,
        const TYtReaderSettings& readerSettings
    ) override {
        auto richPath = ytTablePart.RichPath;
        YQL_ENSURE(richPath.Cluster_);
        TFmrTableId fmrId(*richPath.Cluster_, richPath.Path_);
        auto client = CreateClient(clusterConnection);
        auto transaction = client->AttachTransaction(GetGuid(clusterConnection.TransactionId));

        auto controlAttributes = NYT::TControlAttributes();
        if (!readerSettings.WithAttributes) {
            controlAttributes.EnableRangeIndex(false).EnableRowIndex(false);
        }
        auto readerOptions = NYT::TTableReaderOptions().ControlAttributes(controlAttributes);
        return transaction->CreateRawReader(richPath, NYT::TFormat::YsonBinary(), readerOptions);
    }

    NYT::TRawTableWriterPtr MakeWriter(
        const TYtTableRef& ytTable,
        const TClusterConnection& clusterConnection,
        const TYtWriterSettings& writerSetttings
    ) override {
        auto client = CreateClient(clusterConnection);
        auto transaction = client->AttachTransaction(GetGuid(clusterConnection.TransactionId));
        auto writerOptions = NYT::TTableWriterOptions();
        if (writerSetttings.MaxRowWeight) {
            writerOptions.Config(NYT::TNode()("max_row_weight", *writerSetttings.MaxRowWeight));
        }
        return transaction->CreateRawWriter(ytTable.RichPath, NYT::TFormat::YsonBinary(), writerOptions);
    }
};

} // namespace

IYtJobService::TPtr MakeYtJobSerivce() {
    return MakeIntrusive<TFmrYtJobService>();
}

} // namespace NYql::NFmr
