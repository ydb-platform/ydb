#include <yt/cpp/mapreduce/common/helpers.h>
#include <yt/cpp/mapreduce/interface/client.h>

#include "yql_yt_yt_service_impl.h"

namespace NYql::NFmr {

namespace {

class TFmrYtService: public NYql::NFmr::IYtService {
public:
    NYT::TRawTableReaderPtr MakeReader(
        const TYtTableRef& ytTable,
        const TClusterConnection& clusterConnection,
        const TYtReaderSettings& readerSettings
    ) override {
        auto client = CreateClient(clusterConnection);
        auto transaction = client->AttachTransaction(GetGuid(clusterConnection.TransactionId));
        auto path = NYT::TRichYPath(NYT::AddPathPrefix(ytTable.Path, "//"));
        auto controlAttributes = NYT::TControlAttributes();
        if (!readerSettings.WithAttributes) {
            controlAttributes.EnableRangeIndex(false).EnableRowIndex(false);
        }
        auto readerOptions = NYT::TTableReaderOptions().ControlAttributes(controlAttributes);
        return transaction->CreateRawReader(path, NYT::TFormat::YsonBinary(), readerOptions);
    }

    NYT::TRawTableWriterPtr MakeWriter(
        const TYtTableRef& ytTable,
        const TClusterConnection& clusterConnection,
        const TYtWriterSettings& writerSetttings
    ) override {
        auto client = CreateClient(clusterConnection);
        auto transaction = client->AttachTransaction(GetGuid(clusterConnection.TransactionId));
        auto path = NYT::TRichYPath(NYT::AddPathPrefix(ytTable.Path, "//"));
        auto richPath = NYT::TRichYPath(path).Append(writerSetttings.AppendMode);
        return transaction->CreateRawWriter(richPath, NYT::TFormat::YsonBinary());
    }

private:
    NYT::IClientPtr CreateClient(const TClusterConnection& clusterConnection) {
        NYT::TCreateClientOptions createOpts;
        auto token = clusterConnection.Token;
        if (token) {
            createOpts.Token(*token);
        }
        return NYT::CreateClient(clusterConnection.YtServerName, createOpts);
    }
};

} // namespace

IYtService::TPtr MakeFmrYtSerivce() {
    return MakeIntrusive<TFmrYtService>();
}

} // namespace NYql::NFmr
