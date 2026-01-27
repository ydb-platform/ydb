#include <library/cpp/yt/error/error.h>
#include <library/cpp/yson/node/node_io.h>
#include <yt/cpp/mapreduce/common/helpers.h>
#include <yt/cpp/mapreduce/interface/client.h>
#include <yt/cpp/mapreduce/client/client.h>
#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/utils/yql_panic.h>

#include "yql_yt_job_service_impl.h"
#include "yql_yt_table_write_distributed_session.h"

namespace NYql::NFmr {

namespace {

NYT::IRawClientPtr GetRawClient(const NYT::IClientPtr& client) {
    auto concreteClient = dynamic_cast<NYT::NDetail::TClient*>(client.Get());
    YQL_ENSURE(concreteClient, "Cannot cast IClient to TClient");
    return concreteClient->GetRawClient();
}

class TFmrYtJobService: public IYtJobService {
public:
    NYT::TRawTableReaderPtr MakeReader(
        const TYtTableRef& ytTablePart,
        const TClusterConnection& clusterConnection,
        const TYtReaderSettings& readerSettings
    ) override {

        auto richPath = ytTablePart.RichPath;
        NormalizeRichPath(richPath);
        YQL_ENSURE(richPath.Cluster_);
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
        const TYtWriterSettings& writerSettings
    ) override {
        auto client = CreateClient(clusterConnection);
        auto transaction = client->AttachTransaction(GetGuid(clusterConnection.TransactionId));
        auto writerOptions = NYT::TTableWriterOptions();
        if (writerSettings.MaxRowWeight) {
            writerOptions.Config(NYT::TNode()("max_row_weight", *writerSettings.MaxRowWeight));
        }
        auto richPath = ytTable.RichPath;
        NormalizeRichPath(richPath);
        richPath.Append(true);
        return transaction->CreateRawWriter(richPath, NYT::TFormat::YsonBinary(), writerOptions);
    }

    IWriteDistributedSession::TPtr StartDistributedWriteSession(
        const TYtTableRef& ytTable,
        ui64 cookieCount,
        const TClusterConnection& clusterConnection,
        const TStartDistributedWriteOptions& options) override
    {
        auto client = CreateClient(clusterConnection);

        NYT::TStartDistributedWriteTableOptions startOptions;
        startOptions.Timeout(options.Timeout);

        auto transaction = client->AttachTransaction(GetGuid(clusterConnection.TransactionId));
        auto sessionWithCookies = transaction->StartDistributedWriteTableSession(
            ytTable.RichPath,
            cookieCount,
            startOptions);

        TTableWriteDistributedSessionOptions sessionOptions{.PingInterval = options.PingInterval};
        return MakeIntrusive<TTableWriteDistributedSession>(
            sessionWithCookies.Session_,
            TVector<NYT::TDistributedWriteTableCookie>(sessionWithCookies.Cookies_),
            sessionOptions,
            clusterConnection);
    }

    std::unique_ptr<NYT::IOutputStreamWithResponse> GetDistributedWriter(
        const TString& cookieYson,
        const TClusterConnection& clusterConnection
    ) override {
        auto client = CreateClient(clusterConnection);
        auto raw = GetRawClient(client);

        NYT::TDistributedWriteTableCookie cookie(NYT::NodeFromYsonString(cookieYson));
        NYT::TTableFragmentWriterOptions options;
        return raw->WriteTableFragment(cookie, NYT::TFormat::YsonBinary(), options);
    }

    void Create(
        const TYtTableRef& ytTable,
        const TClusterConnection& clusterConnection,
        const NYT::TNode& attributes
    ) override {
        auto client = CreateClient(clusterConnection);
        auto richPath = ytTable.RichPath;
        NormalizeRichPath(richPath);
        auto transaction = client->AttachTransaction(GetGuid(clusterConnection.TransactionId));
        transaction->Create(richPath.Path_, NYT::NT_TABLE, NYT::TCreateOptions().Recursive(true).IgnoreExisting(true).Attributes(attributes));
    }
};

} // namespace

IYtJobService::TPtr MakeYtJobSerivce() {
    return MakeIntrusive<TFmrYtJobService>();
}

} // namespace NYql::NFmr
