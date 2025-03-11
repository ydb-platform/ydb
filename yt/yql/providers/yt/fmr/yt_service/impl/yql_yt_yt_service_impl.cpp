#include <library/cpp/yson/node/node_io.h>
#include <library/cpp/yson/node/node_visitor.h>
#include <library/cpp/yson/parser.h>
#include <library/cpp/yson/writer.h>
#include <yt/cpp/mapreduce/interface/client.h>

#include "yql_yt_yt_service_impl.h"

namespace NYql::NFmr {

namespace {

class TYtFmrRowConsumer: public NYson::TYsonConsumerBase {
public:
    TYtFmrRowConsumer(NYT::TTableWriterPtr<NYT::TNode> writer): Writer_(writer)
    {
        CurNode_ = NYT::TNode();
        Builder_ = MakeHolder<NYT::TNodeBuilder>(&CurNode_);
    }

    void OnStringScalar(TStringBuf value) override {
        Builder_->OnStringScalar(value);
    }

    void OnInt64Scalar(i64 value) override {
        Builder_->OnInt64Scalar(value);
    }

    void OnUint64Scalar(ui64 value) override {
        Builder_->OnUint64Scalar(value);
    }

    void OnDoubleScalar(double value) override {
        Builder_->OnDoubleScalar(value);
    }

    void OnBooleanScalar(bool value) override {
        Builder_->OnBooleanScalar(value);
    }

    void OnEntity() override {
        Builder_->OnEntity();
    }

    void OnBeginList() override {
        ++Level_;
        Builder_->OnBeginList();
    }

    void OnBeginList(ui64 reserveSize) {
        Builder_->OnBeginList(reserveSize);
    }

    void OnListItem() override {
        if (Level_ == 0) {
            Flush();
        } else {
            Builder_->OnListItem();
        }
    }

    void OnEndList() override {
        --Level_;
        Builder_->OnEndList();
    }

    void OnBeginMap() override {
        ++Level_;
        Builder_->OnBeginMap();
    }

    void OnBeginMap(ui64 reserveSize) {
        Builder_->OnBeginMap(reserveSize);
    }

    void OnKeyedItem(TStringBuf key) override {
        Builder_->OnKeyedItem(key);
    }

    void OnEndMap() override {
        --Level_;
        Builder_->OnEndMap();
    }

    void OnBeginAttributes() override {
        ++Level_;
        Builder_->OnBeginAttributes();
    }

    void OnEndAttributes() override {
        --Level_;
        Builder_->OnEndAttributes();
    }

    void OnNode(NYT::TNode node) {
        Builder_->OnNode(node);
    }

    ~TYtFmrRowConsumer() {
        Y_ABORT_UNLESS(!CurNode_.HasValue());
    }

    void Flush() {
        if (CurNode_.HasValue()) {
            Writer_->AddRow(CurNode_);
        }
        CurNode_ = NYT::TNode();
        Builder_ = MakeHolder<NYT::TNodeBuilder>(&CurNode_);
    }
private:
    NYT::TNode CurNode_;
    NYT::TTableWriterPtr<NYT::TNode> Writer_;
    THolder<NYT::TNodeBuilder> Builder_;
    ui32 Level_ = 0;
};

class TFmrYtService: public NYql::NFmr::IYtService {
public:

    std::variant<THolder<TTempFileHandle>, TError> Download(const TYtTableRef& ytTable, ui64& rowsCount, const TClusterConnection& clusterConnection) override {
        try {
            if (!ClusterConnections_.contains(ytTable.Cluster)) {
                ClusterConnections_[ytTable.Cluster] = clusterConnection;
            }
            auto client = CreateClient(ClusterConnections_[ytTable.Cluster]);
            auto tmpFile = MakeHolder<TTempFileHandle>();
            auto transaction = client->AttachTransaction(GetGuid(clusterConnection.TransactionId));
            TFileOutput downloadStream(tmpFile->Name());

            NYson::TYsonWriter writer {&downloadStream, NYT::NYson::EYsonFormat::Text, NYT::NYson::EYsonType::ListFragment};
            NYT::TNodeVisitor visitor{&writer};
            auto reader = transaction->CreateTableReader<NYT::TNode>("//" + ytTable.Path);
            for (; reader->IsValid(); reader->Next()) {
                auto& row = reader->GetRow();
                writer.OnListItem();
                visitor.Visit(row);
                rowsCount++;
            }
            downloadStream.Flush();
            return tmpFile;
        } catch (...) {
            return TError{CurrentExceptionMessage()};
        }
    }

    TMaybe<TError> Upload(const TYtTableRef& ytTable, IInputStream& tableContent, const TClusterConnection& clusterConnection) override {
        try  {
            if (!ClusterConnections_.contains(ytTable.Cluster)) {
                ClusterConnections_[ytTable.Cluster] = clusterConnection;
            }
            auto client = CreateClient(ClusterConnections_[ytTable.Cluster]);
            auto transaction = client->AttachTransaction(GetGuid(clusterConnection.TransactionId));
            auto options = NYT::TCreateOptions().Recursive(true).IgnoreExisting(true);
            transaction->Create("//" + ytTable.Path, NYT::NT_TABLE, options);
            auto path = NYT::TRichYPath("//" + ytTable.Path).Append(true);
            auto ytWriter = transaction->CreateTableWriter<NYT::TNode>(path);

            TYtFmrRowConsumer builder(ytWriter);
            NYson::TYsonParser parser(&builder, &tableContent, NYT::NYson::EYsonType::ListFragment);
            parser.Parse();
            builder.Flush();
            return Nothing();
        } catch (...) {
            return TError{CurrentExceptionMessage()};
        }
    }

private:
    std::unordered_map<TString, TClusterConnection> ClusterConnections_;

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
