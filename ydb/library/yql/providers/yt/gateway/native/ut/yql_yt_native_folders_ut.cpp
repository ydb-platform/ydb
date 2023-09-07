#include "library/cpp/testing/unittest/registar.h"
#include <library/cpp/yson/node/node_io.h>
#include <ydb/library/yql/core/ut_common/yql_ut_common.h>
#include <library/cpp/testing/common/network.h>
#include <library/cpp/testing/mock_server/server.h>
#include <ydb/library/yql/providers/yt/gateway/native/yql_yt_native.h>
#include <ydb/library/yql/core/file_storage/proto/file_storage.pb.h>
#include <ydb/library/yql/providers/common/proto/gateways_config.pb.h>
#include <ydb/library/yql/providers/yt/provider/yql_yt_provider.h>

namespace NYql {

namespace {

constexpr auto CYPRES_TX_ID = "\"123321\"";
constexpr auto CYPRES_NODE_A_CONTENT = R"(
[
    {
        output = [
            <
                "user_attributes" = {};
                "type" = "table";
            > "a";
            <
                "user_attributes" = {};
                "type" = "table";
            > "b";
            <
                "user_attributes" = {};
                "target_path" = "//link_dest";
                "broken" = %false;
                "type" = "link";
            > "link";
            <
                "user_attributes" = {};
                "target_path" = "//link_broken_dest";
                "broken" = %true;
                "type" = "link";
            > "link_broken";
        ];
    };
]
)";
constexpr auto CYPRES_LINK_DEST = R"(
[
  {
    "output" = <
        "user_attributes" = {};
        "type" = "table";
    > #;
  };
]
)";
TVector<IYtGateway::TFolderResult::TFolderItem> EXPECTED_ITEMS {
    {"test/a/a", "table", R"({"user_attributes"={}})"},
    {"test/a/b", "table", R"({"user_attributes"={}})"},
    {"test/a/link", "table", R"({"user_attributes"={}})"}
};

TGatewaysConfig MakeGatewaysConfig(size_t port)
{
    TGatewaysConfig config {};
    auto* clusters = config.MutableYt()->MutableClusterMapping();
    NYql::TYtClusterConfig cluster;
    cluster.SetName("ut_cluster");
    cluster.SetYTName("ut_cluster");
    cluster.SetCluster("localhost:" + ToString(port));
    clusters->Add(std::move(cluster));
    return config;
}

class TYtReplier : public TRequestReplier {
public:
    bool DoReply(const TReplyParams& params) override {
        const TParsedHttpFull parsed(params.Input.FirstLine());
        Cout << parsed.Path << Endl;

        HttpCodes code = HTTP_NOT_FOUND;
        TString content;
        if (parsed.Path == "/api/v3/start_tx") {
            content = CYPRES_TX_ID;
            code = HTTP_OK;
        }
        else if (parsed.Path == "/api/v3/ping_tx") {
            code = HTTP_OK;
        }
        else if (parsed.Path == "/api/v3/execute_batch") {
            auto executeBatchRes = HandleExecuteBatch(params.Input);
            executeBatchRes.OutTo(params.Output);
            return true;
        }
        THttpResponse resp(code);
        resp.SetContent(content);
        resp.OutTo(params.Output);

        return true;
    }
    explicit TYtReplier(THashMap<TString, THashSet<TString>> requiredAttributes): requiredAttributes(requiredAttributes) {}

private:
    void CheckReqAttributes(TStringBuf path, const NYT::TNode& attributes) {
        THashSet<TString> attributesSet;
        for (const auto& attribute : attributes.AsList()) {
            attributesSet.insert(attribute.AsString());
        }
        UNIT_ASSERT_VALUES_EQUAL(requiredAttributes[path], attributesSet);
    }

    THttpResponse HandleListCommand(const NYT::TNode& path, const NYT::TNode& attributes) {
        CheckReqAttributes(path.AsString(), attributes);

        THttpResponse resp{HTTP_OK};
        if (path == "//test/a") {
            resp.SetContent(CYPRES_NODE_A_CONTENT);
            return resp;
        }
        return THttpResponse{HTTP_NOT_FOUND};
    }

    THttpResponse HandleGetCommand(const NYT::TNode& path, const NYT::TNode& attributes) {
        CheckReqAttributes(path.AsString(), attributes);

        THttpResponse resp{HTTP_OK};
        if (path == "//link_dest") {
            resp.SetContent(CYPRES_LINK_DEST);
            return resp;
        }

        return THttpResponse{HTTP_NOT_FOUND};
    }

    THttpResponse HandleExecuteBatch(THttpInput& input) {
        auto requestBody = input.ReadAll();
        auto requestBodyNode = NYT::NodeFromYsonString(requestBody);
        if (!requestBodyNode.HasKey("requests")) {
            return THttpResponse{HTTP_INTERNAL_SERVER_ERROR};
        }
        auto& requests = requestBodyNode["requests"];
        if (!requests.IsList()) {
            return THttpResponse{HTTP_INTERNAL_SERVER_ERROR};
        }
        for (auto& request : requests.AsList()) {
            auto& command = request["command"];
            auto& parameters = request["parameters"];
            if (command == "list") {
                return HandleListCommand(parameters["path"], parameters.HasKey("attributes") ? parameters["attributes"] : NYT::TNode{});
            }
            if (command == "get") {
                return HandleGetCommand(parameters["path"], parameters.HasKey("attributes") ? parameters["attributes"] : NYT::TNode{});
            }
        }
        return THttpResponse{HTTP_NOT_FOUND};
    }

    THashMap<TString, THashSet<TString>> requiredAttributes;
};

Y_UNIT_TEST_SUITE(YtNativeGateway) {

Y_UNIT_TEST(GetFolder) {
    const auto port = NTesting::GetFreePort();
    THashMap<TString, THashSet<TString>> requiredAttributes {
        {"//test/a", {"type", "broken", "target_path", "user_attributes"}},
        {"//link_dest", {"type", "user_attributes"}}
    };
    NMock::TMockServer mockServer{port, [&requiredAttributes] () {return new TYtReplier(requiredAttributes);}};

    TYtNativeServices nativeServices;
    auto gatewaysConfig = MakeGatewaysConfig(port);
    nativeServices.Config = std::make_shared<TYtGatewayConfig>(gatewaysConfig.GetYt());
    nativeServices.FileStorage = CreateFileStorage(TFileStorageConfig{});

    auto ytGateway = CreateYtNativeGateway(nativeServices);
    auto ytState = MakeIntrusive<TYtState>();
    ytState->Gateway = ytGateway;

    InitializeYtGateway(ytGateway, ytState);

    IYtGateway::TFolderOptions folderOptions{ytState->SessionId};
    TYtSettings ytSettings {};
    folderOptions.Cluster("ut_cluster")
        .Config(std::make_shared<TYtSettings>(ytSettings))
        .Prefix("//test/a")
        .Attributes({"user_attributes"});
    auto folderFuture = ytGateway->GetFolder(std::move(folderOptions));

    folderFuture.Wait();
    ytState->Gateway->CloseSession({ytState->SessionId});
    auto folderRes = folderFuture.GetValue();
    UNIT_ASSERT_EQUAL_C(folderRes.Success(), true, folderRes.Issues().ToString());
    UNIT_ASSERT_EQUAL(
        folderRes.ItemsOrFileLink, 
        (std::variant<TVector<IYtGateway::TFolderResult::TFolderItem>, TFileLinkPtr>(EXPECTED_ITEMS)));
}
}

} // namespace

} // namespace NYql
