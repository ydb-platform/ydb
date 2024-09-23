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
            <
                "user_attributes" = {};
                "target_path" = "//link_access_denied";
                "broken" = %false;
                "type" = "link";
            > "link_access_denied";
        ];
    };
]
)";

constexpr auto CYPRES_NODE_W_LINK = R"(
[
    {
        output = [
            <
                "target_path" = "//link_dest";
                "broken" = %false;
                "type" = "link";
            > "link";
        ];
    }
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

constexpr auto CYPRES_ACCESS_ERROR = R"(
[
    {
        "error" = {
            "code" = 901;
            "message" = "Access denied";
        }
    }
]
)";

constexpr auto CYPRESS_BLACKBOX_ERROR = R"(
[
    {
        "error" = {
            "code" = 111;
            "message" = "Blackbox rejected token";
        }
    }
]
)";

TVector<IYtGateway::TFolderResult::TFolderItem> EXPECTED_ITEMS {
    {"test/a/a", "table", R"({"user_attributes"={}})"},
    {"test/a/b", "table", R"({"user_attributes"={}})"},
    {"test/a/link", "table", R"({"user_attributes"={}})"},
    {"test/a/link_access_denied", "unknown", "{}"}
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
    using THandler = std::function<THttpResponse(TStringBuf path, const NYT::TNode& attributes)>;

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
    explicit TYtReplier(THandler handleListCommand, THandler handleGetCommand, TMaybe<std::function<void(const NYT::TNode& request)>> assertion): 
        HandleListCommand_(handleListCommand), HandleGetCommand_(handleGetCommand) {
            if (assertion) {
                Assertion_ = assertion.GetRef();
            }
        }

private:
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
            Assertion_(request);

            const auto& command = request["command"];
            const auto& parameters = request["parameters"];
            const auto& path = parameters["path"].AsString();
            const auto& attributes = parameters.HasKey("attributes") ? parameters["attributes"] : NYT::TNode{};
            if (command == "list") {
                return HandleListCommand_(path, attributes);
            }
            if (command == "get") {
                return HandleGetCommand_(path, attributes);
            }
        }
        return THttpResponse{HTTP_NOT_FOUND};
    }

    std::function<void(const NYT::TNode& request)> Assertion_ = [] ([[maybe_unused]] auto _) {};
    THandler HandleListCommand_;
    THandler HandleGetCommand_;

};

Y_UNIT_TEST_SUITE(YtNativeGateway) {
    
std::pair<TIntrusivePtr<TYtState>, IYtGateway::TPtr> InitTest(const NTesting::TPortHolder& port) {
    TYtNativeServices nativeServices;
    auto gatewaysConfig = MakeGatewaysConfig(port);
    nativeServices.Config = std::make_shared<TYtGatewayConfig>(gatewaysConfig.GetYt());
    nativeServices.FileStorage = CreateFileStorage(TFileStorageConfig{});

    auto ytGateway = CreateYtNativeGateway(nativeServices);
    auto ytState = MakeIntrusive<TYtState>();
    ytState->Gateway = ytGateway;

    InitializeYtGateway(ytGateway, ytState);
    return {ytState, ytGateway};
}

IYtGateway::TFolderResult GetFolderResult(TYtReplier::THandler handleList, TYtReplier::THandler handleGet, 
TMaybe<std::function<void(const NYT::TNode& request)>> gatewayRequestAssertion, std::function<IYtGateway::TFolderOptions(TString)> makeFolderOptions) {
    const auto port = NTesting::GetFreePort();
    NMock::TMockServer mockServer{port, 
        [gatewayRequestAssertion, handleList, handleGet] () {return new TYtReplier(handleList, handleGet, gatewayRequestAssertion);}
    };

    auto [ytState, ytGateway] = InitTest(port);

    IYtGateway::TFolderOptions folderOptions = makeFolderOptions(ytState->SessionId);
    auto folderFuture = ytGateway->GetFolder(std::move(folderOptions));

    folderFuture.Wait();
    ytState->Gateway->CloseSession({ytState->SessionId});
    auto folderRes = folderFuture.GetValue();
    return folderRes;
}

Y_UNIT_TEST(GetFolder) {
    THashMap<TString, THashSet<TString>> requiredAttributes {
        {"//test/a", {"type", "broken", "target_path", "user_attributes"}},
        {"//link_dest", {"type", "user_attributes"}}
    };
    const auto checkRequiredAttributes = [&requiredAttributes] (const NYT::TNode& request) {
        const auto& parameters = request["parameters"];
        const auto path = parameters["path"].AsString();
        const auto& attributes = parameters.HasKey("attributes") ? parameters["attributes"] : NYT::TNode{};

        if (!requiredAttributes.contains(path)) {
            return;
        }

        THashSet<TString> attributesSet;
        for (const auto& attribute : attributes.AsList()) {
            attributesSet.insert(attribute.AsString());
        }
        UNIT_ASSERT_VALUES_EQUAL(requiredAttributes[path], attributesSet);
    };
    
    const auto handleGet = [] (TStringBuf path, const NYT::TNode& attributes) {
        Y_UNUSED(attributes);
        THttpResponse resp{HTTP_OK};
        if (path == "//link_dest") {
            resp.SetContent(CYPRES_LINK_DEST);
            return resp;
        }
        if (path == "//link_access_denied") {
            resp.SetContent(CYPRES_ACCESS_ERROR);
            return resp;
        }

        return THttpResponse{HTTP_NOT_FOUND};
    };
    
    const auto handleList = [] (TStringBuf path, const NYT::TNode& attributes) {
        Y_UNUSED(attributes);
        THttpResponse resp{HTTP_OK};
        if (path == "//test/a") {
            resp.SetContent(CYPRES_NODE_A_CONTENT);
            return resp;
        }
        return THttpResponse{HTTP_NOT_FOUND};
    };
    
    const auto makeFolderOptions = [] (const TString& sessionId) {
        IYtGateway::TFolderOptions folderOptions{sessionId};
        TYtSettings ytSettings {};
        folderOptions.Cluster("ut_cluster")
            .Config(std::make_shared<TYtSettings>(ytSettings))
            .Prefix("//test/a")
            .Attributes({"user_attributes"});
        return folderOptions;
    };

    auto folderRes 
        = GetFolderResult(handleList, handleGet, checkRequiredAttributes, makeFolderOptions);

    UNIT_ASSERT_EQUAL_C(folderRes.Success(), true, folderRes.Issues().ToString());
    UNIT_ASSERT_EQUAL(
        folderRes.ItemsOrFileLink, 
        (std::variant<TVector<IYtGateway::TFolderResult::TFolderItem>, TFileLinkPtr>(EXPECTED_ITEMS)));
    }

Y_UNIT_TEST(EmptyResolveIsNotError) {
    const auto port = NTesting::GetFreePort();

    const auto handleList = [] (TStringBuf path, const NYT::TNode& attributes) {
        Y_UNUSED(path);
        Y_UNUSED(attributes);

        THttpResponse resp{HTTP_OK};
        resp.SetContent(CYPRES_NODE_W_LINK);
        return resp;
    };

    const auto handleGet = [] (TStringBuf path, const NYT::TNode& attributes) {
        Y_UNUSED(path);
        Y_UNUSED(attributes);

        THttpResponse resp{HTTP_OK};
        resp.SetContent(CYPRES_ACCESS_ERROR);
        return resp;
    };

    const auto makeFolderOptions = [] (const TString& sessionId) {
        IYtGateway::TFolderOptions folderOptions{sessionId};
        TYtSettings ytSettings {};
        folderOptions.Cluster("ut_cluster")
            .Config(std::make_shared<TYtSettings>(ytSettings))
            .Prefix("//test/a")
            .Attributes({"user_attributes"});
        return folderOptions;
    };

    auto folderRes 
        = GetFolderResult(handleList, handleGet, Nothing(), makeFolderOptions);
    
    UNIT_ASSERT_EQUAL_C(folderRes.Success(), true, folderRes.Issues().ToString());
}

Y_UNIT_TEST(GetFolderException) {
    const auto port = NTesting::GetFreePort();

    const auto handleList = [] (TStringBuf path, const NYT::TNode& attributes) {
        Y_UNUSED(path);
        Y_UNUSED(attributes);

        THttpResponse resp{HTTP_UNAUTHORIZED};
        auto header = R"({"code":900,"message":"Authentication failed"})";
        resp.AddHeader(THttpInputHeader("X-YT-Error", header));
        resp.SetContent(CYPRESS_BLACKBOX_ERROR);
        return resp;
    };

    const auto handleGet = [] (TStringBuf path, const NYT::TNode& attributes) {
        Y_UNUSED(path);
        Y_UNUSED(attributes);

        THttpResponse resp{HTTP_OK};
        resp.SetContent("");
        return resp;
    };

    const auto makeFolderOptions = [] (const TString& sessionId) {
        IYtGateway::TFolderOptions folderOptions{sessionId};
        TYtSettings ytSettings {};
        folderOptions.Cluster("ut_cluster")
            .Config(std::make_shared<TYtSettings>(ytSettings))
            .Prefix("//test/a")
            .Attributes({"user_attributes"});
        return folderOptions;
    };

    const auto folderRes 
        = GetFolderResult(handleList, handleGet, Nothing(), makeFolderOptions);
    
    UNIT_ASSERT(!folderRes.Issues().Empty());
    UNIT_ASSERT_STRING_CONTAINS(folderRes.Issues().ToString(), "Authentication failed");
}
}

} // namespace

} // namespace NYql
