#include "cli.h"
#include "cli_cmds.h"
#include <ydb/core/base/logoblob.h>
#include <ydb/core/protos/base.pb.h>

namespace NKikimr {
namespace NDriverClient {

class TClientCommandGetBlob : public TClientCommand {
    ui32 GroupId = 0;
    TMaybe<TLogoBlobID> ExtremeQuery;

public:
    TClientCommandGetBlob()
        : TClientCommand("blob", {}, "Get blob raw content")
    {
    }

    void Config(TConfig& config) override {
        TClientCommand::Config(config);

        config.Opts->AddLongOption("group", "group id").Required().RequiredArgument("NUM").StoreResult(&GroupId);
        config.Opts->AddLongOption("extreme", "perform extreme query").Optional().RequiredArgument("BLOB_ID");
    }

    void Parse(TConfig& config) override {
        TClientCommand::Parse(config);

        if (config.ParseResult->Has("extreme")) {
            TLogoBlobID id;
            TString errorExplanation;
            const TString& blob = config.ParseResult->Get("extreme");
            if (!TLogoBlobID::Parse(id, blob, errorExplanation)) {
                ythrow yexception() << "Failed to parse LogoBlobId '" << blob << "': " << errorExplanation;
            }
            ExtremeQuery = id;
        } else {
            ythrow yexception() << "No query type specified";
        }
    }

    int Run(TConfig& config) override {
        using TRequest = NMsgBusProxy::TBusBsGetRequest;
        using TResponse = NMsgBusProxy::TBusBsGetResponse;

        // prepare request
        TAutoPtr<TRequest> request(new TRequest);
        auto& record = request->Record;
        record.SetGroupId(GroupId);
        if (ExtremeQuery) {
            LogoBlobIDFromLogoBlobID(*ExtremeQuery, record.MutableExtreme());
        }

        auto callback = [](const TResponse& response) -> int {
            if (response.Record.HasErrorDescription()) {
                Cerr << "error: " << response.Record.GetErrorDescription() << Endl;
                return 1;
            } else {
                switch (auto status = response.Record.GetStatus()) {
                    case NKikimrProto::OK:
                        Cout.Write(response.Record.GetBuffer());
                        return 0;

                    case NKikimrProto::NODATA:
                        Cerr << "no data" << Endl;
                        return 3;

                    default:
                        Cerr << "proxy error: " << NKikimrProto::EReplyStatus_Name(status) << Endl;
                        return 2;
                }
            }

            Y_ABORT();
        };

        return MessageBusCall<TRequest, TResponse>(config, request, callback);
    }
};

class TClientCommandGet : public TClientCommandTree {
public:
    TClientCommandGet()
        : TClientCommandTree("get", {}, "Various storage low-level queries")
    {
        AddCommand(std::make_unique<TClientCommandGetBlob>());
    }
};

std::unique_ptr<TClientCommand> CreateClientCommandGet() {
    return std::make_unique<TClientCommandGet>();
}

} // NDriverClient
} // NKikimr
