#include "cli.h"
#include "cli_cmds.h"

#include <ydb/core/protos/base.pb.h>
#include <ydb/library/grpc/client/grpc_common.h>
#include <ydb/public/api/grpc/draft/ydb_tablet_v1.grpc.pb.h>

namespace NKikimr {
namespace NDriverClient {

class TClientCommandKeyValueRequest : public TClientCommandBase {
public:
    TClientCommandKeyValueRequest()
        : TClientCommandBase("request", { "req" }, "Request to KV tablet")
    {}

    TString ProtoBuf;
    TAutoPtr<NKikimrClient::TKeyValueRequest> Request;
    TString OutputFile;
    TString InputFile;

    int OnResponse(const NKikimrClient::TResponse& response) {
        if (!OutputFile.empty()) {
            Y_ABORT_UNLESS(response.ReadResultSize() == 1);
            Y_ABORT_UNLESS(response.GetReadResult(0).HasStatus());
            Y_ABORT_UNLESS(response.GetReadResult(0).GetStatus() == NKikimrProto::OK);
            Y_ABORT_UNLESS(response.GetReadResult(0).HasValue());
            TFile file(OutputFile, CreateNew | WrOnly);
            TString data = response.GetReadResult(0).GetValue();
            file.Write(data.data(), data.size());
            file.Close();
        } else if (!InputFile.empty()) {
            Y_ABORT_UNLESS(response.WriteResultSize() == 1);
            Y_ABORT_UNLESS(response.GetWriteResult(0).HasStatus());
            Y_ABORT_UNLESS(response.GetWriteResult(0).GetStatus() == NKikimrProto::OK);
        } else {
            Cout << GetString(response) << Endl;
        }
        return 0;
    }

    virtual int Run(TConfig& config) override {
        TAutoPtr<NMsgBusProxy::TBusKeyValue> request(new NMsgBusProxy::TBusKeyValue());
        if (Request != nullptr)
            request->Record.MergeFrom(*Request);
        if (config.TabletId != 0)
            request->Record.SetTabletId(config.TabletId);
        return MessageBusCall<NMsgBusProxy::TBusKeyValue, NMsgBusProxy::TBusResponse>(config, request,
            [this](const NMsgBusProxy::TBusResponse& response) -> int { return OnResponse(response.Record); });
    }

    virtual void Config(TConfig& config) override {
        TClientCommand::Config(config);
        config.SetFreeArgsNum(1);
        SetFreeArgTitle(0, "<REQUEST>", "Request protobuf or file with request protobuf");
        config.Opts->AddLongOption("of", "output file path, protobuf must contain single read command!").Optional()
            .RequiredArgument("PATH").StoreResult(&OutputFile);
        config.Opts->AddLongOption("if", "input file path, protobuf must contain single write command!").Optional()
            .RequiredArgument("PATH").StoreResult(&InputFile);
        if (!(OutputFile.empty() || InputFile.empty())) {
            ythrow TWithBackTrace<yexception>() << "Use either --of or --if!";
        }
    }

    virtual void Parse(TConfig& config) override {
        TClientCommand::Parse(config);
        ProtoBuf = config.ParseResult->GetFreeArgs().at(0);
        Request = GetProtobuf<NKikimrClient::TKeyValueRequest>(ProtoBuf);
        if (!InputFile.empty()) {
            Y_ABORT_UNLESS(Request->CmdWriteSize() == 1);
            Y_ABORT_UNLESS(Request->GetCmdWrite(0).HasKey());
            Y_ABORT_UNLESS(!Request->GetCmdWrite(0).HasValue());
            TString data = TUnbufferedFileInput(InputFile).ReadAll();
            Request->MutableCmdWrite(0)->SetValue(data);
        }
    }
};

class TClientCommandKeyValue : public TClientCommandTree {
public:
    TClientCommandKeyValue()
        : TClientCommandTree("keyvalue", { "kv" })
    {
        AddCommand(std::make_unique<TClientCommandKeyValueRequest>());
    }
};

template<class TRequest, class TResponse>
class TClientCommandTabletCommon {
protected:
    int RunTabletCommon(TClientCommand::TConfig& config) {
        grpc::ClientContext context;
        if (auto token = AcquireSecurityToken(config)) {
            context.AddMetadata("x-ydb-auth-ticket", *token);
        }
        auto channel = NYdbGrpc::CreateChannelInterface(CommandConfig.ClientConfig);
        auto stub = Ydb::Tablet::V1::TabletService::NewStub(channel);
        if (NClient::TKikimr::DUMP_REQUESTS) {
            Cerr << "<-- " << TypeName<TRequest>() << "\n" << Request.DebugString();
        }
        auto status = Send(context, stub);
        if (!status.ok()) {
            Cerr << "ERROR: " << int(status.error_code()) << " " << status.error_message() << Endl;
            return 1;
        }
        if (NClient::TKikimr::DUMP_REQUESTS) {
            Cerr << "--> " << TypeName<TResponse>() << "\n" << Response.DebugString();
        }
        if (Response.status() != Ydb::StatusIds::SUCCESS) {
            Cerr << "ERROR: " << Response.status() << Endl;
            for (const auto& issue : Response.issues()) {
                Cerr << issue.message() << Endl;
            }
            return 1;
        }
        return 0;
    }

    virtual grpc::Status Send(
        grpc::ClientContext& context,
        const std::unique_ptr<Ydb::Tablet::V1::TabletService::Stub>& stub) = 0;

protected:
    TRequest Request;
    TResponse Response;
};

class TClientCommandTabletExec
    : public TClientCommandBase
    , public TClientCommandTabletCommon<
        Ydb::Tablet::ExecuteTabletMiniKQLRequest,
        Ydb::Tablet::ExecuteTabletMiniKQLResponse>
{
public:
    TClientCommandTabletExec()
        : TClientCommandBase("execute", { "exec" })
    {
    }

    TString Program;

    virtual void Config(TConfig& config) override {
        TClientCommand::Config(config);
        config.Opts->AddLongOption("dry-run", "test changes without applying").NoArgument();
        config.SetFreeArgsNum(1, 1);
        SetFreeArgTitle(0, "<PROGRAM>", "Program to execute");
    }

    virtual void Parse(TConfig& config) override {
        TClientCommand::Parse(config);

        Program = GetMiniKQL(config.ParseResult->GetFreeArgs().at(0));

        Request.set_tablet_id(config.TabletId);
        Request.set_program(Program);
        Request.set_dry_run(config.ParseResult->Has("dry-run"));
    }

    virtual int Run(TConfig& config) override {
        return RunTabletCommon(config);
    }

    virtual grpc::Status Send(
        grpc::ClientContext& context,
        const std::unique_ptr<Ydb::Tablet::V1::TabletService::Stub>& stub) override
    {
        return stub->ExecuteTabletMiniKQL(&context, Request, &Response);
    }
};

class TClientCommandTabletKill
    : public TClientCommand
    , public TClientCommandTabletCommon<
        Ydb::Tablet::RestartTabletRequest,
        Ydb::Tablet::RestartTabletResponse>
{
public:
    TClientCommandTabletKill()
        : TClientCommand("kill")
    {
    }

    virtual void Config(TConfig& config) override {
        TClientCommand::Config(config);
        config.SetFreeArgsNum(0);
    }

    virtual void Parse(TConfig& config) override {
        TClientCommand::Parse(config);
        Request.set_tablet_id(config.TabletId);
    }

    virtual int Run(TConfig& config) override {
        return RunTabletCommon(config);
    }

    virtual grpc::Status Send(
        grpc::ClientContext& context,
        const std::unique_ptr<Ydb::Tablet::V1::TabletService::Stub>& stub) override
    {
        return stub->RestartTablet(&context, Request, &Response);
    }
};

class TClientCommandTabletSchemeTx
    : public TClientCommand
    , public TClientCommandTabletCommon<
        Ydb::Tablet::ChangeTabletSchemaRequest,
        Ydb::Tablet::ChangeTabletSchemaResponse>
{
public:
    TClientCommandTabletSchemeTx()
        : TClientCommand("scheme-tx", { "scheme" })
    {
    }

    TString SchemeChanges;

    virtual void Config(TConfig& config) override {
        TClientCommand::Config(config);
        config.Opts->AddLongOption("dry-run", "test changes without applying").NoArgument();
        config.SetFreeArgsNum(1, 1);
        SetFreeArgTitle(0, "<SCHEME CHANGES>", "Scheme changes json to apply");
    }

    virtual void Parse(TConfig& config) override {
        TClientCommand::Parse(config);

        SchemeChanges = config.ParseResult->GetFreeArgs().at(0);

        Request.set_tablet_id(config.TabletId);
        Request.set_schema_changes(SchemeChanges);
        Request.set_dry_run(config.ParseResult->Has("dry-run"));
    }

    virtual int Run(TConfig& config) override {
        return RunTabletCommon(config);
    }

    virtual grpc::Status Send(
        grpc::ClientContext& context,
        const std::unique_ptr<Ydb::Tablet::V1::TabletService::Stub>& stub) override
    {
        return stub->ChangeTabletSchema(&context, Request, &Response);
    }
};

class TClientCommandDrainNode : public TClientCommand {
public:
    TClientCommandDrainNode()
        : TClientCommand("drain", {}, "Drain node")
    {}

    virtual void Config(TConfig& config) override {
        TClientCommand::Config(config);
        config.SetFreeArgsNum(1);
        SetFreeArgTitle(0, "<NODE ID>", "Node ID to drain tablets from");
    }

    ui32 NodeId;

    virtual void Parse(TConfig& config) override {
        TClientCommand::Parse(config);
        NodeId = FromString(config.ParseResult->GetFreeArgs().at(0));
    }

    virtual int Run(TConfig& config) override {
        TAutoPtr<NMsgBusProxy::TBusDrainNode> request(new NMsgBusProxy::TBusDrainNode());
        request->Record.SetNodeID(NodeId);
        return MessageBusCall(config, request);
    }
};

class TClientCommandFillNode : public TClientCommand {
public:
    TClientCommandFillNode()
        : TClientCommand("fill", {}, "Fill node")
    {}

    virtual void Config(TConfig& config) override {
        TClientCommand::Config(config);
        config.SetFreeArgsNum(1);
        SetFreeArgTitle(0, "<NODE ID>", "Node ID to fill tablets to");
    }

    ui32 NodeId;

    virtual void Parse(TConfig& config) override {
        TClientCommand::Parse(config);
        NodeId = FromString(config.ParseResult->GetFreeArgs().at(0));
    }

    virtual int Run(TConfig& config) override {
        TAutoPtr<NMsgBusProxy::TBusFillNode> request(new NMsgBusProxy::TBusFillNode());
        request->Record.SetNodeID(NodeId);
        return MessageBusCall(config, request);
    }
};

class TClientCommandTabletN : public TClientCommandTree {
public:
    TClientCommandTabletN()
        : TClientCommandTree("#", {}, "<tablet id>")
    {
        AddCommand(std::make_unique<TClientCommandKeyValue>());
        AddCommand(std::make_unique<TClientCommandTabletExec>());
        AddCommand(std::make_unique<TClientCommandTabletKill>());
        AddCommand(std::make_unique<TClientCommandTabletSchemeTx>());
    }

    virtual void Parse(TConfig& config) override {
        config.TabletId = FromString<ui64>(config.Tokens.back());
        TClientCommandTree::Parse(config);
    }
};

TClientCommandTablet::TClientCommandTablet()
    : TClientCommandTree("tablet", {}, "Tablet infrastructure administration")
{
    AddCommand(std::make_unique<TClientCommandTabletN>());
    AddCommand(std::make_unique<TClientCommandDrainNode>());
    AddCommand(std::make_unique<TClientCommandFillNode>());
}

}
}
