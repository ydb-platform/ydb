#include "cli.h"
#include "cli_cmds.h"

#include <ydb/core/protos/base.pb.h>
#include <ydb/public/sdk/cpp/src/library/grpc/client/grpc_common.h>
#include <ydb/public/api/grpc/draft/ydb_tablet_v1.grpc.pb.h>

namespace NKikimr {
namespace NDriverClient {


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