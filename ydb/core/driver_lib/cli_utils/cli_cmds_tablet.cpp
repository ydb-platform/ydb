#include "cli.h"
#include "cli_cmds.h"

#include <ydb/core/protos/base.pb.h>

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

class TClientCommandTabletExec : public TClientCommandBase {
public:
    TClientCommandTabletExec()
        : TClientCommandBase("execute", { "exec" })
    {
    }

    TAutoPtr<NMsgBusProxy::TBusTabletLocalMKQL> Request;
    TString Program;
    TString Params;

    virtual void Config(TConfig& config) override {
        TClientCommand::Config(config);
        config.Opts->AddLongOption("follower", "connect to follower").NoArgument();
        config.Opts->AddLongOption("json-ui64-as-string", "json output ui64 as string").NoArgument();
        config.Opts->AddLongOption("json-binary-as-base64", "json output binary data in base64").NoArgument();
        config.SetFreeArgsNum(1, 2);
        SetFreeArgTitle(0, "<PROGRAM>", "Program to execute");
        SetFreeArgTitle(1, "<PARAMS>", "Parameters of the program");
    }

    virtual void Parse(TConfig& config) override {
        TClientCommand::Parse(config);

        Program = GetMiniKQL(config.ParseResult->GetFreeArgs().at(0));
        if (config.ParseResult->GetFreeArgCount() > 1)
            Params = GetMiniKQL(config.ParseResult->GetFreeArgs().at(1));

        Request = new NMsgBusProxy::TBusTabletLocalMKQL;
        Request->Record.SetTabletID(config.TabletId);
        auto* pgm = Request->Record.MutableProgram();
        if (IsMiniKQL(Program)) {
            pgm->MutableProgram()->SetText(Program);
        } else {
            pgm->MutableProgram()->SetBin(Program);
        }

        if (!Params.empty()) {
            if (IsMiniKQL(Params)) {
                pgm->MutableParams()->SetText(Params);
            } else {
                pgm->MutableParams()->SetBin(Params);
            }
        }

        Request->Record.SetConnectToFollower(config.ParseResult->Has("follower"));
        config.JsonUi64AsText = config.ParseResult->Has("json-ui64-as-string");
        config.JsonBinaryAsBase64 = config.ParseResult->Has("json-binary-as-base64");
    }

    virtual int Run(TConfig& config) override {
        return MessageBusCall(config, Request);
    }
};

class TClientCommandTabletKill : public TClientCommand {
public:
    TClientCommandTabletKill()
        : TClientCommand("kill")
    {
    }

    TAutoPtr<NMsgBusProxy::TBusTabletKillRequest> Request;

    virtual void Config(TConfig& config) override {
        TClientCommand::Config(config);
        config.SetFreeArgsNum(0);
    }

    virtual void Parse(TConfig& config) override {
        TClientCommand::Parse(config);
        Request = new NMsgBusProxy::TBusTabletKillRequest;
        Request->Record.SetTabletID(config.TabletId);
    }

    virtual int Run(TConfig& config) override {
        return MessageBusCall(config, Request);
    }
};

class TClientCommandTabletSchemeTx : public TClientCommand {
public:
    TClientCommandTabletSchemeTx()
        : TClientCommand("scheme-tx", { "scheme" })
    {
    }

    TAutoPtr<NMsgBusProxy::TBusTabletLocalSchemeTx> Request;
    TString SchemeChanges;

    virtual void Config(TConfig& config) override {
        TClientCommand::Config(config);
        config.Opts->AddLongOption("follower",     "connect to follower");
        config.Opts->AddLongOption("dry-run",   "test changes without applying");
        config.SetFreeArgsNum(1, 1);
        SetFreeArgTitle(0, "<SCHEME CHANGES>", "Scheme changes to apply");
    }

    virtual void Parse(TConfig& config) override {
        TClientCommand::Parse(config);

        SchemeChanges = config.ParseResult->GetFreeArgs().at(0);

        Request = new NMsgBusProxy::TBusTabletLocalSchemeTx;
        Request->Record.SetTabletID(config.TabletId);
        auto* schemeChanges = Request->Record.MutableSchemeChanges();
        if (!google::protobuf::TextFormat::ParseFromString(SchemeChanges, schemeChanges)) {
            ythrow TWithBackTrace<yexception>() << "Invalid scheme changes protobuf passed";
        }

        if (config.ParseResult->Has("follower"))
            Request->Record.SetConnectToFollower(true);
        Request->Record.SetDryRun(config.ParseResult->Has("dry-run"));
    }

    virtual int Run(TConfig& config) override {
        return MessageBusCall(config, Request);
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
