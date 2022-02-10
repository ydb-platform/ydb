#include <util/string/type.h>
#include <util/system/hostname.h>
#include "cli.h"
#include "cli_cmds.h"

namespace NKikimr {
namespace NDriverClient {

class TClientCommandDrain : public TClientCommand {
public:
    TClientCommandDrain()
        : TClientCommand("drain", {}, "Drain node")
    {}

    virtual int Run(TConfig& config) override {
        TAutoPtr<NMsgBusProxy::TBusDrainNode> request(new NMsgBusProxy::TBusDrainNode());
        request->Record.SetNodeID(config.NodeId);
        return MessageBusCall(config, request);
    }
};

class TClientCommandFill : public TClientCommand {
public:
    TClientCommandFill()
        : TClientCommand("fill", {}, "Fill node")
    {}

    virtual int Run(TConfig& config) override {
        TAutoPtr<NMsgBusProxy::TBusFillNode> request(new NMsgBusProxy::TBusFillNode());
        request->Record.SetNodeID(config.NodeId);
        return MessageBusCall(config, request);
    }
};

class TClientCommandNodeN : public TClientCommandTree {
public:
    TClientCommandNodeN()
        : TClientCommandTree("*", {}, "<node id or hostname>")
    {
        AddCommand(std::make_unique<TClientCommandDrain>());
        AddCommand(std::make_unique<TClientCommandFill>());
    }

    virtual void Config(TConfig& config) override {
        TClientCommandTree::Config(config);
        config.SetFreeArgsMin(0);
    }

    NKikimrClient::TResponse BusResponse;
    bool Error = false;

    virtual void Parse(TConfig& config) override {
        TAutoPtr<NMsgBusProxy::TBusResolveNode> request(new NMsgBusProxy::TBusResolveNode());
        TString node(config.Tokens.back());
        if (IsNumber(node)) {
            request->Record.SetNodeId(FromString(node));
        } else {
            // vvv remove in future versions
            if (node == ".") {
                node = HostName();
                request->Record.SetResolveLocalNode(true);
            }
            // ^^^ remove in future versions
            request->Record.SetHost(node);
        }
        MessageBusCall<NMsgBusProxy::TBusResolveNode, NMsgBusProxy::TBusResponse>(config, request,
            [this](const NMsgBusProxy::TBusResponse& response) -> int {
                BusResponse = response.Record;
                return 0;
        });
        if (BusResponse.GetStatus() != NMsgBusProxy::MSTATUS_OK) {
            Error = true;
            return;
        }
        config.NodeId = BusResponse.GetResolveNodeResponse().GetNodeId();
        TClientCommand::Parse(config);
        if (!config.ParseResult->GetFreeArgs().empty()) {
            TClientCommandTree::Parse(config);
        }
    }

    virtual int Run(TConfig& config) override {
        if (!config.ParseResult->GetFreeArgs().empty()) {
            return TClientCommandTree::Run(config);
        } else {
            if (BusResponse.HasResolveNodeResponse()) {
                Cout << BusResponse.GetResolveNodeResponse().GetNodeId() << '\t' << BusResponse.GetResolveNodeResponse().GetHost() << Endl;
            }
            return 0;
        }
    }
};

TClientCommandNode::TClientCommandNode()
    : TClientCommandTree("node", {}, "Nodes infrastructure administration")
{
    AddCommand(std::make_unique<TClientCommandNodeN>());
}

}
}
