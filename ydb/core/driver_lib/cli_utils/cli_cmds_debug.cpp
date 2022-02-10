#include "cli.h"
#include "cli_cmds.h"
#include <util/string/split.h>

namespace NKikimr {
namespace NDriverClient {

class TInterconnectLoad : public TClientCommand {
    TString Name;
    ui32 Channel = 0;
    TString Hops;
    ui32 SizeMin = 0;
    ui32 SizeMax = 0;
    ui32 InFlyMax = 0;
    TDuration IntervalMin;
    TDuration IntervalMax;
    bool Soft = false;
    TDuration Duration;
    bool UseProtobufWithPayload = false;
    TString ServicePool;

public:
    TInterconnectLoad()
        : TClientCommand("load", {}, "Initiate load actor on Interconnect")
    {}

    void Config(TConfig& config) override {
        config.Opts->AddLongOption("name", "load name to distinguish between several load processes")
            .Optional()
            .RequiredArgument()
            .StoreResult(&Name);

        config.Opts->AddLongOption("channel", "Interconnect channel number")
            .Optional()
            .RequiredArgument()
            .StoreResult(&Channel);

        config.Opts->AddLongOption("hops", "comma-separated list of node ids making up packet route")
            .Required()
            .RequiredArgument()
            .StoreResult(&Hops);

        config.Opts->AddLongOption("size-min", "minimal size of payload")
            .Optional()
            .RequiredArgument()
            .StoreResult(&SizeMin);

        config.Opts->AddLongOption("size-max", "maximal size of payload")
            .Optional()
            .RequiredArgument()
            .StoreResult(&SizeMax);

        config.Opts->AddLongOption("infly", "maximal number of unanswered packets")
            .Required()
            .RequiredArgument()
            .StoreResult(&InFlyMax);

        config.Opts->AddLongOption("interval-min", "minimal interval between messages")
            .Optional()
            .RequiredArgument()
            .StoreResult(&IntervalMin);

        config.Opts->AddLongOption("interval-max", "maximal interval between messages")
            .Optional()
            .RequiredArgument()
            .StoreResult(&IntervalMax);

        config.Opts->AddLongOption("soft", "enable soft load")
            .Optional()
            .NoArgument()
            .StoreTrue(&Soft);

        config.Opts->AddLongOption("rope", "use ropes for payload instead of protobuf")
            .Optional()
            .NoArgument()
            .StoreTrue(&UseProtobufWithPayload);

        config.Opts->AddLongOption("duration", "test duration")
            .Required()
            .RequiredArgument()
            .StoreResult(&Duration);

        config.Opts->AddLongOption("service-pool", "service pool name")
            .RequiredArgument()
            .StoreResult(&ServicePool);
    }

    int Run(TConfig& config) override {
        TAutoPtr<NMsgBusProxy::TBusInterconnectDebug> msg(new NMsgBusProxy::TBusInterconnectDebug);

        NKikimrClient::TInterconnectDebug& request = msg->Record;
        request.SetName(Name);
        request.SetChannel(Channel);

        TVector<TString> nodes;
        StringSplitter(Hops).Split(',').AddTo(&nodes);
        for (const TString &node : nodes) {
            request.AddHops(FromString<ui32>(node));
        }

        request.SetSizeMin(SizeMin);
        request.SetSizeMax(SizeMax);
        request.SetInFlyMax(InFlyMax);
        request.SetIntervalMin(IntervalMin.GetValue());
        request.SetIntervalMax(IntervalMax.GetValue());
        request.SetSoftLoad(Soft);
        request.SetUseProtobufWithPayload(UseProtobufWithPayload);
        request.SetDuration(Duration.GetValue());
        if (ServicePool) {
            request.SetServicePool(ServicePool);
        }

        auto callback = [](const NMsgBusProxy::TBusResponse& response) {
            return response.Record.GetStatus() == NMsgBusProxy::MSTATUS_OK ? 0 : 1;
        };

        return MessageBusCall<NMsgBusProxy::TBusInterconnectDebug, NMsgBusProxy::TBusResponse>(config, msg, callback);
    }
};

class TInterconnectClosePeerSocket : public TClientCommand {
    ui32 NodeId = 0;

public:
    TInterconnectClosePeerSocket()
        : TClientCommand("closepeersocket", {}, "Close peer socket on existing session")
    {}

    void Config(TConfig& config) override {
        config.Opts->AddLongOption("node", "peer node id to close socket for")
            .Required()
            .RequiredArgument()
            .StoreResult(&NodeId);
    }

    int Run(TConfig& config) override {
        TAutoPtr<NMsgBusProxy::TBusInterconnectDebug> msg(new NMsgBusProxy::TBusInterconnectDebug);

        NKikimrClient::TInterconnectDebug& request = msg->Record;
        request.SetClosePeerSocketNodeId(NodeId);

        auto callback = [](const NMsgBusProxy::TBusResponse& response) {
            return response.Record.GetStatus() == NMsgBusProxy::MSTATUS_OK ? 0 : 1;
        };

        return MessageBusCall<NMsgBusProxy::TBusInterconnectDebug, NMsgBusProxy::TBusResponse>(config, msg, callback);
    }
};

class TInterconnectCloseInputSession : public TClientCommand {
    ui32 NodeId = 0;

public:
    TInterconnectCloseInputSession()
        : TClientCommand("closeinputsession", {}, "Terminate input session")
    {}

    void Config(TConfig& config) override {
        config.Opts->AddLongOption("node", "peer node id to close input session for")
            .Required()
            .RequiredArgument()
            .StoreResult(&NodeId);
    }

    int Run(TConfig& config) override {
        TAutoPtr<NMsgBusProxy::TBusInterconnectDebug> msg(new NMsgBusProxy::TBusInterconnectDebug);

        NKikimrClient::TInterconnectDebug& request = msg->Record;
        request.SetCloseInputSessionNodeId(NodeId);

        auto callback = [](const NMsgBusProxy::TBusResponse& response) {
            return response.Record.GetStatus() == NMsgBusProxy::MSTATUS_OK ? 0 : 1;
        };

        return MessageBusCall<NMsgBusProxy::TBusInterconnectDebug, NMsgBusProxy::TBusResponse>(config, msg, callback);
    }
};

class TInterconnectPoisonSession : public TClientCommand {
    ui32 NodeId = 0;

public:
    TInterconnectPoisonSession()
        : TClientCommand("poisonsession", {}, "Poison session")
    {}

    void Config(TConfig& config) override {
        config.Opts->AddLongOption("node", "peer node id to poison session for")
            .Required()
            .RequiredArgument()
            .StoreResult(&NodeId);
    }

    int Run(TConfig& config) override {
        TAutoPtr<NMsgBusProxy::TBusInterconnectDebug> msg(new NMsgBusProxy::TBusInterconnectDebug);

        NKikimrClient::TInterconnectDebug& request = msg->Record;
        request.SetPoisonSessionNodeId(NodeId);

        auto callback = [](const NMsgBusProxy::TBusResponse& response) {
            return response.Record.GetStatus() == NMsgBusProxy::MSTATUS_OK ? 0 : 1;
        };

        return MessageBusCall<NMsgBusProxy::TBusInterconnectDebug, NMsgBusProxy::TBusResponse>(config, msg, callback);
    }
};

class TInterconnectSlowpoke : public TClientCommand {
    ui32 NumActors = 0;
    ui32 PoolId = 0;
    TDuration SleepMin;
    TDuration SleepMax;
    TDuration RescheduleMin;
    TDuration RescheduleMax;
    TDuration Duration;

public:
    TInterconnectSlowpoke()
        : TClientCommand("slowpoke", {}, "Make actor system work a bit worse")
    {}

    void Config(TConfig& config) override {
        config.Opts->AddLongOption("num-actors", "number of actors to spawn")
            .Required()
            .RequiredArgument()
            .StoreResult(&NumActors);

        config.Opts->AddLongOption("pool-id", "pool id to spawn actors within")
            .Required()
            .RequiredArgument()
            .StoreResult(&PoolId);

        config.Opts->AddLongOption("duration", "test duration")
            .Required()
            .RequiredArgument()
            .StoreResult(&Duration);

        config.Opts->AddLongOption("sleep-min", "min uniform interval for event processing")
            .Required()
            .RequiredArgument()
            .StoreResult(&SleepMin);

        config.Opts->AddLongOption("sleep-max", "max uniform interval for event processing")
            .Required()
            .RequiredArgument()
            .StoreResult(&SleepMax);

        config.Opts->AddLongOption("reschedule-min", "min uniform interval between events")
            .Required()
            .RequiredArgument()
            .StoreResult(&RescheduleMin);

        config.Opts->AddLongOption("reschedule-max", "max uniform interval between events")
            .Required()
            .RequiredArgument()
            .StoreResult(&RescheduleMax);
    }

    int Run(TConfig& config) override {
        TAutoPtr<NMsgBusProxy::TBusInterconnectDebug> msg(new NMsgBusProxy::TBusInterconnectDebug);

        NKikimrClient::TInterconnectDebug& request = msg->Record;
        request.SetNumSlowpokeActors(NumActors);
        request.SetPoolId(PoolId);
        request.SetDuration(Duration.GetValue());
        request.SetSleepMin(SleepMin.GetValue());
        request.SetSleepMax(SleepMax.GetValue());
        request.SetRescheduleMin(RescheduleMin.GetValue());
        request.SetRescheduleMax(RescheduleMax.GetValue());

        auto callback = [](const NMsgBusProxy::TBusResponse& response) {
            return response.Record.GetStatus() == NMsgBusProxy::MSTATUS_OK ? 0 : 1;
        };

        return MessageBusCall<NMsgBusProxy::TBusInterconnectDebug, NMsgBusProxy::TBusResponse>(config, msg, callback);
    }
};

class TInterconnect : public TClientCommandTree {
public:
    TInterconnect()
        : TClientCommandTree("interconnect", {"ic"}, "Interconnect debugging facilities")
    {
        AddCommand(std::make_unique<TInterconnectLoad>());
        AddCommand(std::make_unique<TInterconnectClosePeerSocket>());
        AddCommand(std::make_unique<TInterconnectCloseInputSession>());
        AddCommand(std::make_unique<TInterconnectPoisonSession>());
        AddCommand(std::make_unique<TInterconnectSlowpoke>());
    }
};

TClientCommandDebug::TClientCommandDebug()
    : TClientCommandTree("debug")
{
    AddCommand(std::make_unique<TInterconnect>());
}

}
}
