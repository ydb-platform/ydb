#include <ydb/core/erasure/erasure.h>
#include "cli.h"
#include "cli_cmds.h"
#include "proto_common.h"

namespace NKikimr {
namespace NDriverClient {

class TClientCommandGroupReconfigureWipe: public TClientCommand {
public:
    TClientCommandGroupReconfigureWipe()
        : TClientCommand("wipe", {}, "Wipe out all PDisk data for one VDisk")
    {}

    struct TVDiskLocation {
        ui32 Node;
        ui32 PDisk;
        ui32 VSlot;

        void Clear() {
            Node = Max<ui32>();
            PDisk = Max<ui32>();
            VSlot = Max<ui32>();
        }
    };

    ui32 Domain;
    TVDiskLocation Location;

    virtual void Config(TConfig& config) override {
        TClientCommand::Config(config);

        Domain = Max<ui32>();
        Location.Clear();

        config.Opts->AddLongOption("domain", "domain").Required().RequiredArgument("NUM").StoreResult(&Domain);

        config.Opts->AddLongOption("node", "Node carrying VDisk to wipe").Required().RequiredArgument("NUM").
            StoreResult(&Location.Node);
        config.Opts->AddLongOption("pdisk", "PDisk carrying VDisk to wipe").Required().RequiredArgument("NUM").
            StoreResult(&Location.PDisk);
        config.Opts->AddLongOption("vslot", "VSlot carrying VDisk to wipe").Required().RequiredArgument("NUM").
            StoreResult(&Location.VSlot);
    }

    virtual void Parse(TConfig& config) override {
        TClientCommand::Parse(config);
    }

    virtual int Run(TConfig& config) override {
        TAutoPtr<NMsgBusProxy::TBusBSAdm> request(new NMsgBusProxy::TBusBSAdm);
        request->Record.SetDomain(Domain);
        if (config.SecurityToken) {
            request->Record.SetSecurityToken(config.SecurityToken);
        }
        auto *x = request->Record.MutableGroupReconfigureWipe();

        auto *location = x->MutableLocation();
        location->SetNodeId(Location.Node);
        location->SetPDiskId(Location.PDisk);
        location->SetVSlotId(Location.VSlot);

        return MessageBusCall(config, request);
    }
};


class TClientCommandGroupReconfigure : public TClientCommandTree {
public:
    TClientCommandGroupReconfigure()
        : TClientCommandTree("reconfigure", {}, "Change the configuration of a group")
    {
        AddCommand(std::make_unique<TClientCommandGroupReconfigureWipe>());
    }
};

class TClientCommandGroup : public TClientCommandTree {
public:
    TClientCommandGroup()
        : TClientCommandTree("group", {}, "Group management")
    {
        AddCommand(std::make_unique<TClientCommandGroupReconfigure>());
    }
};

std::unique_ptr<TClientCommand> CreateClientCommandGroup() {
    return std::make_unique<TClientCommandGroup>();
}


}
}
