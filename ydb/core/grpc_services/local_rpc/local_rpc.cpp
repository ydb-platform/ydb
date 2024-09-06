#include "local_rpc.h"


namespace NKikimr::NRpcService {

namespace {

class TFacilityProviderCommon : public NGRpcService::IFacilityProvider {
public:
    explicit TFacilityProviderCommon(ui64 channelBufferSize)
        : ChannelBufferSize(channelBufferSize)
    {}

    ui64 GetChannelBufferSize() const override {
        return ChannelBufferSize;
    }

    virtual ~TFacilityProviderCommon() {
    }

private:
    const ui64 ChannelBufferSize;
};

class TFacilityProviderSameMailbox : public TFacilityProviderCommon {
    using TBase = TFacilityProviderCommon;

public:
    TFacilityProviderSameMailbox(TActorContext actorContext, ui64 channelBufferSize)
        : TBase(channelBufferSize)
        , ActorContext(actorContext)
    {}

    TActorId RegisterActor(IActor* actor) const override {
        return ActorContext.RegisterWithSameMailbox(actor);
    }

private:
    const TActorContext ActorContext;
};

}  // anonymous namespace

TFacilityProviderPtr CreateFacilityProviderSameMailbox(TActorContext actorContext, ui64 channelBufferSize) {
    return std::make_shared<TFacilityProviderSameMailbox>(actorContext, channelBufferSize);
}

}  // namespace NKikimr::NRpcService
