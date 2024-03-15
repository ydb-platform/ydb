#include "mvp_log.h"
#include "parser.h"

using namespace NMVP;

class TJsonParserActor : public NActors::TActor<TJsonParserActor> {
public:
    using TBase = NActors::TActor<TJsonParserActor>;
    static NJson::TJsonReaderConfig JsonConfig;

    TJsonParserActor()
        : TBase(&TJsonParserActor::StateWork)
    {
        JsonConfig.DontValidateUtf8 = true;
    }

    void Handle(TEvJsonMerger::TEvJsonParse::TPtr event, const NActors::TActorContext& ctx) {
        const TJsonMergePeer& peer(event->Get()->Peer);
        NJson::TJsonValue jsonValue;
        bool success = NJson::ReadJsonTree(event->Get()->HttpResponse->Body, &JsonConfig, &jsonValue);
        if (success) {
            ctx.Send(event->Sender, new TEvJsonMerger::TEvJsonParseResult(peer, std::move(jsonValue)));
        } else {
            ctx.Send(event->Sender, new TEvJsonMerger::TEvJsonParseResult(peer,
                                                                          TString("Failed to parse json received from ")
                                                                          + peer.GetName()));
        }
        Die(ctx);
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvJsonMerger::TEvJsonParse, Handle);
        }
    }
};

NJson::TJsonReaderConfig TJsonParserActor::JsonConfig;

NActors::TActorId NMVP::CreateJsonParser(const NActors::TActorContext& ctx) {
    return ctx.Register(new TJsonParserActor());
}
