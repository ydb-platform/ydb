#include "mvp_log.h"
#include "mapper.h"

using namespace NMVP;

void MapJsonMap(NJson::TJsonValue& input, TJsonMergeContext& context) {
    NJson::TJsonValue::TMapType& source(input.GetMapSafe());
    for (auto& pair : source) {
        TJsonMergeContext ctx(context, pair.first, pair.second);
        MapJsonValues(pair.second, ctx);
    }
}

void MapJsonArray(NJson::TJsonValue& input, TJsonMergeContext& context) {
    NJson::TJsonValue::TArray& source(input.GetArraySafe());
    size_t index = 0;
    for (NJson::TJsonValue& value : source) {
        TJsonMergeContext ctx(context, index, value);
        MapJsonValues(value, ctx);
        ++index;
    }
}

void NMVP::MapJsonValues(NJson::TJsonValue& input, TJsonMergeContext& context) {
    //Cerr << "MAP " << context.GetPath() << " / " << context.GetGroupPath() << Endl;
    context.CallMappers(input);
    if (!context.Stop) {
        auto inputType = input.GetType();
        switch (inputType) {
        case NJson::JSON_MAP:
            MapJsonMap(input, context);
            break;
        case NJson::JSON_ARRAY:
            MapJsonArray(input, context);
            break;
        default:
            break;
        }
    }
}

TJsonMapper NMVP::MapAll() {
    return [](NJson::TJsonValue& input, TJsonMergeContext& context) -> NJson::TJsonValue {
        NJson::TJsonValue result;
        TString path = context.GetPath();
        if (path.StartsWith('.')) {
            context.Stop = true;
            result.SetValueByPath(TStringBuf(path).substr(1), std::move(input));
        }
        return result;
    };
}

class TJsonMapperActor : public NActors::TActor<TJsonMapperActor> {
public:
    using TBase = NActors::TActor<TJsonMapperActor>;

    TJsonMergeContextPtr Context;

    TJsonMapperActor(TJsonMergeContextPtr context)
        : TBase(&TJsonMapperActor::StateWork)
        , Context(std::move(context))
    {}

    void Handle(TEvJsonMerger::TEvJsonMap::TPtr event, const NActors::TActorContext& ctx) {
        try {
            const NActors::TActorId& reducer(event->Get()->Reducer);
            NJson::TJsonValue& value(event->Get()->JsonValue);
            {
                TJsonMergeContext context(*Context, value);
                context.OperationState.SendToReducer = [&reducer, &ctx](NJson::TJsonValue value) {
                    ctx.Send(reducer, new TEvJsonMerger::TEvJsonReduceValue(std::move(value)));
                };
                MapJsonValues(value, context);
            }
            ctx.Send(event->Sender, new TEvJsonMerger::TEvJsonMapResult(std::move(Context)));
        }
        catch(const yexception& e) {
            ctx.Send(event->Sender, new TEvJsonMerger::TEvJsonMapResult(std::move(Context), TStringBuilder()
                                                                        << "Failed to process json: "
                                                                        << e.what()));
        }
        Die(ctx);
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvJsonMerger::TEvJsonMap, Handle);
        }
    }
};

NActors::TActorId NMVP::CreateJsonMapper(TJsonMergeContextPtr context, const NActors::TActorContext& ctx) {
    return ctx.Register(new TJsonMapperActor(std::move(context)));
}

