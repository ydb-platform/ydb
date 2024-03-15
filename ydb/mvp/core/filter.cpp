#include "mvp_log.h"
#include "filter.h"

using namespace NMVP;

void FilterJsonMap(NJson::TJsonValue& output, TJsonMergeContext& context) {
    NJson::TJsonValue::TMapType& target(output.GetMapSafe());
    for (auto it = target.begin(); it != target.end();) {
        auto& pair(*it);
        TJsonMergeContext ctx(context, pair.first, pair.second);
        FilterJsonValues(pair.second, ctx);
        if (pair.second.IsDefined()) {
            ++it;
        } else {
            // it = target.erase(it);
            // wat?!
            auto eit = it;
            ++it;
            target.erase(eit);
        }
    }
}

void FilterJsonArray(NJson::TJsonValue& output, TJsonMergeContext& context) {
    NJson::TJsonValue::TArray& target(output.GetArraySafe());
    size_t index = 0;
    for (auto it = target.begin(); it != target.end();) {
        NJson::TJsonValue& value(*it);
        TJsonMergeContext ctx(context, index, value);
        FilterJsonValues(value, ctx);
        if (value.IsDefined()) {
            ++it;
        } else {
            it = target.erase(it);
        }
        ++index;
    }
}

void NMVP::FilterJsonValues(NJson::TJsonValue& output, TJsonMergeContext& context) {
    //Cerr << "FILTER " << context.GetPath() << " / " << context.GetGroupPath() << Endl;
    auto outputType = output.GetType();
    switch (outputType) {
    case NJson::JSON_MAP:
        FilterJsonMap(output, context);
        break;
    case NJson::JSON_ARRAY:
        FilterJsonArray(output, context);
        break;
    default:
        break;
    }
    context.CallFilters(output);
}

class TJsonFilterActor : public NActors::TActor<TJsonFilterActor> {
public:
    using TBase = NActors::TActor<TJsonFilterActor>;

    TJsonMergeContextPtr Context;

    TJsonFilterActor(TJsonMergeContextPtr context)
        : TBase(&TJsonFilterActor::StateWork)
        , Context(context)
    {}

    void Handle(TEvJsonMerger::TEvJsonFilter::TPtr event, const NActors::TActorContext& ctx) {
        try {
            NJson::TJsonValue& value(event->Get()->JsonValue);
            {
                TJsonMergeContext context(*Context, value);
                FilterJsonValues(value, context);
            }
            ctx.Send(event->Sender, new TEvJsonMerger::TEvJsonFilterResult(std::move(Context), std::move(value)));
        }
        catch(const yexception& e) {
            ctx.Send(event->Sender, new TEvJsonMerger::TEvJsonFilterResult(std::move(Context), TStringBuilder() << "Failed to process json: " << e.what()));
        }
        Die(ctx);
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvJsonMerger::TEvJsonFilter, Handle);
        }
    }
};

NMVP::TJsonFilter NMVP::FilterArrayToCount() {
    return [](NJson::TJsonValue& output, TJsonMergeContext& context) {
        context.Stop = true;
        if (output.IsArray()) {
            NJson::TJsonValue count(output.GetArray().size());
            output.Swap(count);
        }
    };
}

NMVP::TJsonFilter NMVP::SortArrayOfObjects(const TString& sortKey) {
    char direction = '+';
    TString key = sortKey;
    if (!key.empty() && (key[0] == '+' || key[0] == '-')) {
        direction = key[0];
        key.erase(0, 1);
    }
    if (key.empty()) {
        return [](NJson::TJsonValue&, TJsonMergeContext&) {};
    }
    return [direction, key](NJson::TJsonValue& output, TJsonMergeContext& context) {
        if (output.IsArray()) {
            switch (direction) {
            case '+':
                Sort(output.GetArraySafe(), [&key](const NJson::TJsonValue& a, const NJson::TJsonValue& b) -> bool {
                    return NMVP::Compare(a[key], b[key]);
                });
                break;
            case '-':
                Sort(output.GetArraySafe(), [&key](const NJson::TJsonValue& a, const NJson::TJsonValue& b) -> bool {
                    return NMVP::Compare(b[key], a[key]);
                });
                break;
            }
        } else {
#ifndef NDEBUG
            Cerr << "Wrong type (" << output.GetType() << ") for sort on " << context.GetPath() << Endl;
#else
            Y_UNUSED(context);
#endif
        }
    };
}

NMVP::TJsonFilter NMVP::LimitArraySize(size_t size) {
    if (size == 0) {
        return [](NJson::TJsonValue&, TJsonMergeContext&) {};
    }
    return [size](NJson::TJsonValue& output, TJsonMergeContext& context) {
        if (output.IsArray()) {
            auto& array(output.GetArraySafe());
            if (array.size() > size) {
                array.resize(size);
            }
        } else {
#ifndef NDEBUG
            Cerr << "Wrong type (" << output.GetType() << ") for limit on " << context.GetPath() << Endl;
#else
            Y_UNUSED(context);
#endif
        }
    };
}

NMVP::TJsonFilter NMVP::MultipleFilters(const TVector<TJsonFilter>& filters) {
    return [filters](NJson::TJsonValue& output, TJsonMergeContext& context) {
        for (auto& filter : filters) {
            if (!output.IsDefined()) {
                break;
            }
            filter(output, context);
        }
    };
}

NActors::TActorId NMVP::CreateJsonFilter(TJsonMergeContextPtr context, const NActors::TActorContext& ctx) {
    return ctx.Register(new TJsonFilterActor(std::move(context)));
}

