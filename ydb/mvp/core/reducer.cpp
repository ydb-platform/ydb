#include "mvp_log.h"
#include "reducer.h"

using namespace NMVP;

bool ReduceJsonMap(NJson::TJsonValue& output, NJson::TJsonValue& input, TJsonMergeContext& context) {
    if (!output.IsDefined()) {
        output.SetType(NJson::JSON_MAP);
    }
    NJson::TJsonValue::TMapType& target(output.GetMapSafe());
    NJson::TJsonValue::TMapType& source(input.GetMapSafe());
    for (auto& pair : source) {
        TJsonMergeContext ctx(context, pair.first, pair.second);
        if (!ReduceJsonValues(target[pair.first], pair.second, ctx)) {
            return false;
        }
        if (ctx.Stop) {
            break;
        }
    }
    return true;
}

bool ReduceJsonArray(NJson::TJsonValue& output, NJson::TJsonValue& input, TJsonMergeContext& context) {
    if (!output.IsDefined()) {
        output.SetType(NJson::JSON_ARRAY);
    }
    NJson::TJsonValue::TArray& target(output.GetArraySafe());
    NJson::TJsonValue::TArray& source(input.GetArraySafe());
    for (NJson::TJsonValue& value : source) {
        TJsonMergeContext ctx(context, target.size(), value);
        target.push_back(NJson::TJsonValue());
        if (!ReduceJsonValues(target.back(), value, ctx)) {
            return false;
        }
        if (ctx.Stop) {
            break;
        }
    }
    return true;
}

bool NMVP::ReduceJsonValues(NJson::TJsonValue& output, NJson::TJsonValue& input, TJsonMergeContext& context) {
    //Cerr << "REDUCE " << context.GetPath() << " / " << context.GetGroupPath() << Endl;
    if (!input.IsDefined()) {
        return true;
    }

    bool success = false;
    success = context.CallReducers(output, input);
    if (success) {
        return success;
    }

    auto inputType = input.GetType();
    switch (inputType) {
    case NJson::JSON_MAP:
        success = ReduceJsonMap(output, input, context);
        break;
    case NJson::JSON_ARRAY:
        success = ReduceJsonArray(output, input, context);
        break;
    default:
        //output = std::move(input);
        JoinJsonValues(output, std::move(input));
        success = true;
    }

    return success;
}

void ReduceOneMapGroupBy(NJson::TJsonValue::TArray& target, TReduceIndex& index, NJson::TJsonValue& sourceValue, const TVector<TString>& paths, TJsonMergeContext& context) {
    if (!sourceValue.IsDefined()) {
        return;
    }
    TStackVec<const NJson::TJsonValue*> sourceKeys;
    for (const TString& path : paths) {
        const NJson::TJsonValue* sourceKey = sourceValue.GetValueByPath(path);
        if (sourceKey == nullptr) {
#ifndef NDEBUG
            Cerr << "Source key (" << path << ") not found for reduce with group by on " << context.GetPath() << Endl;
#endif
        } else {
            sourceKeys.push_back(sourceKey);
        }
    }

    if (!sourceKeys.empty()) {
        TString indexKey;
        for (const NJson::TJsonValue* sourceKey : sourceKeys) {
            if (!indexKey.empty()) {
                indexKey += '-';
            }
            indexKey += sourceKey->GetStringRobust();
        }
        auto itIndex = indexKey.empty() ? index.end() : index.find(indexKey);
        size_t idx = 0;
        if (itIndex != index.end()) {
            idx = itIndex->second;
        } else {
            idx = target.size();
            target.emplace_back(NJson::TJsonValue());
            index.emplace(indexKey, idx);
        }

        TJsonMergeContext ctx(context, idx, sourceValue);
        ReduceJsonValues(target[idx], sourceValue, ctx);
    }
}

NMVP::TJsonReducer NMVP::ReduceGroupBy(const TVector<TString>& paths) {
    return [paths](NJson::TJsonValue& output, NJson::TJsonValue& input, TJsonMergeContext& context) -> bool {
        TReduceIndex& index = context.OperationState.Indexes[context.GetPath()];
        output.SetType(NJson::JSON_ARRAY);
        NJson::TJsonValue::TArray& target = output.GetArraySafe();
        switch (input.GetType()) {
        case NJson::JSON_ARRAY: {
                NJson::TJsonValue::TArray& source = input.GetArraySafe();
                for (NJson::TJsonValue& sourceValue : source) {
                    ReduceOneMapGroupBy(target, index, sourceValue, paths, context);
                }
            }
            break;
        case NJson::JSON_MAP:
            ReduceOneMapGroupBy(target, index, input, paths, context);
            break;
        default:
#ifndef NDEBUG
            Cerr << "Wrong type (" << input.GetType() << ") for reduce with group by on " << context.GetPath() << Endl;
#endif
            Y_UNUSED(context);
        }
        return true;
    };
}

NMVP::TJsonReducer NMVP::ReduceGroupBy(const TString& path) {
    return ReduceGroupBy(TVector<TString>({path}));
}

NMVP::TJsonReducer NMVP::ReduceWithSum() {
    return [](NJson::TJsonValue& output, NJson::TJsonValue& input, TJsonMergeContext& context) {
        if (output.IsDefined()) {
            switch (input.GetType()) {
            case NJson::JSON_UINTEGER:
                output = output.GetUIntegerRobust() + input.GetUIntegerRobust();
                break;
            case NJson::JSON_INTEGER:
                output = output.GetIntegerRobust() + input.GetIntegerRobust();
                break;
            case NJson::JSON_DOUBLE:
                output = output.GetDoubleRobust() + input.GetDoubleRobust();
                break;
            default:
#ifndef NDEBUG
                Cerr << "Wrong type (" << input.GetType() << ") for reduce with sum on " << context.GetPath() << Endl;
#endif
                Y_UNUSED(context);
                return false;
            };
        } else {
            output = std::move(input);
        }
        return true;
    };
}

NMVP::TJsonReducer NMVP::ReduceWithMax() {
    return [](NJson::TJsonValue& output, NJson::TJsonValue& input, TJsonMergeContext& context) {
        if (output.IsDefined()) {
            switch (input.GetType()) {
            case NJson::JSON_UINTEGER:
                output = std::max(output.GetUIntegerRobust(), input.GetUIntegerRobust());
                break;
            case NJson::JSON_INTEGER:
                output = std::max(output.GetIntegerRobust(), input.GetIntegerRobust());
                break;
            case NJson::JSON_DOUBLE:
                output = std::max(output.GetDoubleRobust(), input.GetDoubleRobust());
                break;
            case NJson::JSON_BOOLEAN:
                output = std::max(output.GetBooleanRobust(), input.GetBooleanRobust());
                break;
            default:
#ifndef NDEBUG
                Cerr << "Wrong type (" << input.GetType() << ") for reduce with max on " << context.GetPath() << Endl;
#endif
                Y_UNUSED(context);
                return false;
            };
        } else {
            output = std::move(input);
        }
        return true;
    };
}

NMVP::TJsonReducer NMVP::ReduceWithMin() {
    return [](NJson::TJsonValue& output, NJson::TJsonValue& input, TJsonMergeContext& context) {
        if (output.IsDefined()) {
            switch (input.GetType()) {
            case NJson::JSON_UINTEGER:
                output = std::min(output.GetUIntegerRobust(), input.GetUIntegerRobust());
                break;
            case NJson::JSON_INTEGER:
                output = std::min(output.GetIntegerRobust(), input.GetIntegerRobust());
                break;
            case NJson::JSON_DOUBLE:
                output = std::min(output.GetDoubleRobust(), input.GetDoubleRobust());
                break;
            default:
#ifndef NDEBUG
                Cerr << "Wrong type (" << input.GetType() << ") for reduce with min on " << context.GetPath() << Endl;
#endif
                Y_UNUSED(context);
                return false;
            };
        } else {
            output = std::move(input);
        }
        return true;
    };
}

NMVP::TJsonReducer NMVP::ReduceArray() {
    return [](NJson::TJsonValue& output, NJson::TJsonValue& input, TJsonMergeContext& context) {
        return ReduceJsonArray(output, input, context);
    };
}

NMVP::TJsonReducer NMVP::ReduceMap() {
    return [](NJson::TJsonValue& output, NJson::TJsonValue& input, TJsonMergeContext& context) {
        return ReduceJsonMap(output, input, context);
    };
}

NMVP::TJsonReducer NMVP::ReduceChooseAny() {
    return [](NJson::TJsonValue& output, NJson::TJsonValue& input, TJsonMergeContext&) {
        if (!output.IsDefined()) {
            output = std::move(input);
        }
        return true;
    };
}

NMVP::TJsonReducer NMVP::ReduceWithArray() {
    return [](NJson::TJsonValue& output, NJson::TJsonValue& input, TJsonMergeContext&) {
        JoinJsonValuesToArray(output, std::move(input));
        return true;
    };
}

NMVP::TJsonReducer NMVP::ReduceWithUnique() {
    return [](NJson::TJsonValue& output, NJson::TJsonValue& input, TJsonMergeContext&) {
        if (output.IsDefined() && input.IsDefined() && !output.IsArray() && !input.IsArray() && output.GetStringRobust() == input.GetStringRobust()) {
            return true;
        }
        JoinJsonValues(output, std::move(input));
        if (output.IsArray()) {
            NJson::TJsonValue::TArray& array = output.GetArraySafe();
            std::sort(array.begin(), array.end(), [](const NJson::TJsonValue& a, const NJson::TJsonValue& b) -> bool {
                return a.GetStringRobust() < b.GetStringRobust();
            });
            auto end = std::unique(array.begin(), array.end(), [](const NJson::TJsonValue& a, const NJson::TJsonValue& b) -> bool {
                return a.GetStringRobust() == b.GetStringRobust();
            });
            array.erase(end, array.end());
        }
        return true;
    };
}

NMVP::TJsonReducer NMVP::ReduceWithUniqueArray() {
    return [](NJson::TJsonValue& output, NJson::TJsonValue& input, TJsonMergeContext&) {
        JoinJsonValuesToArray(output, std::move(input));
        Y_DEBUG_ABORT_UNLESS(output.IsArray());
        NJson::TJsonValue::TArray& array = output.GetArraySafe();
        std::sort(array.begin(), array.end(), [](const NJson::TJsonValue& a, const NJson::TJsonValue& b) -> bool {
            return a.GetStringRobust() < b.GetStringRobust();
        });
        auto end = std::unique(array.begin(), array.end(), [](const NJson::TJsonValue& a, const NJson::TJsonValue& b) -> bool {
            return a.GetStringRobust() == b.GetStringRobust();
        });
        array.erase(end, array.end());
        return true;
    };
}

NMVP::TJsonReducer NMVP::ReduceWithUniqueValue() {
    return [](NJson::TJsonValue& output, NJson::TJsonValue& input, TJsonMergeContext& context) {
        if (!output.IsDefined()) {
            output = std::move(input);
        } else {
            Y_DEBUG_ABORT_UNLESS(output == input, "%s", (TStringBuilder() << "path " << context.Path << " value " << input << " is not equal to " << output).c_str());
            Y_UNUSED(context);
        }
        return true;
    };
}

class TJsonReducerActor : public NActors::TActor<TJsonReducerActor> {
public:
    using TBase = NActors::TActor<TJsonReducerActor>;
    TJsonMergeContextPtr Context;
    TStringBuilder Error;
    NJson::TJsonValue Result;
    NActors::TActorId Owner;

    TJsonReducerActor(TJsonMergeContextPtr context, const NActors::TActorId& owner)
        : TBase(&TJsonReducerActor::StateWork)
        , Context(std::move(context))
        , Owner(owner)
    {}

    void Handle(TEvJsonMerger::TEvJsonReduceValue::TPtr event, const NActors::TActorContext&) {
        try {
            NJson::TJsonValue& result(event->Get()->JsonValue);
            TJsonMergeContext context(*Context, result);
            if (result.IsDefined()) {
                if (result.IsArray()) {
                    for (NJson::TJsonValue& value : result.GetArraySafe()) {
                        ReduceJsonValues(Result, value, context);
                    }
                } else {
                    ReduceJsonValues(Result, result, context);
                }
            }
        }
        catch(const yexception& e) {
            Error << "Failed to reduce: " << e.what();
        }
    }

    void Handle(TEvJsonMerger::TEvJsonReduceFinished::TPtr, const NActors::TActorContext& ctx) {
        if (!Error.empty()) {
            ctx.Send(Owner, new TEvJsonMerger::TEvJsonReduceResult(std::move(Context), Error));
        } else {
            ctx.Send(Owner, new TEvJsonMerger::TEvJsonReduceResult(std::move(Context), std::move(Result)));
        }
        Die(ctx);
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvJsonMerger::TEvJsonReduceFinished, Handle);
            HFunc(TEvJsonMerger::TEvJsonReduceValue, Handle);
        }
    }
};

NActors::TActorId NMVP::CreateJsonReducer(TJsonMergeContextPtr context, const NActors::TActorId& owner, const NActors::TActorContext& ctx) {
    return ctx.Register(new TJsonReducerActor(std::move(context), owner));
}

