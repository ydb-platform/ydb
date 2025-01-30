#pragma once
#include <util/generic/hash_set.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>
#include <ydb/library/actors/http/http_proxy.h>

namespace NMVP {

struct TJsonMergeContext;

using TJsonMapper = std::function<NJson::TJsonValue(NJson::TJsonValue& input, TJsonMergeContext& context)>;
using TJsonReducer = std::function<bool(NJson::TJsonValue& output, NJson::TJsonValue& input, TJsonMergeContext& context)>;
using TJsonFilter = std::function<void(NJson::TJsonValue& output, TJsonMergeContext& context)>;
using TErrorHandler = std::function<NJson::TJsonValue(const TString& error, TStringBuf body, TStringBuf contentType)>;

struct TJsonMergeRules {
    THashMap<TString, TJsonMapper> Mappers;
    THashMap<TString, TJsonReducer> Reducers;
    THashMap<TString, TJsonFilter> Filters;
    NJson::TJsonValue Result;
};

void JoinJsonValues(NJson::TJsonValue& output, NJson::TJsonValue input);
void JoinJsonValuesToArray(NJson::TJsonValue& output, NJson::TJsonValue input);
void MapJsonValues(NJson::TJsonValue& input, TJsonMergeContext& context);
bool ReduceJsonValues(NJson::TJsonValue& output, NJson::TJsonValue& input, TJsonMergeContext& context);
void FilterJsonValues(NJson::TJsonValue& output, TJsonMergeContext& context);
bool Compare(const NJson::TJsonValue& a, const NJson::TJsonValue& b);
TJsonMapper MapAll();
TJsonReducer ReduceChooseAny();
TJsonReducer ReduceArray();
TJsonReducer ReduceMap();
TJsonReducer ReduceGroupBy(const TString& path);
TJsonReducer ReduceGroupBy(const TVector<TString>& paths);
template <typename... TStrings>
inline TJsonReducer ReduceGroupBy(const TStrings&... paths) {
    return ReduceGroupBy(TVector<TString>({paths...}));
}
TJsonReducer ReduceWithSum();
TJsonReducer ReduceWithMax();
TJsonReducer ReduceWithMin();
TJsonReducer ReduceWithArray();
TJsonReducer ReduceWithUnique();
TJsonReducer ReduceWithUniqueArray();
TJsonReducer ReduceWithUniqueValue();
TJsonFilter FilterArrayToCount();
TJsonFilter SortArrayOfObjects(const TString& key);
TJsonFilter LimitArraySize(size_t size);
TJsonFilter MultipleFilters(const TVector<TJsonFilter>& filters);
TErrorHandler IgnoreAllErrors();

using TJsonSendToReducer = std::function<void(NJson::TJsonValue)>;
using TReduceIndex = THashMap<TString, size_t>;
using TReduceIndexes = THashMap<TString, TReduceIndex>;

struct TUserState {
    virtual ~TUserState() = default;
};

struct TOperationState {
    TJsonSendToReducer SendToReducer;
    TReduceIndexes Indexes; // we guarantee synchronous access to reducer indexes
    THolder<TUserState> UserState; // we guarantee synchronous access to user states
};

struct TJsonMergeContext;
using TJsonMergeContextPtr = TIntrusivePtr<TJsonMergeContext>;

struct TJsonMergeContext : TRefCounted<TJsonMergeContext, TAtomicCounter> {
    const TJsonMergeContext& RootContext;
    const TJsonMergeRules& MergeRules;
    const TJsonMergeContext& ParentContext;
    TOperationState& OperationState;
    TString Path;
    TString GroupPath;
    bool Stop = false;
    NJson::TJsonValue& Value;

    TJsonMergeContext(const TJsonMergeContext& context) = delete;

    static TOperationState& GetDefaultOperationState() {
        static TOperationState defaultOperationState;
        return defaultOperationState;
    }

    static const TJsonMergeRules& GetDefaultMergeRules() {
        static TJsonMergeRules defaultMergeRules;
        return defaultMergeRules;
    }

    static NJson::TJsonValue& GetDefaultJsonValue() {
        static NJson::TJsonValue defaultJsonValue;
        return defaultJsonValue;
    }

    TJsonMergeContext(const TJsonMergeRules& mergeRules, TOperationState& operationState) // root context
        : RootContext(*this)
        , MergeRules(mergeRules)
        , ParentContext(*this)
        , OperationState(operationState)
        , Value(GetDefaultJsonValue())
    {}

//    TJsonMergeContext(TJsonMergeContextPtr globalContext, const TJsonMergeRules& mergeRules, TOperationState& operationState, NJson::TJsonValue& value) // root local context
//        : GlobalContext(globalContext)
//        , MergeRules(mergeRules)
//        , ParentContext(*this)
//        , OperationState(operationState)
//        , Path(".")
//        , GroupPath(".")
//        , Value(value)
//    {}

    TJsonMergeContext(const TJsonMergeContext& rootContext, NJson::TJsonValue& value) // root local context
        : RootContext(rootContext)
        , MergeRules(rootContext.MergeRules)
        , ParentContext(*this)
        , OperationState(rootContext.OperationState)
        , Path(".")
        , GroupPath(".")
        , Value(value)
    {}

    TJsonMergeContext(const TJsonMergeContext& context, TStringBuf name, NJson::TJsonValue& value)
        : RootContext(context.RootContext)
        , MergeRules(context.MergeRules)
        , ParentContext(context)
        , OperationState(context.OperationState)
        , Path(context.Path == "." ? context.Path + name : context.Path + '.' + name)
        , GroupPath(context.GroupPath == "." ? context.GroupPath + name : context.GroupPath + '.' + name)
        , Value(value)
    {}

    TJsonMergeContext(const TJsonMergeContext& context, size_t index, NJson::TJsonValue& value)
        : RootContext(context.RootContext)
        , MergeRules(context.MergeRules)
        , ParentContext(context)
        , OperationState(context.OperationState)
        , Path(context.Path + '[' + ToString(index) + ']')
        , GroupPath(context.GroupPath + "[]")
        , Value(value)
    {}

    const TString& GetPath() const {
        return Path;
    }

    const TString& GetGroupPath() const {
        return GroupPath;
    }

    void CallMappers(NJson::TJsonValue& input, TJsonMergeContext& context) const {
        auto it = MergeRules.Mappers.find(context.Path);
        if (it != MergeRules.Mappers.end()) {
            NJson::TJsonValue result = it->second(input, context);
            context.SendToReducer(std::move(result));
        }
        if (context.GroupPath != context.Path) {
            it = MergeRules.Mappers.find(context.GroupPath);
            if (it != MergeRules.Mappers.end()) {
                NJson::TJsonValue result = it->second(input, context);
                context.SendToReducer(std::move(result));
            }
        }
    }

    void CallMappers(NJson::TJsonValue& input) {
        CallMappers(input, *this);
    }

    bool CallReducers(NJson::TJsonValue& output, NJson::TJsonValue& input, TJsonMergeContext& context) const {
        bool handled = false;
        auto it = MergeRules.Reducers.find(context.Path);
        if (it != MergeRules.Reducers.end()) {
            handled |= it->second(output, input, context);
        }
        if (context.GroupPath != context.Path) {
            it = MergeRules.Reducers.find(context.GroupPath);
            if (it != MergeRules.Reducers.end()) {
                handled |= it->second(output, input, context);
            }
        }
        return handled;
    }

    bool CallReducers(NJson::TJsonValue& output, NJson::TJsonValue& input) {
        return CallReducers(output, input, *this);
    }

    void CallFilters(NJson::TJsonValue& output, TJsonMergeContext& context) const {
        auto it = MergeRules.Filters.find(context.Path);
        if (it != MergeRules.Filters.end()) {
            it->second(output, context);
        }
        if (context.GroupPath != context.Path) {
            it = MergeRules.Filters.find(context.GroupPath);
            if (it != MergeRules.Filters.end()) {
                it->second(output, context);
            }
        }
    }

    void CallFilters(NJson::TJsonValue& output) {
        return CallFilters(output, *this);
    }

    void SendToReducer(NJson::TJsonValue value) {
        Y_ABORT_UNLESS(OperationState.SendToReducer);
        if (value.IsDefined()) {
            OperationState.SendToReducer(std::move(value));
        }
    }

    template <typename UserState>
    UserState& GetUserState() const {
        if (OperationState.UserState == nullptr) {
            OperationState.UserState.Reset(new UserState);
        }
        return *static_cast<UserState*>(OperationState.UserState.Get());
    }
};

struct TJsonMergePeer {
    TStringBuf Method = "GET";
    TString URL;
    TString ContentType;
    TString Body;
    NHttp::THeadersBuilder Headers;
    TDuration Timeout;
    NJson::TJsonValue ParsedDocument;
    TErrorHandler ErrorHandler;
    TJsonMergeRules Rules;

    TString GetName() const {
        /*if (URL.empty()) {
            return TString();
        }
        size_t pos = 0;
        for (int i = 0; i < 3 && pos != TString::npos; pos = URL.find('/', pos + 1), ++i);
        if (pos != TString::npos) {
            pos += 30;
            if (URL.size() < pos) {
                return URL;
            } else {
                return URL.substr(0, pos) + "...";
            }
        } else {
            return URL;
        }*/
        return URL;
    }
};

struct TEvJsonMerger {
    enum EEv {
        EvJsonParse = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
        EvJsonParseResult,
        EvJsonMap,
        EvJsonMapResult,
        EvJsonReduceValue,
        EvJsonReduceFinished,
        EvJsonReduceResult,
        EvJsonFilter,
        EvJsonFilterResult,
        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "ES_PRIVATE event space is too small.");

    struct TEvJsonParse : NActors::TEventLocal<TEvJsonParse, EvJsonParse> {
        const TJsonMergePeer& Peer;
        NHttp::THttpIncomingResponsePtr HttpResponse;

        TEvJsonParse(const TJsonMergePeer& peer, NHttp::THttpIncomingResponsePtr httpResponse)
            : Peer(peer)
            , HttpResponse(httpResponse)
        {}
    };

    struct TEvJsonParseResult : NActors::TEventLocal<TEvJsonParseResult, EvJsonParseResult> {
        const TJsonMergePeer& Peer;
        NJson::TJsonValue JsonValue;
        TString Error;

        TEvJsonParseResult(const TJsonMergePeer& peer, NJson::TJsonValue jsonValue)
            : Peer(peer)
            , JsonValue(std::move(jsonValue))
        {}

        TEvJsonParseResult(const TJsonMergePeer& peer, const TString& error)
            : Peer(peer)
            , Error(error)
        {}
    };

    struct TEvJsonMap : NActors::TEventLocal<TEvJsonMap, EvJsonMap> {
        NActors::TActorId Reducer;
        NJson::TJsonValue JsonValue;

        TEvJsonMap(const NActors::TActorId& reducer, NJson::TJsonValue jsonValue)
            : Reducer(reducer)
            , JsonValue(std::move(jsonValue))
        {}
    };

    struct TEvJsonMapResult : NActors::TEventLocal<TEvJsonMapResult, EvJsonMapResult> {
        TJsonMergeContextPtr Context;
        TString Error;

        TEvJsonMapResult(TJsonMergeContextPtr context)
            : Context(std::move(context))
        {}

        TEvJsonMapResult(TJsonMergeContextPtr context, const TString& error)
            : Context(std::move(context))
            , Error(error)
        {}
    };

    struct TEvJsonReduceValue : NActors::TEventLocal<TEvJsonReduceValue, EvJsonReduceValue> {
        NJson::TJsonValue JsonValue;

        TEvJsonReduceValue(NJson::TJsonValue jsonValue)
            : JsonValue(std::move(jsonValue))
        {}
    };

    struct TEvJsonReduceFinished : NActors::TEventLocal<TEvJsonReduceFinished, EvJsonReduceFinished> {};

    struct TEvJsonReduceResult : NActors::TEventLocal<TEvJsonReduceResult, EvJsonReduceResult> {
        TJsonMergeContextPtr Context;
        NJson::TJsonValue JsonValue;
        TString Error;

        TEvJsonReduceResult(TJsonMergeContextPtr context, NJson::TJsonValue jsonValue)
            : Context(std::move(context))
            , JsonValue(std::move(jsonValue))

        {}

        TEvJsonReduceResult(TJsonMergeContextPtr context, const TString& error)
            : Context(std::move(context))
            , Error(error)
        {}
    };

    struct TEvJsonFilter : NActors::TEventLocal<TEvJsonFilter, EvJsonFilter> {
        NJson::TJsonValue JsonValue;

        TEvJsonFilter(NJson::TJsonValue jsonValue)
            : JsonValue(std::move(jsonValue))
        {}
    };

    struct TEvJsonFilterResult : NActors::TEventLocal<TEvJsonFilterResult, EvJsonFilterResult> {
        TJsonMergeContextPtr Context;
        NJson::TJsonValue JsonValue;
        TString Error;

        TEvJsonFilterResult(TJsonMergeContextPtr context, NJson::TJsonValue jsonValue)
            : Context(std::move(context))
            , JsonValue(std::move(jsonValue))
        {}

        TEvJsonFilterResult(TJsonMergeContextPtr context, const TString& error)
            : Context(std::move(context))
            , Error(error)
        {}
    };
};

NActors::TActorId CreateJsonMerger(const NActors::TActorId& httpProxyId,
                                   const NActors::TActorId& incomingConnectionId,
                                   NHttp::THttpIncomingRequestPtr request,
                                   TVector<TJsonMergeRules> rules,
                                   TVector<TJsonMergePeer> peers,
                                   const NActors::TActorContext& ctx,
                                   const TDuration timeout = TDuration::Seconds(60));

NActors::TActorId CreateJsonMerger(const NActors::TActorId& httpProxyId,
                                   const NActors::TActorId& incomingConnectionId,
                                   NHttp::THttpIncomingRequestPtr request,
                                   TJsonMergeRules rules,
                                   TVector<TJsonMergePeer> peers,
                                   const NActors::TActorContext& ctx,
                                   const TDuration timeout = TDuration::Seconds(60));

NActors::TActorId CreateJsonMerger(const NActors::TActorId& httpProxyId,
                                   const NActors::TActorId& incomingConnectionId,
                                   NHttp::THttpIncomingRequestPtr request,
                                   TVector<TJsonMergeRules> rules,
                                   TVector<TJsonMergePeer> peers,
                                   NActors::TActorSystem& actorSystem,
                                   const TDuration timeout,
                                   NActors::TMailboxType::EType mailboxType = NActors::TMailboxType::HTSwap,
                                   ui32 executorPool = 0);

} // namespace NMVP
