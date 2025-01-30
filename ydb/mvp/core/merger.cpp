#include "mvp_log.h"
#include "merger.h"
#include "parser.h"
#include "mapper.h"
#include "reducer.h"
#include "filter.h"
#include <library/cpp/string_utils/quote/quote.h>

using namespace NMVP;

void NMVP::JoinJsonValues(NJson::TJsonValue& output, NJson::TJsonValue input) {
    if (!input.IsDefined()) {
        return;
    }
    if (!output.IsDefined()) {
        output = std::move(input);
        return;
    }
    if (!output.IsArray()) {
        NJson::TJsonValue array;
        array.AppendValue(std::move(output));
        array.Swap(output);
    }
    if (!input.IsArray()) {
        output.AppendValue(std::move(input));
        return;
    }
    NJson::TJsonValue::TArray& inputArray = input.GetArraySafe();
    for (NJson::TJsonValue& item : inputArray) {
        output.AppendValue(std::move(item));
    }
}

void NMVP::JoinJsonValuesToArray(NJson::TJsonValue& output, NJson::TJsonValue input) {
    if (!output.IsDefined()) {
        if (input.IsArray()) {
            output = std::move(input);
        } else {
            output.SetType(NJson::JSON_ARRAY);
            if (input.IsDefined()) {
                output.AppendValue(std::move(input));
            }
        }
        return;
    }
    if (!input.IsArray()) {
        if (input.IsDefined()) {
            output.AppendValue(std::move(input));
        }
        return;
    }
    NJson::TJsonValue::TArray& inputArray = input.GetArraySafe();
    for (NJson::TJsonValue& item : inputArray) {
        output.AppendValue(std::move(item));
    }
}

bool NMVP::Compare(const NJson::TJsonValue& a, const NJson::TJsonValue& b) {
    switch (a.GetType()) {
    case NJson::JSON_ARRAY:
        return a.GetArraySafe().size() < b.GetArraySafe().size();
    case NJson::JSON_MAP:
        return a.GetMapSafe().size() < b.GetMapSafe().size();
    case NJson::JSON_BOOLEAN:
        return a.GetBoolean() < b.GetBooleanRobust();
    case NJson::JSON_INTEGER:
        return a.GetInteger() < b.GetIntegerRobust();
    case NJson::JSON_UINTEGER:
        return a.GetUInteger() < b.GetUIntegerRobust();
    case NJson::JSON_DOUBLE:
        return a.GetDouble() < b.GetDoubleRobust();
    case NJson::JSON_STRING:
        return strcmp(a.GetString().c_str(), b.GetStringRobust().c_str()) < 0;
    case NJson::JSON_NULL:
    case NJson::JSON_UNDEFINED:
        return b.IsDefined() && !b.IsNull();
    }
}

class TJsonMergerActor : public NActors::TActorBootstrapped<TJsonMergerActor> {
public:
    struct TPeerState {
        TDuration ReceiveTime;
        TInstant ParseStartTime;
        TDuration ParseTime;
        TOperationState MapOperationState;
        NActors::TActorId Mapper;
        TInstant MapStartTime;
        TDuration MapTime;
        TOperationState ReduceOperationState;
        NActors::TActorId Reducer;
        TInstant ReduceStartTime;
        TDuration ReduceTime;
        NActors::TActorId Filter;
        TInstant FilterStartTime;
        TDuration FilterTime;
        TString Error;
    };

    using TBase = NActors::TActor<TJsonMergerActor>;
    NActors::TActorId HttpProxyId;
    NActors::TActorId IncomingConnectionId;
    NHttp::THttpIncomingRequestPtr Request;
    NHttp::THttpOutgoingResponsePtr Response;
    THashMap<NHttp::THttpRequest*, TVector<TJsonMergePeer>::const_iterator> PeersRequests;
    THashMap<const TJsonMergePeer*, TPeerState> PeerState;
    THashMap<const TJsonMergeContext*, const TJsonMergePeer*> ContextPeer;
    TVector<TJsonMergeRules> Rules;
    const TVector<TJsonMergePeer> Peers;
    NJson::TJsonValue Result;
    // TJsonMergeContextPtr FinalMapContext;
    NActors::TActorId FinalReducer;
    // TJsonMergeContextPtr FinalReduceContext;
    TOperationState FinalReduceOperationState;
    TOperationState FinalMapOperationState;
    ui32 Requests = 0; // in flight
    ui32 ReceivedResponses = 0;
    ui32 MapsToGo = 0; // until reducer
    ui32 Step = 0;
    TInstant TimeOfRequest;
    TInstant TimeOfLastResponse;
    TInstant TimeOfLastMapResponse;
    TInstant TimeOfFilter;
    TVector<TString> Error;
    THashSet<std::pair<TString, TString>> HttpErrors;
    TDuration Timeout;

    TJsonMergerActor(
            const NActors::TActorId& httpProxyId,
            const NActors::TActorId& incomingConnectionId,
            NHttp::THttpIncomingRequestPtr request,
            TVector<TJsonMergeRules> rules,
            TVector<TJsonMergePeer> peers,
            const TDuration timeout = TDuration::Seconds(60))
        : HttpProxyId(httpProxyId)
        , IncomingConnectionId(incomingConnectionId)
        , Request(std::move(request))
        , Rules(std::move(rules))
        , Peers(std::move(peers))
        , Timeout(timeout)
    {}

    void Bootstrap(const NActors::TActorContext& ctx) {
        TimeOfRequest = ctx.Now();
        //
        // peer_p1 -> map_p1 -> reduce_p1 -> filter_p1 -> map_s1 -> =
        //                                                           \\
        // peer_p2 -> map_p2 -> reduce_p2 -> filter_p2 -> map_s1 -> = reduce_s1 -> filter_s1 ->  ... map_s2 -> reduce_s2 -> filter_s2 ...
        //                                                           //
        // peer_p3 -> map_p3 -> reduce_p3 -> filter_p3 -> map_s1 -> =
        //
        MapsToGo = Peers.size();
        for (auto it = Peers.begin(); it != Peers.end(); ++it) {
            const TJsonMergePeer& peer = *it;
            if (peer.ParsedDocument.IsDefined()) {
                ++ReceivedResponses;
                TPeerState& peerState(PeerState[&peer]);
                peerState.ParseStartTime = ctx.Now();
                StartMap(peer, std::move(peer.ParsedDocument), ctx);
            } else {
                LOG_DEBUG_S(ctx, EService::MVP, "Requesting " << peer.URL);
                NHttp::THttpOutgoingRequestPtr httpRequest = new NHttp::THttpOutgoingRequest(peer.Method, peer.URL, "HTTP", "1.1");
                httpRequest->Set<&NHttp::THttpRequest::Accept>("*/*");
                if (!peer.ContentType.empty()) {
                    httpRequest->Set<&NHttp::THttpRequest::ContentType>(peer.ContentType);
                }
                httpRequest->Set(peer.Headers);
                if (!peer.ContentType.empty() && !peer.Body.empty()) {
                    httpRequest->Set<&NHttp::THttpRequest::Body>(peer.Body);
                }
                PeersRequests.emplace(httpRequest.Get(), it);
                PeerState[&peer];
                ctx.Send(HttpProxyId, new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(httpRequest, peer.Timeout));
                ++Requests;
            }
        }
        Become(&TJsonMergerActor::StateWork, Timeout, new NActors::TEvents::TEvWakeup());
    }

    struct TStringTimeInterval {
        TInstant Start;
        TDuration Duration;

        TStringTimeInterval(TInstant start, TDuration duration)
            : Start(start)
            , Duration(duration)
        {}

        operator TString() const {
            if (Duration) {
                return TStringBuilder() << Duration.MilliSeconds();
            } else {
                return "-";
            }
        }

        operator NJson::TJsonValue() const {
            if (Duration) {
                return Duration.MilliSeconds();
            } else {
                return NJson::TJsonValue(NJson::JSON_NULL);
            }
        }
    };

    void HandleTimeout(const NActors::TActorContext& ctx) {
        NJson::TJsonValue root;
        root["error"] = "Timeout";
        NJson::TJsonValue& peers = root["peers"];
        peers.SetType(NJson::JSON_ARRAY);
        for (const auto& pr : PeerState) {
            NJson::TJsonValue& peer = peers.AppendValue(NJson::TJsonValue());
            peer["url"] = pr.first->URL;
            if (!pr.second.Error.empty()) {
                peer["error"] = pr.second.Error;
            }
            NJson::TJsonValue& progress = peer["progress"];
            progress["receive"] = TStringTimeInterval(TimeOfRequest, pr.second.ReceiveTime);
            progress["parse"] = TStringTimeInterval(pr.second.ParseStartTime, pr.second.ParseTime);
            progress["map"] = TStringTimeInterval(pr.second.MapStartTime, pr.second.MapTime);
            progress["reduce"] = TStringTimeInterval(pr.second.ReduceStartTime, pr.second.ReduceTime);
            progress["filter"] = TStringTimeInterval(pr.second.FilterStartTime, pr.second.FilterTime);
        }
        TString body(NJson::WriteJson(root));
        Response = Request->CreateResponseGatewayTimeout(body, "application/json; charset=utf-8");
        ctx.Send(IncomingConnectionId, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(Response));
        //Die(ctx);
    }

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr event, const NActors::TActorContext& ctx) {
        --Requests;
        NHttp::THttpOutgoingRequestPtr request(event->Get()->Request);
        NHttp::THttpIncomingResponsePtr response(event->Get()->Response);
        const auto& itRequest = PeersRequests.find(request.Get());
        if (itRequest != PeersRequests.end()) {
            const TJsonMergePeer& peer(*itRequest->second);
            TStringBuilder log;
            log << "Received response from " << peer.GetName();
            TPeerState& peerState(PeerState[&peer]);
            peerState.ReceiveTime = ctx.Now() - TimeOfRequest;
            peerState.ParseStartTime = ctx.Now();
            if (event->Get()->Error.empty() && response != nullptr && response->Status == "200") {
                log << " - " << response->Status << " " << response->Message << " - size " << response->Body.size() << " " << (ctx.Now() - TimeOfRequest).MilliSeconds() << "ms";
                ctx.Send(CreateJsonParser(ctx), new TEvJsonMerger::TEvJsonParse(peer, response));
                ++Requests;
            } else {
                if (response == nullptr || !event->Get()->Error.empty()) {
                    log << " - " << event->Get()->Error;
                } else {
                    log << " - " << response->Status << " " << response->Message;
                }
                TString error = event->Get()->Error;
                if (error.empty() && response != nullptr) {
                    error = TString(response->Status) + " " + response->Message;
                }
                if (response != nullptr) {
                    HttpErrors.emplace(std::make_pair(TString(response->Status), TString(response->Message)));
                }
                NJson::TJsonValue errorBody;
                if (peer.ErrorHandler) {
                    errorBody = peer.ErrorHandler(error,
                                                  response != nullptr ? response->Body : TStringBuf(),
                                                  response != nullptr ? response->ContentType : TStringBuf());
                }
                if (errorBody == NJson::TJsonValue()) {
                    Error.emplace_back(TStringBuilder() << "Failed to retrieve data from " << peer.GetName() << ": " << error);
                    peerState.Error = error;
                }
                if (errorBody.IsDefined()) {
                    ctx.Send(ctx.SelfID, new TEvJsonMerger::TEvJsonParseResult(peer, errorBody));
                    ++Requests;
                } else {
                    OnMapDone(ctx);
                }
            }
            LOG_DEBUG_S(ctx, EService::MVP, log);
            ++ReceivedResponses;
            if (ReceivedResponses == Peers.size()) {
                TimeOfLastResponse = ctx.Now();
            }
        }
        if (Requests == 0) {
            ReplyAndDie(ctx);
        }
    }

    void StartMap(const TJsonMergePeer& peer, NJson::TJsonValue value, const NActors::TActorContext& ctx) {
        TPeerState& peerState(PeerState[&peer]);
        TJsonMergeContextPtr reduceContext = new TJsonMergeContext(peer.Rules, peerState.MapOperationState);
        ContextPeer[reduceContext.Get()] = &peer;
        peerState.Reducer = CreateJsonReducer(reduceContext, ctx.SelfID, ctx);
        ++Requests;
        TJsonMergeContextPtr mapContext = new TJsonMergeContext(peer.Rules, peerState.ReduceOperationState);
        ContextPeer[mapContext.Get()] = &peer;
        peerState.Mapper = CreateJsonMapper(mapContext, ctx);
        peerState.MapStartTime = ctx.Now();
        ++Requests;
        ctx.Send(peerState.Mapper, new TEvJsonMerger::TEvJsonMap(peerState.Reducer, std::move(value)));
    }

    void OnMapDone(const NActors::TActorContext& ctx) {
        --MapsToGo;
        LOG_DEBUG_S(ctx, EService::MVP, "Map done (step " << Step + 1 << "/" << Rules.size() << ", " << MapsToGo << " maps to go)");
        if (MapsToGo == 0) {
            if (FinalReducer) {
                ctx.Send(FinalReducer, new TEvJsonMerger::TEvJsonReduceFinished());
            }
        }
    }

    void Handle(TEvJsonMerger::TEvJsonParseResult::TPtr event, const NActors::TActorContext& ctx) {
        --Requests;
        const TJsonMergePeer& peer(event->Get()->Peer);
        TPeerState& peerState(PeerState[&peer]);
        peerState.ParseTime = ctx.Now() - peerState.ParseStartTime;
        LOG_DEBUG_S(ctx, EService::MVP, "Parse done " << peerState.ParseTime.MilliSeconds() << "ms for " << peer.GetName());

        if (!event->Get()->Error.empty()) {
            NJson::TJsonValue errorBody;
            if (peer.ErrorHandler) {
                errorBody = peer.ErrorHandler(event->Get()->Error, TStringBuf(), TStringBuf());
            }
            if (errorBody == NJson::TJsonValue()) {
                Error.emplace_back(event->Get()->Error);
                peerState.Error = event->Get()->Error;
                OnMapDone(ctx);
            }
            if (errorBody.IsDefined()) {
                StartMap(peer, std::move(errorBody), ctx);
            } else {
                OnMapDone(ctx);
            }
        } else {
            StartMap(peer, std::move(event->Get()->JsonValue), ctx);
        }

        if (Requests == 0) {
            ReplyAndDie(ctx);
        }
    }

    void Handle(TEvJsonMerger::TEvJsonMapResult::TPtr event, const NActors::TActorContext& ctx) {
        --Requests;
        auto itPeerState = ContextPeer.find(event->Get()->Context.Get());
        if (itPeerState != ContextPeer.end()) {
            // peer map
            const TJsonMergePeer& peer(*itPeerState->second);
            TPeerState& peerState(PeerState[&peer]);
            peerState.MapTime = ctx.Now() - peerState.MapStartTime;
            LOG_DEBUG_S(ctx, EService::MVP, "Map done " << peerState.MapTime.MilliSeconds() << "ms for " << peer.GetName());
            if (!event->Get()->Error.empty()) {
                Error.emplace_back(event->Get()->Error);
                peerState.Error = event->Get()->Error;
                OnMapDone(ctx);
            } else {
                peerState.ReduceStartTime = ctx.Now();
            }
            ctx.Send(peerState.Reducer, new TEvJsonMerger::TEvJsonReduceFinished());
            ContextPeer.erase(itPeerState);
        } else {
            OnMapDone(ctx);
        }
        if (Requests == 0) {
            ReplyAndDie(ctx);
        }
    }

    void Handle(TEvJsonMerger::TEvJsonReduceResult::TPtr event, const NActors::TActorContext& ctx) {
        --Requests;
        auto itPeerState = ContextPeer.find(event->Get()->Context.Get());
        if (itPeerState != ContextPeer.end()) {
            const TJsonMergePeer& peer(*itPeerState->second);
            TPeerState& peerState(PeerState[&peer]);
            peerState.ReduceTime = ctx.Now() - peerState.ReduceStartTime;
            LOG_DEBUG_S(ctx, EService::MVP, "Reducer done " << peerState.ReduceTime.MilliSeconds() << "ms for " << peer.GetName());
            if (!event->Get()->Error.empty()) {
                Error.emplace_back(event->Get()->Error);
                peerState.Error = event->Get()->Error;
                OnMapDone(ctx);
                ContextPeer.erase(itPeerState);
            } else {
                TJsonMergeContextPtr filterContext = event->Get()->Context; // we reuse reducer context for filter
                NActors::TActorId filter = CreateJsonFilter(filterContext, ctx);
                ctx.Send(filter, new TEvJsonMerger::TEvJsonFilter(std::move(event->Get()->JsonValue)));
                peerState.FilterStartTime = ctx.Now();
                ++Requests;
            }
        } else {
            LOG_DEBUG_S(ctx, EService::MVP, "Reducer done (step " << Step + 1 << "/" << Rules.size() << ")");
            TJsonMergeContextPtr filterContext = event->Get()->Context; // we reuse reducer context for filter
            NActors::TActorId filter = CreateJsonFilter(filterContext, ctx);
            ctx.Send(filter, new TEvJsonMerger::TEvJsonFilter(std::move(event->Get()->JsonValue)));
            ++Requests;
        }

        if (Requests == 0) {
            ReplyAndDie(ctx);
        }
    }

    void Handle(TEvJsonMerger::TEvJsonFilterResult::TPtr event, const NActors::TActorContext& ctx) {
        --Requests;
        auto itPeerState = ContextPeer.find(event->Get()->Context.Get());
        if (itPeerState != ContextPeer.end()) {
            const TJsonMergePeer& peer(*itPeerState->second);
            TPeerState& peerState(PeerState[&peer]);
            peerState.FilterTime = ctx.Now() - peerState.FilterStartTime;
            LOG_DEBUG_S(ctx, EService::MVP, "Filter done " << peerState.FilterTime.MilliSeconds() << "ms for " << peer.GetName());
            if (!event->Get()->Error.empty()) {
                Error.emplace_back(event->Get()->Error);
                peerState.Error = event->Get()->Error;
                OnMapDone(ctx);
            } else {
                if (!FinalReducer) {
                    TJsonMergeContextPtr reduceContext = new TJsonMergeContext(Rules[Step], FinalReduceOperationState);
                    FinalReducer = CreateJsonReducer(reduceContext, ctx.SelfID, ctx);
                    ++Requests;
                }
                if (!Rules[Step].Mappers.empty()) {
                    TJsonMergeContextPtr mapContext = new TJsonMergeContext(Rules[Step], peerState.MapOperationState); // we reuse mapper context from peer step
                    NActors::TActorId mapper = CreateJsonMapper(mapContext, ctx);
                    ++Requests;
                    ctx.Send(mapper, new TEvJsonMerger::TEvJsonMap(FinalReducer, std::move(event->Get()->JsonValue)));
                } else {
                    ctx.Send(FinalReducer, new TEvJsonMerger::TEvJsonReduceValue(std::move(event->Get()->JsonValue)));
                    OnMapDone(ctx);
                }
            }
            ContextPeer.erase(itPeerState);
        } else {
            LOG_DEBUG_S(ctx, EService::MVP, "Filter done (step " << Step + 1 << "/" << Rules.size() << ")");
            ++Step;
            if (Step < Rules.size()) {
                MapsToGo = 1;
                TJsonMergeContextPtr reduceContext = new TJsonMergeContext(Rules[Step], FinalReduceOperationState);
                FinalReducer = CreateJsonReducer(reduceContext, ctx.SelfID, ctx);
                ++Requests;
                if (!Rules[Step].Mappers.empty()) {
                    TJsonMergeContextPtr mapContext = new TJsonMergeContext(Rules[Step], FinalMapOperationState);
                    NActors::TActorId mapper = CreateJsonMapper(mapContext, ctx);
                    ++Requests;
                    ctx.Send(mapper, new TEvJsonMerger::TEvJsonMap(FinalReducer, std::move(event->Get()->JsonValue)));
                } else {
                    ctx.Send(FinalReducer, new TEvJsonMerger::TEvJsonReduceValue(std::move(event->Get()->JsonValue)));
                    OnMapDone(ctx);
                }
            } else {
                Result = std::move(event->Get()->JsonValue);
            }
        }
        if (Requests == 0) {
            ReplyAndDie(ctx);
        }
    }

    void ReplyAndDie(const NActors::TActorContext& ctx) {
        if (Response == nullptr) {
            if (!Error.empty()) {
                NJson::TJsonValue root;
                if (Error.size() == 1) {
                    root["error"] = Error.front();
                } else {
                    NJson::TJsonValue& error(root["error"]);
                    for (const TString& err : Error) {
                        error.AppendValue(err);
                    }
                }
                TString body(NJson::WriteJson(root));
                if (HttpErrors.size() == 1) {
                    Response = Request->CreateResponse(HttpErrors.begin()->first, HttpErrors.begin()->second, "application/json; charset=utf-8", body);
                } else {
                    Response = Request->CreateResponseServiceUnavailable(body, "application/json; charset=utf-8");
                }
            } else {
                if (!Result.IsDefined()) {
                    Result = Rules.front().Result;
                }
                if (Result.IsMap() && Result.Has("_http")) {
                    const NJson::TJsonValue& http(Result["_http"]);
                    TString status;
                    if (http.Has("status")) {
                        status = http["status"].GetStringRobust();
                    }
                    TString message;
                    if (http.Has("message")) {
                        message = http["message"].GetStringRobust();
                    }
                    TString contentType;
                    if (http.Has("contentType")) {
                        contentType = http["contentType"].GetStringRobust();
                    }
                    TString body;
                    if (http.Has("body")) {
                        body = http["body"].GetStringRobust();
                    }
                    Response = Request->CreateResponse(status, message, contentType, body);
                } else {
                    TInstant now(TInstant::Now());
                    TString body(NJson::WriteJson(Result, false));
                    Response = Request->CreateResponseOK(body, "application/json; charset=utf-8");
                    LOG_DEBUG_S(ctx, EService::MVP, "Renderer lag " << (TInstant::Now() - now).MilliSeconds() << "ms");
                }
            }
            ctx.Send(IncomingConnectionId, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(Response));
            LOG_DEBUG_S(ctx, EService::MVP, "Response size " << Response->Body.size()
                        << " with lag " << (ctx.Now() - TimeOfLastResponse).MilliSeconds() << "ms"
                        << " total time " << (ctx.Now() - TimeOfRequest).MilliSeconds() << "ms");
        }
        Die(ctx);
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NHttp::TEvHttpProxy::TEvHttpIncomingResponse, Handle);
            HFunc(TEvJsonMerger::TEvJsonParseResult, Handle);
            HFunc(TEvJsonMerger::TEvJsonMapResult, Handle);
            HFunc(TEvJsonMerger::TEvJsonReduceResult, Handle);
            HFunc(TEvJsonMerger::TEvJsonFilterResult, Handle);
            CFunc(NActors::TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }
};

TErrorHandler NMVP::IgnoreAllErrors() {
    return [](const TString&, TStringBuf, TStringBuf) -> NJson::TJsonValue { return NJson::JSON_NULL; };
}

template <>
void Out<TJsonMergerActor::TStringTimeInterval>(IOutputStream& str, const TJsonMergerActor::TStringTimeInterval& interval) {
    str << (TString)interval;
}

NActors::TActorId NMVP::CreateJsonMerger(const NActors::TActorId& httpProxyId,
                                         const NActors::TActorId& incomingConnectionId,
                                         NHttp::THttpIncomingRequestPtr request,
                                         TJsonMergeRules rules,
                                         TVector<TJsonMergePeer> peers,
                                         const NActors::TActorContext& ctx,
                                         const TDuration timeout) {
    TVector<TJsonMergeRules> array;
    array.emplace_back(std::move(rules));
    return CreateJsonMerger(httpProxyId, incomingConnectionId, std::move(request), std::move(array), std::move(peers), ctx, timeout);
}

NActors::TActorId NMVP::CreateJsonMerger(const NActors::TActorId& httpProxyId,
                                         const NActors::TActorId& incomingConnectionId,
                                         NHttp::THttpIncomingRequestPtr request,
                                         TVector<TJsonMergeRules> rules,
                                         TVector<TJsonMergePeer> peers,
                                         const NActors::TActorContext& ctx,
                                         const TDuration timeout) {
    return ctx.Register(new TJsonMergerActor(httpProxyId, incomingConnectionId, request, std::move(rules), std::move(peers), timeout));
}


NActors::TActorId NMVP::CreateJsonMerger(const NActors::TActorId& httpProxyId,
                                         const NActors::TActorId& incomingConnectionId,
                                         NHttp::THttpIncomingRequestPtr request,
                                         TVector<TJsonMergeRules> rules,
                                         TVector<TJsonMergePeer> peers,
                                         NActors::TActorSystem& actorSystem,
                                         const TDuration timeout,
                                         NActors::TMailboxType::EType mailboxType,
                                         ui32 executorPool) {
    return actorSystem.Register(
        new TJsonMergerActor(
            httpProxyId,
            incomingConnectionId,
            request,
            std::move(rules),
            std::move(peers),
            timeout
        ),
        mailboxType,
        executorPool
    );
}
