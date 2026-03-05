#pragma once

#include "events.h"

#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <memory>

namespace NMVP {

class ILinkSource;

struct TSourceOutput {
    TString Name;
    bool Waiting = true;
    TVector<NSupportLinks::TResolvedLink> Links;
    TVector<NSupportLinks::TSupportError> Errors;
};

class TResolutionContext {
public:
    struct TParams {
        TVector<const ILinkSource*> Sources;
        THashMap<TString, TString> ClusterColumns;
        TVector<std::pair<TString, TString>> QueryParams;
        NActors::TActorId Parent;
        NActors::TActorId HttpProxyId;
    };

    static std::unique_ptr<TResolutionContext> Build(TParams params);
    void Start();
    TVector<NActors::IActor*> DetachActorsToRegister();

    void OnSourceResponse(const NSupportLinks::TEvPrivate::TEvSourceResponse::TPtr& event);
    void HandleTimeout();
    void ReportResolved(size_t place, TVector<NSupportLinks::TResolvedLink> links, TVector<NSupportLinks::TSupportError> errors);
    const TVector<TSourceOutput>& GetSourceOutput() const;
    bool Waiting() const;
    bool IsFinished() const;

private:
    TResolutionContext() = default;
    void Initialize(TParams params);

    TVector<const ILinkSource*> Sources;
    TVector<TSourceOutput> SourceOutputs;
    THashMap<TString, TString> ClusterColumns;
    TVector<std::pair<TString, TString>> QueryParams;
    NActors::TActorId Parent;
    NActors::TActorId HttpProxyId;
    TVector<NActors::IActor*> ActorsToRegister;
};

} // namespace NMVP
