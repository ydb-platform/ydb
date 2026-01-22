#pragma once

#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/login/sasl/scram.h>


namespace NKikimr::NSasl {

class TAuthActorBase : public TActorBootstrapped<TAuthActorBase> {
public:
    TAuthActorBase(TActorId sender, const std::string& database, const std::string& peerName);

    STATEFN(StateNavigate) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleNavigate);
            CFunc(TEvents::TEvPoison::EventType, CleanupAndDie);
        }
    }

    STATEFN(StateLogin) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvents::TEvUndelivered, HandleUndelivered);
            HFunc(TEvTabletPipe::TEvClientConnected, HandleConnect);
            HFunc(TEvTabletPipe::TEvClientDestroyed, HandleDestroyed);
            HFunc(NSchemeShard::TEvSchemeShard::TEvLoginResult, HandleLoginResult);
            CFunc(TEvents::TSystem::Wakeup, HandleTimeout);
            CFunc(TEvents::TEvPoison::EventType, CleanupAndDie);
        }
    }

    virtual void Bootstrap(const TActorContext &ctx) = 0;

    void HandleNavigate(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev, const TActorContext &ctx);
    void HandleUndelivered(TEvents::TEvUndelivered::TPtr&, const TActorContext &ctx);
    void HandleDestroyed(TEvTabletPipe::TEvClientDestroyed::TPtr&, const TActorContext &ctx);
    void HandleConnect(TEvTabletPipe::TEvClientConnected::TPtr& ev, const TActorContext &ctx);
    void HandleLoginResult(NSchemeShard::TEvSchemeShard::TEvLoginResult::TPtr& ev, const TActorContext &ctx);
    void HandleTimeout(const TActorContext &ctx);

protected:
    void ResolveSchemeShard(const TActorContext &ctx);
    virtual NKikimrScheme::TEvLogin CreateLoginRequest() const = 0;
    virtual void SendIssuedToken(const NKikimrScheme::TEvLoginResult& loginResult) const = 0;
    virtual void SendError(NKikimrIssues::TIssuesIds::EIssueCode issueCode, const std::string& message,
        NLogin::NSasl::EScramServerError scramErrorCode = NLogin::NSasl::EScramServerError::OtherError,
        const std::string& reason = "") const = 0;
    void CleanupAndDie(const TActorContext &ctx);

private:
    NTabletPipe::TClientConfig GetPipeClientConfig() const;

protected:
    std::string_view DerivedActorName = "";

    const TActorId Sender;
    const std::string Database;
    const std::string PeerName;

    std::string AuthcId;

private:
    TActorId PipeClient;

    static const TDuration Timeout;
};

class TPlainAuthActorBase : public TAuthActorBase {
public:
    TPlainAuthActorBase(TActorId sender, const std::string& database, const std::string& authMsg,
        const std::string& peerName);

protected:
    void ProcessAuthMsg(const TActorContext &ctx);

protected:
    const std::string AuthMsg;

    std::string AuthzId;
    std::string Passwd;

};

}
