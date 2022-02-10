#include "user_impl.h" 
 
#include "parser.h" 
 
#include <library/cpp/tvmauth/exception.h>
#include <library/cpp/tvmauth/ticket_status.h>
 
#include <util/generic/strbuf.h> 
#include <util/string/cast.h> 
#include <util/string/split.h> 
 
#include <algorithm> 
 
namespace NTvmAuth {
    static const char* EX_MSG = "Method cannot be used in non-valid ticket";

    TStringBuf GetBlackboxEnvAsString(EBlackboxEnv environment) {
        switch (environment) {
            case (EBlackboxEnv::Prod):
                return TStringBuf("Prod");
            case (EBlackboxEnv::Test):
                return TStringBuf("Test");
            case (EBlackboxEnv::ProdYateam):
                return TStringBuf("ProdYateam");
            case (EBlackboxEnv::TestYateam):
                return TStringBuf("TestYateam");
            case (EBlackboxEnv::Stress):
                return TStringBuf("Stress");
            default: 
                throw yexception() << "Unknown environment";
        }
    }

    TCheckedUserTicket::TImpl::operator bool() const {
        return (Status_ == ETicketStatus::Ok);
    } 
 
    TUid TCheckedUserTicket::TImpl::GetDefaultUid() const {
        Y_ENSURE_EX(bool(*this), TNotAllowedException() << EX_MSG);
        return ProtobufTicket_.user().defaultuid();
    } 

    time_t TCheckedUserTicket::TImpl::GetExpirationTime() const {
        Y_ENSURE_EX(bool(*this), TNotAllowedException() << EX_MSG);
        return ProtobufTicket_.expirationtime();
    } 
 
    const TScopes& TCheckedUserTicket::TImpl::GetScopes() const {
        Y_ENSURE_EX(bool(*this), TNotAllowedException() << EX_MSG);
        if (CachedScopes_.empty()) {
            for (const auto& el : ProtobufTicket_.user().scopes()) {
                CachedScopes_.push_back(el);
            } 
        } 
        return CachedScopes_;
    } 
 
    bool TCheckedUserTicket::TImpl::HasScope(TStringBuf scopeName) const {
        Y_ENSURE_EX(bool(*this), TNotAllowedException() << EX_MSG);
        return std::binary_search(ProtobufTicket_.user().scopes().begin(), ProtobufTicket_.user().scopes().end(), scopeName);
    } 
 
    ETicketStatus TCheckedUserTicket::TImpl::GetStatus() const {
        return Status_;
    } 
 
    const TUids& TCheckedUserTicket::TImpl::GetUids() const {
        Y_ENSURE_EX(bool(*this), TNotAllowedException() << EX_MSG);
        if (CachedUids_.empty()) {
            for (const auto& user : ProtobufTicket_.user().users()) {
                CachedUids_.push_back(user.uid());
            } 
        } 
        return CachedUids_;
    } 
 
    TString TCheckedUserTicket::TImpl::DebugInfo() const {
        if (CachedDebugInfo_) {
            return CachedDebugInfo_;
        }

        if (Status_ == ETicketStatus::Malformed) {
            CachedDebugInfo_ = "status=malformed;";
            return CachedDebugInfo_;
        } 

        TString targetString = "ticket_type="; 
        targetString.reserve(256);
        if (Status_ == ETicketStatus::InvalidTicketType) {
            targetString.append("not-user;"); 
            CachedDebugInfo_ = targetString;
            return targetString; 
        } 

        targetString.append("user"); 
        if (ProtobufTicket_.expirationtime() > 0)
            targetString.append(";expiration_time=").append(IntToString<10>(ProtobufTicket_.expirationtime()));
        for (const auto& scope : ProtobufTicket_.user().scopes()) {
            targetString.append(";scope=").append(scope); 
        } 

        if (ProtobufTicket_.user().defaultuid() > 0)
            targetString.append(";default_uid=").append(IntToString<10>(ProtobufTicket_.user().defaultuid()));
        for (const auto& user : ProtobufTicket_.user().users()) {
            targetString.append(";uid=").append(IntToString<10>(user.uid())); 
        } 

        targetString.append(";env=");
        EBlackboxEnv environment = static_cast<EBlackboxEnv>(ProtobufTicket_.user().env());
        targetString.append(GetBlackboxEnvAsString(environment));
        targetString.append(";"); 

        CachedDebugInfo_ = targetString;
        return targetString; 
    } 
 
    EBlackboxEnv TCheckedUserTicket::TImpl::GetEnv() const {
        return (EBlackboxEnv)ProtobufTicket_.user().env();
    }

    void TCheckedUserTicket::TImpl::SetStatus(ETicketStatus status) {
        Status_ = status;
    }

    TCheckedUserTicket::TImpl::TImpl(ETicketStatus status, ticket2::Ticket&& protobufTicket)
        : Status_(status)
        , ProtobufTicket_(std::move(protobufTicket))
    { 
    } 
 
    TUserTicketImplPtr TCheckedUserTicket::TImpl::CreateTicketForTests(ETicketStatus status,
                                                                       TUid defaultUid,
                                                                       TScopes scopes,
                                                                       TUids uids,
                                                                       EBlackboxEnv env) {
        auto prepareCont = [](auto& cont) {
            std::sort(cont.begin(), cont.end());
            cont.erase(std::unique(cont.begin(), cont.end()), cont.end());
        };
        auto erase = [](auto& cont, auto val) {
            auto it = std::find(cont.begin(), cont.end(), val);
            if (it != cont.end()) {
                cont.erase(it);
            }
        };

        prepareCont(scopes);
        erase(scopes, "");

        uids.push_back(defaultUid);
        prepareCont(uids);
        erase(uids, 0);
        Y_ENSURE(!uids.empty(), "User ticket cannot contain empty uid list");

        ticket2::Ticket proto;
        for (TUid uid : uids) {
            proto.mutable_user()->add_users()->set_uid(uid);
        }
        proto.mutable_user()->set_defaultuid(defaultUid);
        proto.mutable_user()->set_entrypoint(100500);
        for (TStringBuf scope : scopes) {
            proto.mutable_user()->add_scopes(TString(scope));
        }

        proto.mutable_user()->set_env((tvm_keys::BbEnvType)env);

        return MakeHolder<TImpl>(status, std::move(proto));
    }

    TUserContext::TImpl::TImpl(EBlackboxEnv env, TStringBuf tvmKeysResponse) 
        : Env_(env)
    { 
        ResetKeys(tvmKeysResponse); 
    } 
 
    void TUserContext::TImpl::ResetKeys(TStringBuf tvmKeysResponse) { 
        tvm_keys::Keys protoKeys; 
        if (!protoKeys.ParseFromString(TParserTvmKeys::ParseStrV1(tvmKeysResponse))) { 
            ythrow TMalformedTvmKeysException() << "Malformed TVM keys";
        } 
 
        NRw::TPublicKeys keys; 
        for (int idx = 0; idx < protoKeys.bb_size(); ++idx) { 
            const tvm_keys::BbKey& k = protoKeys.bb(idx); 
            if (IsAllowed(k.env())) {
                keys.emplace(k.gen().id(), 
                             k.gen().body()); 
            } 
        } 
 
        if (keys.empty()) { 
            ythrow TEmptyTvmKeysException() << "Empty TVM keys";
        } 
 
        Keys_ = std::move(keys);
    } 
 
    TUserTicketImplPtr TUserContext::TImpl::Check(TStringBuf ticketBody) const { 
        TParserTickets::TRes res = TParserTickets::ParseV3(ticketBody, Keys_, TParserTickets::UserFlag());
        ETicketStatus status = CheckProtobufUserTicket(res.Ticket);
 
        if (res.Status != ETicketStatus::Ok && !(res.Status == ETicketStatus::MissingKey && status == ETicketStatus::InvalidBlackboxEnv)) {
            status = res.Status; 
        } 
        return MakeHolder<TCheckedUserTicket::TImpl>(status, std::move(res.Ticket));
    } 
 
    ETicketStatus TUserContext::TImpl::CheckProtobufUserTicket(const ticket2::Ticket& ticket) const {
        if (!ticket.has_user()) { 
            return ETicketStatus::Malformed;
        } 
        if (!IsAllowed(ticket.user().env())) {
            return ETicketStatus::InvalidBlackboxEnv;
        } 
        return ETicketStatus::Ok;
    } 
 
    const NRw::TPublicKeys& TUserContext::TImpl::GetKeys() const { 
        return Keys_;
    } 
 
    bool TUserContext::TImpl::IsAllowed(tvm_keys::BbEnvType env) const {
        if (env == tvm_keys::Prod && (Env_ == EBlackboxEnv::Prod || Env_ == EBlackboxEnv::Stress)) {
            return true; 
        } 
        if (env == tvm_keys::ProdYateam && Env_ == EBlackboxEnv::ProdYateam) {
            return true; 
        } 
        if (env == tvm_keys::Test && Env_ == EBlackboxEnv::Test) {
            return true; 
        } 
        if (env == tvm_keys::TestYateam && Env_ == EBlackboxEnv::TestYateam) {
            return true; 
        } 
        if (env == tvm_keys::Stress && Env_ == EBlackboxEnv::Stress) {
            return true; 
        } 
 
        return false; 
    } 
}
