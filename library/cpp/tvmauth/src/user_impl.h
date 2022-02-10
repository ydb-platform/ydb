#pragma once 
 
#include <library/cpp/tvmauth/src/protos/ticket2.pb.h>
#include <library/cpp/tvmauth/src/protos/tvm_keys.pb.h>
#include <library/cpp/tvmauth/src/rw/keys.h>
 
#include <library/cpp/tvmauth/deprecated/user_context.h>
 
#include <library/cpp/charset/ci_string.h>
 
#include <unordered_map> 
 
namespace NTvmAuth {
    using TUserTicketImplPtr = THolder<TCheckedUserTicket::TImpl>;
    class TCheckedUserTicket::TImpl {
    public: 
        explicit operator bool() const;
 
        TUid GetDefaultUid() const; 
        time_t GetExpirationTime() const; 
        const TScopes& GetScopes() const; 
        bool HasScope(TStringBuf scopeName) const; 
        ETicketStatus GetStatus() const;
        const TUids& GetUids() const; 
 
        TString DebugInfo() const; 
 
        EBlackboxEnv GetEnv() const;

        void SetStatus(ETicketStatus status);

        /*! 
     * Constructor for creation invalid ticket storing error status in TServiceContext 
     * @param status 
     * @param protobufTicket 
     */ 
        TImpl(ETicketStatus status, ticket2::Ticket&& protobufTicket);
 
        static TUserTicketImplPtr CreateTicketForTests(ETicketStatus status,
                                                       TUid defaultUid,
                                                       TScopes scopes,
                                                       TUids uids,
                                                       EBlackboxEnv env = EBlackboxEnv::Test);

    private: 
        static const int MaxUserCount = 15; 
 
        ETicketStatus Status_;
        ticket2::Ticket ProtobufTicket_;
        mutable TScopes CachedScopes_;
        mutable TUids CachedUids_;
        mutable TString CachedDebugInfo_;
    }; 
 
    class TUserContext::TImpl { 
    public: 
        TImpl(EBlackboxEnv env, TStringBuf tvmKeysResponse); 
        void ResetKeys(TStringBuf tvmKeysResponse); 
 
        TUserTicketImplPtr Check(TStringBuf ticketBody) const; 
        const NRw::TPublicKeys& GetKeys() const; 
 
        bool IsAllowed(tvm_keys::BbEnvType env) const;
 
    private: 
        ETicketStatus CheckProtobufUserTicket(const ticket2::Ticket& ticket) const;
 
        NRw::TPublicKeys Keys_;
        EBlackboxEnv Env_;
        ::google::protobuf::LogSilencer LogSilencer_;
    }; 
}
