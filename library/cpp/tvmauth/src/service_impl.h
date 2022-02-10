#pragma once

#include <library/cpp/tvmauth/src/protos/ticket2.pb.h>
#include <library/cpp/tvmauth/src/protos/tvm_keys.pb.h>
#include <library/cpp/tvmauth/src/rw/keys.h>

#include <library/cpp/tvmauth/type.h>
#include <library/cpp/tvmauth/deprecated/service_context.h>

#include <library/cpp/charset/ci_string.h>
#include <library/cpp/string_utils/secret_string/secret_string.h>

#include <util/generic/maybe.h>

#include <string>

namespace NTvmAuth {
    using TServiceTicketImplPtr = THolder<TCheckedServiceTicket::TImpl>;
    class TCheckedServiceTicket::TImpl {
    public:
        explicit operator bool() const;

        TTvmId GetSrc() const;
        const TScopes& GetScopes() const;
        bool HasScope(TStringBuf scopeName) const;
        ETicketStatus GetStatus() const;
        time_t GetExpirationTime() const;

        TString DebugInfo() const;
        TMaybe<TUid> GetIssuerUid() const;

        void SetStatus(ETicketStatus status);

        /*!
     * Constructor for creation invalid ticket storing error status in TServiceContext
     * @param status
     * @param protobufTicket
     */
        TImpl(ETicketStatus status, ticket2::Ticket&& protobufTicket);

        static TServiceTicketImplPtr CreateTicketForTests(ETicketStatus status,
                                                          TTvmId src,
                                                          TMaybe<TUid> issuerUid);

    private:
        ETicketStatus Status_;
        ticket2::Ticket ProtobufTicket_;
        mutable TScopes CachedScopes_;
        mutable TString CachedDebugInfo_;
    };

    class TServiceContext::TImpl {
    public:
        TImpl(TStringBuf secretBase64, TTvmId selfTvmId, TStringBuf tvmKeysResponse);
        TImpl(TTvmId selfTvmId, TStringBuf tvmKeysResponse);
        TImpl(TStringBuf secretBase64);

        void ResetKeys(TStringBuf tvmKeysResponse);

        TServiceTicketImplPtr Check(TStringBuf ticketBody) const;
        TString SignCgiParamsForTvm(TStringBuf ts, TStringBuf dst, TStringBuf scopes = TStringBuf()) const;

        const NRw::TPublicKeys& GetKeys() const { // for tests
            return Keys_;
        }

    private:
        ETicketStatus CheckProtobufServiceTicket(const ticket2::Ticket& ticket) const;
        static TString ParseSecret(TStringBuf secretBase64);

        NRw::TPublicKeys Keys_;
        const NSecretString::TSecretString Secret_;
        const TTvmId SelfTvmId_ = 0;

        ::google::protobuf::LogSilencer LogSilencer_;
    };
}
