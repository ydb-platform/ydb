#include "service_impl.h"

#include "parser.h"
#include "utils.h"

#include <library/cpp/tvmauth/exception.h>
#include <library/cpp/tvmauth/ticket_status.h>

#include <util/generic/strbuf.h>
#include <util/string/cast.h>
#include <util/string/split.h>

namespace NTvmAuth {
    static const char* EX_MSG = "Method cannot be used in non-valid ticket";

    TCheckedServiceTicket::TImpl::operator bool() const {
        return (Status_ == ETicketStatus::Ok);
    }

    TTvmId TCheckedServiceTicket::TImpl::GetSrc() const {
        Y_ENSURE_EX(bool(*this), TNotAllowedException() << EX_MSG);
        return ProtobufTicket_.service().srcclientid();
    }

    const TScopes& TCheckedServiceTicket::TImpl::GetScopes() const {
        Y_ENSURE_EX(bool(*this), TNotAllowedException() << EX_MSG);
        if (CachedScopes_.empty()) {
            for (const auto& el : ProtobufTicket_.service().scopes()) {
                CachedScopes_.push_back(el);
            }
        }
        return CachedScopes_;
    }

    bool TCheckedServiceTicket::TImpl::HasScope(TStringBuf scopeName) const {
        Y_ENSURE_EX(bool(*this), TNotAllowedException() << EX_MSG);
        return std::binary_search(ProtobufTicket_.service().scopes().begin(), ProtobufTicket_.service().scopes().end(), scopeName);
    }

    ETicketStatus TCheckedServiceTicket::TImpl::GetStatus() const {
        return Status_;
    }

    time_t TCheckedServiceTicket::TImpl::GetExpirationTime() const {
        Y_ENSURE_EX(bool(*this), TNotAllowedException() << EX_MSG);
        return ProtobufTicket_.expirationtime();
    }

    TString TCheckedServiceTicket::TImpl::DebugInfo() const {
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
            targetString.append("not-serv;");
            CachedDebugInfo_ = targetString;
            return targetString;
        }

        targetString.append("serv");
        if (ProtobufTicket_.has_expirationtime())
            targetString.append(";expiration_time=").append(IntToString<10>(ProtobufTicket_.expirationtime()));
        if (ProtobufTicket_.service().has_srcclientid()) {
            targetString.append(";src=").append(IntToString<10>(ProtobufTicket_.service().srcclientid()));
        }
        if (ProtobufTicket_.service().has_dstclientid()) {
            targetString.append(";dst=").append(IntToString<10>(ProtobufTicket_.service().dstclientid()));
        }
        for (const auto& scope : ProtobufTicket_.service().scopes()) {
            targetString.append(";scope=").append(scope);
        }
        if (ProtobufTicket_.service().has_issueruid()) {
            targetString.append(";issuer_uid=").append(IntToString<10>(ProtobufTicket_.service().GetissuerUid()));
        }
        targetString.append(";");

        CachedDebugInfo_ = targetString;
        return targetString;
    }

    TMaybe<TUid> TCheckedServiceTicket::TImpl::GetIssuerUid() const {
        Y_ENSURE_EX(bool(*this), TNotAllowedException() << EX_MSG);
        return ProtobufTicket_.service().has_issueruid()
                   ? ProtobufTicket_.service().GetissuerUid()
                   : TMaybe<TUid>();
    }

    void TCheckedServiceTicket::TImpl::SetStatus(ETicketStatus status) {
        Status_ = status;
    }

    TCheckedServiceTicket::TImpl::TImpl(ETicketStatus status, ticket2::Ticket&& protobufTicket)
        : Status_(status)
        , ProtobufTicket_(std::move(protobufTicket))
    {
    }

    TServiceTicketImplPtr TCheckedServiceTicket::TImpl::CreateTicketForTests(ETicketStatus status,
                                                                             TTvmId src,
                                                                             TMaybe<TUid> issuerUid) {
        ticket2::Ticket proto;
        proto.mutable_service()->set_srcclientid(src);
        proto.mutable_service()->set_dstclientid(100500);
        if (issuerUid) {
            proto.mutable_service()->set_issueruid(*issuerUid);
        }
        return MakeHolder<TImpl>(status, std::move(proto));
    }

    TServiceContext::TImpl::TImpl(TStringBuf secretBase64, TTvmId selfTvmId, TStringBuf tvmKeysResponse)
        : Secret_(ParseSecret(secretBase64))
        , SelfTvmId_(selfTvmId)
    {
        ResetKeys(tvmKeysResponse);
    }

    TServiceContext::TImpl::TImpl(TTvmId selfTvmId, TStringBuf tvmKeysResponse)
        : SelfTvmId_(selfTvmId)
    {
        ResetKeys(tvmKeysResponse);
    }

    TServiceContext::TImpl::TImpl(TStringBuf secretBase64)
        : Secret_(ParseSecret(secretBase64))
    {
    }

    void TServiceContext::TImpl::ResetKeys(TStringBuf tvmKeysResponse) {
        tvm_keys::Keys protoKeys;
        if (!protoKeys.ParseFromString(TParserTvmKeys::ParseStrV1(tvmKeysResponse))) {
            ythrow TMalformedTvmKeysException() << "Malformed TVM keys";
        }

        NRw::TPublicKeys keys;
        for (int idx = 0; idx < protoKeys.tvm_size(); ++idx) {
            const tvm_keys::TvmKey& k = protoKeys.tvm(idx);
            keys.emplace(k.gen().id(),
                         k.gen().body());
        }

        if (keys.empty()) {
            ythrow TEmptyTvmKeysException() << "Empty TVM keys";
        }

        Keys_ = std::move(keys);
    }

    TServiceTicketImplPtr TServiceContext::TImpl::Check(TStringBuf ticketBody) const {
        if (Keys_.empty()) {
            ythrow TEmptyTvmKeysException() << "Empty TVM keys";
        }

        TParserTickets::TRes res = TParserTickets::ParseV3(ticketBody, Keys_, TParserTickets::ServiceFlag());
        if (res.Status != ETicketStatus::Ok) {
            return MakeHolder<TCheckedServiceTicket::TImpl>(res.Status, std::move(res.Ticket));
        }

        const ETicketStatus status = CheckProtobufServiceTicket(res.Ticket);
        return MakeHolder<TCheckedServiceTicket::TImpl>(status, std::move(res.Ticket));
    }

    TString TServiceContext::TImpl::SignCgiParamsForTvm(TStringBuf ts, TStringBuf dst, TStringBuf scopes) const {
        if (Secret_.Value().empty()) {
            ythrow TMalformedTvmSecretException() << "Malformed TVM secret: it is empty";
        }
        return NUtils::SignCgiParamsForTvm(Secret_, ts, dst, scopes);
    }

    ETicketStatus TServiceContext::TImpl::CheckProtobufServiceTicket(const ticket2::Ticket& ticket) const {
        if (!ticket.has_service()) {
            return ETicketStatus::Malformed;
        }
        if (ticket.service().dstclientid() != SelfTvmId_) {
            return ETicketStatus::InvalidDst;
        }
        return ETicketStatus::Ok;
    }

    TString TServiceContext::TImpl::ParseSecret(TStringBuf secretBase64) {
        while (secretBase64 && secretBase64.back() == '\n') {
            secretBase64.Chop(1);
        }

        if (secretBase64.empty()) {
            ythrow TMalformedTvmSecretException() << "Malformed TVM secret: it is empty";
        }

        const TString secret = NUtils::Base64url2bin(secretBase64);
        if (secret.empty()) {
            ythrow TMalformedTvmSecretException() << "Malformed TVM secret: invalid base64url";
        }

        return secret;
    }

}
