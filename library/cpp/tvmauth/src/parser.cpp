#include "parser.h"

#include "utils.h"

#include <library/cpp/tvmauth/exception.h> 

#include <util/generic/strbuf.h>
#include <util/string/split.h>

#include <ctime>

namespace NTvmAuth { 
    TString TParserTvmKeys::ParseStrV1(TStringBuf str) {
        while (str && str.back() == '\n') {
            str.Chop(1);
        }

        TStringBuf ver = str.NextTok(DELIM); 
        if (!str || !ver || ver != "1") {
            throw TMalformedTvmKeysException() << "Malformed TVM keys"; 
        }
        TString res = NUtils::Base64url2bin(str);
        if (res.empty()) {
            throw TMalformedTvmKeysException() << "Malformed TVM keys"; 
        }
        return res;
    }

    TStringBuf TParserTickets::UserFlag() {
        static const char BUF_[] = "user";
        return TStringBuf(BUF_, sizeof(BUF_) - 1);
    }

    TStringBuf TParserTickets::ServiceFlag() {
        static const char BUF_[] = "serv";
        return TStringBuf(BUF_, sizeof(BUF_) - 1);
    }

    TParserTickets::TRes TParserTickets::ParseV3(TStringBuf body, const NRw::TPublicKeys& keys, TStringBuf type) {
        TStrRes str = ParseStrV3(body, type);
        TRes res(str.Status);
        if (str.Status != ETicketStatus::Ok) { 
            return TRes(str.Status);
        }
        if (!res.Ticket.ParseFromString(str.Proto)) {
            res.Status = ETicketStatus::Malformed; 
            return res;
        }
        if (res.Ticket.expirationtime() <= time(nullptr)) {
            res.Status = ETicketStatus::Expired; 
            return res;
        }

        auto itKey = keys.find(res.Ticket.keyid());
        if (itKey == keys.end()) {
            res.Status = ETicketStatus::MissingKey; 
            return res;
        }
        if (!itKey->second.CheckSign(str.ForCheck, str.Sign)) {
            res.Status = ETicketStatus::SignBroken; 
            return res;
        }
        return res;
    }

    TParserTickets::TStrRes TParserTickets::ParseStrV3(TStringBuf body, TStringBuf type) {
        TStringBuf forCheck = body;
        TStringBuf version = body.NextTok(DELIM);
        if (!body || version.size() != 1) { 
            return {ETicketStatus::Malformed, {}, {}, {}}; 
        } 
        if (version != "3") {
            return {ETicketStatus::UnsupportedVersion, {}, {}, {}}; 
        }

        TStringBuf ticketType = body.NextTok(DELIM);
        if (ticketType != type) {
            return {ETicketStatus::InvalidTicketType, {}, {}, {}}; 
        }

        TStringBuf proto = body.NextTok(DELIM);
        TStringBuf sign = body.NextTok(DELIM);

        if (!proto || !sign || body.size() > 0) {
            return {ETicketStatus::Malformed, {}, {}, {}}; 
        }

        TString protoBin = NUtils::Base64url2bin(proto);
        TString signBin = NUtils::Base64url2bin(sign);

        if (!protoBin || !signBin) {
            return {ETicketStatus::Malformed, {}, {}, {}}; 
        }

        return {ETicketStatus::Ok, std::move(protoBin), std::move(signBin), forCheck.Chop(sign.size())}; 
    }
} 
