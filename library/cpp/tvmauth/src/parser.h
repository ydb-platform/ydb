#pragma once

#include <library/cpp/tvmauth/src/protos/ticket2.pb.h>
#include <library/cpp/tvmauth/src/rw/keys.h>

#include <library/cpp/tvmauth/ticket_status.h>

#include <util/generic/fwd.h>

#include <string>

namespace NTvmAuth {
    struct TParserTvmKeys {
        static inline const char DELIM = ':';
        static TString ParseStrV1(TStringBuf str);
    };

    struct TParserTickets {
        static const char DELIM = ':';

        static TStringBuf UserFlag();
        static TStringBuf ServiceFlag();

        struct TRes {
            TRes(ETicketStatus status)
                : Status(status)
            {
            }

            ETicketStatus Status;

            ticket2::Ticket Ticket;
        };
        static TRes ParseV3(TStringBuf body, const NRw::TPublicKeys& keys, TStringBuf type);

        // private:
        struct TStrRes {
            const ETicketStatus Status;

            TString Proto;
            TString Sign;

            TStringBuf ForCheck;

            bool operator==(const TStrRes& o) const { // for tests
                return Status == o.Status && Proto == o.Proto && Sign == o.Sign && ForCheck == o.ForCheck;
            }
        };
        static TStrRes ParseStrV3(TStringBuf body, TStringBuf type);
    };
}
