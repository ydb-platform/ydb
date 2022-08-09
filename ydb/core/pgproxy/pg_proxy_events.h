#pragma once

#include <library/cpp/actors/core/events.h>
#include <library/cpp/actors/core/event_local.h>
#include "pg_proxy_types.h"

namespace NPG {

struct TEvPGEvents {
    enum EEv {
        EvConnectionOpened = EventSpaceBegin(NActors::TEvents::ES_PGWIRE),
        EvConnectionClosed,
        EvAuthRequest,
        EvAuthResponse,
        EvQuery,
        EvRowDescription,
        EvDataRows,
        EvCommandComplete,
        EvErrorResponse,
        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PGWIRE), "ES_PGWIRE event space is too small.");

    struct TEvConnectionOpened : NActors::TEventLocal<TEvConnectionOpened, EvConnectionOpened> {
        std::shared_ptr<TPGInitial> Message;

        TEvConnectionOpened(std::shared_ptr<TPGInitial> message)
            : Message(std::move(message))
        {}
    };

    struct TEvConnectionClosed : NActors::TEventLocal<TEvConnectionClosed, EvConnectionClosed> {
    };

    struct TEvAuthRequest : NActors::TEventLocal<TEvAuthRequest, EvAuthRequest> {
        std::shared_ptr<TPGInitial> InitialMessage;
        std::unique_ptr<TPGPasswordMessage> PasswordMessage;

        TEvAuthRequest(std::shared_ptr<TPGInitial> initialMessage)
            : InitialMessage(std::move(initialMessage))
        {}

        TEvAuthRequest(std::shared_ptr<TPGInitial> initialMessage, std::unique_ptr<TPGPasswordMessage> message)
            : InitialMessage(std::move(initialMessage))
            , PasswordMessage(std::move(message))
        {}
    };

    struct TEvAuthResponse : NActors::TEventLocal<TEvAuthResponse, EvAuthResponse> {
        TString Error;
    };

    // -> TEvConnectionOpened
    // -> TEvQuery
    // <- TEvRowDescription
    // <- TEvDataRows
    // <- ...
    // <- TEvCommandComplete

    // or

    // -> TEvConnectionOpened
    // -> TEvQuery
    // <- TEvErrorResponse

    struct TEvQuery : NActors::TEventLocal<TEvQuery, EvQuery> {
        std::unique_ptr<TPGQuery> Message;

        TEvQuery(std::unique_ptr<TPGQuery> message)
            : Message(std::move(message))
        {}
    };

    struct TEvRowDescription : NActors::TEventLocal<TEvRowDescription, EvRowDescription> {
        struct TField {
            TString Name;
            uint32_t TableId = 0;
            uint16_t ColumnId = 0;
            uint32_t DataType;
            uint16_t DataTypeSize;
            //uint32_t DataTypeModifier;
            //uint16_t Format;
        };
        std::vector<TField> Fields;
    };

    struct TEvDataRows : NActors::TEventLocal<TEvDataRows, EvDataRows> {
        using TDataRow = std::vector<TString>;
        std::vector<TDataRow> Rows;
    };

    struct TEvCommandComplete : NActors::TEventLocal<TEvCommandComplete, EvCommandComplete> {
        TString Tag;

        TEvCommandComplete(const TString& tag)
            : Tag(tag)
        {}
    };

        /*
        0x0040:  7236 c5c6 7236 c5c6 4500 0000 6053 4552  r6..r6..E...`SER
        0x0050:  524f 5200 5645 5252 4f52 0043 3432 3630  ROR.VERROR.C4260
        0x0060:  3100 4d73 796e 7461 7820 6572 726f 7220  1.Msyntax.error.
        0x0070:  6174 206f 7220 6e65 6172 2022 7365 6c65  at.or.near."sele
        0x0080:  6565 6666 2200 5031 0046 7363 616e 2e6c  eeff".P1.Fscan.l
        0x0090:  004c 3131 3435 0052 7363 616e 6e65 725f  .L1145.Rscanner_
        0x00a0:  7979 6572 726f 7200 00                   yyerror..
        */

        /*
        alexey=# seleeeff;
        ERROR:  syntax error at or near "seleeeff"
        LINE 1: seleeeff;
                ^
        alexey=#
        */

        /*
        S = "ERROR"
        V = "ERROR"
        C = "42601"
        M = "syntax error at or near \"seleeef\""
        P = "1"
        F = "scan.l"
        L = "1145"
        R = "scanner_yyerror"
        */

    struct TEvErrorResponse : NActors::TEventLocal<TEvErrorResponse, EvErrorResponse> {
        std::vector<std::pair<char, TString>> ErrorFields;
    };
};

}