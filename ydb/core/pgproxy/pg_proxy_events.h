#pragma once

#include <library/cpp/actors/core/events.h>
#include <library/cpp/actors/core/event_local.h>
#include <ydb/core/raw_socket/sock_config.h>
#include "pg_proxy_types.h"

namespace NPG {

using namespace NKikimr::NRawSocket;

struct TEvPGEvents {
    enum EEv {
        EvConnectionOpened = EventSpaceBegin(NActors::TEvents::ES_PGWIRE),
        EvConnectionClosed,
        EvAuth,
        EvAuthResponse,
        EvQuery,
        EvQueryResponse,
        EvParse,
        EvParseResponse,
        EvBind,
        EvBindResponse,
        EvDescribe,
        EvDescribeResponse,
        EvExecute,
        EvExecuteResponse,
        EvClose,
        EvCloseResponse,
        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PGWIRE), "ES_PGWIRE event space is too small.");

    struct TRowDescriptionField {
        TString Name;
        uint32_t TableId = 0;
        uint16_t ColumnId = 0;
        uint32_t DataType;
        int16_t DataTypeSize;
        int32_t DataTypeModifier;
        int16_t Format = 0; // 0 = text, 1 = binary
    };

    struct TRowValueField {
        std::optional<std::variant<TString, std::vector<uint8_t>>> Value;
    };

    using TDataRow = std::vector<TRowValueField>;

    struct TEvConnectionOpened : NActors::TEventLocal<TEvConnectionOpened, EvConnectionOpened> {
        std::shared_ptr<TPGInitial> Message;
        TNetworkConfig::TSocketAddressType Address;

        TEvConnectionOpened(std::shared_ptr<TPGInitial> message, TNetworkConfig::TSocketAddressType address)
            : Message(std::move(message))
            , Address(address)
        {}
    };

    struct TEvConnectionClosed : NActors::TEventLocal<TEvConnectionClosed, EvConnectionClosed> {
    };

    struct TEvAuth : NActors::TEventLocal<TEvAuth, EvAuth> {
        std::shared_ptr<TPGInitial> InitialMessage;
        TNetworkConfig::TSocketAddressType Address;
        std::unique_ptr<TPGPasswordMessage> PasswordMessage;

        TEvAuth(std::shared_ptr<TPGInitial> initialMessage, TNetworkConfig::TSocketAddressType address)
            : InitialMessage(std::move(initialMessage))
            , Address(address)
        {}

        TEvAuth(std::shared_ptr<TPGInitial> initialMessage, TNetworkConfig::TSocketAddressType address, std::unique_ptr<TPGPasswordMessage> message)
            : InitialMessage(std::move(initialMessage))
            , Address(address)
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
        char TransactionStatus;

        TEvQuery(std::unique_ptr<TPGQuery> message, char transactionStatus)
            : Message(std::move(message))
            , TransactionStatus(transactionStatus)
        {}
    };

    struct TEvQueryResponse : NActors::TEventLocal<TEvQueryResponse, EvQueryResponse> {
        std::vector<TRowDescriptionField> DataFields;
        std::vector<TDataRow> DataRows;
        std::vector<std::pair<char, TString>> ErrorFields;
        TString Tag;
        bool EmptyQuery = false;
        bool CommandCompleted = true;
        char TransactionStatus = 0;
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

    struct TEvParseResponse : NActors::TEventLocal<TEvParseResponse, EvParseResponse> {
        std::unique_ptr<TPGParse> OriginalMessage;
        std::vector<std::pair<char, TString>> ErrorFields;

        TEvParseResponse(std::unique_ptr<TPGParse> originalMessage)
            : OriginalMessage(std::move(originalMessage))
        {}
    };

    struct TEvParse : NActors::TEventLocal<TEvParse, EvParse> {
        std::unique_ptr<TPGParse> Message;

        TEvParse(std::unique_ptr<TPGParse> message)
            : Message(std::move(message))
        {}

        std::unique_ptr<TEvParseResponse> Reply() {
            return std::make_unique<TEvParseResponse>(std::move(Message));
        }
    };

    struct TEvBindResponse : NActors::TEventLocal<TEvBindResponse, EvBindResponse> {
        std::unique_ptr<TPGBind> OriginalMessage;

        TEvBindResponse(std::unique_ptr<TPGBind> originalMessage)
            : OriginalMessage(std::move(originalMessage))
        {}
    };

    struct TEvBind : NActors::TEventLocal<TEvBind, EvBind> {
        std::unique_ptr<TPGBind> Message;

        TEvBind(std::unique_ptr<TPGBind> message)
            : Message(std::move(message))
        {}

        std::unique_ptr<TEvBindResponse> Reply() {
            return std::make_unique<TEvBindResponse>(std::move(Message));
        }
    };

    struct TEvDescribe : NActors::TEventLocal<TEvDescribe, EvDescribe> {
        std::unique_ptr<TPGDescribe> Message;

        TEvDescribe(std::unique_ptr<TPGDescribe> message)
            : Message(std::move(message))
        {}
    };

    struct TEvDescribeResponse : NActors::TEventLocal<TEvDescribeResponse, EvDescribeResponse> {
        std::vector<TRowDescriptionField> DataFields;
        std::vector<uint32_t> ParameterTypes;
        std::vector<std::pair<char, TString>> ErrorFields;
    };

    struct TEvExecute : NActors::TEventLocal<TEvExecute, EvExecute> {
        std::unique_ptr<TPGExecute> Message;
        char TransactionStatus;

        TEvExecute(std::unique_ptr<TPGExecute> message, char transactionStatus)
            : Message(std::move(message))
            , TransactionStatus(transactionStatus)
        {}
    };

    struct TEvExecuteResponse : NActors::TEventLocal<TEvExecuteResponse, EvExecuteResponse> {
        std::vector<TDataRow> DataRows;
        std::vector<std::pair<char, TString>> ErrorFields;
        TString Tag;
        bool EmptyQuery = false;
        bool CommandCompleted = true;
        char TransactionStatus = 0;
    };

    struct TEvCloseResponse : NActors::TEventLocal<TEvCloseResponse, EvCloseResponse> {
        std::unique_ptr<TPGClose> OriginalMessage;

        TEvCloseResponse(std::unique_ptr<TPGClose> originalMessage)
            : OriginalMessage(std::move(originalMessage))
        {}
    };

    struct TEvClose : NActors::TEventLocal<TEvClose, EvClose> {
        std::unique_ptr<TPGClose> Message;

        TEvClose(std::unique_ptr<TPGClose> message)
            : Message(std::move(message))
        {}

        std::unique_ptr<TEvCloseResponse> Reply() {
            return std::make_unique<TEvCloseResponse>(std::move(Message));
        }
    };
};

}