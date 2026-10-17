#pragma once

#include <util/generic/array_ref.h>
#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NKikimr::NSql {

constexpr ui32 ProtocolVersion3 = 196608;
constexpr ui32 SslRequestCode = 80877103;
constexpr ui32 CancelRequestCode = 80877102;

struct TStartupMessage {
    bool SslRequest = false;
    bool CancelRequest = false;
    ui32 Protocol = 0;
    THashMap<TString, TString> Parameters;
};

struct TClientMessage {
    char Type = 0;
    TString Payload;
};

// Consumes one startup message from the front of buffer.
// Returns false when more bytes are required. On a malformed message, error is set.
bool TryParseStartup(TString& buffer, TStartupMessage& message, TString& error);
bool TryParseMessage(TString& buffer, TClientMessage& message, TString& error);

TString MakeAuthenticationOk();
TString MakeParameterStatus(TStringBuf name, TStringBuf value);
TString MakeBackendKeyData(ui32 backendPid, ui32 secret);
TString MakeReadyForQuery();
TString MakeErrorResponse(TStringBuf sqlState, TStringBuf message);
TString MakeHandshake(ui32 backendPid, ui32 secret);

struct TResultColumn {
    TString Name;
    ui32 TypeOid = 25;
    i16 TypeSize = -1;
};

// Text-format result: RowDescription, DataRow per row, CommandComplete, ReadyForQuery.
TString MakeTextResult(TArrayRef<const TResultColumn> columns, TArrayRef<const TVector<TString>> rows);

} // namespace NKikimr::NSql
