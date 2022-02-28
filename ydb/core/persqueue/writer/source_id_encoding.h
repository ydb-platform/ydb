#pragma once

#include <util/generic/fwd.h>
#include <util/generic/string.h>

namespace NKikimr {
namespace NPQ {

TString GetSourceIdSelectQuery(const TString& root);
TString GetUpdateIdSelectQuery(const TString& root);

TString GetSourceIdSelectQueryFromPath(const TString& path);
TString GetUpdateIdSelectQueryFromPath(const TString& path);


namespace NSourceIdEncoding {

TString EncodeSimple(const TString& sourceId);
TString Encode(const TString& sourceId);

TString Decode(const TString& encodedSourceId);

bool IsValidEncoded(const TString& encodedSourceId);


struct TEncodedSourceId {
    TString EscapedSourceId;
    ui32 Hash = 0;
};

TEncodedSourceId EncodeSrcId(const TString& topic, const TString& userSourceId);

} // NSourceIdEncoding
} // NPQ
} // NKikimr
