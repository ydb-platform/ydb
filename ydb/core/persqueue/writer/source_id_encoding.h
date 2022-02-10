#pragma once

#include <util/generic/fwd.h>

namespace NKikimr {
namespace NPQ { 
namespace NSourceIdEncoding { 

TString EncodeSimple(const TString& sourceId);
TString Encode(const TString& sourceId);

TString Decode(const TString& encodedSourceId);

bool IsValidEncoded(const TString& encodedSourceId);

} // NSourceIdEncoding 
} // NPQ 
} // NKikimr 
