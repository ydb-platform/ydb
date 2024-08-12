#pragma once

#include <util/generic/string.h>
#include <util/generic/strbuf.h>

namespace NKikimr {
namespace NSecurity {

TStringBuf DefaultUserIp();
TString BlackBoxTokenFromSessionId(TStringBuf sessionId, TStringBuf userIp = DefaultUserIp());

}
}
