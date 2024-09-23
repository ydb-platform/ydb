#include "security.h"

TStringBuf NKikimr::NSecurity::DefaultUserIp() {
    return "";
}

TString NKikimr::NSecurity::BlackBoxTokenFromSessionId(TStringBuf sessionId, TStringBuf userIp) {
    return TString("blackbox?method=sessionid&userip=") + userIp + "&host=yandex-team.ru&sessionid=" + sessionId;
}
