#include "security.h"

TStringBuf NKikimr::NSecurity::DefaultUserIp() {
    return "2a02:6b8:0:80b::5f6c:8ead";
}

TString NKikimr::NSecurity::BlackBoxTokenFromSessionId(TStringBuf sessionId, TStringBuf userIp) {
    return TString("blackbox?method=sessionid&userip=") + userIp + "&host=yandex-team.ru&sessionid=" + sessionId;
}
