#include "yt.h"

#include <util/folder/dirut.h>
#include <util/folder/path.h>
#include <util/generic/maybe.h>
#include <util/stream/file.h>
#include <util/string/split.h>
#include <util/string/strip.h>

namespace NYdb {
namespace NConsoleClient {

void TCommandWithYtProxy::ParseYtProxy(const TString& proxy) {
    if (proxy.empty()) {
        throw TMisuseException() << "No YT proxy provided.";
    }

    TMaybe<ui16> port;

    try {
        Split(proxy, ':', YtHost, port);
    } catch (const yexception& ex) {
        throw TMisuseException() << "Bad YT proxy format: \"" << proxy << "\".";
    }

    if (YtHost.find('.') == TString::npos && YtHost.find("localhost") == TString::npos) {
        YtHost.append(".yt.yandex.net");
    }

    YtPort = port.GetOrElse(DefaultYtPort);
}

const TString TCommandWithYtToken::YtTokenFile = "~/.yt/token";

void TCommandWithYtToken::ReadYtToken() {
    TString tokenFile = YtTokenFile;
    YtToken = ReadFromFile(tokenFile, "YT token");
}

}
}
