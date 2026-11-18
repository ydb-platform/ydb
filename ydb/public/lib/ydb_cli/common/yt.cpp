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
    TStringBuf address = proxy;
    TStringBuf scheme;
    ui16 defaultPort = DefaultYtPort;

    if (address.SkipPrefix("https://")) {
        scheme = "https://";
        defaultPort = 443;
    } else if (address.SkipPrefix("http://")) {
        scheme = "http://";
    }

    try {
        Split(address, ':', YtHost, port);
    } catch (const yexception& ex) {
        throw TMisuseException() << "Bad YT proxy format: \"" << proxy << "\".";
    }

    if (YtHost.empty() || YtHost.find_first_of("/?#@") != TString::npos) {
        throw TMisuseException() << "Bad YT proxy format: \"" << proxy << "\".";
    }

    if (YtHost.find('.') == TString::npos && YtHost.find("localhost") == TString::npos) {
        YtHost.append(".yt.yandex.net");
    }

    YtHost.prepend(scheme);
    YtPort = port.GetOrElse(defaultPort);
}

const TString TCommandWithYtToken::YtTokenFile = "~/.yt/token";

void TCommandWithYtToken::ReadYtToken() {
    TString tokenFile = YtTokenFile;
    YtToken = ReadFromFile(tokenFile, "YT token");
}

}
}
