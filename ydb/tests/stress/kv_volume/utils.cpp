#include "utils.h"

#include <util/string/builder.h>

namespace NKvVolumeStress {

namespace {

constexpr ui16 DefaultKvPort = 2135;
const TString DataPattern = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";

} // namespace

std::mt19937_64& RandomEngine() {
    thread_local std::mt19937_64 engine(std::random_device{}());
    return engine;
}

TString MakeVolumePath(const TString& database, const TString& path) {
    if (database.empty()) {
        return path;
    }
    if (database.back() == '/') {
        return database + path;
    }
    return database + "/" + path;
}

TString ParseHostPort(const TString& endpoint) {
    TString hostPort = endpoint;
    const TString scheme = "://";
    const size_t pos = hostPort.find(scheme);
    if (pos != TString::npos) {
        hostPort = hostPort.substr(pos + scheme.size());
    }

    if (hostPort.empty()) {
        return TStringBuilder() << "localhost:" << DefaultKvPort;
    }

    if (hostPort.find(':') == TString::npos) {
        hostPort += TStringBuilder() << ":" << DefaultKvPort;
    }

    return hostPort;
}

TString GeneratePatternData(ui32 size) {
    if (!size) {
        return {};
    }

    const size_t repeats = (size + DataPattern.size() - 1) / DataPattern.size();
    TString result;
    result.reserve(repeats * DataPattern.size());
    for (size_t i = 0; i < repeats; ++i) {
        result += DataPattern;
    }
    result.resize(size);
    return result;
}

} // namespace NKvVolumeStress
