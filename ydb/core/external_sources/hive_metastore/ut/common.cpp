#include <ydb/core/external_sources/hive_metastore/hive_metastore_client.h>

#include <library/cpp/testing/common/env.h>
#include <util/string/split.h>
#include <util/string/strip.h>

namespace NKikimr::NExternalSource {

namespace {

TString Exec(const TString& cmd) {
    std::array<char, 128> buffer;
    TString result;
    std::unique_ptr<FILE, decltype(&pclose)> pipe(popen(cmd.c_str(), "r"), pclose);
    if (!pipe) {
        throw std::runtime_error("popen() failed!");
    }
    while (fgets(buffer.data(), static_cast<int>(buffer.size()), pipe.get()) != nullptr) {
        result += buffer.data();
    }
    return result;
}

}

TString GetExternalPort(const TString& service, const TString& port) {
    auto dockerComposeBin = BinaryPath("library/recipes/docker_compose/bin/docker-compose");
    auto composeFileYml = ArcadiaFromCurrentLocation(__SOURCE_FILE__, "docker-compose.yml");
    auto result = StringSplitter(Exec(dockerComposeBin + " -f " + composeFileYml + " port " + service + " " + port)).Split(':').ToList<TString>();
    return result ? Strip(result.back()) : TString{};
}

void WaitHiveMetastore(const TString& host, int32_t port, const TString& database) {
    NKikimr::NExternalSource::THiveMetastoreClient client(host, port);
    for (int i = 0; i < 60; i++) {
        try {
            client.GetDatabase(database).GetValue(TDuration::Seconds(10));
            return;
        } catch (...) {
            Sleep(TDuration::Seconds(1));
        }
    }
    ythrow yexception() << "Hive metastore isn't ready, host: " << host << " port: " << port;
}

}
