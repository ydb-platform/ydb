#include "normalize_path.h"
#include "scoped_driver.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/scheme/scheme.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/status/status.h>

namespace NYdb {
namespace NConsoleClient {

    TString NormalizePath(const TString &path) {
        TString result;
        int state = 0;
        result.reserve(path.size() + 1);
        for (char c : path) {
            switch (state) {
                case 0: { // default
                    if (c == '/') {
                        state = 1;
                    } else {
                        result += c;
                    }
                    break;
                }
                case 1: {  // last seen characters: "/"
                    if (c == '.') {
                        state = 2;
                    } else if (c == '/') {
                        state = 1;
                    } else {
                        state = 0;
                        result += '/';
                        result += c;
                    }
                    break;
                }
                case 2: { // // last seen characters: "/."
                    if (c == '/') {
                        state = 1;
                    } else {
                        state = 0;
                        result += "/.";
                        result += c;
                    }
                    break;
                }
            }
        }
        return result;
    }

    void AdjustPath(TString& path, const TClientCommand::TConfig& config) {
        const auto& base = config.Path ? config.Path : config.Database;
        if (!path.StartsWith('/') && (config.Path || base.StartsWith('/'))) {
            path = base + '/' + path;
        }

        // Retain the existing CLI normalization without making a relative path absolute.
        if (path != "/") {
            const bool relative = !path.StartsWith('/');
            path = NormalizePath(relative ? "/" + path : path);
            if (relative && !path.empty()) {
                path.erase(0, 1);
            }
        }
    }

    void AdjustPathToDatabase(TString& path, TClientCommand::TConfig& config) {
        AdjustPath(path, config);
        if (path.empty() && !config.Database.empty() && !config.Database.StartsWith('/')) {
            TScopedDriver driver{TDriver(config.CreateDriverConfigWithBuildInfo())};
            NScheme::TSchemeClient client(driver);
            auto root = client.ListDirectory("/").GetValueSync();
            NStatusHelpers::ThrowOnErrorOrPrintIssues(root);
            Y_ENSURE(root.GetChildren().size() == 1, "Exactly one cluster root expected");
            path = "/" + root.GetChildren().front().Name + "/" + config.Database;
        }
    }

}
}
