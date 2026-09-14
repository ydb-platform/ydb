#include "normalize_path.h"

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
        if (!path.StartsWith('/') && config.Path) {
            path = config.Path + '/' + path;
        }

        // The server knows the cluster root and resolves database-relative paths.
        path = NormalizePath(path);
        while (path.StartsWith("./")) {
            path = path.substr(2);
        }
    }

}
}
