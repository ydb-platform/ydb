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
        if (path.StartsWith('/')) {
            if (!path.StartsWith(config.Database)) {
                throw TMisuseException() << "Provided path \"" << path << "\" starts with '/'. "
                    << "That means you are using an absolute path that should start with the path "
                    << "to your database \"" << config.Database << "\", but it doesn't. " << Endl
                    << "Please, provide full path starting from the domain root "
                    << "(example: \"/domain/my_base/dir1/table1\"). " << Endl
                    << "Or consider using relative path from your database (example: \"dir1/table1\").";
            }
        } else {
            // allow relative path
            path = (config.Path ? config.Path : config.Database) + '/' + path;
        }

        path = NormalizePath(path);
    }

}
}
