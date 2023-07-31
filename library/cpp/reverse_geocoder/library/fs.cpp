#include "fs.h"

#include <util/folder/dirut.h>
#include <util/string/split.h>

namespace NReverseGeocoder {
    TVector<TString> GetDataFilesList(const char* input) {
        if (IsDir(input)) {
            return GetFileListInDirectory<TVector<TString>>(input);
        }

        TVector<TString> result;
        for (const auto& partIt : StringSplitter(input).Split(',')) {
            result.push_back(TString(partIt.Token()));
        }
        return result;
    }
}
