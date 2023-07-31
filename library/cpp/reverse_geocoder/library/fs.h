#pragma once

#include <util/folder/iterator.h>
#include <util/string/vector.h>

namespace NReverseGeocoder {
    template <typename Cont>
    Cont GetFileListInDirectory(const char* dirName) {
        TDirIterator dirIt(dirName, TDirIterator::TOptions(FTS_LOGICAL));
        Cont dirContent;
        for (auto file = dirIt.begin(); file != dirIt.end(); ++file) {
            if (strcmp(file->fts_path, dirName))
                dirContent.push_back(file->fts_path);
        }
        return dirContent;
    }

    TVector<TString> GetDataFilesList(const char* input);
}
