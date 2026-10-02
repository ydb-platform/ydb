#include "fs_utils.h"
#include <library/cpp/json/json_writer.h>

int NKikimr::NMiniKQL::FilesIn(std::filesystem::path path) {
    using std::filesystem::directory_iterator;
    std::filesystem::create_directories(path);
    return std::distance(directory_iterator(path), directory_iterator{});
}

void NKikimr::NMiniKQL::SaveJsonAt(NJson::TJsonValue value, TFixedBufferFileOutput* jsonlSaveFile) {

    Cout << NJson::WriteJson(value, false, false, false) << Endl;
    if (jsonlSaveFile != nullptr) {
        *jsonlSaveFile << NJson::WriteJson(value, false, false, false) << Endl;
    }
}