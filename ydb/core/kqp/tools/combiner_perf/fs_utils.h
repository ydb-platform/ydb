#pragma once
#include <library/cpp/json/writer/json_value.h>
#include <filesystem>
#include <util/generic/buffer.h>
#include <util/stream/file.h>

namespace NKikimr::NMiniKQL {

int FilesIn(std::filesystem::path path);

void SaveJsonAt(NJson::TJsonValue value, TFixedBufferFileOutput* jsonlSaveFile);

} // namespace NKikimr::NMiniKQL
