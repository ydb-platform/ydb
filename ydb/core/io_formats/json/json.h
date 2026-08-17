#pragma once

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>

namespace NKikimr::NFormats {

NJson::TJsonWriterConfig DefaultJsonWriterConfig();
NJson::TJsonReaderConfig DefaultJsonReaderConfig();

} // namespace NKikimr::NFormats
