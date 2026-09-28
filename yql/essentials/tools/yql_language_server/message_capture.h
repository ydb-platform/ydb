#pragma once

#include <util/stream/output.h>
#include <util/folder/path.h>

namespace NLsp::NYql {

std::pair<IOutputStream*, THolder<IOutputStream>> OpenMessageCapture(TMaybe<TFsPath> path);

} // namespace NLsp::NYql
