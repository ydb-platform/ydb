#pragma once

#include <util/generic/string.h>

namespace NYql {

TString NormalizeS3Path(const TString& path, char slash = '/');

}
