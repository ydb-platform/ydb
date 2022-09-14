#pragma once

#include <util/generic/string.h>

namespace NYql::NDq {

TString ParseS3ErrorResponse(long httpCode, const TString& response);

}
