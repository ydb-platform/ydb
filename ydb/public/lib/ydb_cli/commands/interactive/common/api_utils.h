#pragma once

#include <util/generic/fwd.h>

namespace NYdb::NConsoleClient::NAi {

TString CreateApiUrl(const TString& baseUrl, const TString& uri);

} // namespace NYdb::NConsoleClient::NAi
