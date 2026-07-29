#pragma once

#include <util/generic/strbuf.h>
#include <util/generic/string.h>

class TCgiParameters;

namespace NKikimr::NViewer {

TString GetDatabaseParam(const TCgiParameters& params, const TStringBuf method, const TStringBuf body);

} // namespace NKikimr::NViewer
