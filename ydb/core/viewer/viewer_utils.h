#pragma once

#include <library/cpp/cgiparam/cgiparam.h>

#include <util/generic/strbuf.h>
#include <util/generic/string.h>

namespace NKikimr::NViewer {

TString GetDatabaseParam(const TCgiParameters& params, const TStringBuf& method, const TStringBuf& body);

} // namespace NKikimr::NViewer
