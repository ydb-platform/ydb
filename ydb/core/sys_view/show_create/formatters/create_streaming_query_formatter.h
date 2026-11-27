#pragma once

#include <util/generic/string.h>

namespace NKikimrSchemeOp {
class TStreamingQueryDescription;
}

namespace NKikimr::NSysView {

TString FormatCreateStreamingQuery(const TString& path, const NKikimrSchemeOp::TStreamingQueryDescription& description);

}   // namespace NKikimr::NSysView
