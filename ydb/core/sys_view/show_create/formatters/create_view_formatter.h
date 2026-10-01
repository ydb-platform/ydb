#pragma once

#include "formatters_common.h"

#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <util/stream/str.h>

namespace NKikimr::NSysView {

class TCreateViewFormatter {
public:
    TFormatResult Format(const TString& viewRelativePath, const TString& viewAbsolutePath, const NKikimrSchemeOp::TViewDescription& viewDesc);

private:
    TStringStream Stream;
};

}
