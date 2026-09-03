#pragma once

#include "formatters_common.h"

#include <ydb/core/protos/flat_scheme_op.pb.h>

namespace NKikimr::NSysView {

class TCreateExternalTableFormatter {
public:
    TFormatResult Format(
        const TString& tablePath,
        const NKikimrSchemeOp::TExternalTableDescription& tableDesc,
        const NKikimrSchemeOp::TDirEntry& dirEntry);
};

}
