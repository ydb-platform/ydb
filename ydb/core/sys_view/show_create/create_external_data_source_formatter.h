#pragma once

#include "formatters_common.h"

#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <util/stream/str.h>

namespace NKikimr::NSysView {

class TCreateExternalDataSourceFormatter {
public:
    TFormatResult Format(
        const TString& dataSourcePath,
        const NKikimrSchemeOp::TExternalDataSourceDescription& dataSourceDesc,
        const NKikimrSchemeOp::TDirEntry& dirEntry);

private:
    TStringStream Stream;
};

}
