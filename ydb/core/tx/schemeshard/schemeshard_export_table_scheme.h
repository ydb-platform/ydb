#pragma once

#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <util/generic/string.h>

namespace NKikimr::NSchemeShard {

struct TExportTableSchemeContext {
    TString SourcePath;
    NKikimrSchemeOp::TPathDescription PathDescription;
    NKikimrSchemeOp::TChangefeedUnderlyingTopics ChangefeedUnderlyingTopics;
};

} // namespace NKikimr::NSchemeShard
