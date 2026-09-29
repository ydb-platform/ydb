#pragma once

#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <util/generic/string.h>

namespace NKikimr::NSchemeShard {

struct TExportTableSchemeContext {
    TString SourcePath; // Absolute path used by auxiliary ALTER statements
    TString TablePath; // Database-relative path used by CREATE TABLE and CDC statements
    NKikimrSchemeOp::TPathDescription PathDescription;
    NKikimrSchemeOp::TChangefeedUnderlyingTopics ChangefeedUnderlyingTopics;
};

} // namespace NKikimr::NSchemeShard
