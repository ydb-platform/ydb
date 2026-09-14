#pragma once

#include <ydb/library/formats/arrow/protos/ssa.pb.h>

namespace NKikimr::NArrow::NPrinter {

TString SSAToPrettyString(const NKikimrSSA::TProgram& program);

}   // namespace NKikimr::NArrow::NPrinter
