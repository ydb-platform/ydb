#pragma once

#include "program.h"

#include <ydb/core/protos/ssa.pb.h>
#include <ydb/core/tablet_flat/flat_dbase_scheme.h>

namespace NKikimr::NSsaOptimizer {

std::vector<std::shared_ptr<NArrow::TProgramStep>> OptimizeProgram(const std::vector<std::shared_ptr<NArrow::TProgramStep>>& program, const NTable::TScheme::TTableSchema& tableSchema);

}
