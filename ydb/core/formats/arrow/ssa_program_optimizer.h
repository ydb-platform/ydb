#pragma once

#include "program.h"

#include <ydb/core/protos/ssa.pb.h>
#include <ydb/core/tablet_flat/flat_dbase_scheme.h>

namespace NKikimr::NSsa {

void OptimizeProgram(TProgram& program);

}
