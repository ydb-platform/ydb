#pragma once

#include <ydb/core/scheme/scheme_tabledefs.h>

namespace NKikimr::NChangeExchange {

ui64 ResolveSchemaBoundaryPartitionId(const NKikimr::TKeyDesc& keyDesc, TConstArrayRef<TCell> key);

}
