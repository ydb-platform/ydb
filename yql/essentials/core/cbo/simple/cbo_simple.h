#pragma once

#include <yql/essentials/core/cbo/cbo_optimizer_new.h>

namespace NYql {

IOptimizerFactory::TPtr MakeSimpleCBOOptimizerFactory();

} // namespace NYql
