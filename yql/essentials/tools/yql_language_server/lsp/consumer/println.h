#pragma once

#include "base.h"

#include <util/stream/output.h>

namespace NLsp {

IConsumer<TString>::TPtr LinePrinting(IOutputStream& out, IConsumer<TString>::TPtr consumer);

} // namespace NLsp
