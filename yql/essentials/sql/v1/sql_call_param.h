#pragma once

#include <util/system/types.h>

namespace NSQLTranslationV1 {

///////////////////////////////////////////////////////////////////////////////////////////////

enum class ESqlCallParam: ui32 {
    InputType       /* "INPUT_TYPE" */, // as is
    OutputType      /* "OUTPUT_TYPE" */, // as is
    Concurrency     /* "CONCURRENCY" */,  // as is
    BatchSize       /* "BATCH_SIZE" */, // as is
    OptimizeFor     /* "OPTIMIZE_FOR" */, // evaluate atom
    Connection      /* "CONNECTION" */, // evaluate atom
    Init            /* "INIT" */,  // as is
};

///////////////////////////////////////////////////////////////////////////////////////////////
}
