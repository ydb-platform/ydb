#pragma once
#include "fwd.h"

namespace NTi {

    ///
    /// Compute type complexity.
    /// Roughly speaking type complexity is a number of nodes in the schema tree.
    /// Examples:
    ///    - Type complexity of simple or singular type (i.e. Int64, String, Null, Decimal) is 1.
    ///    - Type complexity of `Optional<Int64>` is 2
    ///    - Type complexity of `Struct<a:Int64,b:Optional<String>` is 4 (1 for Int64, 2 for Optional<String> and 1 for struct)
    ///
    /// Systems might impose restrictions on the type complexity.
    int ComputeTypeComplexity(const TTypePtr& type);
    int ComputeTypeComplexity(const TType* type);

} // namespace NTi
