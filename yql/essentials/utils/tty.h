#pragma once

namespace NYql {

enum class EStdStream {
    In,
    Out,
    Err
};

bool IsTty(EStdStream stream);

} // namespace NYql
