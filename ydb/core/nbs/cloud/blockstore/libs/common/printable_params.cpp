#include "printable_params.h"

namespace NYdb::NBS::NBlockStore {

///////////////////////////////////////////////////////////////////////////////

void PrintParams(IOutputStream& out, TPrintableParams keyValues)
{
    bool first = true;
    for (const auto& [key, value]: keyValues) {
        if (!first) {
            out << " ";
        }
        out << key;
        std::visit(
            [&out](const auto& v)
            {
                using T = std::decay_t<decltype(v)>;
                if constexpr (!std::is_same_v<T, std::monostate>) {
                    out << ":" << v;
                }
            },
            value);
        first = false;
    }
}

TString PrintParams(TPrintableParams keyValues)
{
    TStringBuilder sb;
    PrintParams(sb.Out, keyValues);
    return sb;
}

}   // namespace NYdb::NBS::NBlockStore
