#pragma once
#include <string_view>

namespace NKikimr {
namespace NMiniKQL {

void* PgInitializeContext(const std::string_view& contextType);
void PgDestroyContext(const std::string_view& contextType, void* ctx);

} // namespace MiniKQL
} // namespace NKikimr
