#pragma once

#include <string_view>

namespace NKikimr {
namespace NMiniKQL {

void* PgInitializeMainContext();
void PgDestroyMainContext(void* ctx);

void PgAcquireThreadContext(void* ctx);
void PgReleaseThreadContext(void* ctx);

void* PgInitializeContext(const std::string_view& contextType);
void PgDestroyContext(const std::string_view& contextType, void* ctx);

} // namespace NMiniKQL
} // namespace NKikimr
