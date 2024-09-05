#pragma once

#include <string_view>
#include <ydb/library/yql/core/pg_settings/guc_settings.h>
#include <ydb/library/yql/parser/pg_catalog/catalog.h>

namespace NKikimr {
namespace NMiniKQL {

void* PgInitializeMainContext();
void PgDestroyMainContext(void* ctx);

void PgAcquireThreadContext(void* ctx);
void PgReleaseThreadContext(void* ctx);

std::unique_ptr<NYql::NPg::IExtensionLoader> CreateExtensionLoader();

void* PgInitializeContext(const std::string_view& contextType);
void PgDestroyContext(const std::string_view& contextType, void* ctx);

void PgSetGUCSettings(void* ctx, const TGUCSettings::TPtr& GUCSettings);
std::optional<std::string> PGGetGUCSetting(const std::string& key);

void PgCreateSysCacheEntries(void* ctx);
} // namespace NMiniKQL
} // namespace NKikimr
