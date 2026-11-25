#include <yql/essentials/public/sql_inspect/abi/interface/symbols.h>

#include <yql/essentials/public/abi_impl/message.h>
#include <yql/essentials/public/abi_impl/result.h>
#include <yql/essentials/public/sql_inspect/abi/serialization/serialization.h>
#include <yql/essentials/public/sql_inspect/internal/symbols.h>

struct TYqlSqlInspectHandle: NYql::NAbi::TResult {};

extern "C" TYqlSqlInspectHandle* YqlSqlInspectV1(const char* request, size_t size) noexcept {
    return NYql::NAbi::Execute<TYqlSqlInspectHandle>([&](auto&) {
        const auto message = NYql::NAbi::ParseMessage<NYql::NSqlInspect::NAbi::NProto::TSymbolsRequest>(request, size);
        const auto result = NYql::NSqlInspect::GetSymbolsImpl(message.GetSql(), NYql::NSqlInspect::NAbi::DecodeSettings(message));
        return NYql::NAbi::SerializeMessage(NYql::NSqlInspect::NAbi::EncodeResult(result));
    });
}

extern "C" void YqlSqlInspectFreeV1(TYqlSqlInspectHandle* handle) noexcept {
    NYql::NAbi::Free(handle);
}

extern "C" const char* YqlSqlInspectResultV1(const TYqlSqlInspectHandle* handle, size_t* size) noexcept {
    return NYql::NAbi::GetOutput(*handle, size);
}

extern "C" const char* YqlSqlInspectErrorV1(const TYqlSqlInspectHandle* handle, size_t* size) noexcept {
    return NYql::NAbi::GetError(*handle, size);
}
