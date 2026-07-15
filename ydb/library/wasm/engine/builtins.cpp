#include "builtins.h"

#include <library/cpp/resource/resource.h>

namespace NYdb::NWasm {

////////////////////////////////////////////////////////////////////////////////

static constexpr auto BuiltinEmscriptenSystemLibrariesResource = "libemscripten-system-libraries-dll.so";
static constexpr auto BuiltinEmscriptenSystemLibrariesObjectCodeResource = "libemscripten-system-libraries-dll.so.compiled";
static constexpr auto BuiltinYtQlUdfsResource = "libwasm-udfs-builtin-ytql-udfs.so";

////////////////////////////////////////////////////////////////////////////////

TModuleBytecode GetBuiltinMinimalRuntimeSdk()
{
    static auto sdk = [] {
        auto bytecode = TSharedRef::FromString(TString(R"(
            (module
                (type (;0;) (func))
                (type (;1;) (func (param i64) (result i64)))
                (type (;2;) (func (param i64)))

                (import "env" "__linear_memory" (memory (;0;) i64 0))
                (import "env" "__heap_base" (global (;0;) (mut i64)))

                (func $malloc (type 1) (param i64) (result i64)
                    (local $address i64)
                    (local.set $address (global.get 0))
                    (global.set 0 (i64.add (local.get $address) (local.get 0)))
                    (local.get $address)
                )

                (func $free (type 2) (param i64))

                (export "malloc" (func $malloc))
                (export "free" (func $free))
            ))"));

        return TModuleBytecode{
            .Format = EBytecodeFormat::HumanReadable,
            .Data = bytecode,
            .ObjectCode = TSharedRef(),
        };
    } ();

    return sdk;
}

TModuleBytecode GetBuiltinYtQlUdfs()
{
    static auto udfs = [] {
        auto bytecode = ::NResource::Has(BuiltinYtQlUdfsResource)
            ? TSharedRef::FromString(::NResource::Find(BuiltinYtQlUdfsResource))
            : TSharedRef();

        return TModuleBytecode{
            .Format = EBytecodeFormat::Binary,
            .Data = bytecode,
            .ObjectCode = TSharedRef(),
        };
    } ();

    return udfs;
}

TModuleBytecode GetBuiltinSdk()
{
    static auto sdk = [] {
        auto bytecode = ::NResource::Has(BuiltinEmscriptenSystemLibrariesResource)
            ? TSharedRef::FromString(::NResource::Find(BuiltinEmscriptenSystemLibrariesResource))
            : TSharedRef();

        auto objectCode = ::NResource::Has(BuiltinEmscriptenSystemLibrariesObjectCodeResource)
            ? TSharedRef::FromString(::NResource::Find(BuiltinEmscriptenSystemLibrariesObjectCodeResource))
            : TSharedRef();

        return TModuleBytecode{
            .Format = EBytecodeFormat::Binary,
            .Data = bytecode,
            .ObjectCode = objectCode,
        };
    } ();

    return sdk;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYdb::NWasm
