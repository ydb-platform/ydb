#pragma once

#include "udf_version.h"
#include "udf_types.h"
#include "udf_ptr.h"
#include "udf_string.h"
#include "udf_type_size_check.h"
#include "udf_value.h"

#include <functional>

#include <stdarg.h>

#ifdef _win_
#  ifdef BUILD_UDF
#    define UDF_API __declspec(dllexport)
#  else
#    define UDF_API __declspec(dllimport)
#  endif
#else
#  define UDF_API __attribute__ ((visibility("default")))
#endif

#ifdef BUILD_UDF
#define REGISTER_MODULES(...) \
    extern "C" UDF_API void Register( \
            ::NYql::NUdf::IRegistrator& registrator, ui32 flags) { \
        Y_UNUSED(flags); \
        ::NYql::NUdf::RegisterHelper<__VA_ARGS__>(registrator); \
    } \
    extern "C" UDF_API ui32 AbiVersion() { \
        return ::NYql::NUdf::CurrentAbiVersion(); \
    }\
    extern "C" UDF_API void SetBackTraceCallback(::NYql::NUdf::TBackTraceCallback callback) { \
        ::NYql::NUdf::SetBackTraceCallbackImpl(callback); \
    }
#else
#define REGISTER_MODULES(...) \
    namespace { \
        struct TYqlStaticUdfRegistrator { \
            inline TYqlStaticUdfRegistrator() { \
                ::NYql::NUdf::AddToStaticUdfRegistry<__VA_ARGS__>(); \
            } \
        } YQL_REGISTRATOR; \
    }
#endif

namespace NYql {
namespace NUdf {

class IFunctionTypeInfoBuilder;

struct TStaticSymbols {
    void* (*UdfAllocateFunc)(ui64 size);
    void (*UdfFreeFunc)(const void* mem);
    void (*UdfTerminate)(const char* message);
    void (*UdfRegisterObject)(TBoxedValue* object);
    void (*UdfUnregisterObject)(TBoxedValue* object);
#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 8)
    void* (*UdfAllocateWithSizeFunc)(ui64 size);
    void (*UdfFreeWithSizeFunc)(const void* mem, ui64 size);
#endif
#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 37)
    void* (*UdfArrowAllocateFunc)(ui64 size);
    void* (*UdfArrowReallocateFunc)(const void* mem, ui64 prevSize, ui64 size);
    void (*UdfArrowFreeFunc)(const void* mem, ui64 size);
#endif
};

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 37)
UDF_ASSERT_TYPE_SIZE(TStaticSymbols, 80);
#elif UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 8)
UDF_ASSERT_TYPE_SIZE(TStaticSymbols, 56);
#else
UDF_ASSERT_TYPE_SIZE(TStaticSymbols, 40);
#endif

inline TStaticSymbols GetStaticSymbols();

//////////////////////////////////////////////////////////////////////////////
// IFunctionNamesSink
//////////////////////////////////////////////////////////////////////////////
class IFunctionDescriptor
{
public:
    typedef TUniquePtr<IFunctionDescriptor> TPtr;

    virtual ~IFunctionDescriptor() = default;

    virtual void SetTypeAwareness() = 0;
};

UDF_ASSERT_TYPE_SIZE(IFunctionDescriptor, 8);

//////////////////////////////////////////////////////////////////////////////
// IFunctionNamesSink
//////////////////////////////////////////////////////////////////////////////
class IFunctionsSink
{
public:
    virtual ~IFunctionsSink() = default;

    virtual IFunctionDescriptor::TPtr Add(const TStringRef& name) = 0;
};

UDF_ASSERT_TYPE_SIZE(IFunctionsSink, 8);

typedef IFunctionsSink IFunctionNamesSink;

//////////////////////////////////////////////////////////////////////////////
// IUdfModule
//////////////////////////////////////////////////////////////////////////////
class IUdfModule
{
public:
    struct TFlags {
        enum {
            TypesOnly = 0x01
        };
    };

public:
    virtual ~IUdfModule() = default;

    virtual void GetAllFunctions(IFunctionsSink& sink) const = 0;

    virtual void BuildFunctionTypeInfo(
            const TStringRef& name,
            TType* userType,
            const TStringRef& typeConfig,
            ui32 flags,
            IFunctionTypeInfoBuilder& builder) const = 0;

    virtual void CleanupOnTerminate() const = 0;
};

UDF_ASSERT_TYPE_SIZE(IUdfModule, 8);

//////////////////////////////////////////////////////////////////////////////
// TRegistrator
//////////////////////////////////////////////////////////////////////////////
class IRegistrator
{
public:
    struct TFlags {
        enum {
            TypesOnly = 0x01,
        };
    };

public:
    virtual ~IRegistrator() = default;

    virtual void AddModule(
            const TStringRef& name,
            TUniquePtr<IUdfModule> module) = 0;
};

UDF_ASSERT_TYPE_SIZE(IRegistrator, 8);

typedef void(*TBackTraceCallback)();

using TRegisterFunctionPtr = void (*)(IRegistrator& registrator, ui32 flags);
using TAbiVersionFunctionPtr = ui32 (*)();
using TBindSymbolsFunctionPtr = void (*)(const TStaticSymbols& symbols);
using TSetBackTraceCallbackPtr = void(*)(TBackTraceCallback callback);

template<typename TModule>
static inline void RegisterHelper(IRegistrator& registrator) {
    TUniquePtr<TModule> ptr(new TModule());
    auto name = ptr->Name();
    registrator.AddModule(name, ptr.Release());
}

template<typename THead1, typename THead2, typename... TTail>
static inline void RegisterHelper(IRegistrator& registrator) {
    RegisterHelper<THead1>(registrator);
    RegisterHelper<THead2, TTail...>(registrator);
}

void SetBackTraceCallbackImpl(TBackTraceCallback callback);


using TUdfModuleWrapper = std::function<std::pair<TStringRef, TUniquePtr<IUdfModule>>()>;

void AddToStaticUdfRegistry(TUdfModuleWrapper&&);

template<typename TModule>
static inline void AddToStaticUdfRegistry() {
    AddToStaticUdfRegistry([]() {
        TUniquePtr<TModule> ptr(new TModule());
        auto name = ptr->Name();
        return TUdfModuleWrapper::result_type(name, ptr.Release());
    });
}

template<typename THead1, typename THead2, typename... TTail>
static inline void AddToStaticUdfRegistry() {
    AddToStaticUdfRegistry<THead1>();
    AddToStaticUdfRegistry<THead2, TTail...>();
}


} // namspace NUdf
} // namspace NYql

extern "C" UDF_API void Register(NYql::NUdf::IRegistrator& registrator, ui32 flags);
extern "C" UDF_API ui32 AbiVersion();
#if defined(_win_) || defined(_darwin_)
extern "C" UDF_API void BindSymbols(const NYql::NUdf::TStaticSymbols& symbols);
#endif
extern "C" UDF_API void SetBackTraceCallback(NYql::NUdf::TBackTraceCallback callback);

namespace NYql {
namespace NUdf {

#ifndef BUILD_UDF

Y_PRAGMA_DIAGNOSTIC_PUSH
Y_PRAGMA_NO_DEPRECATED

inline TStaticSymbols GetStaticSymbols() {
    return {&UdfAllocate, &UdfFree, &UdfTerminate, &UdfRegisterObject, &UdfUnregisterObject
#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 8)
        ,&UdfAllocateWithSize, &UdfFreeWithSize
#endif
#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 37)
        ,&UdfArrowAllocate, &UdfArrowReallocate, &UdfArrowFree
#endif
    };
}

Y_PRAGMA_DIAGNOSTIC_POP

#endif

}
}
