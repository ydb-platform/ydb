#include <string.h>
#include <string>
#include "WAVM/IR/Module.h"
#include "WAVM/IR/Types.h"
#include "WAVM/Inline/BasicTypes.h"
#include "WAVM/LLVMJIT/LLVMJIT.h"
#include "WAVM/Logging/Logging.h"
#include "WAVM/Platform/Diagnostics.h"
#include "WAVM/Runtime/Intrinsics.h"
#include "WAVM/Runtime/Runtime.h"
#include "WAVM/RuntimeABI/RuntimeABI.h"
#include "WAVM/WASM/WASM.h"
#include "WAVM/WASTParse/WASTParse.h"
#include "WAVM/WASTPrint/WASTPrint.h"

using namespace WAVM;
using namespace WAVM::IR;
using namespace WAVM::Runtime;

// Before including the C API definitions, typedef the C API's opaque types to map directly to the
// WAVM Runtime's opaque types.
#define WASM_OPAQUE_TYPES_DEFINED

typedef Compartment wasm_compartment_t;
typedef Context wasm_store_t;
typedef Object wasm_ref_t;
typedef Exception wasm_trap_t;
typedef Foreign wasm_foreign_t;
typedef Function wasm_func_t;
typedef Table wasm_table_t;
typedef Memory wasm_memory_t;
typedef Global wasm_global_t;
typedef Object wasm_extern_t;

typedef Instance wasm_instance_t;
typedef Function wasm_shared_func_t;
typedef Table wasm_shared_table_t;
typedef Memory wasm_shared_memory_t;
typedef Foreign wasm_shared_foreign_t;

struct wasm_config_t;
struct wasm_engine_t;
struct wasm_valtype_t;
struct wasm_functype_t;
struct wasm_tabletype_t;
struct wasm_memorytype_t;
struct wasm_globaltype_t;
struct wasm_externtype_t;

struct wasm_module_t;
typedef struct wasm_module_t wasm_shared_module_t;

#include "WAVM/wavm-c/wavm-c.h"

static_assert(sizeof(wasm_val_t) == sizeof(UntaggedValue), "wasm_val_t should match UntaggedValue");

struct wasm_config_t
{
	FeatureSpec featureSpec;
};

struct wasm_engine_t
{
	wasm_config_t config;
};

struct wasm_valtype_t
{
	ValueType type;
};

struct wasm_externtype_t
{
	ExternKind kind;
	wasm_externtype_t(ExternKind inKind) : kind(inKind) {}
};
struct wasm_functype_t : wasm_externtype_t
{
	FunctionType type;

	wasm_functype_t(FunctionType inType) : wasm_externtype_t(ExternKind::function), type(inType) {}
};
struct wasm_globaltype_t : wasm_externtype_t
{
	GlobalType type;
	wasm_valtype_t* valtype;

	wasm_globaltype_t(GlobalType inType, wasm_valtype_t* inValtype)
	: wasm_externtype_t(ExternKind::global), type(inType), valtype(inValtype)
	{
	}
};
struct wasm_tabletype_t : wasm_externtype_t
{
	TableType type;
	wasm_valtype_t* element;
	wasm_limits_t limits;

	wasm_tabletype_t(TableType inType, wasm_valtype_t* inElement, wasm_limits_t inLimits)
	: wasm_externtype_t(ExternKind::table), type(inType), element(inElement), limits(inLimits)
	{
	}
};
struct wasm_memorytype_t : wasm_externtype_t
{
	MemoryType type;
	wasm_limits_t limits;

	wasm_memorytype_t(MemoryType inType, wasm_limits_t inLimits)
	: wasm_externtype_t(ExternKind::memory), type(inType), limits(inLimits)
	{
	}
};
struct wasm_module_t
{
	ModuleRef module;

	wasm_module_t(ModuleRef inModule) : module(inModule) {}
};

static wasm_index_t as_index(IndexType indexType)
{
	switch(indexType)
	{
	case IndexType::i32: return WASM_INDEX_I32;
	case IndexType::i64: return WASM_INDEX_I64;
	default: WAVM_UNREACHABLE();
	};
}
static wasm_limits_t as_limits(const SizeConstraints& size)
{
	WAVM_ERROR_UNLESS(size.min <= UINT32_MAX);
	WAVM_ERROR_UNLESS(size.max == UINT64_MAX || size.max <= UINT32_MAX);
	return {U32(size.min), size.max == UINT64_MAX ? UINT32_MAX : U32(size.max)};
}
static wasm_functype_t* as_externtype(FunctionType type) { return new wasm_functype_t(type); }
static wasm_tabletype_t* as_externtype(TableType type)
{
	return new wasm_tabletype_t(
		type, new wasm_valtype_t{ValueType(type.elementType)}, as_limits(type.size));
}
static wasm_memorytype_t* as_externtype(MemoryType type)
{
	return new wasm_memorytype_t(type, as_limits(type.size));
}
static wasm_globaltype_t* as_externtype(GlobalType type)
{
	return new wasm_globaltype_t(type, new wasm_valtype_t{ValueType(type.valueType)});
}
static wasm_externtype_t* as_externtype(ExternType type)
{
	switch(type.kind)
	{
	case ExternKind::function: return as_externtype(asFunctionType(type));
	case ExternKind::table: return as_externtype(asTableType(type));
	case ExternKind::memory: return as_externtype(asMemoryType(type));
	case ExternKind::global: return as_externtype(asGlobalType(type));
	case ExternKind::exceptionType:
		Errors::unimplemented("Converting exception type to C API wasm_externtype_t");

	case ExternKind::invalid:
	default: WAVM_UNREACHABLE();
	}
}

static IndexType asIndexType(wasm_index_t index)
{
	switch(index)
	{
	case WASM_INDEX_I32: return IndexType::i32;
	case WASM_INDEX_I64: return IndexType::i64;
	default: WAVM_UNREACHABLE();
	};
}
static ValueType asValueType(wasm_valkind_t kind)
{
	switch(kind)
	{
	case WASM_I32: return ValueType::i32;
	case WASM_I64: return ValueType::i64;
	case WASM_F32: return ValueType::f32;
	case WASM_F64: return ValueType::f64;
	case WASM_V128: return ValueType::v128;
	case WASM_ANYREF: return ValueType::externref;
	case WASM_FUNCREF: return ValueType::funcref;
	default: Errors::fatalf("Unknown wasm_valkind_t value: %u", kind);
	}
}
static Value asValue(ValueType type, const wasm_val_t* value)
{
	switch(type)
	{
	case ValueType::i32: return Value(value->i32);
	case ValueType::i64: return Value(value->i64);
	case ValueType::f32: return Value(value->f32);
	case ValueType::f64: return Value(value->f64);
	case ValueType::v128: {
		V128 v128;
		v128.u64x2[0] = value->v128.u64x2[0];
		v128.u64x2[1] = value->v128.u64x2[1];
		return Value(v128);
	}
	case ValueType::externref: return Value(value->ref);
	case ValueType::funcref: return Value(asFunction(value->ref));

	case ValueType::none:
	case ValueType::any:
	default: WAVM_UNREACHABLE();
	}
}

static wasm_val_t as_val(const Value& value)
{
	wasm_val_t result;
	switch(value.type)
	{
	case ValueType::i32: result.i32 = value.i32; break;
	case ValueType::i64: result.i64 = value.i64; break;
	case ValueType::f32: result.f32 = value.f32; break;
	case ValueType::f64: result.f64 = value.f64; break;
	case ValueType::v128: {
		result.v128.u64x2[0] = value.v128.u64x2[0];
		result.v128.u64x2[1] = value.v128.u64x2[1];
		break;
	}
	case ValueType::externref: result.ref = value.object; break;
	case ValueType::funcref: result.ref = asObject(value.function); break;

	case ValueType::none:
	case ValueType::any:
	default: WAVM_UNREACHABLE();
	}
	return result;
}

extern "C" {

// wasm_config_t
void wasm_config_delete(wasm_config_t* config) { delete config; }
wasm_config_t* wasm_config_new() { return new wasm_config_t; }

#define IMPLEMENT_FEATURE(cAPIName, wavmName)                                                      \
	WASM_C_API void wasm_config_feature_set_##cAPIName(wasm_config_t* config, bool enable)         \
	{                                                                                              \
		config->featureSpec.wavmName = enable;                                                     \
	}

IMPLEMENT_FEATURE(import_export_mutable_globals, importExportMutableGlobals)
IMPLEMENT_FEATURE(nontrapping_float_to_int, nonTrappingFloatToInt)
IMPLEMENT_FEATURE(sign_extension, signExtension)
IMPLEMENT_FEATURE(bulk_memory_ops, bulkMemoryOperations)

IMPLEMENT_FEATURE(simd, simd)
IMPLEMENT_FEATURE(atomics, atomics)
IMPLEMENT_FEATURE(exception_handling, exceptionHandling)
IMPLEMENT_FEATURE(multivalue, multipleResultsAndBlockParams)
IMPLEMENT_FEATURE(reference_types, referenceTypes)
IMPLEMENT_FEATURE(extended_name_section, extendedNameSection)
IMPLEMENT_FEATURE(multimemory, multipleMemories)

IMPLEMENT_FEATURE(shared_tables, sharedTables)
IMPLEMENT_FEATURE(allow_legacy_inst_names, allowLegacyInstructionNames)
IMPLEMENT_FEATURE(any_extern_kind_elems, allowAnyExternKindElemSegments)
IMPLEMENT_FEATURE(wat_quoted_names, quotedNamesInTextFormat)
IMPLEMENT_FEATURE(wat_custom_sections, customSectionsInTextFormat)

// wasm_engine_t
wasm_engine_t* wasm_engine_new() { return new wasm_engine_t; }
wasm_engine_t* wasm_engine_new_with_config(wasm_config_t* config)
{
	wasm_engine_t* engine = new wasm_engine_t{*config};
	delete config;
	return engine;
}
void wasm_engine_delete(wasm_engine_t* engine) { delete engine; }

// wasm_compartment_t
void wasm_compartment_delete(wasm_compartment_t* compartment)
{
	GCPointer<Compartment> compartmentGCRef = compartment;
	removeGCRoot(compartment);
	WAVM_ERROR_UNLESS(tryCollectCompartment(std::move(compartmentGCRef)));
}
wasm_compartment_t* wasm_compartment_new(wasm_engine_t*, const char* debug_name)
{
	Compartment* compartment = createCompartment(std::string(debug_name));
	addGCRoot(compartment);
	return compartment;
}
wasm_compartment_t* wasm_compartment_clone(const wasm_compartment_t* compartment)
{
	return cloneCompartment(compartment);
}
bool wasm_compartment_contains(const wasm_compartment_t* compartment, const wasm_ref_t* ref)
{
	return isInCompartment(ref, compartment);
}

// wasm_store_t
void wasm_store_delete(wasm_store_t* store) { removeGCRoot(store); }
wasm_store_t* wasm_store_new(wasm_compartment_t* compartment, const char* debug_name)
{
	Context* context = createContext(compartment, std::string(debug_name));
	addGCRoot(context);
	return context;
}

// wasm_valtype_t
void wasm_valtype_delete(wasm_valtype_t* type) { delete type; }
wasm_valtype_t* wasm_valtype_copy(wasm_valtype_t* type) { return new wasm_valtype_t{type->type}; }
wasm_valtype_t* wasm_valtype_new(wasm_valkind_t kind)
{
	ValueType type = asValueType(kind);
	return new wasm_valtype_t{type};
}
wasm_valkind_t wasm_valtype_kind(const wasm_valtype_t* type)
{
	switch(type->type)
	{
	case ValueType::i32: return WASM_I32;
	case ValueType::i64: return WASM_I64;
	case ValueType::f32: return WASM_F32;
	case ValueType::f64: return WASM_F64;
	case ValueType::v128: return WASM_V128;
	case ValueType::externref: return WASM_ANYREF;
	case ValueType::funcref: return WASM_FUNCREF;

	case ValueType::none:
	case ValueType::any:
	default: WAVM_UNREACHABLE();
	};
}

// wasm_functype_t
void wasm_functype_delete(wasm_functype_t* type) { delete type; }
wasm_functype_t* wasm_functype_copy(wasm_functype_t* type)
{
	return new wasm_functype_t(type->type);
}
wasm_functype_t* wasm_functype_new(wasm_valtype_t** params,
								   uintptr_t numParams,
								   wasm_valtype_t** results,
								   uintptr_t numResults)
{
	ValueType* paramsTemp = new(alloca(sizeof(ValueType) * numParams)) ValueType[numParams];
	ValueType* resultsTemp = new(alloca(sizeof(ValueType) * numResults)) ValueType[numResults];
	for(Uptr paramIndex = 0; paramIndex < numParams; ++paramIndex)
	{
		paramsTemp[paramIndex] = params[paramIndex]->type;
		wasm_valtype_delete(params[paramIndex]);
	}
	for(Uptr resultIndex = 0; resultIndex < numResults; ++resultIndex)
	{
		resultsTemp[resultIndex] = results[resultIndex]->type;
		wasm_valtype_delete(results[resultIndex]);
	}

	return new wasm_functype_t(
		FunctionType(TypeTuple(resultsTemp, numResults), TypeTuple(paramsTemp, numParams)));
}
size_t wasm_functype_num_params(const wasm_functype_t* type) { return type->type.params().size(); }
wasm_valtype_t* wasm_functype_param(const wasm_functype_t* type, size_t index)
{
	return new wasm_valtype_t{type->type.params()[index]};
}
size_t wasm_functype_num_results(const wasm_functype_t* type)
{
	return type->type.results().size();
}
wasm_valtype_t* wasm_functype_result(const wasm_functype_t* type, size_t index)
{
	return new wasm_valtype_t{type->type.results()[index]};
}

// wasm_globaltype_t
void wasm_globaltype_delete(wasm_globaltype_t* type)
{
	wasm_valtype_delete(type->valtype);
	delete type;
}
wasm_globaltype_t* wasm_globaltype_copy(wasm_globaltype_t* type)
{
	return new wasm_globaltype_t(type->type, wasm_valtype_copy(type->valtype));
}
wasm_globaltype_t* wasm_globaltype_new(wasm_valtype_t* valtype, wasm_mutability_t mutability)
{
	return new wasm_globaltype_t(GlobalType(valtype->type, mutability == WASM_VAR), valtype);
}
const wasm_valtype_t* wasm_globaltype_content(const wasm_globaltype_t* type)
{
	return type->valtype;
}
wasm_mutability_t wasm_globaltype_mutability(const wasm_globaltype_t* type)
{
	return wasm_mutability_t(type->type.isMutable ? WASM_VAR : WASM_CONST);
}

// wasm_tabletype_t
void wasm_tabletype_delete(wasm_tabletype_t* type)
{
	wasm_valtype_delete(type->element);
	delete type;
}
wasm_tabletype_t* wasm_tabletype_copy(wasm_tabletype_t* type)
{
	return new wasm_tabletype_t(type->type, wasm_valtype_copy(type->element), type->limits);
}
wasm_tabletype_t* wasm_tabletype_new(wasm_valtype_t* element,
									 const wasm_limits_t* limits,
									 wasm_shared_t shared,
									 wasm_index_t index)
{
	WAVM_ERROR_UNLESS(isReferenceType(element->type));
	return new wasm_tabletype_t(TableType(ReferenceType(element->type),
										  shared == WASM_SHARED,
										  asIndexType(index),
										  SizeConstraints{limits->min, limits->max}),
								element,
								*limits);
}
const wasm_valtype_t* wasm_tabletype_element(const wasm_tabletype_t* type) { return type->element; }
const wasm_limits_t* wasm_tabletype_limits(const wasm_tabletype_t* type) { return &type->limits; }
wasm_shared_t wasm_tabletype_shared(const wasm_tabletype_t* type)
{
	return wasm_shared_t(type->type.isShared ? WASM_SHARED : WASM_NOTSHARED);
}
wasm_index_t wasm_tabletype_index(const wasm_tabletype_t* type)
{
	return as_index(type->type.indexType);
}

// wasm_memorytype_t
void wasm_memorytype_delete(wasm_memorytype_t* type) { delete type; }
wasm_memorytype_t* wasm_memorytype_copy(wasm_memorytype_t* type)
{
	return new wasm_memorytype_t(type->type, type->limits);
}
wasm_memorytype_t* wasm_memorytype_new(const wasm_limits_t* limits,
									   wasm_shared_t shared,
									   wasm_index_t index)
{
	return new wasm_memorytype_t(
		MemoryType(
			shared == WASM_SHARED, asIndexType(index), SizeConstraints{limits->min, limits->max}),
		*limits);
}
const wasm_limits_t* wasm_memorytype_limits(const wasm_memorytype_t* type) { return &type->limits; }
wasm_shared_t wasm_memorytype_shared(const wasm_memorytype_t* type)
{
	return wasm_shared_t(type->type.isShared ? WASM_SHARED : WASM_NOTSHARED);
}
wasm_index_t wasm_memorytype_index(const wasm_memorytype_t* type)
{
	return as_index(type->type.indexType);
}

// wasm_externtype_t
void wasm_externtype_delete(wasm_externtype_t* type)
{
	switch(type->kind)
	{
	case ExternKind::function: wasm_functype_delete(wasm_externtype_as_functype(type)); break;
	case ExternKind::table: wasm_tabletype_delete(wasm_externtype_as_tabletype(type)); break;
	case ExternKind::memory: wasm_memorytype_delete(wasm_externtype_as_memorytype(type)); break;
	case ExternKind::global: wasm_globaltype_delete(wasm_externtype_as_globaltype(type)); break;
	case ExternKind::exceptionType: Errors::unimplemented("exception types in C API");

	case ExternKind::invalid:
	default: WAVM_UNREACHABLE();
	};
}
wasm_externtype_t* wasm_externtype_copy(wasm_externtype_t* type)
{
	switch(type->kind)
	{
	case ExternKind::function:
		return wasm_functype_as_externtype(wasm_functype_copy(wasm_externtype_as_functype(type)));
	case ExternKind::table:
		return wasm_tabletype_as_externtype(
			wasm_tabletype_copy(wasm_externtype_as_tabletype(type)));
	case ExternKind::memory:
		return wasm_memorytype_as_externtype(
			wasm_memorytype_copy(wasm_externtype_as_memorytype(type)));
	case ExternKind::global:
		return wasm_globaltype_as_externtype(
			wasm_globaltype_copy(wasm_externtype_as_globaltype(type)));
	case ExternKind::exceptionType: Errors::unimplemented("exception types in C API");

	case ExternKind::invalid:
	default: WAVM_UNREACHABLE();
	};
}
wasm_externkind_t wasm_externtype_kind(const wasm_externtype_t* type)
{
	switch(type->kind)
	{
	case ExternKind::function: return WASM_EXTERN_FUNC;
	case ExternKind::table: return WASM_EXTERN_TABLE;
	case ExternKind::memory: return WASM_EXTERN_MEMORY;
	case ExternKind::global: return WASM_EXTERN_GLOBAL;
	case ExternKind::exceptionType: Errors::unimplemented("exception types in C API");

	case ExternKind::invalid:
	default: WAVM_UNREACHABLE();
	}
}
wasm_externtype_t* wasm_functype_as_externtype(wasm_functype_t* type)
{
	return (wasm_externtype_t*)type;
}
wasm_externtype_t* wasm_globaltype_as_externtype(wasm_globaltype_t* type)
{
	return (wasm_externtype_t*)type;
}
wasm_externtype_t* wasm_tabletype_as_externtype(wasm_tabletype_t* type)
{
	return (wasm_externtype_t*)type;
}
wasm_externtype_t* wasm_memorytype_as_externtype(wasm_memorytype_t* type)
{
	return (wasm_externtype_t*)type;
}
wasm_functype_t* wasm_externtype_as_functype(wasm_externtype_t* type)
{
	return (wasm_functype_t*)type;
}
wasm_globaltype_t* wasm_externtype_as_globaltype(wasm_externtype_t* type)
{
	return (wasm_globaltype_t*)type;
}
wasm_tabletype_t* wasm_externtype_as_tabletype(wasm_externtype_t* type)
{
	return (wasm_tabletype_t*)type;
}
wasm_memorytype_t* wasm_externtype_as_memorytype(wasm_externtype_t* type)
{
	return (wasm_memorytype_t*)type;
}
const wasm_externtype_t* wasm_functype_as_externtype_const(const wasm_functype_t* type)
{
	return (wasm_externtype_t*)type;
}
const wasm_externtype_t* wasm_globaltype_as_externtype_const(const wasm_globaltype_t* type)
{
	return (wasm_externtype_t*)type;
}
const wasm_externtype_t* wasm_tabletype_as_externtype_const(const wasm_tabletype_t* type)
{
	return (wasm_externtype_t*)type;
}
const wasm_externtype_t* wasm_memorytype_as_externtype_const(const wasm_memorytype_t* type)
{
	return (wasm_externtype_t*)type;
}
const wasm_functype_t* wasm_externtype_as_functype_const(const wasm_externtype_t* type)
{
	return (wasm_functype_t*)type;
}
const wasm_globaltype_t* wasm_externtype_as_globaltype_const(const wasm_externtype_t* type)
{
	return (wasm_globaltype_t*)type;
}
const wasm_tabletype_t* wasm_externtype_as_tabletype_const(const wasm_externtype_t* type)
{
	return (wasm_tabletype_t*)type;
}
const wasm_memorytype_t* wasm_externtype_as_memorytype_const(const wasm_externtype_t* type)
{
	return (wasm_memorytype_t*)type;
}

// wasm_ref_t
#define IMPLEMENT_REF_BASE(name, Type)                                                             \
	void wasm_##name##_delete(wasm_##name##_t* ref) { removeGCRoot(ref); }                         \
                                                                                                   \
	wasm_##name##_t* wasm_##name##_copy(const wasm_##name##_t* constRef)                           \
	{                                                                                              \
		wasm_##name##_t* ref = const_cast<wasm_##name##_t*>(constRef);                             \
		addGCRoot(ref);                                                                            \
		return ref;                                                                                \
	}                                                                                              \
	bool wasm_##name##_same(const wasm_##name##_t* a, const wasm_##name##_t* b) { return a == b; } \
                                                                                                   \
	void* wasm_##name##_get_host_info(const wasm_##name##_t* ref) { return getUserData(ref); }     \
	void wasm_##name##_set_host_info(wasm_##name##_t* ref, void* userData)                         \
	{                                                                                              \
		setUserData(ref, userData, nullptr);                                                       \
	}                                                                                              \
	void wasm_##name##_set_host_info_with_finalizer(                                               \
		wasm_##name##_t* ref, void* userData, void (*finalizeUserData)(void*))                     \
	{                                                                                              \
		setUserData(ref, userData, finalizeUserData);                                              \
	}                                                                                              \
                                                                                                   \
	wasm_##name##_t* wasm_##name##_remap_to_cloned_compartment(                                    \
		const wasm_##name##_t* ref, const wasm_compartment_t* compartment)                         \
	{                                                                                              \
		wasm_##name##_t* remappedRef = remapToClonedCompartment(ref, compartment);                 \
		addGCRoot(remappedRef);                                                                    \
		return remappedRef;                                                                        \
	}                                                                                              \
                                                                                                   \
	const char* wasm_##name##_name(const wasm_##name##_t* ref) { return getDebugName(ref).c_str(); }

#define IMPLEMENT_REF(name, Type)                                                                  \
	IMPLEMENT_REF_BASE(name, Type)                                                                 \
                                                                                                   \
	wasm_ref_t* wasm_##name##_as_ref(wasm_##name##_t* ref) { return asObject(ref); }               \
	wasm_##name##_t* wasm_ref_as_##name(wasm_ref_t* ref) { return as##Type(ref); }                 \
	const wasm_ref_t* wasm_##name##_as_ref_const(const wasm_##name##_t* ref)                       \
	{                                                                                              \
		return asObject(ref);                                                                      \
	}                                                                                              \
	const wasm_##name##_t* wasm_ref_as_##name##_const(const wasm_ref_t* ref)                       \
	{                                                                                              \
		return as##Type(ref);                                                                      \
	}

#define IMPLEMENT_SHAREABLE_REF(name, Type)                                                        \
	IMPLEMENT_REF(name, Type)                                                                      \
                                                                                                   \
	void wasm_shared_##name##_delete(wasm_shared_##name##_t* ref) { removeGCRoot(ref); }           \
                                                                                                   \
	wasm_shared_##name##_t* wasm_##name##_share(const wasm_##name##_t* constRef)                   \
	{                                                                                              \
		wasm_shared_##name##_t* ref = const_cast<wasm_shared_##name##_t*>(constRef);               \
		addGCRoot(ref);                                                                            \
		return ref;                                                                                \
	}                                                                                              \
	wasm_##name##_t* wasm_##name##_obtain(wasm_store_t* store,                                     \
										  const wasm_shared_##name##_t* constRef)                  \
	{                                                                                              \
		wasm_shared_##name##_t* ref = const_cast<wasm_shared_##name##_t*>(constRef);               \
		WAVM_ASSERT(isInCompartment(asObject(ref), getCompartment(store)));                        \
		addGCRoot(ref);                                                                            \
		return ref;                                                                                \
	}

IMPLEMENT_REF_BASE(ref, Object)

// wasm_trap_t

void wasm_trap_delete(wasm_trap_t* trap) { destroyException(trap); }

wasm_trap_t* wasm_trap_copy(const wasm_trap_t* trap) { return new Exception(*trap); }
bool wasm_trap_same(const wasm_trap_t* a, const wasm_trap_t* b) { return a == b; }

void* wasm_trap_get_host_info(const wasm_trap_t* trap) { return getUserData(trap); }
void wasm_trap_set_host_info(wasm_trap_t* trap, void* userData)
{
	setUserData(trap, userData, nullptr);
}
void wasm_trap_set_host_info_with_finalizer(wasm_trap_t* trap,
											void* userData,
											void (*finalizeUserData)(void*))
{
	setUserData(trap, userData, finalizeUserData);
}

wasm_ref_t* wasm_trap_as_ref(wasm_trap_t* trap) { Errors::unimplemented("wasm_trap_as_ref"); }
wasm_trap_t* wasm_ref_as_trap(wasm_ref_t* object) { Errors::unimplemented("wasm_ref_as_trap"); }
const wasm_ref_t* wasm_trap_as_ref_const(const wasm_trap_t*)
{
	Errors::unimplemented("wasm_trap_as_ref_const");
}
const wasm_trap_t* wasm_ref_as_trap_const(const wasm_ref_t*)
{
	Errors::unimplemented("wasm_ref_as_trap_const");
}

wasm_trap_t* wasm_trap_new(wasm_compartment_t* compartment,
						   const char* message,
						   size_t num_message_bytes)
{
	return createException(ExceptionTypes::calledAbort, nullptr, 0, Platform::captureCallStack(1));
}
bool wasm_trap_message(const wasm_trap_t* trap, char* out_message, size_t* inout_num_message_bytes)
{
	const std::string description = describeExceptionType(getExceptionType(trap));
	if(*inout_num_message_bytes < description.size())
	{
		*inout_num_message_bytes = description.size();
		return false;
	}
	else
	{
		WAVM_ASSERT(out_message);
		memcpy(out_message, description.c_str(), description.size());
		*inout_num_message_bytes = description.size();
		return true;
	}
}
size_t wasm_trap_stack_num_frames(const wasm_trap_t* trap)
{
	const Platform::CallStack& callStack = getExceptionCallStack(trap);
	return callStack.frames.size();
}
void wasm_trap_stack_frame(const wasm_trap_t* trap, size_t index, wasm_frame_t* out_frame)
{
	const Platform::CallStack& callStack = getExceptionCallStack(trap);
	const Platform::CallStack::Frame& frame = callStack.frames[index];
	InstructionSource source;
	if(getInstructionSourceByAddress(frame.ip, source)
	   && source.type == InstructionSource::Type::wasm)
	{
		out_frame->function = source.wasm.function;
		out_frame->instr_index = source.wasm.instructionIndex;
	}
	else
	{
		out_frame->function = nullptr;
		out_frame->instr_index = 0;
	}
}

// wasm_foreign_t

IMPLEMENT_SHAREABLE_REF(foreign, Foreign)

wasm_foreign_t* wasm_foreign_new(wasm_compartment_t* compartment, const char* debug_name)
{
	Foreign* foreign = createForeign(compartment, nullptr, nullptr, std::string(debug_name));
	addGCRoot(foreign);
	return foreign;
}

// wasm_val_t
void wasm_val_delete(wasm_valkind_t kind, wasm_val_t* val) {}
void wasm_val_copy(wasm_valkind_t kind, wasm_val_t* out, const wasm_val_t* val)
{
	memcpy(out, val, sizeof(wasm_val_t));
}

// wasm_module_t
void wasm_module_delete(wasm_module_t* module) { delete module; }
wasm_module_t* wasm_module_copy(wasm_module_t* module) { return new wasm_module_t{module->module}; }
wasm_module_t* wasm_module_new(wasm_engine_t* engine, const char* wasmBytes, uintptr_t numWASMBytes)
{
	WASM::LoadError loadError;
	ModuleRef module;
	if(loadBinaryModule(
		   (const U8*)wasmBytes, numWASMBytes, module, engine->config.featureSpec, &loadError))
	{ return new wasm_module_t{module}; }
	else
	{
		Log::printf(Log::debug, "%s\n", loadError.message.c_str());
		return nullptr;
	}
}
wasm_module_t* wasm_module_new_text(wasm_engine_t* engine, const char* text, size_t num_text_chars)
{
	// wasm_module_new_text requires the input string to be null terminated.
	WAVM_ERROR_UNLESS(text[num_text_chars - 1] == 0);

	std::vector<WAST::Error> parseErrors;
	IR::Module irModule(engine->config.featureSpec);
	if(!WAST::parseModule(text, num_text_chars, irModule, parseErrors))
	{
		if(Log::isCategoryEnabled(Log::debug))
		{ WAST::reportParseErrors("wasm_module_new_text", text, parseErrors, Log::debug); }
		return nullptr;
	}

	ModuleRef module = compileModule(irModule);
	return new wasm_module_t{module};
}

char* wasm_module_print(const wasm_module_t* module, size_t* out_num_chars)
{
	const std::string wastString = WAST::print(getModuleIR(module->module));

	char* returnBuffer = (char*)malloc(wastString.size() + 1);
	memcpy(returnBuffer, wastString.c_str(), wastString.size());
	returnBuffer[wastString.size()] = 0;

	*out_num_chars = wastString.size();
	return returnBuffer;
}

bool wasm_module_validate(const char* binary, size_t numBinaryBytes)
{
	IR::Module irModule;
	WASM::LoadError loadError;
	if(WASM::loadBinaryModule((const U8*)binary, numBinaryBytes, irModule, &loadError))
	{ return true; }
	else
	{
		Log::printf(Log::debug, "%s\n", loadError.message.c_str());
		return false;
	}
}

size_t wasm_module_num_imports(const wasm_module_t* module)
{
	return getModuleIR(module->module).imports.size();
}
void wasm_module_import(const wasm_module_t* module, size_t index, wasm_import_t* out_import)
{
	const IR::Module& irModule = getModuleIR(module->module);
	const KindAndIndex& kindAndIndex = irModule.imports[index];
	switch(kindAndIndex.kind)
	{
	case ExternKind::function: {
		const auto& functionImport = irModule.functions.imports[kindAndIndex.index];
		out_import->module = functionImport.moduleName.c_str();
		out_import->num_module_bytes = functionImport.moduleName.size();
		out_import->name = functionImport.exportName.c_str();
		out_import->num_name_bytes = functionImport.exportName.size();
		out_import->type = as_externtype(irModule.types[functionImport.type.index]);
		break;
	}
	case ExternKind::table: {
		const auto& tableImport = irModule.tables.imports[kindAndIndex.index];
		out_import->module = tableImport.moduleName.c_str();
		out_import->num_module_bytes = tableImport.moduleName.size();
		out_import->name = tableImport.exportName.c_str();
		out_import->num_name_bytes = tableImport.exportName.size();
		out_import->type = as_externtype(tableImport.type);
		break;
	}
	case ExternKind::memory: {
		const auto& memoryImport = irModule.memories.imports[kindAndIndex.index];
		out_import->module = memoryImport.moduleName.c_str();
		out_import->num_module_bytes = memoryImport.moduleName.size();
		out_import->name = memoryImport.exportName.c_str();
		out_import->num_name_bytes = memoryImport.exportName.size();
		out_import->type = as_externtype(memoryImport.type);
		break;
	}
	case ExternKind::global: {
		const auto& globalImport = irModule.globals.imports[kindAndIndex.index];
		out_import->module = globalImport.moduleName.c_str();
		out_import->num_module_bytes = globalImport.moduleName.size();
		out_import->name = globalImport.exportName.c_str();
		out_import->num_name_bytes = globalImport.exportName.size();
		out_import->type = as_externtype(globalImport.type);
		break;
	}
	case ExternKind::exceptionType: {
		Errors::fatal("wasm_module_import can't handle exception type imports");
	}

	case ExternKind::invalid:
	default: WAVM_UNREACHABLE();
	};
}
size_t wasm_module_num_exports(const wasm_module_t* module)
{
	return getModuleIR(module->module).exports.size();
}
void wasm_module_export(const wasm_module_t* module, size_t index, wasm_export_t* out_export)
{
	const IR::Module& irModule = getModuleIR(module->module);
	const Export& export_ = irModule.exports[index];
	out_export->name = export_.name.c_str();
	out_export->num_name_bytes = export_.name.size();
	switch(export_.kind)
	{
	case ExternKind::function:
		out_export->type
			= as_externtype(irModule.types[irModule.functions.getType(export_.index).index]);
		break;
	case ExternKind::table:
		out_export->type = as_externtype(irModule.tables.getType(export_.index));
		break;
	case ExternKind::memory:
		out_export->type = as_externtype(irModule.memories.getType(export_.index));
		break;
	case ExternKind::global:
		out_export->type = as_externtype(irModule.globals.getType(export_.index));
		break;
	case ExternKind::exceptionType:
		Errors::fatal("wasm_module_export can't handle exception type exports");

	case ExternKind::invalid:
	default: WAVM_UNREACHABLE();
	};
}

// wasm_func_t

IMPLEMENT_SHAREABLE_REF(func, Function)

wasm_func_t* wasm_func_new(wasm_compartment_t* compartment,
						   const wasm_functype_t* type,
						   wasm_func_callback_t callback,
						   const char* debug_name)
{
	FunctionType callbackType(
		type->type.results(), type->type.params(), CallingConvention::cAPICallback);
	Intrinsics::Module intrinsicModule;
	Intrinsics::Function intrinsicFunction(
		&intrinsicModule, debug_name, (void*)callback, callbackType);
	Instance* instance = Intrinsics::instantiateModule(compartment, {&intrinsicModule}, debug_name);
	Function* function = getTypedInstanceExport(instance, debug_name, type->type);
	addGCRoot(function);
	return function;
}
wasm_func_t* wasm_func_new_with_env(wasm_compartment_t*,
									const wasm_functype_t* type,
									wasm_func_callback_with_env_t,
									void* env,
									void (*finalizer)(void*),
									const char* debug_name)
{
	Errors::fatal("wasm_func_new_with_env is not yet supported");
}
wasm_functype_t* wasm_func_type(const wasm_func_t* function)
{
	return new wasm_functype_t(getFunctionType(function));
}
size_t wasm_func_param_arity(const wasm_func_t* function)
{
	return getFunctionType(function).params().size();
}
size_t wasm_func_result_arity(const wasm_func_t* function)
{
	return getFunctionType(function).results().size();
}

wasm_trap_t* wasm_func_call(wasm_store_t* store,
							const wasm_func_t* function,
							const wasm_val_t args[],
							wasm_val_t outResults[])
{
	Exception* exception = nullptr;
	catchRuntimeExceptions(
		[store, function, &args, &outResults]() {
			FunctionType functionType = getFunctionType((Function*)function);
			auto wavmArgs
				= (UntaggedValue*)alloca(functionType.params().size() * sizeof(UntaggedValue));
			for(Uptr argIndex = 0; argIndex < functionType.params().size(); ++argIndex)
			{ memcpy(&wavmArgs[argIndex].bytes, &args[argIndex], sizeof(wasm_val_t)); }

			auto wavmResults
				= (UntaggedValue*)alloca(functionType.results().size() * sizeof(UntaggedValue));
			invokeFunction(store, function, functionType, wavmArgs, wavmResults);

			for(Uptr resultIndex = 0; resultIndex < functionType.results().size(); ++resultIndex)
			{
				memcpy(
					&outResults[resultIndex], &wavmResults[resultIndex].bytes, sizeof(wasm_val_t));
			}
		},
		[&exception](Exception* caughtException) { exception = caughtException; });

	return exception;
}

// wasm_global_t
IMPLEMENT_REF(global, Global)

wasm_global_t* wasm_global_new(wasm_compartment_t* compartment,
							   const wasm_globaltype_t* type,
							   const wasm_val_t* value,
							   const char* debug_name)
{
	Global* global = createGlobal(compartment, type->type, std::string(debug_name));
	addGCRoot(global);
	initializeGlobal(global, asValue(type->type.valueType, value));
	return global;
}

wasm_globaltype_t* wasm_global_type(const wasm_global_t* global)
{
	return as_externtype(getGlobalType(global));
}

void wasm_global_get(wasm_store_t* store, const wasm_global_t* global, wasm_val_t* out)
{
	*out = as_val(getGlobalValue(store, global));
}

void wasm_global_set(wasm_global_t*, const wasm_val_t*);

// wasm_table_t
IMPLEMENT_SHAREABLE_REF(table, Table)

wasm_table_t* wasm_table_new(wasm_compartment_t* compartment,
							 const wasm_tabletype_t* type,
							 wasm_ref_t* init,
							 const char* debug_name)
{
	Table* table = createTable(compartment, type->type, init, std::string(debug_name));
	addGCRoot(table);
	return table;
}

wasm_tabletype_t* wasm_table_type(const wasm_table_t* table)
{
	return as_externtype(getTableType(table));
}

wasm_ref_t* wasm_table_get(const wasm_table_t* table, wasm_table_size_t index)
{
	return getTableElement(table, index);
}
bool wasm_table_set(wasm_table_t* table, wasm_table_size_t index, wasm_ref_t* value)
{
	bool result = true;
	catchRuntimeExceptions([table, index, value]() { setTableElement(table, index, value); },
						   [&result](Runtime::Exception* exception) { result = false; });
	return result;
}

wasm_table_size_t wasm_table_size(const wasm_table_t* table)
{
	Uptr numElements = getTableNumElements(table);
	WAVM_ERROR_UNLESS(numElements <= WASM_TABLE_SIZE_MAX);
	return wasm_table_size_t(numElements);
}

bool wasm_table_grow(wasm_table_t* table,
					 wasm_table_size_t delta,
					 wasm_ref_t* init,
					 wasm_table_size_t* out_previous_size)
{
	Uptr oldNumElements = 0;
	if(growTable(table, delta, &oldNumElements, init) != GrowResult::success) { return false; }
	else
	{
		WAVM_ERROR_UNLESS(oldNumElements <= WASM_TABLE_SIZE_MAX);
		if(out_previous_size) { *out_previous_size = wasm_table_size_t(oldNumElements); }
		return true;
	}
}

// wasm_memory_t
IMPLEMENT_SHAREABLE_REF(memory, Memory)

wasm_memory_t* wasm_memory_new(wasm_compartment_t* compartment,
							   const wasm_memorytype_t* type,
							   const char* debug_name)
{
	Memory* memory = createMemory(compartment, type->type, std::string(debug_name));
	addGCRoot(memory);
	return memory;
}

wasm_memorytype_t* wasm_memory_type(const wasm_memory_t* memory)
{
	return as_externtype(getMemoryType(memory));
}

char* wasm_memory_data(wasm_memory_t* memory) { return (char*)getMemoryBaseAddress(memory); }
size_t wasm_memory_data_size(const wasm_memory_t* memory)
{
	return getMemoryNumPages(memory) * IR::numBytesPerPage;
}

wasm_memory_pages_t wasm_memory_size(const wasm_memory_t* memory)
{
	Uptr numPages = getMemoryNumPages(memory);
	WAVM_ERROR_UNLESS(numPages <= WASM_MEMORY_PAGES_MAX);
	return wasm_memory_pages_t(numPages);
}
bool wasm_memory_grow(wasm_memory_t* memory,
					  wasm_memory_pages_t delta,
					  wasm_memory_pages_t* out_previous_size)
{
	Uptr oldNumPages = 0;
	if(growMemory(memory, delta, &oldNumPages) != GrowResult::success) { return false; }
	else
	{
		WAVM_ERROR_UNLESS(oldNumPages <= WASM_MEMORY_PAGES_MAX);
		if(out_previous_size) { *out_previous_size = wasm_memory_pages_t(oldNumPages); }
		return true;
	}
}

// wasm_extern_t
IMPLEMENT_REF(extern, Object)

wasm_externkind_t wasm_extern_kind(const wasm_extern_t* object)
{
	switch(object->kind)
	{
	case ObjectKind::function: return WASM_EXTERN_FUNC;
	case ObjectKind::table: return WASM_EXTERN_TABLE;
	case ObjectKind::memory: return WASM_EXTERN_MEMORY;
	case ObjectKind::global: return WASM_EXTERN_GLOBAL;
	case ObjectKind::exceptionType: Errors::fatal("wasm_extern_kind can't handle exception types");

	case ObjectKind::instance:
	case ObjectKind::context:
	case ObjectKind::compartment:
	case ObjectKind::foreign:
	case ObjectKind::invalid:
	default: WAVM_UNREACHABLE();
	};
}
wasm_externtype_t* wasm_extern_type(const wasm_extern_t* object)
{
	return as_externtype(getExternType(object));
}

#define IMPLEMENT_EXTERN_SUBTYPE(name, Name)                                                       \
	wasm_extern_t* wasm_##name##_as_extern(wasm_##name##_t* name) { return asObject(name); }       \
	const wasm_extern_t* wasm_##name##_as_extern_const(const wasm_##name##_t* name)                \
	{                                                                                              \
		return asObject(name);                                                                     \
	}                                                                                              \
	wasm_##name##_t* wasm_extern_as_##name(wasm_extern_t* object) { return as##Name(object); }     \
	const wasm_##name##_t* wasm_extern_as_##name##_const(const wasm_extern_t* object)              \
	{                                                                                              \
		return as##Name(object);                                                                   \
	}

IMPLEMENT_EXTERN_SUBTYPE(func, Function)
IMPLEMENT_EXTERN_SUBTYPE(global, Global)
IMPLEMENT_EXTERN_SUBTYPE(table, Table)
IMPLEMENT_EXTERN_SUBTYPE(memory, Memory)

// wasm_instance_t
wasm_instance_t* wasm_instance_new(wasm_store_t* store,
								   const wasm_module_t* module,
								   const wasm_extern_t* const imports[],
								   wasm_trap_t** out_trap,
								   const char* debug_name)
{
	const IR::Module& irModule = getModuleIR(module->module);

	if(out_trap) { *out_trap = nullptr; }

	ImportBindings importBindings;
	for(Uptr importIndex = 0; importIndex < irModule.imports.size(); ++importIndex)
	{ importBindings.push_back(const_cast<Object*>(imports[importIndex])); }

	Instance* instance = nullptr;
	catchRuntimeExceptions(
		[store, module, &importBindings, &instance, debug_name]() {
			instance = instantiateModule(getCompartment(store),
										 module->module,
										 std::move(importBindings),
										 std::string(debug_name));

			addGCRoot(instance);

			Function* startFunction = getStartFunction(instance);
			if(startFunction) { invokeFunction(store, startFunction); }
		},
		[&](Runtime::Exception* exception) {
			if(out_trap) { *out_trap = exception; }
			else
			{
				destroyException(exception);
			}
		});

	return instance;
}

void wasm_instance_delete(wasm_instance_t* instance) { removeGCRoot(instance); }

size_t wasm_instance_num_exports(const wasm_instance_t* instance)
{
	return getInstanceExports(instance).size();
}

wasm_extern_t* wasm_instance_export(const wasm_instance_t* instance, size_t index)
{
	return getInstanceExports(instance)[index];
}
}
