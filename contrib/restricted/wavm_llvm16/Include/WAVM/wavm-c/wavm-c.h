// WebAssembly C API

#ifndef __WASM_H
#define __WASM_H

#include <assert.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifndef WASM_C_API
#ifdef _WIN32
#define WASM_C_API __declspec(dllimport)
#else
#define WASM_C_API
#endif
#endif

// Opaque types

#ifndef WASM_OPAQUE_TYPES_DEFINED
typedef struct wasm_config_t wasm_config_t;
typedef struct wasm_engine_t wasm_engine_t;
typedef struct wasm_compartment_t wasm_compartment_t;
typedef struct wasm_store_t wasm_store_t;
typedef struct wasm_valtype_t wasm_valtype_t;
typedef struct wasm_functype_t wasm_functype_t;
typedef struct wasm_tabletype_t wasm_tabletype_t;
typedef struct wasm_memorytype_t wasm_memorytype_t;
typedef struct wasm_globaltype_t wasm_globaltype_t;
typedef struct wasm_externtype_t wasm_externtype_t;
typedef struct wasm_ref_t wasm_ref_t;
typedef struct wasm_trap_t wasm_trap_t;
typedef struct wasm_foreign_t wasm_foreign_t;
typedef struct wasm_module_t wasm_module_t;
typedef struct wasm_func_t wasm_func_t;
typedef struct wasm_table_t wasm_table_t;
typedef struct wasm_memory_t wasm_memory_t;
typedef struct wasm_global_t wasm_global_t;
typedef struct wasm_extern_t wasm_extern_t;
typedef struct wasm_instance_t wasm_instance_t;

typedef struct wasm_shared_module_t wasm_shared_module_t;
typedef struct wasm_shared_func_t wasm_shared_func_t;
typedef struct wasm_shared_table_t wasm_shared_table_t;
typedef struct wasm_shared_memory_t wasm_shared_memory_t;
typedef struct wasm_shared_foreign_t wasm_shared_foreign_t;
#endif

#ifdef __cplusplus
extern "C" {
#endif

///////////////////////////////////////////////////////////////////////////////
// Auxiliaries

typedef float wasm_float32_t;
typedef double wasm_float64_t;

// Ownership

#define own

// The qualifier `own` is used to indicate ownership of data in this API.
// It is intended to be interpreted similar to a `const` qualifier:
//
// - `own wasm_xxx_t*` owns the pointed-to data
// - `own wasm_xxx_t` distributes to all fields of a struct or union `xxx`
// - `own wasm_xxx_vec_t` owns the vector as well as its elements(!)
// - an `own` function parameter passes ownership from caller to callee
// - an `own` function result passes ownership from callee to caller
// - an exception are `own` pointer parameters named `out`, which are copy-back
//   output parameters passing back ownership from callee to caller
//
// Own data is created by `wasm_xxx_new` functions and some others.
// It must be released with the corresponding `wasm_xxx_delete` function.
//
// Deleting a reference does not necessarily delete the underlying object,
// it merely indicates that this owner no longer uses it.
//
// For vectors, `const wasm_xxx_vec_t` is used informally to indicate that
// neither the vector nor its elements should be modified.
// TODO: introduce proper `wasm_xxx_const_vec_t`?

#define WASM_DECLARE_OWN(name) WASM_C_API void wasm_##name##_delete(own wasm_##name##_t*);

///////////////////////////////////////////////////////////////////////////////
// Runtime Environment

// Configuration

WASM_DECLARE_OWN(config)

WASM_C_API own wasm_config_t* wasm_config_new();

#define WASM_DECLARE_FEATURE(feature)                                                              \
	WASM_C_API void wasm_config_feature_set_##feature(wasm_config_t* config, bool enable);

// Standardized, or mature proposed standard extensions that are expected to be standardized without
// breaking backward compatibility: enabled by default.
WASM_DECLARE_FEATURE(import_export_mutable_globals)
WASM_DECLARE_FEATURE(nontrapping_float_to_int)
WASM_DECLARE_FEATURE(sign_extension)
WASM_DECLARE_FEATURE(bulk_memory_ops)

// Proposed standard extensions: disabled by default.
WASM_DECLARE_FEATURE(simd)
WASM_DECLARE_FEATURE(atomics)
WASM_DECLARE_FEATURE(exception_handling)
WASM_DECLARE_FEATURE(multivalue)
WASM_DECLARE_FEATURE(reference_types)
WASM_DECLARE_FEATURE(extended_name_section)
WASM_DECLARE_FEATURE(multimemory)

// Non-standard extensions.
WASM_DECLARE_FEATURE(shared_tables)
WASM_DECLARE_FEATURE(allow_legacy_inst_names)
WASM_DECLARE_FEATURE(any_extern_kind_elems)
WASM_DECLARE_FEATURE(wat_quoted_names)
WASM_DECLARE_FEATURE(wat_custom_sections)

#undef WASM_DECLARE_FEATURE

// Engine

WASM_DECLARE_OWN(engine)

WASM_C_API own wasm_engine_t* wasm_engine_new();
WASM_C_API own wasm_engine_t* wasm_engine_new_with_config(own wasm_config_t*);

// Compartment

WASM_DECLARE_OWN(compartment)

WASM_C_API own wasm_compartment_t* wasm_compartment_new(wasm_engine_t* engine,
														const char* debug_name);
WASM_C_API own wasm_compartment_t* wasm_compartment_clone(const wasm_compartment_t*);

WASM_C_API bool wasm_compartment_contains(const wasm_compartment_t*, const wasm_ref_t*);

// Store

WASM_DECLARE_OWN(store)

WASM_C_API own wasm_store_t* wasm_store_new(wasm_compartment_t*, const char* debug_name);

///////////////////////////////////////////////////////////////////////////////
// Type Representations

// Type attributes

typedef uint8_t wasm_mutability_t;
enum wasm_mutability_enum
{
	WASM_CONST,
	WASM_VAR,
};

typedef uint8_t wasm_shared_t;
enum wasm_shared_enum
{
	WASM_NOTSHARED,
	WASM_SHARED,
};

typedef uint8_t wasm_index_t;
enum wasm_index_enum
{
	WASM_INDEX_I32,
	WASM_INDEX_I64,
};

typedef struct wasm_limits_t
{
	uint32_t min;
	uint32_t max;
} wasm_limits_t;

static const uint32_t wasm_limits_max_default = 0xffffffff;

// Generic

#define WASM_DECLARE_TYPE(name)                                                                    \
	WASM_DECLARE_OWN(name)                                                                         \
                                                                                                   \
	WASM_C_API own wasm_##name##_t* wasm_##name##_copy(wasm_##name##_t*);

// Value Types

WASM_DECLARE_TYPE(valtype)

typedef uint8_t wasm_valkind_t;
enum wasm_valkind_enum
{
	WASM_I32,
	WASM_I64,
	WASM_F32,
	WASM_F64,
	WASM_V128,
	WASM_ANYREF = 128,
	WASM_FUNCREF,
};

WASM_C_API own wasm_valtype_t* wasm_valtype_new(wasm_valkind_t);

WASM_C_API wasm_valkind_t wasm_valtype_kind(const wasm_valtype_t*);

static inline bool wasm_valkind_is_num(wasm_valkind_t k) { return k < WASM_ANYREF; }
static inline bool wasm_valkind_is_ref(wasm_valkind_t k) { return k >= WASM_ANYREF; }

static inline bool wasm_valtype_is_num(const wasm_valtype_t* t)
{
	return wasm_valkind_is_num(wasm_valtype_kind(t));
}
static inline bool wasm_valtype_is_ref(const wasm_valtype_t* t)
{
	return wasm_valkind_is_ref(wasm_valtype_kind(t));
}

// Function Types

WASM_DECLARE_TYPE(functype)

WASM_C_API own wasm_functype_t* wasm_functype_new(own wasm_valtype_t** params,
												  size_t num_params,
												  own wasm_valtype_t** results,
												  size_t num_results);

WASM_C_API size_t wasm_functype_num_params(const wasm_functype_t* type);
WASM_C_API wasm_valtype_t* wasm_functype_param(const wasm_functype_t* type, size_t index);

WASM_C_API size_t wasm_functype_num_results(const wasm_functype_t* type);
WASM_C_API wasm_valtype_t* wasm_functype_result(const wasm_functype_t* type, size_t index);

// Global Types

WASM_DECLARE_TYPE(globaltype)

WASM_C_API own wasm_globaltype_t* wasm_globaltype_new(own wasm_valtype_t*, wasm_mutability_t);

WASM_C_API const wasm_valtype_t* wasm_globaltype_content(const wasm_globaltype_t*);
WASM_C_API wasm_mutability_t wasm_globaltype_mutability(const wasm_globaltype_t*);

// Table Types

WASM_DECLARE_TYPE(tabletype)

WASM_C_API own wasm_tabletype_t* wasm_tabletype_new(own wasm_valtype_t*,
													const wasm_limits_t*,
													wasm_shared_t,
													wasm_index_t);

WASM_C_API const wasm_valtype_t* wasm_tabletype_element(const wasm_tabletype_t*);
WASM_C_API const wasm_limits_t* wasm_tabletype_limits(const wasm_tabletype_t*);
WASM_C_API wasm_shared_t wasm_tabletype_shared(const wasm_tabletype_t*);
WASM_C_API wasm_index_t wasm_tabletype_index(const wasm_tabletype_t*);

// Memory Types

WASM_DECLARE_TYPE(memorytype)

WASM_C_API own wasm_memorytype_t* wasm_memorytype_new(const wasm_limits_t*,
													  wasm_shared_t,
													  wasm_index_t);

WASM_C_API const wasm_limits_t* wasm_memorytype_limits(const wasm_memorytype_t*);
WASM_C_API wasm_shared_t wasm_memorytype_shared(const wasm_memorytype_t*);
WASM_C_API wasm_index_t wasm_memorytype_index(const wasm_memorytype_t*);

// Extern Types

WASM_DECLARE_TYPE(externtype)

typedef uint8_t wasm_externkind_t;
enum wasm_externkind_enum
{
	WASM_EXTERN_FUNC,
	WASM_EXTERN_GLOBAL,
	WASM_EXTERN_TABLE,
	WASM_EXTERN_MEMORY,
};

WASM_C_API wasm_externkind_t wasm_externtype_kind(const wasm_externtype_t*);

WASM_C_API wasm_externtype_t* wasm_functype_as_externtype(wasm_functype_t*);
WASM_C_API wasm_externtype_t* wasm_globaltype_as_externtype(wasm_globaltype_t*);
WASM_C_API wasm_externtype_t* wasm_tabletype_as_externtype(wasm_tabletype_t*);
WASM_C_API wasm_externtype_t* wasm_memorytype_as_externtype(wasm_memorytype_t*);

WASM_C_API wasm_functype_t* wasm_externtype_as_functype(wasm_externtype_t*);
WASM_C_API wasm_globaltype_t* wasm_externtype_as_globaltype(wasm_externtype_t*);
WASM_C_API wasm_tabletype_t* wasm_externtype_as_tabletype(wasm_externtype_t*);
WASM_C_API wasm_memorytype_t* wasm_externtype_as_memorytype(wasm_externtype_t*);

WASM_C_API const wasm_externtype_t* wasm_functype_as_externtype_const(const wasm_functype_t*);
WASM_C_API const wasm_externtype_t* wasm_globaltype_as_externtype_const(const wasm_globaltype_t*);
WASM_C_API const wasm_externtype_t* wasm_tabletype_as_externtype_const(const wasm_tabletype_t*);
WASM_C_API const wasm_externtype_t* wasm_memorytype_as_externtype_const(const wasm_memorytype_t*);

WASM_C_API const wasm_functype_t* wasm_externtype_as_functype_const(const wasm_externtype_t*);
WASM_C_API const wasm_globaltype_t* wasm_externtype_as_globaltype_const(const wasm_externtype_t*);
WASM_C_API const wasm_tabletype_t* wasm_externtype_as_tabletype_const(const wasm_externtype_t*);
WASM_C_API const wasm_memorytype_t* wasm_externtype_as_memorytype_const(const wasm_externtype_t*);

// Imports

typedef struct wasm_import_t
{
	const char* module;
	size_t num_module_bytes;
	const char* name;
	size_t num_name_bytes;
	wasm_externtype_t* type;
} wasm_import_t;

// Exports

typedef struct wasm_export_t
{
	const char* name;
	size_t num_name_bytes;
	wasm_externtype_t* type;
} wasm_export_t;

///////////////////////////////////////////////////////////////////////////////
// Runtime Objects

// Values

// NOTE: not 128-bit aligned
typedef struct wasm_v128_t
{
	uint64_t u64x2[2];
} wasm_v128_t;

typedef union wasm_val_t
{
	int32_t i32;
	int64_t i64;
	wasm_float32_t f32;
	wasm_float64_t f64;
	wasm_v128_t v128;
	wasm_ref_t* ref;
} wasm_val_t;

WASM_C_API void wasm_val_delete(wasm_valkind_t kind, own wasm_val_t* v);
WASM_C_API void wasm_val_copy(wasm_valkind_t kind, own wasm_val_t* out, const wasm_val_t*);

// References

#define WASM_DECLARE_REF_BASE(name)                                                                \
	WASM_DECLARE_OWN(name)                                                                         \
                                                                                                   \
	WASM_C_API own wasm_##name##_t* wasm_##name##_copy(const wasm_##name##_t*);                    \
	WASM_C_API bool wasm_##name##_same(const wasm_##name##_t*, const wasm_##name##_t*);            \
                                                                                                   \
	WASM_C_API void* wasm_##name##_get_host_info(const wasm_##name##_t*);                          \
	WASM_C_API void wasm_##name##_set_host_info(wasm_##name##_t*, void*);                          \
	WASM_C_API void wasm_##name##_set_host_info_with_finalizer(                                    \
		wasm_##name##_t*, void*, void (*)(void*));                                                 \
                                                                                                   \
	WASM_C_API own wasm_##name##_t* wasm_##name##_remap_to_cloned_compartment(                     \
		const wasm_##name##_t*, const wasm_compartment_t*);                                        \
                                                                                                   \
	WASM_C_API const char* wasm_##name##_name(const wasm_##name##_t*);

#define WASM_DECLARE_REF(name)                                                                     \
	WASM_DECLARE_REF_BASE(name)                                                                    \
                                                                                                   \
	WASM_C_API wasm_ref_t* wasm_##name##_as_ref(wasm_##name##_t*);                                 \
	WASM_C_API wasm_##name##_t* wasm_ref_as_##name(wasm_ref_t*);                                   \
	WASM_C_API const wasm_ref_t* wasm_##name##_as_ref_const(const wasm_##name##_t*);               \
	WASM_C_API const wasm_##name##_t* wasm_ref_as_##name##_const(const wasm_ref_t*);

#define WASM_DECLARE_SHAREABLE_REF(name)                                                           \
	WASM_DECLARE_REF(name)                                                                         \
	WASM_DECLARE_OWN(shared_##name)                                                                \
                                                                                                   \
	WASM_C_API own wasm_shared_##name##_t* wasm_##name##_share(const wasm_##name##_t*);            \
	WASM_C_API own wasm_##name##_t* wasm_##name##_obtain(wasm_store_t*,                            \
														 const wasm_shared_##name##_t*);

WASM_DECLARE_REF_BASE(ref)

// Frames

typedef struct wasm_frame_t
{
	wasm_func_t* function;
	size_t instr_index;
} wasm_frame_t;

// Traps

WASM_DECLARE_REF(trap)

WASM_C_API own wasm_trap_t* wasm_trap_new(wasm_compartment_t*,
										  const char* message,
										  size_t num_message_bytes);

WASM_C_API bool wasm_trap_message(const wasm_trap_t*,
								  char* out_message,
								  size_t* inout_num_message_bytes);
WASM_C_API size_t wasm_trap_stack_num_frames(const wasm_trap_t*);
WASM_C_API void wasm_trap_stack_frame(const wasm_trap_t*,
									  size_t index,
									  own wasm_frame_t* out_frame);

// Foreign Objects

WASM_DECLARE_SHAREABLE_REF(foreign)

WASM_C_API own wasm_foreign_t* wasm_foreign_new(wasm_compartment_t*, const char* debug_name);

// Modules

WASM_DECLARE_TYPE(module)

WASM_C_API own wasm_module_t* wasm_module_new(wasm_engine_t*,
											  const char* binary,
											  size_t num_binary_bytes);

WASM_C_API own wasm_module_t* wasm_module_new_text(wasm_engine_t*,
												   const char* text,
												   size_t num_text_chars);

WASM_C_API own char* wasm_module_print(const wasm_module_t* module, size_t* out_num_chars);

WASM_C_API bool wasm_module_validate(const char* binary, size_t num_binary_bytes);

WASM_C_API size_t wasm_module_num_imports(const wasm_module_t* module);
WASM_C_API void wasm_module_import(const wasm_module_t* module,
								   size_t index,
								   own wasm_import_t* out_import);
WASM_C_API size_t wasm_module_num_exports(const wasm_module_t* module);
WASM_C_API void wasm_module_export(const wasm_module_t* module,
								   size_t index,
								   own wasm_export_t* out_export);

// Function Instances

WASM_DECLARE_SHAREABLE_REF(func)

typedef own wasm_trap_t* (*wasm_func_callback_t)(const wasm_val_t args[], wasm_val_t results[]);
typedef own wasm_trap_t* (*wasm_func_callback_with_env_t)(void* env,
														  const wasm_val_t args[],
														  wasm_val_t results[]);

WASM_C_API own wasm_func_t* wasm_func_new(wasm_compartment_t*,
										  const wasm_functype_t*,
										  wasm_func_callback_t,
										  const char* debug_name);
WASM_C_API own wasm_func_t* wasm_func_new_with_env(wasm_compartment_t*,
												   const wasm_functype_t* type,
												   wasm_func_callback_with_env_t,
												   void* env,
												   void (*finalizer)(void*),
												   const char* debug_name);

WASM_C_API own wasm_functype_t* wasm_func_type(const wasm_func_t*);
WASM_C_API size_t wasm_func_param_arity(const wasm_func_t*);
WASM_C_API size_t wasm_func_result_arity(const wasm_func_t*);

WASM_C_API own wasm_trap_t* wasm_func_call(wasm_store_t*,
										   const wasm_func_t*,
										   const wasm_val_t args[],
										   wasm_val_t results[]);

// Global Instances

WASM_DECLARE_REF(global)

WASM_C_API own wasm_global_t* wasm_global_new(wasm_compartment_t*,
											  const wasm_globaltype_t*,
											  const wasm_val_t*,
											  const char* debug_name);

WASM_C_API own wasm_globaltype_t* wasm_global_type(const wasm_global_t*);

WASM_C_API void wasm_global_get(wasm_store_t*, const wasm_global_t*, own wasm_val_t* out);
WASM_C_API void wasm_global_set(wasm_global_t*, const wasm_val_t*);

// Table Instances

WASM_DECLARE_SHAREABLE_REF(table)

typedef uint32_t wasm_table_size_t;

static const wasm_table_size_t WASM_TABLE_SIZE_MAX = UINT32_MAX;

WASM_C_API own wasm_table_t* wasm_table_new(wasm_compartment_t*,
											const wasm_tabletype_t*,
											wasm_ref_t* init,
											const char* debug_name);

WASM_C_API own wasm_tabletype_t* wasm_table_type(const wasm_table_t*);

WASM_C_API own wasm_ref_t* wasm_table_get(const wasm_table_t* table, wasm_table_size_t index);
WASM_C_API bool wasm_table_set(wasm_table_t* table, wasm_table_size_t index, wasm_ref_t* value);

WASM_C_API wasm_table_size_t wasm_table_size(const wasm_table_t* table);
WASM_C_API bool wasm_table_grow(wasm_table_t* table,
								wasm_table_size_t delta,
								wasm_ref_t* init,
								wasm_table_size_t* out_previous_size);

// Memory Instances

WASM_DECLARE_SHAREABLE_REF(memory)

typedef uint32_t wasm_memory_pages_t;

static const wasm_memory_pages_t WASM_MEMORY_PAGES_MAX = UINT32_MAX;

static const size_t MEMORY_PAGE_SIZE = 0x10000;

WASM_C_API own wasm_memory_t* wasm_memory_new(wasm_compartment_t*,
											  const wasm_memorytype_t*,
											  const char* debug_name);

WASM_C_API own wasm_memorytype_t* wasm_memory_type(const wasm_memory_t*);

WASM_C_API char* wasm_memory_data(wasm_memory_t*);
WASM_C_API size_t wasm_memory_data_size(const wasm_memory_t*);

WASM_C_API wasm_memory_pages_t wasm_memory_size(const wasm_memory_t*);
WASM_C_API bool wasm_memory_grow(wasm_memory_t*,
								 wasm_memory_pages_t delta,
								 wasm_memory_pages_t* out_previous_size);

// Externals

WASM_DECLARE_REF(extern)

WASM_C_API wasm_externkind_t wasm_extern_kind(const wasm_extern_t*);
WASM_C_API own wasm_externtype_t* wasm_extern_type(const wasm_extern_t*);

WASM_C_API wasm_extern_t* wasm_func_as_extern(wasm_func_t*);
WASM_C_API wasm_extern_t* wasm_global_as_extern(wasm_global_t*);
WASM_C_API wasm_extern_t* wasm_table_as_extern(wasm_table_t*);
WASM_C_API wasm_extern_t* wasm_memory_as_extern(wasm_memory_t*);

WASM_C_API wasm_func_t* wasm_extern_as_func(wasm_extern_t*);
WASM_C_API wasm_global_t* wasm_extern_as_global(wasm_extern_t*);
WASM_C_API wasm_table_t* wasm_extern_as_table(wasm_extern_t*);
WASM_C_API wasm_memory_t* wasm_extern_as_memory(wasm_extern_t*);

WASM_C_API const wasm_extern_t* wasm_func_as_extern_const(const wasm_func_t*);
WASM_C_API const wasm_extern_t* wasm_global_as_extern_const(const wasm_global_t*);
WASM_C_API const wasm_extern_t* wasm_table_as_extern_const(const wasm_table_t*);
WASM_C_API const wasm_extern_t* wasm_memory_as_extern_const(const wasm_memory_t*);

WASM_C_API const wasm_func_t* wasm_extern_as_func_const(const wasm_extern_t*);
WASM_C_API const wasm_global_t* wasm_extern_as_global_const(const wasm_extern_t*);
WASM_C_API const wasm_table_t* wasm_extern_as_table_const(const wasm_extern_t*);
WASM_C_API const wasm_memory_t* wasm_extern_as_memory_const(const wasm_extern_t*);

// Module Instances

WASM_DECLARE_REF(instance)

WASM_C_API own wasm_instance_t* wasm_instance_new(wasm_store_t*,
												  const wasm_module_t*,
												  const wasm_extern_t* const imports[],
												  own wasm_trap_t**,
												  const char* debug_name);

WASM_C_API size_t wasm_instance_num_exports(const wasm_instance_t*);
WASM_C_API wasm_extern_t* wasm_instance_export(const wasm_instance_t*, size_t index);

///////////////////////////////////////////////////////////////////////////////
// Convenience

// Value Type construction short-hands

static inline own wasm_valtype_t* wasm_valtype_new_i32() { return wasm_valtype_new(WASM_I32); }
static inline own wasm_valtype_t* wasm_valtype_new_i64() { return wasm_valtype_new(WASM_I64); }
static inline own wasm_valtype_t* wasm_valtype_new_f32() { return wasm_valtype_new(WASM_F32); }
static inline own wasm_valtype_t* wasm_valtype_new_f64() { return wasm_valtype_new(WASM_F64); }
static inline own wasm_valtype_t* wasm_valtype_new_v128() { return wasm_valtype_new(WASM_V128); }

static inline own wasm_valtype_t* wasm_valtype_new_externref()
{
	return wasm_valtype_new(WASM_ANYREF);
}
static inline own wasm_valtype_t* wasm_valtype_new_funcref()
{
	return wasm_valtype_new(WASM_FUNCREF);
}

// Function Types construction short-hands

static inline own wasm_functype_t* wasm_functype_new_0_0()
{
	return wasm_functype_new((wasm_valtype_t**)0, 0, (wasm_valtype_t**)0, 0);
}

static inline own wasm_functype_t* wasm_functype_new_1_0(own wasm_valtype_t* p)
{
	wasm_valtype_t* ps[1] = {p};
	return wasm_functype_new(ps, 1, (wasm_valtype_t**)0, 0);
}

static inline own wasm_functype_t* wasm_functype_new_2_0(own wasm_valtype_t* p1,
														 own wasm_valtype_t* p2)
{
	wasm_valtype_t* ps[2] = {p1, p2};
	return wasm_functype_new(ps, 2, (wasm_valtype_t**)0, 0);
}

static inline own wasm_functype_t* wasm_functype_new_3_0(own wasm_valtype_t* p1,
														 own wasm_valtype_t* p2,
														 own wasm_valtype_t* p3)
{
	wasm_valtype_t* ps[3] = {p1, p2, p3};
	return wasm_functype_new(ps, 3, (wasm_valtype_t**)0, 0);
}

static inline own wasm_functype_t* wasm_functype_new_0_1(own wasm_valtype_t* r)
{
	wasm_valtype_t* rs[1] = {r};
	return wasm_functype_new((wasm_valtype_t**)0, 0, rs, 1);
}

static inline own wasm_functype_t* wasm_functype_new_1_1(own wasm_valtype_t* p,
														 own wasm_valtype_t* r)
{
	wasm_valtype_t* ps[1] = {p};
	wasm_valtype_t* rs[1] = {r};
	return wasm_functype_new(ps, 1, rs, 1);
}

static inline own wasm_functype_t* wasm_functype_new_2_1(own wasm_valtype_t* p1,
														 own wasm_valtype_t* p2,
														 own wasm_valtype_t* r)
{
	wasm_valtype_t* ps[2] = {p1, p2};
	wasm_valtype_t* rs[1] = {r};
	return wasm_functype_new(ps, 2, rs, 1);
}

static inline own wasm_functype_t* wasm_functype_new_3_1(own wasm_valtype_t* p1,
														 own wasm_valtype_t* p2,
														 own wasm_valtype_t* p3,
														 own wasm_valtype_t* r)
{
	wasm_valtype_t* ps[3] = {p1, p2, p3};
	wasm_valtype_t* rs[1] = {r};
	return wasm_functype_new(ps, 3, rs, 1);
}

static inline own wasm_functype_t* wasm_functype_new_0_2(own wasm_valtype_t* r1,
														 own wasm_valtype_t* r2)
{
	wasm_valtype_t* rs[2] = {r1, r2};
	return wasm_functype_new((wasm_valtype_t**)0, 0, rs, 2);
}

static inline own wasm_functype_t* wasm_functype_new_1_2(own wasm_valtype_t* p,
														 own wasm_valtype_t* r1,
														 own wasm_valtype_t* r2)
{
	wasm_valtype_t* ps[1] = {p};
	wasm_valtype_t* rs[2] = {r1, r2};
	return wasm_functype_new(ps, 1, rs, 2);
}

static inline own wasm_functype_t* wasm_functype_new_2_2(own wasm_valtype_t* p1,
														 own wasm_valtype_t* p2,
														 own wasm_valtype_t* r1,
														 own wasm_valtype_t* r2)
{
	wasm_valtype_t* ps[2] = {p1, p2};
	wasm_valtype_t* rs[2] = {r1, r2};
	return wasm_functype_new(ps, 2, rs, 2);
}

static inline own wasm_functype_t* wasm_functype_new_3_2(own wasm_valtype_t* p1,
														 own wasm_valtype_t* p2,
														 own wasm_valtype_t* p3,
														 own wasm_valtype_t* r1,
														 own wasm_valtype_t* r2)
{
	wasm_valtype_t* ps[3] = {p1, p2, p3};
	wasm_valtype_t* rs[2] = {r1, r2};
	return wasm_functype_new(ps, 3, rs, 2);
}

///////////////////////////////////////////////////////////////////////////////

#undef own

#ifdef __cplusplus
} // extern "C"
#endif

#endif // #ifdef __WASM_H
