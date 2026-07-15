#pragma once

#include "WAVM/Inline/Assert.h"
#include "WAVM/Inline/BasicTypes.h"

// Features that have been standardized by the W3C WebAssembly Working Group.
#define WAVM_ENUM_STANDARD_FEATURES(V)                                                             \
	V(mvp, "mvp", "WebAssembly MVP")                                                               \
	V(importExportMutableGlobals,                                                                  \
	  "import-export-mutable-globals",                                                             \
	  "Allows importing and exporting mutable globals")                                            \
	V(nonTrappingFloatToInt, "non-trapping-float-to-int", "Non-trapping float-to-int conversion")  \
	V(signExtension, "sign-extension", "Sign-extension")                                           \
	V(multipleResultsAndBlockParams, "multivalue", "Multiple results and block parameters")        \
	V(bulkMemoryOperations, "bulk-memory", "Bulk memory")                                          \
	V(referenceTypes, "ref-types", "Reference types")                                              \
	V(simd, "simd", "128-bit SIMD")

// Extensions that are expected to be standardized without breaking backward compatibility. These
// are enabled by default.
#define WAVM_ENUM_MATURE_FEATURES(V)

// Proposed standard extensions. These are disabled by default, but may be enabled on the
// command-line.
#define WAVM_ENUM_PROPOSED_FEATURES(V)                                                             \
	V(atomics, "atomics", "Shared memories and atomic instructions")                               \
	V(exceptionHandling, "exception-handling", "Exception handling")                               \
	V(extendedNameSection, "extended-name-section", "Extended name section")                       \
	V(multipleMemories, "multi-memory", "Multiple memories")                                       \
	V(memory64, "memory64", "Memories with 64-bit addresses")

// Non-standard extensions. These are disabled by default, but may be enabled on the command-line.
#define WAVM_ENUM_NONSTANDARD_FEATURES(V)                                                          \
	V(sharedTables, "shared-tables", "Shared tables")                                              \
	V(allowLegacyInstructionNames, "legacy-instr-names", "Legacy instruction names")               \
	V(allowAnyExternKindElemSegments,                                                              \
	  "any-extern-kind-elems",                                                                     \
	  "Elem segments containing non-func externs")                                                 \
	V(quotedNamesInTextFormat, "quoted-names", "Quoted names in text format")                      \
	V(customSectionsInTextFormat, "wat-custom-sections", "Custom sections in text format")         \
	V(interleavedLoadStore, "interleaved-load-store", "Interleaved SIMD load&store instructions")  \
	V(table64, "table64", "Tables with 64-bit indices")

// WAVM extensions meant for internal use only (not exposed to users).
#define WAVM_ENUM_INTERNAL_FEATURES(V)                                                             \
	V(nonWASMFunctionTypes, "non-wasm-func-types", "Non-WebAssembly function calling conventions")

#define WAVM_ENUM_FEATURES(V)                                                                      \
	WAVM_ENUM_STANDARD_FEATURES(V)                                                                 \
	WAVM_ENUM_MATURE_FEATURES(V)                                                                   \
	WAVM_ENUM_PROPOSED_FEATURES(V)                                                                 \
	WAVM_ENUM_NONSTANDARD_FEATURES(V)                                                              \
	WAVM_ENUM_INTERNAL_FEATURES(V)

namespace WAVM { namespace IR {

#define VISIT_FEATURE(name, ...) name,
	enum class Feature
	{
		WAVM_ENUM_FEATURES(VISIT_FEATURE)
	};
#undef VISIT_FEATURE

	// A feature level hierarchy where each feature level is a superset of the previous.
	enum class FeatureLevel
	{
		mvp,      // Only the WebAssembly MVP.
		standard, // The above + standard extensions.
		mature,   // The above + mature proposed standard extensions.
		proposed, // The above + all proposed standard extensions.
		wavm,     // The above + non-standard WAVM extensions.
	};

	struct FeatureSpec
	{
		// Declare a bool member for each feature.
#define VISIT_FEATURE_DEFAULT_ON(name, ...) bool name;
#define VISIT_FEATURE_DEFAULT_OFF(name, ...) bool name;
		WAVM_ENUM_STANDARD_FEATURES(VISIT_FEATURE_DEFAULT_ON)
		WAVM_ENUM_MATURE_FEATURES(VISIT_FEATURE_DEFAULT_ON)
		WAVM_ENUM_PROPOSED_FEATURES(VISIT_FEATURE_DEFAULT_OFF)
		WAVM_ENUM_NONSTANDARD_FEATURES(VISIT_FEATURE_DEFAULT_OFF)
		WAVM_ENUM_INTERNAL_FEATURES(VISIT_FEATURE_DEFAULT_OFF)
#undef VISIT_FEATURE_DEFAULT_ON
#undef VISIT_FEATURE_DEFAULT_OFF

		Uptr maxLocals = 65536;
		Uptr maxLabelsPerFunction = UINTPTR_MAX;
		Uptr maxDataSegments = UINTPTR_MAX;
		Uptr maxSyntaxRecursion = 500;

		FeatureSpec(FeatureLevel featureLevel = FeatureLevel::mature)
		{
			setFeatureLevel(featureLevel);
		}

		WAVM_API void setFeatureLevel(FeatureLevel featureLevel);
	};

	WAVM_API Feature getFeatureByName(const char* name);
	WAVM_API const char* getFeatureName(Feature feature);
}}
