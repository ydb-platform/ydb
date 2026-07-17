#pragma once

#include <stdint.h>
#include <cstddef>
#include <memory>
#include <string>
#include <vector>
#include "WAVM/IR/FeatureSpec.h"
#include "WAVM/IR/IR.h"
#include "WAVM/IR/Types.h"
#include "WAVM/Inline/Assert.h"
#include "WAVM/Inline/BasicTypes.h"
#include "WAVM/Inline/Errors.h"

namespace WAVM { namespace IR {
	enum class Opcode : U16;

	// An initializer expression: serialized like any other code, but only supports a few specific
	// instructions.
	template<typename Ref> struct InitializerExpressionBase
	{
		enum class Type : U16
		{
			i32_const = 0x0041,
			i64_const = 0x0042,
			f32_const = 0x0043,
			f64_const = 0x0044,
			v128_const = 0xfd02,
			global_get = 0x0023,
			ref_null = 0x00d0,
			ref_func = 0x00d2,
			invalid = 0xffff
		};
		union
		{
			Type type;
			Opcode typeOpcode;
		};
		union
		{
			I32 i32;
			I64 i64;
			F32 f32;
			F64 f64;
			V128 v128;
			Ref ref;
			ReferenceType nullReferenceType;
		};
		InitializerExpressionBase() : type(Type::invalid) {}
		InitializerExpressionBase(I32 inI32) : type(Type::i32_const), i32(inI32) {}
		InitializerExpressionBase(I64 inI64) : type(Type::i64_const), i64(inI64) {}
		InitializerExpressionBase(F32 inF32) : type(Type::f32_const), f32(inF32) {}
		InitializerExpressionBase(F64 inF64) : type(Type::f64_const), f64(inF64) {}
		InitializerExpressionBase(V128 inV128) : type(Type::v128_const), v128(inV128) {}
		InitializerExpressionBase(Type inType, Ref inRef) : type(inType), ref(inRef)
		{
			WAVM_ASSERT(type == Type::global_get || type == Type::ref_func);
		}
		InitializerExpressionBase(ReferenceType inNullReferenceType)
		: type(Type::ref_null), nullReferenceType(inNullReferenceType)
		{
		}

		friend bool operator==(const InitializerExpressionBase& a,
							   const InitializerExpressionBase& b)
		{
			if(a.type != b.type) { return false; }
			switch(a.type)
			{
			case Type::i32_const: return a.i32 == b.i32;
			case Type::i64_const: return a.i64 == b.i64;
			// For FP constants, use integer comparison to test for bitwise equality. Using FP
			// comparison can't distinguish when NaNs are identical.
			case Type::f32_const: return a.i32 == b.i32;
			case Type::f64_const: return a.i64 == b.i64;
			case Type::v128_const:
				return a.v128.u64x2[0] == b.v128.u64x2[0] && a.v128.u64x2[1] == b.v128.u64x2[1];
			case Type::global_get: return a.ref == b.ref;
			case Type::ref_null: return true;
			case Type::ref_func: return a.ref == b.ref;
			case Type::invalid: return true;
			default: WAVM_UNREACHABLE();
			};
		}

		friend bool operator!=(const InitializerExpressionBase& a,
							   const InitializerExpressionBase& b)
		{
			return !(a == b);
		}
	};

	typedef InitializerExpressionBase<Uptr> InitializerExpression;

	// A function definition
	struct FunctionDef
	{
		IndexedFunctionType type;
		std::vector<ValueType> nonParameterLocalTypes;
		std::vector<U8> code;
		std::vector<std::vector<Uptr>> branchTables;
	};

	// A table definition
	struct TableDef
	{
		TableType type;
	};

	// A memory definition
	struct MemoryDef
	{
		MemoryType type;
	};

	// A global definition
	struct GlobalDef
	{
		GlobalType type;
		InitializerExpression initializer;
	};

	// A tagged tuple type definition
	struct ExceptionTypeDef
	{
		ExceptionType type;
	};

	// Describes an object imported into a module or a specific type
	template<typename Type> struct Import
	{
		Type type;
		std::string moduleName;
		std::string exportName;
	};

	typedef Import<IndexedFunctionType> FunctionImport;
	typedef Import<TableType> TableImport;
	typedef Import<MemoryType> MemoryImport;
	typedef Import<GlobalType> GlobalImport;
	typedef Import<ExceptionType> ExceptionTypeImport;

	// Describes an export from a module.
	struct Export
	{
		std::string name;

		ExternKind kind;

		// An index into the module's kind-specific IndexSpace.
		Uptr index;
	};

	// Identifies an element of a kind-specific IndexSpace in a module.
	struct KindAndIndex
	{
		ExternKind kind;
		Uptr index;

		friend bool operator==(const KindAndIndex& left, const KindAndIndex& right)
		{
			return left.kind == right.kind && left.index == right.index;
		}
	};

	// A data segment: a literal sequence of bytes that is copied into a Runtime::Memory when
	// instantiating a module
	struct DataSegment
	{
		bool isActive;
		Uptr memoryIndex;
		InitializerExpression baseOffset;
		std::shared_ptr<std::vector<U8>> data;
	};

	// An element expression: a literal reference used to initialize a table element.
	struct ElemExpr
	{
		enum class Type
		{
			invalid = 0,

			// These must match the corresponding Opcode members.
			ref_null = 0xd0,
			ref_func = 0xd2
		};
		union
		{
			Type type;
			Opcode typeOpcode;
		};
		union
		{
			Uptr index;
			ReferenceType nullReferenceType;
		};

		ElemExpr() : type(Type::invalid) {}

		ElemExpr(ReferenceType inNullReferenceType)
		: type(Type::ref_null), nullReferenceType(inNullReferenceType)
		{
		}

		ElemExpr(Type inType, Uptr inIndex = UINTPTR_MAX) : type(inType), index(inIndex) {}

		friend bool operator==(const ElemExpr& a, const ElemExpr& b)
		{
			if(a.type != b.type) { return false; }
			switch(a.type)
			{
			case ElemExpr::Type::ref_func: return a.index == b.index;
			case ElemExpr::Type::ref_null: return true;

			case ElemExpr::Type::invalid:
			default: WAVM_UNREACHABLE();
			}
		}

		friend bool operator!=(const ElemExpr& a, const ElemExpr& b)
		{
			if(a.type != b.type) { return true; }
			switch(a.type)
			{
			case ElemExpr::Type::ref_func: return a.index != b.index;
			case ElemExpr::Type::ref_null: return false;

			case ElemExpr::Type::invalid:
			default: WAVM_UNREACHABLE();
			}
		}
	};

	// An elem segment: a literal sequence of table elements.
	struct ElemSegment
	{
		enum class Encoding
		{
			index,
			expr,
		};

		enum class Type
		{
			active,
			passive,
			declared
		};
		Type type;

		// Only valid if type == active.
		Uptr tableIndex;
		InitializerExpression baseOffset;

		struct Contents
		{
			Encoding encoding;

			// Only valid if encoding == expr.
			ReferenceType elemType;
			std::vector<ElemExpr> elemExprs;

			// Only valid if encoding == index.
			ExternKind externKind;
			std::vector<Uptr> elemIndices;
		};
		std::shared_ptr<Contents> contents;
	};

	// Identifies sections in the binary format of a module in the order they are required to occur.
	enum class OrderedSectionID : U8
	{
		moduleBeginning,

		type,
		import,
		function,
		table,
		memory,
		global,
		exceptionType,
		export_,
		start,
		elem,
		dataCount,
		code,
		data,
	};

	WAVM_API const char* asString(OrderedSectionID id);

	// A custom module section as an array of bytes
	struct CustomSection
	{
		OrderedSectionID afterSection{OrderedSectionID::moduleBeginning};
		std::string name;
		std::vector<U8> data;

		CustomSection() = default;
		CustomSection(OrderedSectionID inAfterSection,
					  std::string&& inName,
					  std::vector<U8>&& inData)
		: afterSection(inAfterSection), name(std::move(inName)), data(std::move(inData))
		{
		}
	};

	// An index-space for imports and definitions of a specific kind.
	template<typename Definition, typename Type> struct IndexSpace
	{
		std::vector<Import<Type>> imports;
		std::vector<Definition> defs;

		Uptr size() const { return imports.size() + defs.size(); }
		const Type& getType(Uptr index) const
		{
			if(index < imports.size()) { return imports[index].type; }
			else
			{
				return defs[index - imports.size()].type;
			}
		}
		bool isImport(Uptr index) const
		{
			WAVM_ASSERT(index < size());
			return index < imports.size();
		}
		bool isDef(Uptr index) const
		{
			WAVM_ASSERT(index < size());
			return index >= imports.size();
		}
		const Definition& getDef(Uptr index) const
		{
			WAVM_ASSERT(isDef(index));
			return defs[index - imports.size()];
		}
	};

	// A WebAssembly module definition
	struct Module
	{
		FeatureSpec featureSpec;

		std::vector<FunctionType> types;

		IndexSpace<FunctionDef, IndexedFunctionType> functions;
		IndexSpace<TableDef, TableType> tables;
		IndexSpace<MemoryDef, MemoryType> memories;
		IndexSpace<GlobalDef, GlobalType> globals;
		IndexSpace<ExceptionTypeDef, ExceptionType> exceptionTypes;

		std::vector<KindAndIndex> imports;
		std::vector<Export> exports;
		std::vector<DataSegment> dataSegments;
		std::vector<ElemSegment> elemSegments;
		std::vector<CustomSection> customSections;

		Uptr startFunctionIndex;

		Module(const FeatureSpec& inFeatureSpec = FeatureSpec())
		: featureSpec(inFeatureSpec), startFunctionIndex(UINTPTR_MAX)
		{
		}
	};

	// Finds a named custom section in a module.
	WAVM_API bool findCustomSection(const Module& module,
									const char* customSectionName,
									Uptr& outCustomSectionIndex);

	// Inserts a named custom section in a module, before any custom sections that come after later
	// known sections, but after all custom sections that come after the same known section.
	WAVM_API void insertCustomSection(Module& module, CustomSection&& customSection);

	// Functions that determine whether the binary form of a module will have specific sections.

	inline bool hasTypeSection(const Module& module) { return module.types.size() > 0; }
	inline bool hasImportSection(const Module& module)
	{
		WAVM_ASSERT((module.imports.size() > 0)
					== (module.functions.imports.size() > 0 || module.tables.imports.size() > 0
						|| module.memories.imports.size() > 0 || module.globals.imports.size() > 0
						|| module.exceptionTypes.imports.size() > 0));
		return module.imports.size() > 0;
	}
	inline bool hasFunctionSection(const Module& module)
	{
		return module.functions.defs.size() > 0;
	}
	inline bool hasTableSection(const Module& module) { return module.tables.defs.size() > 0; }
	inline bool hasMemorySection(const Module& module) { return module.memories.defs.size() > 0; }
	inline bool hasGlobalSection(const Module& module) { return module.globals.defs.size() > 0; }
	inline bool hasExceptionTypeSection(const Module& module)
	{
		return module.exceptionTypes.defs.size() > 0;
	}
	inline bool hasExportSection(const Module& module) { return module.exports.size() > 0; }
	inline bool hasStartSection(const Module& module)
	{
		return module.startFunctionIndex != UINTPTR_MAX;
	}
	inline bool hasElemSection(const Module& module) { return module.elemSegments.size() > 0; }
	inline bool hasDataCountSection(const Module& module)
	{
		return module.dataSegments.size() > 0 && module.featureSpec.bulkMemoryOperations;
	}
	inline bool hasCodeSection(const Module& module) { return module.functions.defs.size() > 0; }
	inline bool hasDataSection(const Module& module) { return module.dataSegments.size() > 0; }

	WAVM_API OrderedSectionID getMaxPresentSection(const Module& module,
												   OrderedSectionID maxSection);

	// Resolve an indexed block type to a FunctionType.
	inline FunctionType resolveBlockType(const Module& module, const IndexedBlockType& indexedType)
	{
		switch(indexedType.format)
		{
		case IndexedBlockType::noParametersOrResult: return FunctionType();
		case IndexedBlockType::oneResult: return FunctionType(TypeTuple(indexedType.resultType));
		case IndexedBlockType::functionType: return module.types[indexedType.index];
		default: WAVM_UNREACHABLE();
		};
	}

	// Maps declarations in a module to names to use in disassembly.
	struct DisassemblyNames
	{
		struct Function
		{
			std::string name;
			std::vector<std::string> locals;
			std::vector<std::string> labels;

			Function(std::string&& inName = std::string(),
					 std::initializer_list<std::string>&& inLocals = {},
					 std::initializer_list<std::string>&& inLabels = {})
			: name(std::move(inName)), locals(inLocals), labels(inLabels)
			{
			}
		};

		std::string moduleName;
		std::vector<std::string> types;
		std::vector<Function> functions;
		std::vector<std::string> tables;
		std::vector<std::string> memories;
		std::vector<std::string> globals;
		std::vector<std::string> elemSegments;
		std::vector<std::string> dataSegments;
		std::vector<std::string> exceptionTypes;
	};

	// Looks for a name section in a module. If it exists, deserialize it into outNames.
	// If it doesn't exist, fill outNames with sensible defaults.
	WAVM_API void getDisassemblyNames(const Module& module, DisassemblyNames& outNames);

	// Serializes a DisassemblyNames structure and adds it to the module as a name section.
	WAVM_API void setDisassemblyNames(Module& module, const DisassemblyNames& names);
}}
