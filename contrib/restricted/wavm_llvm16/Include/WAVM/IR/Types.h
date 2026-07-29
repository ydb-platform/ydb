#pragma once

#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <initializer_list>
#include <string>
#include <vector>
#include "WAVM/IR/IR.h"
#include "WAVM/Inline/Assert.h"
#include "WAVM/Inline/BasicTypes.h"
#include "WAVM/Inline/Errors.h"
#include "WAVM/Inline/Hash.h"

namespace WAVM { namespace Runtime {
	struct Object;
	struct Function;
}}

namespace WAVM { namespace IR {
	// The type of a WebAssembly operand
	enum class ValueType : U8
	{
		none,
		any,
		i32,
		i64,
		f32,
		f64,
		v128,
		externref,
		funcref
	};

	static constexpr U8 numValueTypes = U8(ValueType::funcref) + 1;

	// The reference types subset of ValueType.
	enum class ReferenceType : U8
	{
		none = U8(ValueType::none),

		externref = U8(ValueType::externref),
		funcref = U8(ValueType::funcref)
	};

	inline ValueType asValueType(ReferenceType type) { return ValueType(type); }

	inline bool isNumericType(ValueType type)
	{
		return type == ValueType::i32 || type == ValueType::i64 || type == ValueType::f32
			   || type == ValueType::f64 || type == ValueType::v128;
	}

	inline bool isReferenceType(ValueType type)
	{
		return type == ValueType::externref || type == ValueType::funcref;
	}

	inline bool isSubtype(ValueType subtype, ValueType supertype)
	{
		return subtype == supertype || subtype == ValueType::none || supertype == ValueType::any;
	}

	inline bool isSubtype(ReferenceType subtype, ReferenceType supertype)
	{
		return isSubtype(asValueType(subtype), asValueType(supertype));
	}

	inline std::string asString(I32 value) { return std::to_string(value); }
	inline std::string asString(I64 value) { return std::to_string(value); }
	WAVM_API std::string asString(F32 value);
	WAVM_API std::string asString(F64 value);

	inline std::string asString(const V128& v128)
	{
		// buffer needs 50 characters:
		// i32x4 0xHHHHHHHH 0xHHHHHHHH 0xHHHHHHHH 0xHHHHHHHH\0
		char buffer[50];
		snprintf(buffer,
				 sizeof(buffer),
				 "i32x4 0x%.8x 0x%.8x 0x%.8x 0x%.8x",
				 v128.u32x4[0],
				 v128.u32x4[1],
				 v128.u32x4[2],
				 v128.u32x4[3]);
		return std::string(buffer);
	}

	inline U8 getTypeByteWidth(ValueType type)
	{
		switch(type)
		{
		case ValueType::i32: return 4;
		case ValueType::i64: return 8;
		case ValueType::f32: return 4;
		case ValueType::f64: return 8;
		case ValueType::v128: return 16;
		case ValueType::externref:
		case ValueType::funcref: return sizeof(void*);

		case ValueType::none:
		case ValueType::any:
		default: WAVM_UNREACHABLE();
		};
	}

	inline U8 getTypeBitWidth(ValueType type) { return getTypeByteWidth(type) * 8; }

	inline const char* asString(ValueType type)
	{
		switch(type)
		{
		case ValueType::none: return "none";
		case ValueType::any: return "any";
		case ValueType::i32: return "i32";
		case ValueType::i64: return "i64";
		case ValueType::f32: return "f32";
		case ValueType::f64: return "f64";
		case ValueType::v128: return "v128";
		case ValueType::externref: return "externref";
		case ValueType::funcref: return "funcref";
		default: WAVM_UNREACHABLE();
		};
	}

	inline const char* asString(ReferenceType type)
	{
		switch(type)
		{
		case ReferenceType::none: return "none";
		case ReferenceType::externref: return "externref";
		case ReferenceType::funcref: return "funcref";
		default: WAVM_UNREACHABLE();
		};
	}

	// The tuple of value types.
	struct TypeTuple
	{
		TypeTuple() : impl(getUniqueImpl(0, nullptr)) {}
		WAVM_API TypeTuple(ValueType inElem);
		WAVM_API TypeTuple(const std::initializer_list<ValueType>& inElems);
		WAVM_API TypeTuple(const std::vector<ValueType>& inElems);
		WAVM_API TypeTuple(const ValueType* inElems, Uptr numElems);

		const ValueType* begin() const { return impl->elems; }
		const ValueType* end() const { return impl->elems + impl->numElems; }
		const ValueType* data() const { return impl->elems; }

		ValueType operator[](Uptr index) const
		{
			WAVM_ASSERT(index < impl->numElems);
			return impl->elems[index];
		}

		Uptr getHash() const { return impl->hash; }
		Uptr size() const { return impl->numElems; }

		friend bool operator==(const TypeTuple& left, const TypeTuple& right)
		{
			return left.impl == right.impl;
		}
		friend bool operator!=(const TypeTuple& left, const TypeTuple& right)
		{
			return left.impl != right.impl;
		}

	private:
		struct Impl
		{
			Uptr hash;
			Uptr numElems;
			ValueType elems[1];

			Impl(Uptr inNumElems, const ValueType* inElems);
			Impl(const Impl& inCopy);

			static Uptr calcNumBytes(Uptr numElems)
			{
				return offsetof(Impl, elems) + numElems * sizeof(ValueType);
			}
		};

		const Impl* impl;

		TypeTuple(const Impl* inImpl) : impl(inImpl) {}

		WAVM_API static const Impl* getUniqueImpl(Uptr numElems, const ValueType* inElems);
	};

	inline std::string asString(TypeTuple typeTuple)
	{
		if(typeTuple.size() == 1) { return asString(typeTuple[0]); }
		else
		{
			std::string result = "(";
			for(Uptr elementIndex = 0; elementIndex < typeTuple.size(); ++elementIndex)
			{
				if(elementIndex != 0) { result += ", "; }
				result += asString(typeTuple[elementIndex]);
			}
			result += ")";
			return result;
		}
	}

	inline bool isSubtype(TypeTuple subtype, TypeTuple supertype)
	{
		if(subtype == supertype) { return true; }
		else if(subtype.size() != supertype.size())
		{
			return false;
		}
		else
		{
			for(Uptr elementIndex = 0; elementIndex < subtype.size(); ++elementIndex)
			{
				if(!isSubtype(subtype[elementIndex], supertype[elementIndex])) { return false; }
			}
			return true;
		}
	}

	// Infer value and result types from a C type.

	template<typename> constexpr ValueType inferValueType();
	template<> constexpr ValueType inferValueType<I8>() { return ValueType::i32; }
	template<> constexpr ValueType inferValueType<U8>() { return ValueType::i32; }
	template<> constexpr ValueType inferValueType<I16>() { return ValueType::i32; }
	template<> constexpr ValueType inferValueType<U16>() { return ValueType::i32; }
	template<> constexpr ValueType inferValueType<I32>() { return ValueType::i32; }
	template<> constexpr ValueType inferValueType<U32>() { return ValueType::i32; }
	template<> constexpr ValueType inferValueType<I64>() { return ValueType::i64; }
	template<> constexpr ValueType inferValueType<U64>() { return ValueType::i64; }
	template<> constexpr ValueType inferValueType<F32>() { return ValueType::f32; }
	template<> constexpr ValueType inferValueType<F64>() { return ValueType::f64; }
	template<> constexpr ValueType inferValueType<Runtime::Object*>()
	{
		return ValueType::externref;
	}
	template<> constexpr ValueType inferValueType<Runtime::Function*>()
	{
		return ValueType::funcref;
	}
	template<> constexpr ValueType inferValueType<const Runtime::Object*>()
	{
		return ValueType::externref;
	}
	template<> constexpr ValueType inferValueType<const Runtime::Function*>()
	{
		return ValueType::funcref;
	}

	template<typename T> inline TypeTuple inferResultType()
	{
		return TypeTuple(inferValueType<T>());
	}
	template<> inline TypeTuple inferResultType<void>() { return TypeTuple(); }

	// Don't allow quietly promoting I8/I16 return types to an I32 WebAssembly type: the C function
	// may not zero the extra bits in the I32 register before returning, and the WebAssembly
	// function will see that junk in the returned I32.
	template<> inline TypeTuple inferResultType<I8>();
	template<> inline TypeTuple inferResultType<U8>();
	template<> inline TypeTuple inferResultType<I16>();
	template<> inline TypeTuple inferResultType<U16>();

	// The calling convention for a function.
	enum class CallingConvention
	{
		wasm,
		intrinsic,
		intrinsicWithContextSwitch,
		c,
		cAPICallback,
	};

	inline std::string asString(CallingConvention callingConvention)
	{
		switch(callingConvention)
		{
		case CallingConvention::wasm: return "wasm";
		case CallingConvention::intrinsic: return "intrinsic";
		case CallingConvention::intrinsicWithContextSwitch: return "intrinsic_with_context_switch";
		case CallingConvention::c: return "c";
		case CallingConvention::cAPICallback: return "c_api_callback";

		default: WAVM_UNREACHABLE();
		};
	}

	// The type of a WebAssembly function
	struct FunctionType
	{
		// Used to represent a function type as an abstract pointer-sized value in the runtime.
		struct Encoding
		{
			Uptr impl;
		};

		FunctionType(TypeTuple inResults = TypeTuple(),
					 TypeTuple inParams = TypeTuple(),
					 CallingConvention inCallingConvention = CallingConvention::wasm)
		: impl(getUniqueImpl(inResults, inParams, inCallingConvention))
		{
		}

		FunctionType(Encoding encoding) : impl(reinterpret_cast<const Impl*>(encoding.impl)) {}

		TypeTuple results() const { return impl->results; }
		TypeTuple params() const { return impl->params; }
		CallingConvention callingConvention() const { return impl->callingConvention; }
		Uptr getHash() const { return impl->hash; }
		Encoding getEncoding() const { return Encoding{reinterpret_cast<Uptr>(impl)}; }

		friend bool operator==(const FunctionType& left, const FunctionType& right)
		{
			return left.impl == right.impl;
		}

		friend bool operator!=(const FunctionType& left, const FunctionType& right)
		{
			return left.impl != right.impl;
		}

	private:
		struct Impl
		{
			Uptr hash;
			TypeTuple results;
			TypeTuple params;
			CallingConvention callingConvention;

			Impl(TypeTuple inResults, TypeTuple inParams, CallingConvention inCallingConvention);
		};

		const Impl* impl;

		FunctionType(const Impl* inImpl) : impl(inImpl) {}

		WAVM_API static const Impl* getUniqueImpl(TypeTuple results,
												  TypeTuple params,
												  CallingConvention callingConvention);
	};

	struct IndexedFunctionType
	{
		Uptr index;
	};

	struct IndexedBlockType
	{
		enum Format
		{
			noParametersOrResult,
			oneResult,
			functionType
		};
		Format format;
		union
		{
			ValueType resultType;
			Uptr index;
		};
	};

	inline std::string asString(const FunctionType& functionType)
	{
		std::string result
			= asString(functionType.params()) + "->" + asString(functionType.results());
		if(functionType.callingConvention() != CallingConvention::wasm)
		{ result += "(calling_conv " + asString(functionType.callingConvention()) + ')'; }
		return result;
	}

	inline bool isSubtype(FunctionType subtype, FunctionType supertype)
	{
		if(subtype == supertype) { return true; }
		else
		{
			return isSubtype(supertype.params(), subtype.params())
				   && isSubtype(subtype.results(), supertype.results())
				   && supertype.callingConvention() == subtype.callingConvention();
		}
	}

	// The index type for a memory or table.
	enum class IndexType : U8
	{
		i32 = U8(ValueType::i32),
		i64 = U8(ValueType::i64),
	};

	inline ValueType asValueType(IndexType indexType)
	{
		switch(indexType)
		{
		case IndexType::i32: return ValueType::i32;
		case IndexType::i64: return ValueType::i64;
		default: WAVM_UNREACHABLE();
		};
	}

	// A size constraint: a range of expected sizes for some size-constrained type.
	// If max==UINT64_MAX, the maximum size is unbounded.
	struct SizeConstraints
	{
		U64 min;
		U64 max;

		friend bool operator==(const SizeConstraints& left, const SizeConstraints& right)
		{
			return left.min == right.min && left.max == right.max;
		}
		friend bool operator!=(const SizeConstraints& left, const SizeConstraints& right)
		{
			return left.min != right.min || left.max != right.max;
		}
		friend bool isSubset(const SizeConstraints& sub, const SizeConstraints& super)
		{
			return sub.min >= super.min && sub.max <= super.max;
		}
	};

	inline std::string asString(const SizeConstraints& sizeConstraints)
	{
		return std::to_string(sizeConstraints.min)
			   + (sizeConstraints.max == UINT64_MAX ? ".."
													: ".." + std::to_string(sizeConstraints.max));
	}

	// The type of a table
	struct TableType
	{
		ReferenceType elementType;
		bool isShared;
		IndexType indexType;
		SizeConstraints size;

		TableType()
		: elementType(ReferenceType::none), isShared(false), indexType(IndexType::i32), size()
		{
		}
		TableType(ReferenceType inElementType,
				  bool inIsShared,
				  IndexType inIndexType,
				  SizeConstraints inSize)
		: elementType(inElementType), isShared(inIsShared), indexType(inIndexType), size(inSize)
		{
		}

		friend bool operator==(const TableType& left, const TableType& right)
		{
			return left.elementType == right.elementType && left.isShared == right.isShared
				   && left.indexType == right.indexType && left.size == right.size;
		}
		friend bool operator!=(const TableType& left, const TableType& right)
		{
			return left.elementType != right.elementType || left.isShared != right.isShared
				   || left.indexType != right.indexType || left.size != right.size;
		}
		friend bool isSubtype(const TableType& sub, const TableType& super)
		{
			return super.elementType == sub.elementType && super.isShared == sub.isShared
				   && super.indexType == sub.indexType && isSubset(sub.size, super.size);
		}
	};

	inline std::string asString(const TableType& tableType)
	{
		const char* indexString;
		switch(tableType.indexType)
		{
		case IndexType::i32: indexString = ""; break;
		case IndexType::i64: indexString = "i64 "; break;
		default: WAVM_UNREACHABLE();
		};

		return std::string(indexString) + asString(tableType.size)
			   + (tableType.isShared ? " shared funcref" : " funcref");
	}

	// The type of a memory
	struct MemoryType
	{
		bool isShared;
		IndexType indexType;
		SizeConstraints size;

		MemoryType() : isShared(false), indexType(IndexType::i32), size({0, UINT64_MAX}) {}
		MemoryType(bool inIsShared, IndexType inIndexType, const SizeConstraints& inSize)
		: isShared(inIsShared), indexType(inIndexType), size(inSize)
		{
		}

		friend bool operator==(const MemoryType& left, const MemoryType& right)
		{
			return left.isShared == right.isShared && left.indexType == right.indexType
				   && left.size == right.size;
		}
		friend bool operator!=(const MemoryType& left, const MemoryType& right)
		{
			return left.isShared != right.isShared || left.indexType != right.indexType
				   || left.size != right.size;
		}
		friend bool isSubtype(const MemoryType& sub, const MemoryType& super)
		{
			return super.isShared == sub.isShared && super.indexType == sub.indexType
				   && isSubset(sub.size, super.size);
		}
	};

	inline std::string asString(const MemoryType& memoryType)
	{
		const char* indexString;
		switch(memoryType.indexType)
		{
		case IndexType::i32: indexString = ""; break;
		case IndexType::i64: indexString = "i64 "; break;
		default: WAVM_UNREACHABLE();
		};

		return std::string(indexString) + asString(memoryType.size)
			   + (memoryType.isShared ? " shared" : "");
	}

	// The type of a global
	struct GlobalType
	{
		ValueType valueType;
		bool isMutable;

		GlobalType() : valueType(ValueType::any), isMutable(false) {}
		GlobalType(ValueType inValueType, bool inIsMutable)
		: valueType(inValueType), isMutable(inIsMutable)
		{
		}

		friend bool operator==(const GlobalType& left, const GlobalType& right)
		{
			return left.valueType == right.valueType && left.isMutable == right.isMutable;
		}
		friend bool operator!=(const GlobalType& left, const GlobalType& right)
		{
			return left.valueType != right.valueType || left.isMutable != right.isMutable;
		}
		friend bool isSubtype(const GlobalType& sub, const GlobalType& super)
		{
			if(super.isMutable != sub.isMutable) { return false; }
			else if(super.isMutable)
			{
				return super.valueType == sub.valueType;
			}
			else
			{
				return isSubtype(sub.valueType, super.valueType);
			}
		}
	};

	inline std::string asString(const GlobalType& globalType)
	{
		if(globalType.isMutable) { return std::string("global ") + asString(globalType.valueType); }
		else
		{
			return std::string("immutable ") + asString(globalType.valueType);
		}
	}

	struct ExceptionType
	{
		TypeTuple params;

		friend bool operator==(const ExceptionType& left, const ExceptionType& right)
		{
			return left.params == right.params;
		}
		friend bool operator!=(const ExceptionType& left, const ExceptionType& right)
		{
			return left.params != right.params;
		}
	};

	inline std::string asString(const ExceptionType& exceptionType)
	{
		return asString(exceptionType.params);
	}

	// The type of an external object: something that can be imported or exported from a module.
	enum class ExternKind : U8
	{
		invalid,

		// Standard object kinds that may be imported/exported from WebAssembly modules.
		function,
		table,
		memory,
		global,
		exceptionType,
	};
	struct ExternType
	{
		const ExternKind kind;

		ExternType() : kind(ExternKind::invalid) {}
		ExternType(FunctionType inFunction) : kind(ExternKind::function), function(inFunction) {}
		ExternType(TableType inTable) : kind(ExternKind::table), table(inTable) {}
		ExternType(MemoryType inMemory) : kind(ExternKind::memory), memory(inMemory) {}
		ExternType(GlobalType inGlobal) : kind(ExternKind::global), global(inGlobal) {}
		ExternType(ExceptionType inExceptionType)
		: kind(ExternKind::exceptionType), exceptionType(inExceptionType)
		{
		}
		ExternType(ExternKind inKind) : kind(inKind) {}

		friend FunctionType asFunctionType(const ExternType& objectType)
		{
			WAVM_ASSERT(objectType.kind == ExternKind::function);
			return objectType.function;
		}
		friend TableType asTableType(const ExternType& objectType)
		{
			WAVM_ASSERT(objectType.kind == ExternKind::table);
			return objectType.table;
		}
		friend MemoryType asMemoryType(const ExternType& objectType)
		{
			WAVM_ASSERT(objectType.kind == ExternKind::memory);
			return objectType.memory;
		}
		friend GlobalType asGlobalType(const ExternType& objectType)
		{
			WAVM_ASSERT(objectType.kind == ExternKind::global);
			return objectType.global;
		}
		friend ExceptionType asExceptionType(const ExternType& objectType)
		{
			WAVM_ASSERT(objectType.kind == ExternKind::exceptionType);
			return objectType.exceptionType;
		}

	private:
		union
		{
			FunctionType function;
			TableType table;
			MemoryType memory;
			GlobalType global;
			ExceptionType exceptionType;
		};
	};

	inline std::string asString(const ExternType& objectType)
	{
		switch(objectType.kind)
		{
		case ExternKind::function: return "func " + asString(asFunctionType(objectType));
		case ExternKind::table: return "table " + asString(asTableType(objectType));
		case ExternKind::memory: return "memory " + asString(asMemoryType(objectType));
		case ExternKind::global: return asString(asGlobalType(objectType));
		case ExternKind::exceptionType:
			return "exception_type " + asString(asExceptionType(objectType));

		case ExternKind::invalid:
		default: WAVM_UNREACHABLE();
		};
	}

	inline ReferenceType asReferenceType(const ExternKind kind)
	{
		switch(kind)
		{
		case ExternKind::function: return ReferenceType::funcref;

		case ExternKind::table:
		case ExternKind::memory:
		case ExternKind::global:
		case ExternKind::exceptionType:
		case ExternKind::invalid:
		default: return ReferenceType::externref;
		}
	}

	inline ReferenceType asReferenceType(const ExternType& type)
	{
		return asReferenceType(type.kind);
	}
}}

// These specializations need to be declared within a WAVM namespace scope to work around a GCC bug.
// It should be ok to write "template<> struct WAVM::Hash...", but old versions of GCC will
// erroneously reject that. See https://gcc.gnu.org/bugzilla/show_bug.cgi?id=56480
namespace WAVM {
	template<> struct Hash<IR::TypeTuple>
	{
		Uptr operator()(IR::TypeTuple typeTuple, Uptr seed = 0) const
		{
			return Hash<Uptr>()(typeTuple.getHash(), seed);
		}
	};

	template<> struct Hash<IR::FunctionType>
	{
		Uptr operator()(IR::FunctionType functionType, Uptr seed = 0) const
		{
			return Hash<Uptr>()(functionType.getHash(), seed);
		}
	};
}
