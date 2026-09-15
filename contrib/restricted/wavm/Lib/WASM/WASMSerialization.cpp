#include <stdint.h>
#include <string>
#include <utility>
#include <vector>
#include "WAVM/IR/IR.h"
#include "WAVM/IR/Module.h"
#include "WAVM/IR/Operators.h"
#include "WAVM/IR/Types.h"
#include "WAVM/IR/Validate.h"
#include "WAVM/Inline/Assert.h"
#include "WAVM/Inline/BasicTypes.h"
#include "WAVM/Inline/Errors.h"
#include "WAVM/Inline/LEB128.h"
#include "WAVM/Inline/Serialization.h"
#include "WAVM/Inline/Timing.h"
#include "WAVM/Inline/Unicode.h"
#include "WAVM/Logging/Logging.h"
#include "WAVM/Platform/Defines.h"
#include "WAVM/WASM/WASM.h"

using namespace WAVM;
using namespace WAVM::IR;
using namespace WAVM::Serialization;

static void throwIfNotValidUTF8(const std::string& string)
{
	const U8* endChar = (const U8*)string.data() + string.size();
	if(Unicode::validateUTF8String((const U8*)string.data(), endChar) != endChar)
	{ throw FatalSerializationException("invalid UTF-8 encoding"); }
}

WAVM_FORCEINLINE void serializeOpcode(InputStream& stream, Opcode& opcode)
{
	U8 opcodeU8;
	serializeNativeValue(stream, opcodeU8);
	if(opcodeU8 <= maxSingleByteOpcode) { opcode = Opcode(opcodeU8); }
	else
	{
		U32 opcodeVarUInt;
		serializeVarUInt32(stream, opcodeVarUInt);
		opcode = Opcode((U32(opcodeU8) << 8) | opcodeVarUInt);
	}
}
WAVM_FORCEINLINE void serializeOpcode(OutputStream& stream, Opcode opcode)
{
	if(opcode <= (Opcode)maxSingleByteOpcode)
	{
		U8 opcodeU8 = U8(opcode);
		Serialization::serializeNativeValue(stream, opcodeU8);
	}
	else
	{
		U8 opcodePrefix = U8(U16(opcode) >> 8);
		U32 opcodeVarUInt = U32(opcode) & 0xff;
		serializeNativeValue(stream, opcodePrefix);
		serializeVarUInt32(stream, opcodeVarUInt);
	}
}

// These serialization functions need to be declared in the IR namespace for the array serializer in
// the Serialization namespace to find them.
namespace WAVM { namespace IR {
	static ValueType decodeValueType(Iptr encodedValueType)
	{
		switch(encodedValueType)
		{
		case -1: return ValueType::i32;
		case -2: return ValueType::i64;
		case -3: return ValueType::f32;
		case -4: return ValueType::f64;
		case -5: return ValueType::v128;
		case -16: return ValueType::funcref;
		case -17: return ValueType::externref;
		default: throw FatalSerializationException("invalid value type encoding");
		};
	}
	static I8 encodeValueType(ValueType valueType)
	{
		switch(valueType)
		{
		case ValueType::i32: return -1;
		case ValueType::i64: return -2;
		case ValueType::f32: return -3;
		case ValueType::f64: return -4;
		case ValueType::v128: return -5;
		case ValueType::funcref: return -16;
		case ValueType::externref: return -17;

		case ValueType::none:
		case ValueType::any:
		default: throw FatalSerializationException("invalid value type");
		};
	}
	static void serialize(InputStream& stream, ValueType& type)
	{
		I8 encodedValueType = 0;
		serializeVarInt7(stream, encodedValueType);
		type = decodeValueType(encodedValueType);
	}
	static void serialize(OutputStream& stream, ValueType type)
	{
		I8 encodedValueType = encodeValueType(type);
		serializeVarInt7(stream, encodedValueType);
	}

	WAVM_FORCEINLINE static void serialize(InputStream& stream, TypeTuple& typeTuple)
	{
		Uptr numElems;
		serializeVarUInt32(stream, numElems);

		std::vector<ValueType> elems;
		for(Uptr elemIndex = 0; elemIndex < numElems; ++elemIndex)
		{
			ValueType elem;
			serialize(stream, elem);
			elems.push_back(elem);
		}

		typeTuple = TypeTuple(elems);
	}
	static void serialize(OutputStream& stream, TypeTuple& typeTuple)
	{
		Uptr numElems = typeTuple.size();
		serializeVarUInt32(stream, numElems);

		for(ValueType elem : typeTuple) { serialize(stream, elem); }
	}

	template<typename Stream> void serializeIndex(Stream& stream, U64& size, IndexType sizeType)
	{
		if(sizeType == IndexType::i32) { serializeVarUInt32(stream, size); }
		else
		{
			serializeVarUInt64(stream, size);
		}
	}

	template<typename Stream>
	void serialize(Stream& stream,
				   SizeConstraints& sizeConstraints,
				   bool hasMax,
				   IndexType sizeType)
	{
		serializeIndex(stream, sizeConstraints.min, sizeType);
		if(hasMax) { serializeIndex(stream, sizeConstraints.max, sizeType); }
		else if(Stream::isInput)
		{
			sizeConstraints.max = UINT64_MAX;
		}
	}

	template<typename Stream> void serialize(Stream& stream, ReferenceType& referenceType)
	{
		if(Stream::isInput)
		{
			U8 encodedReferenceType = 0;
			serializeNativeValue(stream, encodedReferenceType);
			switch(encodedReferenceType)
			{
			case 0x70: referenceType = ReferenceType::funcref; break;
			case 0x6F: referenceType = ReferenceType::externref; break;
			default: throw FatalSerializationException("invalid reference type encoding");
			}
		}
		else
		{
			U8 encodedReferenceType;
			switch(referenceType)
			{
			case ReferenceType::funcref: encodedReferenceType = 0x70; break;
			case ReferenceType::externref: encodedReferenceType = 0x6F; break;

			case ReferenceType::none:
			default: WAVM_UNREACHABLE();
			}
			serializeNativeValue(stream, encodedReferenceType);
		}
	}

	template<typename Stream> void serialize(Stream& stream, TableType& tableType)
	{
		serialize(stream, tableType.elementType);

		U8 flags = 0;
		if(!Stream::isInput && tableType.size.max != UINT64_MAX) { flags |= 0x01; }
		if(!Stream::isInput && tableType.isShared) { flags |= 0x02; }
		if(!Stream::isInput && tableType.indexType == IndexType::i64) { flags |= 0x04; }
		serializeVarUInt7(stream, flags);
		if(Stream::isInput)
		{
			tableType.isShared = (flags & 0x02) != 0;
			tableType.indexType = (flags & 0x04) ? IndexType::i64 : IndexType::i32;
			if(flags & ~0x07) { throw FatalSerializationException("unknown table type flag"); }
		}
		serialize(stream, tableType.size, flags & 0x01, tableType.indexType);
	}

	template<typename Stream> void serialize(Stream& stream, MemoryType& memoryType)
	{
		U8 flags = 0;
		if(!Stream::isInput && memoryType.size.max != UINT64_MAX) { flags |= 0x01; }
		if(!Stream::isInput && memoryType.isShared) { flags |= 0x02; }
		if(!Stream::isInput && memoryType.indexType == IndexType::i64) { flags |= 0x04; }
		serializeVarUInt7(stream, flags);
		if(Stream::isInput)
		{
			memoryType.isShared = (flags & 0x02) != 0;
			memoryType.indexType = (flags & 0x04) ? IndexType::i64 : IndexType::i32;
			if(flags & ~0x07) { throw FatalSerializationException("unknown memory type flag"); }
		}
		serialize(stream, memoryType.size, flags & 0x01, memoryType.indexType);
	}

	template<typename Stream> void serialize(Stream& stream, GlobalType& globalType)
	{
		serialize(stream, globalType.valueType);
		U8 isMutable = globalType.isMutable ? 1 : 0;
		serializeVarUInt1(stream, isMutable);
		if(Stream::isInput) { globalType.isMutable = isMutable != 0; }
	}

	template<typename Stream> void serialize(Stream& stream, ExceptionType& exceptionType)
	{
		U8 attribute = 0;
		serializeVarUInt7(stream, attribute);
		if (attribute != 0) {
			throw FatalSerializationException("tag attribute must be 0");
		}

		U32 index = 0;
		serializeVarUInt7(stream, index);
	}

	static void serialize(InputStream& stream, ExternKind& kind)
	{
		U8 encodedKind = 0;
		serializeVarUInt7(stream, encodedKind);
		switch(encodedKind)
		{
		case 0: kind = ExternKind::function; break;
		case 1: kind = ExternKind::table; break;
		case 2: kind = ExternKind::memory; break;
		case 3: kind = ExternKind::global; break;
		case 4: kind = ExternKind::exceptionType; break;
		default: throw FatalSerializationException("invalid reference type encoding");
		};
	}
	static void serialize(OutputStream& stream, ExternKind& kind)
	{
		U8 encodedKind;
		switch(kind)
		{
		case ExternKind::function: encodedKind = 0; break;
		case ExternKind::table: encodedKind = 1; break;
		case ExternKind::memory: encodedKind = 2; break;
		case ExternKind::global: encodedKind = 3; break;
		case ExternKind::exceptionType: encodedKind = 4; break;
		case ExternKind::invalid:
		default: WAVM_UNREACHABLE();
		};
		serializeVarUInt7(stream, encodedKind);
	}

	template<typename Stream> void serialize(Stream& stream, Export& e)
	{
		serialize(stream, e.name);
		throwIfNotValidUTF8(e.name);
		serialize(stream, e.kind);
		serializeVarUInt32(stream, e.index);
	}

	template<typename Stream> void serialize(Stream& stream, InitializerExpression& initializer)
	{
		serializeOpcode(stream, initializer.typeOpcode);
		switch(initializer.type)
		{
		case InitializerExpression::Type::i32_const:
			serializeVarInt32(stream, initializer.i32);
			break;
		case InitializerExpression::Type::i64_const:
			serializeVarInt64(stream, initializer.i64);
			break;
		case InitializerExpression::Type::f32_const: serialize(stream, initializer.f32); break;
		case InitializerExpression::Type::f64_const: serialize(stream, initializer.f64); break;
		case InitializerExpression::Type::v128_const: serialize(stream, initializer.v128); break;
		case InitializerExpression::Type::global_get:
			serializeVarUInt32(stream, initializer.ref);
			break;
		case InitializerExpression::Type::ref_null:
			serialize(stream, initializer.nullReferenceType);
			break;
		case InitializerExpression::Type::ref_func:
			serializeVarUInt32(stream, initializer.ref);
			break;

		case InitializerExpression::Type::invalid:
		default: throw FatalSerializationException("invalid initializer expression opcode");
		}
		serializeConstant(stream, "expected end opcode", (U8)Opcode::end);
	}

	template<typename Stream> void serialize(Stream& stream, TableDef& tableDef)
	{
		serialize(stream, tableDef.type);
	}

	template<typename Stream> void serialize(Stream& stream, MemoryDef& memoryDef)
	{
		serialize(stream, memoryDef.type);
	}

	template<typename Stream> void serialize(Stream& stream, GlobalDef& globalDef)
	{
		serialize(stream, globalDef.type);
		serialize(stream, globalDef.initializer);
	}

	template<typename Stream> void serialize(Stream& stream, ExceptionTypeDef& exceptionTypeDef)
	{
		serialize(stream, exceptionTypeDef.type);
	}

	template<typename Stream> void serialize(Stream& stream, ElemSegment& elemSegment)
	{
		// Serialize the segment flags.
		U32 flags = 0;
		if(!Stream::isInput)
		{
			bool hasNonFunctionElements = false;
			switch(elemSegment.contents->encoding)
			{
			case ElemSegment::Encoding::expr:
				flags |= 4;
				if(elemSegment.contents->elemType != ReferenceType::funcref)
				{ hasNonFunctionElements = true; }
				break;
			case ElemSegment::Encoding::index:
				if(elemSegment.contents->externKind != ExternKind::function)
				{ hasNonFunctionElements = true; }
				break;
			default: WAVM_UNREACHABLE();
			};

			switch(elemSegment.type)
			{
			case ElemSegment::Type::active:
				if(elemSegment.tableIndex != 0 || hasNonFunctionElements) { flags |= 2; }
				break;
			case ElemSegment::Type::passive: flags |= 1; break;
			case ElemSegment::Type::declared: flags |= 3; break;
			default: WAVM_UNREACHABLE();
			};
		}
		serializeVarUInt32(stream, flags);
		if(Stream::isInput)
		{
			if(flags > 7) { throw FatalSerializationException("invalid elem segment flags"); }

			elemSegment.contents = std::make_shared<ElemSegment::Contents>();
			elemSegment.contents->encoding
				= (flags & 4) ? ElemSegment::Encoding::expr : ElemSegment::Encoding::index;

			elemSegment.tableIndex = UINTPTR_MAX;
			elemSegment.baseOffset = {};

			switch(flags & 3)
			{
			case 0:
				elemSegment.type = ElemSegment::Type::active;
				elemSegment.tableIndex = 0;
				elemSegment.contents->elemType = ReferenceType::funcref;
				elemSegment.contents->externKind = ExternKind::function;
				break;
			case 1: elemSegment.type = ElemSegment::Type::passive; break;
			case 2: elemSegment.type = ElemSegment::Type::active; break;
			case 3: elemSegment.type = ElemSegment::Type::declared; break;

			default: WAVM_UNREACHABLE();
			};
		}

		// Serialize the table the element segment writes to.
		if((flags & 3) == 2) { serializeVarUInt32(stream, elemSegment.tableIndex); }

		// Serialize the offset the element segment writes to the table at.
		if(!(flags & 1)) { serialize(stream, elemSegment.baseOffset); }

		switch(elemSegment.contents->encoding)
		{
		case ElemSegment::Encoding::expr: {
			// Serialize the type of the element expressions as a reference type.
			if(flags & 3) { serialize(stream, elemSegment.contents->elemType); }
			serializeArray(
				stream, elemSegment.contents->elemExprs, [](Stream& stream, ElemExpr& elem) {
					serializeOpcode(stream, elem.typeOpcode);
					switch(elem.type)
					{
					case ElemExpr::Type::ref_null: serialize(stream, elem.nullReferenceType); break;
					case ElemExpr::Type::ref_func: serializeVarUInt32(stream, elem.index); break;

					case ElemExpr::Type::invalid:
					default: throw FatalSerializationException("invalid elem opcode");
					};
					serializeConstant(stream, "expected end opcode", (U8)Opcode::end);
				});
			break;
		}
		case ElemSegment::Encoding::index: {
			// Serialize the extern kind referenced by the segment elements.
			if(flags & 3) { serialize(stream, elemSegment.contents->externKind); }
			serializeArray(
				stream, elemSegment.contents->elemIndices, [](Stream& stream, Uptr& externIndex) {
					serializeVarUInt32(stream, externIndex);
				});
			break;
		}
		default: WAVM_UNREACHABLE();
		};
	}

	template<typename Stream> void serialize(Stream& stream, DataSegment& dataSegment)
	{
		if(Stream::isInput)
		{
			U32 flags = 0;
			serializeVarUInt32(stream, flags);

			switch(flags)
			{
			case 0:
				dataSegment.isActive = true;
				dataSegment.memoryIndex = 0;
				serialize(stream, dataSegment.baseOffset);
				break;
			case 1:
				dataSegment.isActive = false;
				dataSegment.memoryIndex = UINTPTR_MAX;
				dataSegment.baseOffset = {};
				break;
			case 2:
				dataSegment.isActive = true;
				serializeVarUInt32(stream, dataSegment.memoryIndex);
				serialize(stream, dataSegment.baseOffset);
				break;
			default: throw FatalSerializationException("invalid data segment flags");
			};
			dataSegment.data = std::make_shared<std::vector<U8>>();
		}
		else
		{
			if(!dataSegment.isActive) { serializeConstant<U8>(stream, "", 1); }
			else
			{
				if(dataSegment.memoryIndex == 0) { serializeConstant<U8>(stream, "", 0); }
				else
				{
					serializeConstant<U8>(stream, "", 2);
					serializeVarUInt32(stream, dataSegment.memoryIndex);
				}
				serialize(stream, dataSegment.baseOffset);
			}
		}
		serialize(stream, *dataSegment.data);
	}
}}

static constexpr U32 magicNumber = 0x6d736100; // "\0asm"
static constexpr U32 currentVersion = 1;

enum class SectionID : U8
{
	custom = 0,
	type = 1,
	import = 2,
	function = 3,
	table = 4,
	memory = 5,
	global = 6,
	export_ = 7,
	start = 8,
	elem = 9,
	code = 10,
	data = 11,
	dataCount = 12,
	exceptionType = 0x7f,
};

static void serialize(InputStream& stream, SectionID& sectionID)
{
	serializeNativeValue(stream, *(U8*)&sectionID);
}

static void serialize(OutputStream& stream, SectionID sectionID)
{
	serializeNativeValue(stream, *(U8*)&sectionID);
}

struct ModuleSerializationState
{
	bool hadDataCountSection = false;
	std::shared_ptr<ModuleValidationState> validationState;
	const Module& module;

	ModuleSerializationState(const Module& inModule) : module(inModule) {}
};

template<typename Stream>
void serialize(Stream& stream, NoImm&, const FunctionDef&, const ModuleSerializationState&)
{
}

static void serialize(InputStream& stream,
					  ControlStructureImm& imm,
					  const FunctionDef&,
					  const ModuleSerializationState&)
{
	Iptr encodedBlockType;
	serializeVarInt32(stream, encodedBlockType);
	if(encodedBlockType >= 0)
	{
		imm.type.format = IndexedBlockType::functionType;
		imm.type.index = encodedBlockType;
	}
	else if(encodedBlockType == -64)
	{
		imm.type.format = IndexedBlockType::noParametersOrResult;
		imm.type.resultType = ValueType::none;
	}
	else
	{
		imm.type.format = IndexedBlockType::oneResult;
		imm.type.resultType = decodeValueType(encodedBlockType);
	}
}

static void serialize(OutputStream& stream,
					  const ControlStructureImm& imm,
					  const FunctionDef&,
					  const ModuleSerializationState&)
{
	Iptr encodedBlockType;
	switch(imm.type.format)
	{
	case IndexedBlockType::noParametersOrResult: encodedBlockType = -64; break;
	case IndexedBlockType::oneResult:
		encodedBlockType = encodeValueType(imm.type.resultType);
		break;
	case IndexedBlockType::functionType: encodedBlockType = imm.type.index; break;
	default: WAVM_UNREACHABLE();
	};
	serializeVarInt32(stream, encodedBlockType);
}

template<typename Stream>
void serialize(Stream& stream, SelectImm& imm, const FunctionDef&, const ModuleSerializationState&)
{
	U32 numResults = 1;
	serializeVarUInt32(stream, numResults);
	if(Stream::isInput && numResults != 1)
	{ throw ValidationException("typed select must have exactly one result"); }
	serialize(stream, imm.type);
}

template<typename Stream>
void serialize(Stream& stream, BranchImm& imm, const FunctionDef&, const ModuleSerializationState&)
{
	serializeVarUInt32(stream, imm.targetDepth);
}

static void serialize(InputStream& stream,
					  BranchTableImm& imm,
					  FunctionDef& functionDef,
					  const ModuleSerializationState&)
{
	std::vector<Uptr> branchTable;
	serializeArray(stream, branchTable, [](InputStream& stream, Uptr& targetDepth) {
		serializeVarUInt32(stream, targetDepth);
	});
	imm.branchTableIndex = functionDef.branchTables.size();
	functionDef.branchTables.push_back(std::move(branchTable));
	serializeVarUInt32(stream, imm.defaultTargetDepth);
}
static void serialize(OutputStream& stream,
					  BranchTableImm& imm,
					  FunctionDef& functionDef,
					  const ModuleSerializationState&)
{
	WAVM_ASSERT(imm.branchTableIndex < functionDef.branchTables.size());
	std::vector<Uptr>& branchTable = functionDef.branchTables[imm.branchTableIndex];
	serializeArray(stream, branchTable, [](OutputStream& stream, Uptr& targetDepth) {
		serializeVarUInt32(stream, targetDepth);
	});
	serializeVarUInt32(stream, imm.defaultTargetDepth);
}

template<typename Stream>
void serialize(Stream& stream,
			   LiteralImm<I32>& imm,
			   const FunctionDef&,
			   const ModuleSerializationState&)
{
	serializeVarInt32(stream, imm.value);
}

template<typename Stream>
void serialize(Stream& stream,
			   LiteralImm<I64>& imm,
			   const FunctionDef&,
			   const ModuleSerializationState&)
{
	serializeVarInt64(stream, imm.value);
}

template<typename Stream, bool isGlobal>
void serialize(Stream& stream,
			   GetOrSetVariableImm<isGlobal>& imm,
			   const FunctionDef&,
			   const ModuleSerializationState&)
{
	serializeVarUInt32(stream, imm.variableIndex);
}

template<typename Stream>
void serialize(Stream& stream,
			   FunctionImm& imm,
			   const FunctionDef&,
			   const ModuleSerializationState&)
{
	serializeVarUInt32(stream, imm.functionIndex);
}

template<typename Stream>
void serialize(Stream& stream,
			   FunctionRefImm& imm,
			   const FunctionDef&,
			   const ModuleSerializationState&)
{
	serializeVarUInt32(stream, imm.functionIndex);
}

template<typename Stream>
void serialize(Stream& stream,
			   CallIndirectImm& imm,
			   const FunctionDef&,
			   const ModuleSerializationState&)
{
	serializeVarUInt32(stream, imm.type.index);
	serializeVarUInt32(stream, imm.tableIndex);
}

template<typename Stream>
void serialize(Stream& stream,
			   BaseLoadOrStoreImm& imm,
			   const FunctionDef&,
			   const ModuleSerializationState& moduleState)
{
	// Use the lower 6 bits of a varuint32 to encode alignment, and the 7th bit as a flag for
	// whether a memory index is present.
	U32 alignmentLog2AndFlags = imm.alignmentLog2;
	if(!Stream::isInput && imm.memoryIndex != 0) { alignmentLog2AndFlags |= 0x40; }
	serializeVarUInt32(stream, alignmentLog2AndFlags);

	imm.alignmentLog2 = alignmentLog2AndFlags & 0x3f;
	if(imm.alignmentLog2 >= 16) { throw FatalSerializationException("Invalid alignment"); }
	imm.alignmentLog2 = (U8)(alignmentLog2AndFlags & 0x3f);

	if(moduleState.module.featureSpec.memory64) { serializeVarUInt64(stream, imm.offset); }
	else
	{
		serializeVarUInt32(stream, imm.offset);
	}

	if(alignmentLog2AndFlags & 0x40) { serializeVarUInt32(stream, imm.memoryIndex); }
	else
	{
		imm.memoryIndex = 0;
	}
}

template<typename Stream, Uptr naturalAlignmentLog2, Uptr numLanes>
void serialize(Stream& stream,
			   LoadOrStoreLaneImm<naturalAlignmentLog2, numLanes>& imm,
			   const FunctionDef& functionDef,
			   const ModuleSerializationState& state)
{
	serialize(stream, static_cast<BaseLoadOrStoreImm&>(imm), functionDef, state);
	serializeNativeValue(stream, imm.laneIndex);
}

template<typename Stream>
void serializeMemoryIndex(Stream& stream, Uptr& memoryIndex, const FeatureSpec& featureSpec)
{
	// Without the multipleMemories feature, the memory index byte must be serialized as a single
	// zero byte rather than any ULEB128 encoding that denotes zero.
	if(featureSpec.multipleMemories) { serializeVarUInt32(stream, memoryIndex); }
	else
	{
		serializeConstant<U8>(stream, "memory index reserved byte must be zero", 0);
		if(Stream::isInput) { memoryIndex = 0; }
	}
}
template<typename Stream>
void serialize(Stream& stream,
			   MemoryImm& imm,
			   const FunctionDef&,
			   const ModuleSerializationState& state)
{
	serializeMemoryIndex(stream, imm.memoryIndex, state.module.featureSpec);
}
template<typename Stream>
void serialize(Stream& stream,
			   MemoryCopyImm& imm,
			   const FunctionDef&,
			   const ModuleSerializationState& state)
{
	serializeMemoryIndex(stream, imm.destMemoryIndex, state.module.featureSpec);
	serializeMemoryIndex(stream, imm.sourceMemoryIndex, state.module.featureSpec);
}
template<typename Stream>
void serializeTableIndex(Stream& stream, Uptr& tableIndex, const FeatureSpec& featureSpec)
{
	// Without the referenceTypes feature, the memory index byte must be serialized as a single zero
	// byte rather than any ULEB128 encoding that denotes zero.
	if(featureSpec.referenceTypes) { serializeVarUInt32(stream, tableIndex); }
	else
	{
		serializeConstant<U8>(stream, "table index reserved byte must be zero", 0);
		if(Stream::isInput) { tableIndex = 0; }
	}
}
template<typename Stream>
void serialize(Stream& stream,
			   TableImm& imm,
			   const FunctionDef&,
			   const ModuleSerializationState& state)
{
	serializeTableIndex(stream, imm.tableIndex, state.module.featureSpec);
}
template<typename Stream>
void serialize(Stream& stream,
			   TableCopyImm& imm,
			   const FunctionDef&,
			   const ModuleSerializationState& state)
{
	serializeTableIndex(stream, imm.destTableIndex, state.module.featureSpec);
	serializeTableIndex(stream, imm.sourceTableIndex, state.module.featureSpec);
}

namespace WAVM {
	template<typename Stream> void serialize(Stream& stream, V128& v128)
	{
		serializeNativeValue(stream, v128);
	}
}

template<typename Stream, Uptr numLanes>
void serialize(Stream& stream,
			   LaneIndexImm<numLanes>& imm,
			   const FunctionDef&,
			   const ModuleSerializationState&)
{
	serializeNativeValue(stream, imm.laneIndex);
}

template<typename Stream, Uptr numLanes>
void serialize(Stream& stream,
			   ShuffleImm<numLanes>& imm,
			   const FunctionDef&,
			   const ModuleSerializationState&)
{
	for(Uptr laneIndex = 0; laneIndex < numLanes; ++laneIndex)
	{ serializeNativeValue(stream, imm.laneIndices[laneIndex]); }
}

template<typename Stream>
void serialize(Stream& stream,
			   AtomicFenceImm& imm,
			   const FunctionDef&,
			   const ModuleSerializationState&)
{
	if(!Stream::isInput) { WAVM_ASSERT(imm.order == MemoryOrder::sequentiallyConsistent); }

	U8 memoryOrder = 0;
	serializeNativeValue(stream, memoryOrder);

	if(Stream::isInput)
	{
		if(memoryOrder != 0)
		{ throw FatalSerializationException("Invalid memory order in atomic.fence instruction"); }
		imm.order = MemoryOrder::sequentiallyConsistent;
	}
}

template<typename Stream>
void serialize(Stream& stream,
			   ExceptionTypeImm& imm,
			   const FunctionDef&,
			   const ModuleSerializationState&)
{
	serializeVarUInt32(stream, imm.exceptionTypeIndex);
}

template<typename Stream>
void serialize(Stream& stream, DelegateImm& imm, const FunctionDef&, const ModuleSerializationState&)
{
	serializeVarUInt32(stream, imm.catchDepth);
}

template<typename Stream>
void serialize(Stream& stream, RethrowImm& imm, const FunctionDef&, const ModuleSerializationState&)
{
	serializeVarUInt32(stream, imm.catchDepth);
}

template<typename Stream>
void serialize(Stream& stream,
			   DataSegmentAndMemImm& imm,
			   const FunctionDef&,
			   const ModuleSerializationState& moduleState)
{
	if(Stream::isInput && !moduleState.hadDataCountSection)
	{
		throw FatalSerializationException(
			"memory.init instruction cannot occur in a module without a DataCount section");
	}

	serializeVarUInt32(stream, imm.dataSegmentIndex);
	serializeVarUInt32(stream, imm.memoryIndex);
}

template<typename Stream>
void serialize(Stream& stream,
			   DataSegmentImm& imm,
			   const FunctionDef&,
			   const ModuleSerializationState& moduleState)
{
	if(Stream::isInput && !moduleState.hadDataCountSection)
	{
		throw FatalSerializationException(
			"data.drop instruction cannot occur in a module without a DataCount section");
	}

	serializeVarUInt32(stream, imm.dataSegmentIndex);
}

template<typename Stream>
void serialize(Stream& stream,
			   ElemSegmentAndTableImm& imm,
			   const FunctionDef&,
			   const ModuleSerializationState&)
{
	serializeVarUInt32(stream, imm.elemSegmentIndex);
	serializeVarUInt32(stream, imm.tableIndex);
}

template<typename Stream>
void serialize(Stream& stream,
			   ElemSegmentImm& imm,
			   const FunctionDef&,
			   const ModuleSerializationState&)
{
	serializeVarUInt32(stream, imm.elemSegmentIndex);
}

template<typename Stream, typename Value>
void serialize(Stream& stream,
			   LiteralImm<Value>& imm,
			   const FunctionDef&,
			   const ModuleSerializationState&)
{
	serialize(stream, imm.value);
}

template<typename Stream>
void serialize(Stream& stream,
			   ReferenceTypeImm& imm,
			   const FunctionDef&,
			   const ModuleSerializationState&)
{
	serialize(stream, imm.referenceType);
}

template<typename SerializeSection>
void serializeSection(OutputStream& stream, SectionID id, SerializeSection serializeSectionBody)
{
	serialize(stream, id);
	ArrayOutputStream sectionStream;
	serializeSectionBody(sectionStream);
	std::vector<U8> sectionBytes = sectionStream.getBytes();
	Uptr sectionNumBytes = sectionBytes.size();
	serializeVarUInt32(stream, sectionNumBytes);
	serializeBytes(stream, sectionBytes.data(), sectionBytes.size());
}
template<typename SerializeSection>
void serializeSection(InputStream& stream, SectionID id, SerializeSection serializeSectionBody)
{
	Uptr numSectionBytes = 0;
	serializeVarUInt32(stream, numSectionBytes);
	MemoryInputStream sectionStream(stream.advance(numSectionBytes), numSectionBytes);
	serializeSectionBody(sectionStream);
	if(sectionStream.capacity())
	{ throw FatalSerializationException("section contained more data than expected"); }
}

static void serialize(OutputStream& stream, CustomSection& customSection)
{
	serialize(stream, SectionID::custom);
	ArrayOutputStream sectionStream;
	serialize(sectionStream, customSection.name);
	serializeBytes(sectionStream, customSection.data.data(), customSection.data.size());
	std::vector<U8> sectionBytes = sectionStream.getBytes();
	serialize(stream, sectionBytes);
}

static void serialize(InputStream& stream, CustomSection& customSection)
{
	Uptr numSectionBytes = 0;
	serializeVarUInt32(stream, numSectionBytes);

	MemoryInputStream sectionStream(stream.advance(numSectionBytes), numSectionBytes);
	serialize(sectionStream, customSection.name);
	throwIfNotValidUTF8(customSection.name);
	customSection.data.resize(sectionStream.capacity());
	serializeBytes(sectionStream, customSection.data.data(), customSection.data.size());
	WAVM_ASSERT(!sectionStream.capacity());
}

struct LocalSet
{
	Uptr num;
	ValueType type;
};

template<typename Stream> void serialize(Stream& stream, LocalSet& localSet)
{
	serializeVarUInt32(stream, localSet.num);
	serialize(stream, localSet.type);
}

struct OperatorSerializerStream
{
	typedef void Result;

	OperatorSerializerStream(Serialization::OutputStream& inByteStream,
							 FunctionDef& inFunctionDef,
							 const ModuleSerializationState& inModuleState)
	: byteStream(inByteStream), functionDef(inFunctionDef), moduleState(inModuleState)
	{
	}

#define VISIT_OPCODE(_, name, nameString, Imm, ...)                                                \
	void name(Imm imm) const                                                                       \
	{                                                                                              \
		Opcode opcode = Opcode::name;                                                              \
		serializeOpcode(byteStream, opcode);                                                       \
		serialize(byteStream, imm, functionDef, moduleState);                                      \
	}
	WAVM_ENUM_NONOVERLOADED_OPERATORS(VISIT_OPCODE)
#undef VISIT_OPCODE

	void select(SelectImm imm) const
	{
		// Serialize different opcodes depending on the select immediates:
		// implicitly-typed select: 0x1b
		// explicitly-typed select: 0x1c
		if(imm.type == ValueType::any)
		{
			Opcode opcode = Opcode(0x1b);
			serializeOpcode(byteStream, opcode);
		}
		else
		{
			Opcode opcode = Opcode(0x1c);
			serializeOpcode(byteStream, opcode);
			serialize(byteStream, imm, functionDef, moduleState);
		}
	}

private:
	Serialization::OutputStream& byteStream;
	FunctionDef& functionDef;
	const ModuleSerializationState& moduleState;
};

static void serializeFunctionBody(OutputStream& sectionStream,
								  Module& module,
								  FunctionDef& functionDef,
								  const ModuleSerializationState& moduleState)
{
	ArrayOutputStream bodyStream;

	// Convert the function's local types into LocalSets: runs of locals of the same type.
	LocalSet* localSets
		= (LocalSet*)alloca(sizeof(LocalSet) * functionDef.nonParameterLocalTypes.size());
	Uptr numLocalSets = 0;
	if(functionDef.nonParameterLocalTypes.size())
	{
		localSets[0].type = ValueType::any;
		localSets[0].num = 0;
		for(auto localType : functionDef.nonParameterLocalTypes)
		{
			if(localSets[numLocalSets].type != localType)
			{
				if(localSets[numLocalSets].type != ValueType::any) { ++numLocalSets; }
				localSets[numLocalSets].type = localType;
				localSets[numLocalSets].num = 0;
			}
			++localSets[numLocalSets].num;
		}
		if(localSets[numLocalSets].type != ValueType::any) { ++numLocalSets; }
	}

	// Serialize the local sets.
	serializeVarUInt32(bodyStream, numLocalSets);
	for(Uptr setIndex = 0; setIndex < numLocalSets; ++setIndex)
	{ serialize(bodyStream, localSets[setIndex]); }

	// Serialize the function code.
	OperatorDecoderStream irDecoderStream(functionDef.code);
	OperatorSerializerStream wasmOpEncoderStream(bodyStream, functionDef, moduleState);
	while(irDecoderStream) { irDecoderStream.decodeOp(wasmOpEncoderStream); };

	std::vector<U8> bodyBytes = bodyStream.getBytes();
	serialize(sectionStream, bodyBytes);
}

static void serializeFunctionBody(InputStream& sectionStream,
								  Module& module,
								  FunctionDef& functionDef,
								  const ModuleSerializationState& moduleState)
{
	Uptr numBodyBytes = 0;
	serializeVarUInt32(sectionStream, numBodyBytes);

	MemoryInputStream bodyStream(sectionStream.advance(numBodyBytes), numBodyBytes);

	// Deserialize local sets and unpack them into a linear array of local types.
	Uptr numLocalSets = 0;
	serializeVarUInt32(bodyStream, numLocalSets);
	for(Uptr setIndex = 0; setIndex < numLocalSets; ++setIndex)
	{
		LocalSet localSet;
		serialize(bodyStream, localSet);
		if(functionDef.nonParameterLocalTypes.size() + localSet.num >= module.featureSpec.maxLocals)
		{ throw FatalSerializationException("too many locals"); }
		for(Uptr index = 0; index < localSet.num; ++index)
		{ functionDef.nonParameterLocalTypes.push_back(localSet.type); }
	}

	// Deserialize the function code, validate it, and re-encode it in the IR format.
	ArrayOutputStream irCodeByteStream;
	OperatorEncoderStream irEncoderStream(irCodeByteStream);
	CodeValidationStream codeValidationStream(*moduleState.validationState, functionDef);
	while(bodyStream.capacity())
	{
		Opcode opcode;
		serializeOpcode(bodyStream, opcode);
		switch(U16(opcode))
		{
#define VISIT_OPCODE(_, name, nameString, Imm, ...)                                                \
	case Uptr(Opcode::name): {                                                                     \
		Imm imm;                                                                                   \
		serialize(bodyStream, imm, functionDef, moduleState);                                      \
		codeValidationStream.name(imm);                                                            \
		irEncoderStream.name(imm);                                                                 \
		break;                                                                                     \
	}
			WAVM_ENUM_NONOVERLOADED_OPERATORS(VISIT_OPCODE)
#undef VISIT_OPCODE
		// Explicitly handle both select opcodes here:
		case 0x1b: {
			SelectImm imm{ValueType::any};

			codeValidationStream.select(imm);
			irEncoderStream.select(imm);
			break;
		}
		case 0x1c: {
			SelectImm imm;
			serialize(bodyStream, imm, functionDef, moduleState);

			codeValidationStream.select(imm);
			irEncoderStream.select(imm);
			break;
		}
		default:
			throw FatalSerializationException(std::string("unknown opcode (")
											  + std::to_string(Uptr(opcode)) + ")");
		};
	};
	codeValidationStream.finish();

	functionDef.code = std::move(irCodeByteStream.getBytes());
}

static void serializeCallingConvention(InputStream& stream, CallingConvention& callingConvention)
{
	U32 encoding = 0;
	serializeVarUInt32(stream, encoding);

	switch(encoding)
	{
	case 0: callingConvention = CallingConvention::wasm; break;
	case 1: callingConvention = CallingConvention::intrinsic; break;
	case 2: callingConvention = CallingConvention::intrinsicWithContextSwitch; break;
	case 3: callingConvention = CallingConvention::c; break;
	case 4: callingConvention = CallingConvention::cAPICallback; break;

	default:
		throw FatalSerializationException("unknown calling convention (" + std::to_string(encoding)
										  + ")");
	};
}

static void serializeCallingConvention(OutputStream& stream, CallingConvention callingConvention)
{
	U32 encoding = 0;
	switch(callingConvention)
	{
	case CallingConvention::wasm: encoding = 0; break;
	case CallingConvention::intrinsic: encoding = 1; break;
	case CallingConvention::intrinsicWithContextSwitch: encoding = 2; break;
	case CallingConvention::c: encoding = 3; break;
	case CallingConvention::cAPICallback: encoding = 4; break;

	default: WAVM_UNREACHABLE();
	};

	serializeVarUInt32(stream, encoding);
}

template<typename Stream> void serializeFunctionType(Stream& stream, FunctionType& functionType)
{
	if(Stream::isInput)
	{
		U8 tag = 0;
		serializeNativeValue(stream, tag);

		CallingConvention callingConvention = CallingConvention::wasm;
		switch(tag)
		{
		case 0x60: break;
		case 0x61: serializeCallingConvention(stream, callingConvention); break;
		default:
			throw FatalSerializationException("unknown function type tag (" + std::to_string(tag)
											  + ")");
		};

		TypeTuple params;
		serialize(stream, params);

		TypeTuple results;
		serialize(stream, results);

		functionType = FunctionType(results, params, callingConvention);
	}
	else
	{
		U8 tag = functionType.callingConvention() == CallingConvention::wasm ? 0x60 : 0x61;
		serializeNativeValue(stream, tag);

		if(tag == 0x61)
		{
			CallingConvention callingConvention = functionType.callingConvention();
			serializeCallingConvention(stream, callingConvention);
		}

		TypeTuple params = functionType.params();
		serialize(stream, params);

		TypeTuple results = functionType.results();
		serialize(stream, results);
	}
}

template<typename Stream> void serializeTypeSection(Stream& moduleStream, Module& module)
{
	serializeSection(moduleStream, SectionID::type, [&module](Stream& sectionStream) {
		serializeArray(sectionStream, module.types, serializeFunctionType<Stream>);
	});
}
template<typename Stream> void serializeImportSection(Stream& moduleStream, Module& module)
{
	serializeSection(moduleStream, SectionID::import, [&module](Stream& sectionStream) {
		Uptr size = module.functions.imports.size() + module.tables.imports.size()
					+ module.memories.imports.size() + module.globals.imports.size()
					+ module.exceptionTypes.imports.size();
		serializeVarUInt32(sectionStream, size);
		if(Stream::isInput)
		{
			for(Uptr index = 0; index < size; ++index)
			{
				std::string moduleName;
				std::string exportName;
				ExternKind kind = ExternKind::invalid;
				serialize(sectionStream, moduleName);
				serialize(sectionStream, exportName);
				throwIfNotValidUTF8(moduleName);
				throwIfNotValidUTF8(exportName);
				serialize(sectionStream, kind);
				Uptr kindIndex = 0;
				switch(kind)
				{
				case ExternKind::function: {
					U32 functionTypeIndex = 0;
					serializeVarUInt32(sectionStream, functionTypeIndex);
					if(functionTypeIndex >= module.types.size())
					{ throw FatalSerializationException("invalid import function type index"); }
					kindIndex = module.functions.imports.size();
					module.functions.imports.push_back(
						{{functionTypeIndex}, std::move(moduleName), std::move(exportName)});
					break;
				}
				case ExternKind::table: {
					TableType tableType;
					serialize(sectionStream, tableType);
					kindIndex = module.tables.imports.size();
					module.tables.imports.push_back(
						{tableType, std::move(moduleName), std::move(exportName)});
					break;
				}
				case ExternKind::memory: {
					MemoryType memoryType;
					serialize(sectionStream, memoryType);
					kindIndex = module.memories.imports.size();
					module.memories.imports.push_back(
						{memoryType, std::move(moduleName), std::move(exportName)});
					break;
				}
				case ExternKind::global: {
					GlobalType globalType;
					serialize(sectionStream, globalType);
					kindIndex = module.globals.imports.size();
					module.globals.imports.push_back(
						{globalType, std::move(moduleName), std::move(exportName)});
					break;
				}
				case ExternKind::exceptionType: {
					ExceptionType exceptionType;
					serialize(sectionStream, exceptionType);
					kindIndex = module.exceptionTypes.imports.size();
					exceptionType.params = TypeTuple({ValueType::i64});
					module.exceptionTypes.imports.push_back(
						{exceptionType, std::move(moduleName), std::move(exportName)});
					break;
				}

				case ExternKind::invalid:
				default: throw FatalSerializationException("invalid ExternKind");
				};

				module.imports.push_back({kind, kindIndex});
			}
		}
		else
		{
			WAVM_ASSERT(module.imports.size()
						== module.functions.imports.size() + module.tables.imports.size()
							   + module.memories.imports.size() + module.globals.imports.size()
							   + module.exceptionTypes.imports.size());

			for(const auto& kindIndex : module.imports)
			{
				ExternKind kind = kindIndex.kind;
				switch(kindIndex.kind)
				{
				case ExternKind::function: {
					auto& functionImport = module.functions.imports[kindIndex.index];
					serialize(sectionStream, functionImport.moduleName);
					serialize(sectionStream, functionImport.exportName);
					serialize(sectionStream, kind);
					serializeVarUInt32(sectionStream, functionImport.type.index);
					break;
				}
				case ExternKind::table: {
					auto& tableImport = module.tables.imports[kindIndex.index];
					serialize(sectionStream, tableImport.moduleName);
					serialize(sectionStream, tableImport.exportName);
					serialize(sectionStream, kind);
					serialize(sectionStream, tableImport.type);
					break;
				}
				case ExternKind::memory: {
					auto& memoryImport = module.memories.imports[kindIndex.index];
					serialize(sectionStream, memoryImport.moduleName);
					serialize(sectionStream, memoryImport.exportName);
					serialize(sectionStream, kind);
					serialize(sectionStream, memoryImport.type);
					break;
				}
				case ExternKind::global: {
					auto& globalImport = module.globals.imports[kindIndex.index];
					serialize(sectionStream, globalImport.moduleName);
					serialize(sectionStream, globalImport.exportName);
					serialize(sectionStream, kind);
					serialize(sectionStream, globalImport.type);
					break;
				}
				case ExternKind::exceptionType: {
					auto& exceptionTypeImport = module.exceptionTypes.imports[kindIndex.index];
					serialize(sectionStream, exceptionTypeImport.moduleName);
					serialize(sectionStream, exceptionTypeImport.exportName);
					serialize(sectionStream, kind);
					serialize(sectionStream, exceptionTypeImport.type);
					break;
				}

				case ExternKind::invalid:
				default: WAVM_UNREACHABLE();
				};
			}
		}
	});
}

template<typename Stream> void serializeFunctionSection(Stream& moduleStream, Module& module)
{
	serializeSection(moduleStream, SectionID::function, [&module](Stream& sectionStream) {
		Uptr numFunctions = module.functions.defs.size();
		serializeVarUInt32(sectionStream, numFunctions);
		if(Stream::isInput)
		{
			// Grow the vector one element at a time: try to get a serialization exception
			// before making a huge allocation for malformed input.
			module.functions.defs.clear();
			for(Uptr functionIndex = 0; functionIndex < numFunctions; ++functionIndex)
			{
				U32 functionTypeIndex = 0;
				serializeVarUInt32(sectionStream, functionTypeIndex);
				if(functionTypeIndex >= module.types.size())
				{ throw FatalSerializationException("invalid function type index"); }
				module.functions.defs.push_back({{functionTypeIndex}, {}, {}, {}});
			}
			module.functions.defs.shrink_to_fit();
		}
		else
		{
			for(FunctionDef& function : module.functions.defs)
			{ serializeVarUInt32(sectionStream, function.type.index); }
		}
	});
}

template<typename Stream> void serializeTableSection(Stream& moduleStream, Module& module)
{
	serializeSection(moduleStream, SectionID::table, [&module](Stream& sectionStream) {
		serialize(sectionStream, module.tables.defs);
	});
}

template<typename Stream> void serializeMemorySection(Stream& moduleStream, Module& module)
{
	serializeSection(moduleStream, SectionID::memory, [&module](Stream& sectionStream) {
		serialize(sectionStream, module.memories.defs);
	});
}

template<typename Stream> void serializeGlobalSection(Stream& moduleStream, Module& module)
{
	serializeSection(moduleStream, SectionID::global, [&module](Stream& sectionStream) {
		serialize(sectionStream, module.globals.defs);
	});
}

template<typename Stream> void serializeExceptionTypeSection(Stream& moduleStream, Module& module)
{
	serializeSection(moduleStream, SectionID::exceptionType, [&module](Stream& sectionStream) {
		serialize(sectionStream, module.exceptionTypes.defs);
	});
}

template<typename Stream> void serializeExportSection(Stream& moduleStream, Module& module)
{
	serializeSection(moduleStream, SectionID::export_, [&module](Stream& sectionStream) {
		serialize(sectionStream, module.exports);
	});
}

template<typename Stream> void serializeStartSection(Stream& moduleStream, Module& module)
{
	serializeSection(moduleStream, SectionID::start, [&module](Stream& sectionStream) {
		serializeVarUInt32(sectionStream, module.startFunctionIndex);
	});
}

template<typename Stream> void serializeElementSection(Stream& moduleStream, Module& module)
{
	serializeSection(moduleStream, SectionID::elem, [&module](Stream& sectionStream) {
		serialize(sectionStream, module.elemSegments);
	});
}

static void serializeCodeSection(InputStream& moduleStream,
								 Module& module,
								 const ModuleSerializationState& moduleState)
{
	serializeSection(
		moduleStream, SectionID::code, [&module, &moduleState](InputStream& sectionStream) {
			Uptr numFunctionBodies = module.functions.defs.size();
			serializeVarUInt32(sectionStream, numFunctionBodies);
			if(numFunctionBodies != module.functions.defs.size())
			{
				throw FatalSerializationException(
					"function and code sections have mismatched function counts");
			}
			for(FunctionDef& functionDef : module.functions.defs)
			{ serializeFunctionBody(sectionStream, module, functionDef, moduleState); }
		});
}

void serializeCodeSection(OutputStream& moduleStream,
						  Module& module,
						  const ModuleSerializationState& moduleState)
{
	serializeSection(
		moduleStream, SectionID::code, [&module, &moduleState](OutputStream& sectionStream) {
			Uptr numFunctionBodies = module.functions.defs.size();
			serializeVarUInt32(sectionStream, numFunctionBodies);
			for(FunctionDef& functionDef : module.functions.defs)
			{ serializeFunctionBody(sectionStream, module, functionDef, moduleState); }
		});
}

template<typename Stream> void serializeDataCountSection(Stream& moduleStream, Module& module)
{
	serializeSection(moduleStream, SectionID::dataCount, [&module](Stream& sectionStream) {
		Uptr numDataSegments = module.dataSegments.size();
		serializeVarUInt32(sectionStream, numDataSegments);
		if(Stream::isInput)
		{
			// To make fuzzing more effective, fail gracefully instead of through OOM if the
			// DataCount section specifies a large number of data segments.
			if(numDataSegments > module.featureSpec.maxDataSegments)
			{ throw FatalSerializationException("too many data segments"); }

			module.dataSegments.resize(numDataSegments);
		}
	});
}

void serializeDataSection(InputStream& moduleStream, Module& module, bool hadDataCountSection)
{
	serializeSection(
		moduleStream, SectionID::data, [&module, hadDataCountSection](InputStream& sectionStream) {
			Uptr numDataSegments = 0;
			serializeVarUInt32(sectionStream, numDataSegments);
			if(!hadDataCountSection)
			{
				// To make fuzzing more effective, fail gracefully instead of
				// through OOM if the DataCount section specifies a large number of
				// data segments.
				if(numDataSegments > module.featureSpec.maxDataSegments)
				{ throw FatalSerializationException("too many data segments"); }
				module.dataSegments.resize(numDataSegments);
			}
			else if(numDataSegments != module.dataSegments.size())
			{
				throw FatalSerializationException(
					"DataCount and Data sections have mismatched segment counts");
			}
			for(Uptr segmentIndex = 0; segmentIndex < module.dataSegments.size(); ++segmentIndex)
			{ serialize(sectionStream, module.dataSegments[segmentIndex]); }
		});
}

void serializeDataSection(OutputStream& moduleStream, Module& module)
{
	serializeSection(moduleStream, SectionID::data, [&module](OutputStream& sectionStream) {
		serialize(sectionStream, module.dataSegments);
	});
}

void serializeCustomSectionsAfterKnownSection(OutputStream& moduleStream,
											  Module& module,
											  OrderedSectionID afterSection)
{
	for(CustomSection& customSection : module.customSections)
	{
		if(customSection.afterSection == afterSection) { serialize(moduleStream, customSection); }
	}
}

static void serializeModule(OutputStream& moduleStream, Module& module)
{
	ModuleSerializationState moduleState(module);

	serializeConstant(moduleStream, "magic number", U32(magicNumber));
	serializeConstant(moduleStream, "version", U32(currentVersion));

	serializeCustomSectionsAfterKnownSection(
		moduleStream, module, OrderedSectionID::moduleBeginning);
	if(hasTypeSection(module)) { serializeTypeSection(moduleStream, module); }
	serializeCustomSectionsAfterKnownSection(moduleStream, module, OrderedSectionID::type);
	if(hasImportSection(module)) { serializeImportSection(moduleStream, module); }
	serializeCustomSectionsAfterKnownSection(moduleStream, module, OrderedSectionID::import);
	if(hasFunctionSection(module)) { serializeFunctionSection(moduleStream, module); }
	serializeCustomSectionsAfterKnownSection(moduleStream, module, OrderedSectionID::function);
	if(hasTableSection(module)) { serializeTableSection(moduleStream, module); }
	serializeCustomSectionsAfterKnownSection(moduleStream, module, OrderedSectionID::table);
	if(hasMemorySection(module)) { serializeMemorySection(moduleStream, module); }
	serializeCustomSectionsAfterKnownSection(moduleStream, module, OrderedSectionID::memory);
	if(hasGlobalSection(module)) { serializeGlobalSection(moduleStream, module); }
	serializeCustomSectionsAfterKnownSection(moduleStream, module, OrderedSectionID::global);
	if(hasExceptionTypeSection(module)) { serializeExceptionTypeSection(moduleStream, module); }
	serializeCustomSectionsAfterKnownSection(moduleStream, module, OrderedSectionID::exceptionType);
	if(hasExportSection(module)) { serializeExportSection(moduleStream, module); }
	serializeCustomSectionsAfterKnownSection(moduleStream, module, OrderedSectionID::export_);
	if(hasStartSection(module)) { serializeStartSection(moduleStream, module); }
	serializeCustomSectionsAfterKnownSection(moduleStream, module, OrderedSectionID::start);
	if(hasElemSection(module)) { serializeElementSection(moduleStream, module); }
	serializeCustomSectionsAfterKnownSection(moduleStream, module, OrderedSectionID::elem);
	if(hasDataCountSection(module))
	{
		serializeDataCountSection(moduleStream, module);
		moduleState.hadDataCountSection = true;
	}
	serializeCustomSectionsAfterKnownSection(moduleStream, module, OrderedSectionID::dataCount);
	if(hasCodeSection(module)) { serializeCodeSection(moduleStream, module, moduleState); }
	serializeCustomSectionsAfterKnownSection(moduleStream, module, OrderedSectionID::code);
	if(hasDataSection(module)) { serializeDataSection(moduleStream, module); }
	serializeCustomSectionsAfterKnownSection(moduleStream, module, OrderedSectionID::data);
}

static void serializeModule(InputStream& moduleStream, Module& module)
{
	serializeConstant(moduleStream, "magic number", U32(magicNumber));
	serializeConstant(moduleStream, "version", U32(currentVersion));

	ModuleSerializationState moduleState(module);
	moduleState.validationState = IR::createModuleValidationState(module);

	OrderedSectionID lastKnownOrderedSectionID = OrderedSectionID::moduleBeginning;
	bool hadFunctionDefinitions = false;
	bool hadDataSection = false;
	while(moduleStream.capacity())
	{
		SectionID sectionID;
		serialize(moduleStream, sectionID);

		if(sectionID != SectionID::custom)
		{
			OrderedSectionID orderedSectionID;
			switch(sectionID)
			{
			case SectionID::type: orderedSectionID = OrderedSectionID::type; break;
			case SectionID::import: orderedSectionID = OrderedSectionID::import; break;
			case SectionID::function: orderedSectionID = OrderedSectionID::function; break;
			case SectionID::table: orderedSectionID = OrderedSectionID::table; break;
			case SectionID::memory: orderedSectionID = OrderedSectionID::memory; break;
			case SectionID::global: orderedSectionID = OrderedSectionID::global; break;
			case SectionID::export_: orderedSectionID = OrderedSectionID::export_; break;
			case SectionID::start: orderedSectionID = OrderedSectionID::start; break;
			case SectionID::elem: orderedSectionID = OrderedSectionID::elem; break;
			case SectionID::code: orderedSectionID = OrderedSectionID::code; break;
			case SectionID::data: orderedSectionID = OrderedSectionID::data; break;
			case SectionID::dataCount: orderedSectionID = OrderedSectionID::dataCount; break;
			case SectionID::exceptionType:
				orderedSectionID = OrderedSectionID::exceptionType;
				break;

			case SectionID::custom: WAVM_UNREACHABLE();
			default:
				throw FatalSerializationException("unknown section ID ("
												  + std::to_string(U8(sectionID)));
			};

			if(orderedSectionID > lastKnownOrderedSectionID)
			{ lastKnownOrderedSectionID = orderedSectionID; }
			else
			{
				throw FatalSerializationException("incorrect order for known section");
			}
		}

		switch(sectionID)
		{
		case SectionID::type:
			serializeTypeSection(moduleStream, module);
			IR::validateTypes(*moduleState.validationState);
			break;
		case SectionID::import:
			serializeImportSection(moduleStream, module);
			IR::validateImports(*moduleState.validationState);
			break;
		case SectionID::function:
			serializeFunctionSection(moduleStream, module);
			IR::validateFunctionDeclarations(*moduleState.validationState);
			break;
		case SectionID::table:
			serializeTableSection(moduleStream, module);
			IR::validateTableDefs(*moduleState.validationState);
			break;
		case SectionID::memory:
			serializeMemorySection(moduleStream, module);
			IR::validateMemoryDefs(*moduleState.validationState);
			break;
		case SectionID::global:
			serializeGlobalSection(moduleStream, module);
			IR::validateGlobalDefs(*moduleState.validationState);
			break;
		case SectionID::exceptionType:
			serializeExceptionTypeSection(moduleStream, module);
			IR::validateExceptionTypeDefs(*moduleState.validationState);
			break;
		case SectionID::export_:
			serializeExportSection(moduleStream, module);
			IR::validateExports(*moduleState.validationState);
			break;
		case SectionID::start:
			serializeStartSection(moduleStream, module);
			IR::validateStartFunction(*moduleState.validationState);
			break;
		case SectionID::elem:
			serializeElementSection(moduleStream, module);
			IR::validateElemSegments(*moduleState.validationState);
			break;
		case SectionID::dataCount:
			serializeDataCountSection(moduleStream, module);
			moduleState.hadDataCountSection = true;
			break;
		case SectionID::code:
			serializeCodeSection(moduleStream, module, moduleState);
			hadFunctionDefinitions = true;
			break;
		case SectionID::data:
			serializeDataSection(moduleStream, module, moduleState.hadDataCountSection);
			hadDataSection = true;
			IR::validateDataSegments(*moduleState.validationState);
			break;
		case SectionID::custom: {
			CustomSection& customSection
				= *module.customSections.insert(module.customSections.end(), CustomSection());
			customSection.afterSection = getMaxPresentSection(module, lastKnownOrderedSectionID);
			serialize(moduleStream, customSection);
			break;
		}
		default: throw FatalSerializationException("unknown section ID");
		};
	};

	if(module.functions.defs.size() && !hadFunctionDefinitions)
	{
		throw FatalSerializationException(
			"module contained function declarations, but no corresponding "
			"function definition section");
	}

	if(module.dataSegments.size() && !hadDataSection)
	{
		throw FatalSerializationException(
			"module contained DataCount section with non-zero segment count, but no corresponding "
			"Data section");
	}
}

std::vector<U8> WASM::saveBinaryModule(const Module& module)
{
	try
	{
		ArrayOutputStream stream;
		serializeModule(stream, const_cast<Module&>(module));
		return stream.getBytes();
	}
	catch(Serialization::FatalSerializationException const& exception)
	{
		Errors::fatalf("Failed to save WASM module: %s", exception.message.c_str());
	}
}

bool WASM::loadBinaryModule(const U8* wasmBytes,
							Uptr numWASMBytes,
							IR::Module& outModule,
							LoadError* outError)
{
	// Load the module from a binary WebAssembly file.
	try
	{
		Timing::Timer loadTimer;
		MemoryInputStream stream(wasmBytes, numWASMBytes);

		serializeModule(stream, outModule);

		Timing::logRatePerSecond("Loaded WASM", loadTimer, numWASMBytes / 1024.0 / 1024.0, "MiB");
		return true;
	}
	catch(Serialization::FatalSerializationException const& exception)
	{
		if(outError)
		{
			outError->type = LoadError::Type::malformed;
			outError->message = "Module was malformed: " + exception.message;
		}
		return false;
	}
	catch(IR::ValidationException const& exception)
	{
		if(outError)
		{
			outError->type = LoadError::Type::invalid;
			outError->message = "Module was invalid: " + exception.message;
		}
		return false;
	}
	catch(std::bad_alloc const&)
	{
		if(outError)
		{
			outError->type = LoadError::Type::malformed;
			outError->message = "Memory allocation failed: input is likely malformed";
		}
		return false;
	}
}
