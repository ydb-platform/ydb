#pragma once

#include <string.h>
#include <vector>
#include "WAVM/IR/IR.h"
#include "WAVM/IR/Types.h"
#include "WAVM/Inline/Assert.h"
#include "WAVM/Inline/BasicTypes.h"
#include "WAVM/Inline/Serialization.h"
#include "WAVM/Platform/Defines.h"

#include "OperatorTable.h"

namespace WAVM { namespace IR {
	// Forward declarations.
	struct Module;

	// Structures for operator immediates

	struct NoImm
	{
	};
	struct MemoryImm
	{
		Uptr memoryIndex;
	};
	struct MemoryCopyImm
	{
		Uptr destMemoryIndex;
		Uptr sourceMemoryIndex;
	};
	struct TableImm
	{
		Uptr tableIndex;
	};
	struct TableCopyImm
	{
		Uptr destTableIndex;
		Uptr sourceTableIndex;
	};

	struct ControlStructureImm
	{
		IndexedBlockType type;
	};

	struct SelectImm
	{
		// ValueType::any represents a legacy select with the type inferred from the first operand
		// following the condition.
		ValueType type;
	};

	struct BranchImm
	{
		Uptr targetDepth;
	};

	struct BranchTableImm
	{
		Uptr defaultTargetDepth;

		// An index into the FunctionDef's branchTables array.
		Uptr branchTableIndex;
	};

	template<typename Value> struct LiteralImm
	{
		Value value;
	};

	template<bool isGlobal> struct GetOrSetVariableImm
	{
		Uptr variableIndex;
	};

	struct FunctionImm
	{
		Uptr functionIndex;
	};

	struct FunctionRefImm
	{
		Uptr functionIndex;
	};

	struct CallIndirectImm
	{
		IndexedFunctionType type;
		Uptr tableIndex;
	};

	struct BaseLoadOrStoreImm
	{
		U8 alignmentLog2;
		U64 offset;
		Uptr memoryIndex;
	};

	template<Uptr naturalAlignmentLog2> struct LoadOrStoreImm : BaseLoadOrStoreImm
	{
	};

	template<Uptr naturalAlignmentLog2, Uptr numLanes>
	struct LoadOrStoreLaneImm : LoadOrStoreImm<naturalAlignmentLog2>
	{
		U8 laneIndex;
	};

	using LoadOrStoreI8x16LaneImm = LoadOrStoreLaneImm<0, 16>;
	using LoadOrStoreI16x8LaneImm = LoadOrStoreLaneImm<1, 8>;
	using LoadOrStoreI32x4LaneImm = LoadOrStoreLaneImm<2, 4>;
	using LoadOrStoreI64x2LaneImm = LoadOrStoreLaneImm<3, 2>;

	template<Uptr numLanes> struct LaneIndexImm
	{
		U8 laneIndex;
	};

	template<Uptr numLanes> struct ShuffleImm
	{
		U8 laneIndices[numLanes];
	};

	template<Uptr naturalAlignmentLog2> struct AtomicLoadOrStoreImm : BaseLoadOrStoreImm
	{
	};

	enum class MemoryOrder
	{
		sequentiallyConsistent = 0
	};

	struct AtomicFenceImm
	{
		MemoryOrder order;
	};

	struct ExceptionTypeImm
	{
		Uptr exceptionTypeIndex;
	};
	struct DelegateImm
	{
		Uptr catchDepth;
	};
	struct RethrowImm
	{
		Uptr catchDepth;
	};

	struct DataSegmentAndMemImm
	{
		Uptr dataSegmentIndex;
		Uptr memoryIndex;
	};

	struct DataSegmentImm
	{
		Uptr dataSegmentIndex;
	};

	struct ElemSegmentAndTableImm
	{
		Uptr elemSegmentIndex;
		Uptr tableIndex;
	};

	struct ElemSegmentImm
	{
		Uptr elemSegmentIndex;
	};

	struct ReferenceTypeImm
	{
		ReferenceType referenceType;
	};

	enum class Opcode : U16
	{
#define VISIT_OPCODE(opcode, name, ...) name = opcode,
		WAVM_ENUM_OPERATORS(VISIT_OPCODE)
#undef VISIT_OPCODE
	};

	static constexpr U64 maxSingleByteOpcode = 0xdf;

	template<typename Imm> struct OpcodeAndImm
	{
		Opcode opcode;
		Imm imm;
	};

	// Specialize for the empty immediate struct so they don't take an extra byte of space.
	template<> struct OpcodeAndImm<NoImm>
	{
		union
		{
			Opcode opcode;
			NoImm imm;
		};
	};

	// Decodes an operator from an input stream and dispatches by opcode.
	struct OperatorDecoderStream
	{
		OperatorDecoderStream(const std::vector<U8>& codeBytes)
		: nextByte(codeBytes.data()), end(codeBytes.data() + codeBytes.size())
		{
		}

		operator bool() const { return nextByte < end; }

		template<typename Visitor> typename Visitor::Result decodeOp(Visitor& visitor)
		{
			WAVM_ASSERT(nextByte + sizeof(Opcode) <= end);
			Opcode opcode;
			memcpy(&opcode, nextByte, sizeof(Opcode));
			switch(opcode)
			{
#define VISIT_OPCODE(opcode, name, nameString, Imm, ...)                                           \
	case Opcode::name: {                                                                           \
		WAVM_ASSERT(nextByte + sizeof(OpcodeAndImm<Imm>) <= end);                                  \
		OpcodeAndImm<Imm> encodedOperator;                                                         \
		memcpy(&encodedOperator, nextByte, sizeof(OpcodeAndImm<Imm>));                             \
		nextByte += sizeof(OpcodeAndImm<Imm>);                                                     \
		return visitor.name(encodedOperator.imm);                                                  \
	}
				WAVM_ENUM_OPERATORS(VISIT_OPCODE)
#undef VISIT_OPCODE
			default: WAVM_UNREACHABLE();
			}
		}

		template<typename Visitor> typename Visitor::Result decodeOpWithoutConsume(Visitor& visitor)
		{
			const U8* savedNextByte = nextByte;
			typename Visitor::Result result = decodeOp(visitor);
			nextByte = savedNextByte;
			return result;
		}

	private:
		const U8* nextByte;
		const U8* end;
	};

	// Encodes an operator to an output stream.
	struct OperatorEncoderStream
	{
		OperatorEncoderStream(Serialization::OutputStream& inByteStream) : byteStream(inByteStream)
		{
		}

#define VISIT_OPCODE(_, name, nameString, Imm, ...)                                                \
	void name(Imm imm = {})                                                                        \
	{                                                                                              \
		OpcodeAndImm<Imm> encodedOperator;                                                         \
		encodedOperator.opcode = Opcode::name;                                                     \
		encodedOperator.imm = imm;                                                                 \
		memcpy((OpcodeAndImm<Imm>*)byteStream.advance(sizeof(OpcodeAndImm<Imm>)),                  \
			   &encodedOperator,                                                                   \
			   sizeof(OpcodeAndImm<Imm>));                                                         \
	}
		WAVM_ENUM_OPERATORS(VISIT_OPCODE)
#undef VISIT_OPCODE

	private:
		Serialization::OutputStream& byteStream;
	};

	WAVM_API const char* getOpcodeName(Opcode opcode);
}}
