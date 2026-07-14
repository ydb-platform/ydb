#pragma once

#include "WAVM/IR/IR.h"
#include "WAVM/IR/Module.h"
#include "WAVM/IR/Operators.h"
#include "WAVM/Inline/Assert.h"
#include "WAVM/Inline/BasicTypes.h"
#include "WAVM/Inline/Serialization.h"

namespace WAVM { namespace IR {
	struct OperatorPrinter
	{
		typedef std::string Result;

		OperatorPrinter(const Module& inModule, const FunctionDef& inFunctionDef)
		: module(inModule), functionDef(inFunctionDef)
		{
		}

#define VISIT_OPCODE(encoding, name, nameString, Imm, ...)                                         \
	std::string name(Imm imm = {}) { return std::string(nameString) + describeImm(imm); }
		WAVM_ENUM_OPERATORS(VISIT_OPCODE)
#undef VISIT_OPCODE

	private:
		const Module& module;
		const FunctionDef& functionDef;

		std::string describeImm(NoImm) { return ""; }
		std::string describeImm(ControlStructureImm imm)
		{
			const FunctionType type = resolveBlockType(module, imm.type);
			return std::string(" : ") + asString(type.params()) + " -> " + asString(type.results());
		}
		std::string describeImm(SelectImm imm) { return std::string(" ") + asString(imm.type); }
		std::string describeImm(BranchImm imm) { return " " + std::to_string(imm.targetDepth); }
		std::string describeImm(BranchTableImm imm)
		{
			std::string result = " " + std::to_string(imm.defaultTargetDepth);
			const char* prefix = " [";
			WAVM_ASSERT(imm.branchTableIndex < functionDef.branchTables.size());
			for(auto depth : functionDef.branchTables[imm.branchTableIndex])
			{
				result += prefix + std::to_string(depth);
				prefix = ",";
			}
			result += "]";
			return result;
		}
		template<typename NativeValue> std::string describeImm(LiteralImm<NativeValue> imm)
		{
			return " " + asString(imm.value);
		}
		template<bool isGlobal> std::string describeImm(GetOrSetVariableImm<isGlobal> imm)
		{
			return " " + std::to_string(imm.variableIndex);
		}
		std::string describeImm(FunctionImm imm)
		{
			const std::string typeString
				= imm.functionIndex >= module.functions.size()
					  ? "<invalid function index>"
					  : asString(module.types[module.functions.getType(imm.functionIndex).index]);
			return " " + std::to_string(imm.functionIndex) + " " + typeString;
		}
		std::string describeImm(FunctionRefImm imm)
		{
			return describeImm(FunctionImm{imm.functionIndex});
		}
		std::string describeImm(CallIndirectImm imm)
		{
			const std::string typeString = imm.type.index >= module.types.size()
											   ? "<invalid type index>"
											   : asString(module.types[imm.type.index]);
			return " " + typeString;
		}
		std::string describeImm(BaseLoadOrStoreImm imm)
		{
			return " " + std::to_string(imm.memoryIndex) + " offset=" + std::to_string(imm.offset)
				   + " align=" + std::to_string(1 << imm.alignmentLog2);
		}
		std::string describeImm(MemoryImm imm) { return " " + std::to_string(imm.memoryIndex); }
		std::string describeImm(MemoryCopyImm imm)
		{
			return " " + std::to_string(imm.sourceMemoryIndex) + " "
				   + std::to_string(imm.destMemoryIndex);
		}
		std::string describeImm(TableImm imm) { return " " + std::to_string(imm.tableIndex); }
		std::string describeImm(TableCopyImm imm)
		{
			return " " + std::to_string(imm.sourceTableIndex) + " "
				   + std::to_string(imm.destTableIndex);
		}

		template<Uptr numLanes> std::string describeImm(LaneIndexImm<numLanes> imm)
		{
			return " " + std::to_string(imm.laneIndex);
		}
		template<Uptr numLanes> std::string describeImm(ShuffleImm<numLanes> imm)
		{
			std::string result = " ";
			char prefix = '[';
			for(Uptr laneIndex = 0; laneIndex < numLanes; ++laneIndex)
			{
				result += prefix;
				result += imm.laneIndices[laneIndex] < numLanes ? 'a' : 'b';
				result += imm.laneIndices[laneIndex] < numLanes
							  ? std::to_string(imm.laneIndices[laneIndex])
							  : std::to_string(imm.laneIndices[laneIndex] - numLanes);
				prefix = ',';
			}
			result += ']';
			return result;
		}

		std::string describeImm(AtomicFenceImm imm)
		{
			switch(imm.order)
			{
			case MemoryOrder::sequentiallyConsistent: return " seqcst";
			default: WAVM_UNREACHABLE();
			};
		}
		std::string describeImm(ExceptionTypeImm) { return ""; }
		std::string describeImm(DelegateImm) { return ""; }
		std::string describeImm(RethrowImm) { return ""; }

		std::string describeImm(DataSegmentAndMemImm imm)
		{
			return std::to_string(imm.dataSegmentIndex) + " " + std::to_string(imm.memoryIndex);
		}
		std::string describeImm(DataSegmentImm imm)
		{
			return " " + std::to_string(imm.dataSegmentIndex);
		}
		std::string describeImm(ElemSegmentAndTableImm imm)
		{
			return " " + std::to_string(imm.elemSegmentIndex) + " "
				   + std::to_string(imm.tableIndex);
		}
		std::string describeImm(ElemSegmentImm imm)
		{
			return " " + std::to_string(imm.elemSegmentIndex);
		}

		std::string describeImm(ReferenceTypeImm imm)
		{
			return std::string(" ") + asString(imm.referenceType);
		}
	};
}}
