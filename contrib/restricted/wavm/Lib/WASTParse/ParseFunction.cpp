#include <inttypes.h>
#include <stdint.h>
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "Lexer.h"
#include "Parse.h"
#include "WAVM/IR/Module.h"
#include "WAVM/IR/Operators.h"
#include "WAVM/IR/Types.h"
#include "WAVM/IR/Validate.h"
#include "WAVM/Inline/Assert.h"
#include "WAVM/Inline/BasicTypes.h"
#include "WAVM/Inline/Errors.h"
#include "WAVM/Inline/HashMap.h"
#include "WAVM/Inline/Serialization.h"
#include "WAVM/Platform/Intrinsic.h"

using namespace WAVM;
using namespace WAVM::IR;
using namespace WAVM::WAST;

namespace WAVM { namespace WAST {

	template<typename InnerStream> struct ResumableCodeValidationProxyStream
	{
		const Token* validationErrorToken{nullptr};

		ResumableCodeValidationProxyStream(ModuleState* moduleState,
										   const FunctionDef& function,
										   InnerStream& inInnerStream)
		: codeValidationStream(*moduleState->validationState, function)
		, innerStream(inInnerStream)
		, parseState(moduleState->parseState)
		{
		}

#define VISIT_OPERATOR(name, Imm, isControlOp)                                                     \
	void name(Imm imm = {})                                                                        \
	{                                                                                              \
		try                                                                                        \
		{                                                                                          \
			codeValidationStream.name(imm);                                                        \
			innerStream.name(imm);                                                                 \
		}                                                                                          \
		catch(ValidationException const& exception)                                                \
		{                                                                                          \
			try                                                                                    \
			{                                                                                      \
				codeValidationStream.unreachable();                                                \
				innerStream.unreachable();                                                         \
			}                                                                                      \
			catch(ValidationException const&)                                                      \
			{                                                                                      \
				/* If a second validation exception occurs trying to emit unreachable, just */     \
				/* throw the original exception up to the top level. */                            \
				throw exception;                                                                   \
			}                                                                                      \
			parseErrorf(parseState,                                                                \
						validationErrorToken,                                                      \
						"validation error: %s",                                                    \
						exception.message.c_str());                                                \
			/* The validator won't have the correct control state after an invalid control */      \
			/* operator, so just continue parsing at the next recovery point. */                   \
			if(isControlOp) { throw RecoverParseException(); }                                     \
		}                                                                                          \
	}
#define VISIT_CONTROL_OPERATOR(_1, name, _2, Imm, ...) VISIT_OPERATOR(name, Imm, true)
#define VISIT_NONCONTROL_OPERATOR(_1, name, _2, Imm, ...) VISIT_OPERATOR(name, Imm, false)

		WAVM_ENUM_NONCONTROL_OPERATORS(VISIT_NONCONTROL_OPERATOR)
		WAVM_ENUM_CONTROL_OPERATORS(VISIT_CONTROL_OPERATOR)
#undef VISIT_CONTROL_OPERATOR
#undef VISIT_NONCONTROL_OPERATOR

		void finishValidation() { codeValidationStream.finish(); }

	private:
		CodeValidationStream codeValidationStream;
		InnerStream& innerStream;
		ParseState* parseState;
	};

	// State associated with parsing a function.
	struct FunctionState
	{
		FunctionDef& functionDef;

		std::shared_ptr<NameToIndexMap> localNameToIndexMap;
		Uptr numLocals;

		NameToIndexMap branchTargetNameToIndexMap;
		Uptr branchTargetDepth;
		std::vector<std::string> labelDisassemblyNames;

		Serialization::ArrayOutputStream codeByteStream;
		OperatorEncoderStream operationEncoder;
		ResumableCodeValidationProxyStream<OperatorEncoderStream> validatingCodeStream;

		FunctionState(const std::shared_ptr<NameToIndexMap>& inLocalNameToIndexMap,
					  FunctionDef& inFunctionDef,
					  ModuleState* moduleState)
		: functionDef(inFunctionDef)
		, localNameToIndexMap(inLocalNameToIndexMap)
		, numLocals(inFunctionDef.nonParameterLocalTypes.size()
					+ moduleState->module.types[inFunctionDef.type.index].params().size())
		, branchTargetDepth(0)
		, operationEncoder(codeByteStream)
		, validatingCodeStream(moduleState, inFunctionDef, operationEncoder)
		{
		}
	};
}}

namespace {
	// While in scope, pushes a branch target onto the branch target stack.
	// Also maintains the branchTargetNameToIndexMap
	struct ScopedBranchTarget
	{
		ScopedBranchTarget(FunctionState* inFunctionState, Name inName)
		: functionState(inFunctionState), name(inName), previousBranchTargetIndex(UINTPTR_MAX)
		{
			branchTargetIndex = ++functionState->branchTargetDepth;
			if(name)
			{
				Uptr& mapValueRef
					= functionState->branchTargetNameToIndexMap.getOrAdd(name, UINTPTR_MAX);
				if(mapValueRef != UINTPTR_MAX)
				{
					// If the name was already bound to a branch target, remember the previously
					// bound branch target.
					previousBranchTargetIndex = mapValueRef;
					mapValueRef = branchTargetIndex;
				}
				else
				{
					mapValueRef = branchTargetIndex;
				}
			}
		}

		~ScopedBranchTarget()
		{
			WAVM_ASSERT(branchTargetIndex == functionState->branchTargetDepth);
			--functionState->branchTargetDepth;
			if(name)
			{
				WAVM_ASSERT(functionState->branchTargetNameToIndexMap.contains(name));
				WAVM_ASSERT(functionState->branchTargetNameToIndexMap[name] == branchTargetIndex);
				if(previousBranchTargetIndex == UINTPTR_MAX)
				{ WAVM_ERROR_UNLESS(functionState->branchTargetNameToIndexMap.remove(name)); }
				else
				{
					functionState->branchTargetNameToIndexMap.set(name, previousBranchTargetIndex);
				}
			}
		}

	private:
		FunctionState* functionState;
		Name name;
		Uptr branchTargetIndex;
		Uptr previousBranchTargetIndex;
	};
}

static bool tryParseAndResolveBranchTargetRef(CursorState* cursor, Uptr& outTargetDepth)
{
	Reference branchTargetRef;
	if(tryParseNameOrIndexRef(cursor, branchTargetRef))
	{
		switch(branchTargetRef.type)
		{
		case Reference::Type::index: outTargetDepth = branchTargetRef.index; break;
		case Reference::Type::name: {
			const HashMapPair<Name, Uptr>* nameIndexPair
				= cursor->functionState->branchTargetNameToIndexMap.getPair(branchTargetRef.name);
			if(!nameIndexPair)
			{
				parseErrorf(cursor->parseState, branchTargetRef.token, "unknown name");
				outTargetDepth = UINTPTR_MAX;
			}
			else
			{
				outTargetDepth = cursor->functionState->branchTargetDepth - nameIndexPair->value;
			}
			break;
		}

		case Reference::Type::invalid:
		default: WAVM_UNREACHABLE();
		};
		return true;
	}
	return false;
}

static void parseAndValidateRedundantBranchTargetName(CursorState* cursor,
													  Name branchTargetName,
													  const char* context,
													  const char* redundantContext)
{
	Name redundantName;
	if(tryParseName(cursor, redundantName) && branchTargetName != redundantName)
	{
		parseErrorf(cursor->parseState,
					cursor->nextToken - 1,
					"%s label doesn't match %s label",
					redundantContext,
					context);
	}
}

static void parseImm(CursorState* cursor, NoImm&) {}
static void parseImm(CursorState* cursor, MemoryImm& outImm)
{
	if(!tryParseAndResolveNameOrIndexRef(cursor,
										 cursor->moduleState->memoryNameToIndexMap,
										 cursor->moduleState->module.memories.size(),
										 "memory",
										 outImm.memoryIndex))
	{ outImm.memoryIndex = 0; }
}
static void parseImm(CursorState* cursor, MemoryCopyImm& outImm)
{
	if(!tryParseAndResolveNameOrIndexRef(cursor,
										 cursor->moduleState->memoryNameToIndexMap,
										 cursor->moduleState->module.memories.size(),
										 "memory",
										 outImm.destMemoryIndex))
	{ outImm.destMemoryIndex = 0; }

	if(!tryParseAndResolveNameOrIndexRef(cursor,
										 cursor->moduleState->memoryNameToIndexMap,
										 cursor->moduleState->module.memories.size(),
										 "memory",
										 outImm.sourceMemoryIndex))
	{ outImm.sourceMemoryIndex = outImm.destMemoryIndex; }
}
static void parseImm(CursorState* cursor, TableImm& outImm)
{
	if(!tryParseAndResolveNameOrIndexRef(cursor,
										 cursor->moduleState->tableNameToIndexMap,
										 cursor->moduleState->module.tables.size(),
										 "table",
										 outImm.tableIndex))
	{ outImm.tableIndex = 0; }
}
static void parseImm(CursorState* cursor, TableCopyImm& outImm)
{
	if(!tryParseAndResolveNameOrIndexRef(cursor,
										 cursor->moduleState->tableNameToIndexMap,
										 cursor->moduleState->module.tables.size(),
										 "table",
										 outImm.destTableIndex))
	{ outImm.destTableIndex = 0; }

	if(!tryParseAndResolveNameOrIndexRef(cursor,
										 cursor->moduleState->tableNameToIndexMap,
										 cursor->moduleState->module.tables.size(),
										 "table",
										 outImm.sourceTableIndex))
	{ outImm.sourceTableIndex = outImm.destTableIndex; }
}

static void parseImm(CursorState* cursor, SelectImm& outImm)
{
	outImm.type = ValueType::any;
	const Token* firstResultToken = nullptr;
	while(cursor->nextToken[0].type == t_leftParenthesis && cursor->nextToken[1].type == t_result)
	{
		parseParenthesized(cursor, [&] {
			const Token* resultToken = cursor->nextToken;
			require(cursor, t_result);
			if(!firstResultToken) { firstResultToken = resultToken; }

			const Token* valueTypeToken = cursor->nextToken;
			ValueType result;
			while(tryParseValueType(cursor, result))
			{
				if(outImm.type == ValueType::any) { outImm.type = result; }
				else
				{
					parseErrorf(cursor->parseState,
								valueTypeToken,
								"validation error: typed select must have exactly one result");
				}
			};
		});
	};

	if(firstResultToken && outImm.type == ValueType::any)
	{
		parseErrorf(cursor->parseState,
					firstResultToken,
					"validation error: typed select must have exactly one result");
	}
}

static void parseImm(CursorState* cursor, LiteralImm<I32>& outImm)
{
	outImm.value = parseI32(cursor);
}
static void parseImm(CursorState* cursor, LiteralImm<I64>& outImm)
{
	outImm.value = parseI64(cursor);
}
static void parseImm(CursorState* cursor, LiteralImm<F32>& outImm)
{
	outImm.value = parseF32(cursor);
}
static void parseImm(CursorState* cursor, LiteralImm<F64>& outImm)
{
	outImm.value = parseF64(cursor);
}

static void parseImm(CursorState* cursor, BranchImm& outImm)
{
	if(!tryParseAndResolveBranchTargetRef(cursor, outImm.targetDepth))
	{
		parseErrorf(cursor->parseState, cursor->nextToken, "expected branch target name or index");
		throw RecoverParseException();
	}
}

static void parseImm(CursorState* cursor, BranchTableImm& outImm)
{
	std::vector<Uptr> targetDepths;
	Uptr targetDepth = 0;
	while(tryParseAndResolveBranchTargetRef(cursor, targetDepth))
	{ targetDepths.push_back(targetDepth); };

	if(!targetDepths.size())
	{
		parseErrorf(cursor->parseState, cursor->nextToken, "expected branch target name or index");
		throw RecoverParseException();
	}
	else
	{
		outImm.defaultTargetDepth = targetDepths.back();
		targetDepths.pop_back();
		outImm.branchTableIndex = cursor->functionState->functionDef.branchTables.size();
		cursor->functionState->functionDef.branchTables.push_back(std::move(targetDepths));
	}
}

template<bool isGlobal>
static void parseImm(CursorState* cursor, GetOrSetVariableImm<isGlobal>& outImm)
{
	outImm.variableIndex = parseAndResolveNameOrIndexRef(
		cursor,
		isGlobal ? cursor->moduleState->globalNameToIndexMap
				 : *cursor->functionState->localNameToIndexMap,
		isGlobal ? cursor->moduleState->module.globals.size() : cursor->functionState->numLocals,
		isGlobal ? "global" : "local");
}

static void parseImm(CursorState* cursor, FunctionImm& outImm)
{
	outImm.functionIndex
		= parseAndResolveNameOrIndexRef(cursor,
										cursor->moduleState->functionNameToIndexMap,
										cursor->moduleState->module.functions.size(),
										"function");
}

static void parseImm(CursorState* cursor, FunctionRefImm& outImm)
{
	outImm.functionIndex
		= parseAndResolveNameOrIndexRef(cursor,
										cursor->moduleState->functionNameToIndexMap,
										cursor->moduleState->module.functions.size(),
										"function");
}

static void parseImm(CursorState* cursor, CallIndirectImm& outImm)
{
	if(cursor->nextToken->type == t_name || cursor->nextToken->type == t_quotedName
	   || cursor->nextToken->type == t_decimalInt || cursor->nextToken->type == t_hexInt)
	{
		// Parse a table name or index.
		outImm.tableIndex = parseAndResolveNameOrIndexRef(cursor,
														  cursor->moduleState->tableNameToIndexMap,
														  cursor->moduleState->module.tables.size(),
														  "table");
	}
	else
	{
		outImm.tableIndex = 0;
	}

	// Parse the callee type, as a reference or explicit declaration.
	const Token* firstTypeToken = cursor->nextToken;
	std::vector<std::string> paramDisassemblyNames;
	NameToIndexMap paramNameToIndexMap;
	const UnresolvedFunctionType unresolvedFunctionType
		= parseFunctionTypeRefAndOrDecl(cursor, paramNameToIndexMap, paramDisassemblyNames);
	outImm.type.index = resolveFunctionType(cursor->moduleState, unresolvedFunctionType).index;

	// Disallow named parameters.
	if(paramNameToIndexMap.size())
	{
		auto paramNameIt = paramNameToIndexMap.begin();
		parseErrorf(cursor->parseState,
					firstTypeToken,
					"call_indirect callee type declaration may not declare parameter names ($%s)",
					paramNameIt->key.getString().c_str());
	}
}

template<Uptr naturalAlignmentLog2>
static void parseImm(CursorState* cursor, LoadOrStoreImm<naturalAlignmentLog2>& outImm)
{
	if(!tryParseAndResolveNameOrIndexRef(cursor,
										 cursor->moduleState->memoryNameToIndexMap,
										 cursor->moduleState->module.memories.size(),
										 "memory",
										 outImm.memoryIndex))
	{ outImm.memoryIndex = 0; }

	outImm.offset = 0;
	if(cursor->nextToken->type == t_offset)
	{
		++cursor->nextToken;
		require(cursor, t_equals);
		outImm.offset = cursor->moduleState->module.featureSpec.memory64 ? parseU64(cursor)
																		 : parseU32(cursor);
	}

	const U32 naturalAlignment = 1 << naturalAlignmentLog2;
	U32 alignment = naturalAlignment;
	if(cursor->nextToken->type == t_align)
	{
		++cursor->nextToken;
		require(cursor, t_equals);
		const Token* alignmentToken = cursor->nextToken;
		alignment = parseU32(cursor);

		if(!alignment || alignment & (alignment - 1))
		{ parseErrorf(cursor->parseState, cursor->nextToken, "alignment must be power of 2"); }
		else if(alignment > naturalAlignment)
		{
			parseErrorf(cursor->parseState,
						alignmentToken,
						"validation error: alignment must be <= natural alignment");
			alignment = naturalAlignment;
		}
	}

	outImm.alignmentLog2 = (U8)floorLogTwo(alignment);
}

static bool isIntLiteral(TokenType tokenType)
{
	return tokenType == t_hexInt || tokenType == t_decimalInt;
}

template<Uptr naturalAlignmentLog2, Uptr numLanes>
static void parseImm(CursorState* cursor,
					 LoadOrStoreLaneImm<naturalAlignmentLog2, numLanes>& outImm)
{
	// If the first immediate is a name, or an integer followed by another integer, alignment, or
	// offset, interpret it as a memory reference.
	if(cursor->nextToken->type == t_name || cursor->nextToken->type == t_quotedName
	   || (isIntLiteral(cursor->nextToken->type)
		   && (isIntLiteral(cursor->nextToken[1].type) || cursor->nextToken[1].type == t_offset
			   || cursor->nextToken[1].type == t_align)))
	{
		outImm.memoryIndex
			= parseAndResolveNameOrIndexRef(cursor,
											cursor->moduleState->memoryNameToIndexMap,
											cursor->moduleState->module.memories.size(),
											"memory");
	}
	else
	{
		outImm.memoryIndex = 0;
	}

	outImm.offset = 0;
	if(cursor->nextToken->type == t_offset)
	{
		++cursor->nextToken;
		require(cursor, t_equals);
		outImm.offset = cursor->moduleState->module.featureSpec.memory64 ? parseU64(cursor)
																		 : parseU32(cursor);
	}

	const U32 naturalAlignment = 1 << naturalAlignmentLog2;
	U32 alignment = naturalAlignment;
	if(cursor->nextToken->type == t_align)
	{
		++cursor->nextToken;
		require(cursor, t_equals);
		const Token* alignmentToken = cursor->nextToken;
		alignment = parseU32(cursor);

		if(!alignment || alignment & (alignment - 1))
		{ parseErrorf(cursor->parseState, cursor->nextToken, "alignment must be power of 2"); }
		else if(alignment > naturalAlignment)
		{
			parseErrorf(cursor->parseState,
						alignmentToken,
						"validation error: alignment must be <= natural alignment");
			alignment = naturalAlignment;
		}
	}

	outImm.alignmentLog2 = (U8)floorLogTwo(alignment);

	U8 laneIndex = parseU8(cursor, false);
	if(Uptr(laneIndex) >= numLanes)
	{
		parseErrorf(cursor->parseState,
					cursor->nextToken - 1,
					"validation error: lane index must be in the range 0..%" WAVM_PRIuPTR,
					numLanes - 1);
		laneIndex = 0;
	}
	outImm.laneIndex = laneIndex;
}

static void parseImm(CursorState* cursor, LiteralImm<V128>& outImm)
{
	outImm.value = parseV128(cursor);
}

template<Uptr numLanes> static void parseImm(CursorState* cursor, LaneIndexImm<numLanes>& outImm)
{
	U8 laneIndex = parseU8(cursor, false);
	if(Uptr(laneIndex) >= numLanes)
	{
		parseErrorf(cursor->parseState,
					cursor->nextToken - 1,
					"validation error: lane index must be in the range 0..%" WAVM_PRIuPTR,
					numLanes - 1);
		laneIndex = 0;
	}
	outImm.laneIndex = laneIndex;
}

template<Uptr numLanes> static void parseImm(CursorState* cursor, ShuffleImm<numLanes>& outImm)
{
	for(Uptr destLaneIndex = 0; destLaneIndex < numLanes; ++destLaneIndex)
	{
		U8 sourceLaneIndex = parseU8(cursor, false);
		if(Uptr(sourceLaneIndex) >= numLanes * 2)
		{
			parseErrorf(cursor->parseState,
						cursor->nextToken - 1,
						"validation error: lane index must be in the range 0..%" WAVM_PRIuPTR,
						numLanes * 2 - 1);
			sourceLaneIndex = 0;
		}
		outImm.laneIndices[destLaneIndex] = sourceLaneIndex;
	}
}

template<Uptr naturalAlignmentLog2>
static void parseImm(CursorState* cursor, AtomicLoadOrStoreImm<naturalAlignmentLog2>& outImm)
{
	LoadOrStoreImm<naturalAlignmentLog2> loadOrStoreImm;
	parseImm(cursor, loadOrStoreImm);
	outImm.memoryIndex = loadOrStoreImm.memoryIndex;
	outImm.alignmentLog2 = loadOrStoreImm.alignmentLog2;
	outImm.offset = loadOrStoreImm.offset;
}

static void parseImm(CursorState* cursor, AtomicFenceImm& outImm)
{
	outImm.order = MemoryOrder::sequentiallyConsistent;
}

static void parseImm(CursorState* cursor, ExceptionTypeImm& outImm)
{
	outImm.exceptionTypeIndex
		= parseAndResolveNameOrIndexRef(cursor,
										cursor->moduleState->exceptionTypeNameToIndexMap,
										cursor->moduleState->module.exceptionTypes.size(),
										"exception type");
}
static void parseImm(CursorState* cursor, DelegateImm& outImm)
{
	if(!tryParseAndResolveBranchTargetRef(cursor, outImm.catchDepth))
	{
		parseErrorf(cursor->parseState, cursor->nextToken, "expected try label or index");
		throw RecoverParseException();
	}
}
static void parseImm(CursorState* cursor, RethrowImm& outImm)
{
	if(!tryParseAndResolveBranchTargetRef(cursor, outImm.catchDepth))
	{
		parseErrorf(cursor->parseState, cursor->nextToken, "expected try label or index");
		throw RecoverParseException();
	}
}

static void parseImm(CursorState* cursor, DataSegmentAndMemImm& outImm)
{
	Reference firstRef;
	if(!tryParseNameOrIndexRef(cursor, firstRef))
	{
		parseErrorf(cursor->parseState, cursor->nextToken, "expected data segment name or index");
		throw RecoverParseException();
	}
	else
	{
		Reference secondRef;
		const bool hasSecondRef = tryParseNameOrIndexRef(cursor, secondRef);

		outImm.memoryIndex = hasSecondRef ? resolveRef(cursor->parseState,
													   cursor->moduleState->memoryNameToIndexMap,
													   cursor->moduleState->module.memories.size(),
													   firstRef)
										  : 0;
		outImm.dataSegmentIndex = resolveRef(cursor->parseState,
											 cursor->moduleState->dataNameToIndexMap,
											 cursor->moduleState->module.dataSegments.size(),
											 hasSecondRef ? secondRef : firstRef);
	}
}

static void parseImm(CursorState* cursor, DataSegmentImm& outImm)
{
	outImm.dataSegmentIndex
		= parseAndResolveNameOrIndexRef(cursor,
										cursor->moduleState->dataNameToIndexMap,
										cursor->moduleState->module.dataSegments.size(),
										"data");
}

static void parseImm(CursorState* cursor, ElemSegmentAndTableImm& outImm)
{
	Reference firstRef;
	if(!tryParseNameOrIndexRef(cursor, firstRef))
	{
		parseErrorf(cursor->parseState, cursor->nextToken, "expected elem segment name or index");
		throw RecoverParseException();
	}
	else
	{
		Reference secondRef;
		const bool hasSecondRef = tryParseNameOrIndexRef(cursor, secondRef);

		outImm.tableIndex = hasSecondRef ? resolveRef(cursor->parseState,
													  cursor->moduleState->tableNameToIndexMap,
													  cursor->moduleState->module.tables.size(),
													  firstRef)
										 : 0;
		outImm.elemSegmentIndex = resolveRef(cursor->parseState,
											 cursor->moduleState->elemNameToIndexMap,
											 cursor->moduleState->module.elemSegments.size(),
											 hasSecondRef ? secondRef : firstRef);
	}
}

static void parseImm(CursorState* cursor, ElemSegmentImm& outImm)
{
	outImm.elemSegmentIndex
		= parseAndResolveNameOrIndexRef(cursor,
										cursor->moduleState->elemNameToIndexMap,
										cursor->moduleState->module.elemSegments.size(),
										"elem");
}

static void parseImm(CursorState* cursor, ReferenceTypeImm& outImm)
{
	outImm.referenceType = parseReferencedType(cursor);
}

static void parseInstrSequence(CursorState* cursor, Uptr depth);
static void parseExpr(CursorState* cursor, Uptr depth);

static void parseControlImm(CursorState* cursor,
							Name& outBranchTargetName,
							ControlStructureImm& imm)
{
	tryParseName(cursor, outBranchTargetName);
	cursor->functionState->labelDisassemblyNames.push_back(outBranchTargetName.getString());

	FunctionType functionType;

	// For backward compatibility, handle a naked result type.
	ValueType singleResultType;
	if(tryParseValueType(cursor, singleResultType))
	{ functionType = FunctionType(TypeTuple(singleResultType)); }
	else
	{
		// Parse the callee type, as a reference or explicit declaration.
		const Token* firstTypeToken = cursor->nextToken;
		std::vector<std::string> paramDisassemblyNames;
		NameToIndexMap paramNameToIndexMap;
		const UnresolvedFunctionType unresolvedFunctionType
			= parseFunctionTypeRefAndOrDecl(cursor, paramNameToIndexMap, paramDisassemblyNames);

		// Disallow named parameters.
		if(paramNameToIndexMap.size())
		{
			auto paramNameIt = paramNameToIndexMap.begin();
			parseErrorf(cursor->parseState,
						firstTypeToken,
						"block type declaration may not declare parameter names ($%s)",
						paramNameIt->key.getString().c_str());
		}

		if(!unresolvedFunctionType.reference)
		{
			// If there wasn't a type reference, just use the inline declared params and results.
			functionType = unresolvedFunctionType.explicitType;
		}
		else
		{
			// If there was a type reference, resolve it. This also verifies that if there were also
			// params and/or results declared inline that they match the resolved type reference.
			const Uptr referencedFunctionTypeIndex
				= resolveFunctionType(cursor->moduleState, unresolvedFunctionType).index;
			if(referencedFunctionTypeIndex != UINTPTR_MAX)
			{
				WAVM_ASSERT(referencedFunctionTypeIndex < cursor->moduleState->module.types.size());
				functionType = cursor->moduleState->module.types[referencedFunctionTypeIndex];
			}
		}
	}

	// Translate the function type into an indexed block type.
	if(functionType.params().size() == 0 && functionType.results().size() == 0)
	{
		imm.type.format = IndexedBlockType::noParametersOrResult;
		imm.type.resultType = ValueType::none;
	}
	else if(functionType.params().size() == 0 && functionType.results().size() == 1)
	{
		imm.type.format = IndexedBlockType::oneResult;
		imm.type.resultType = functionType.results()[0];
	}
	else
	{
		imm.type.format = IndexedBlockType::functionType;
		imm.type.index = getUniqueFunctionTypeIndex(cursor->moduleState, functionType).index;
	}
}

static void checkRecursionDepth(CursorState* cursor, Uptr depth)
{
	if(depth > cursor->moduleState->module.featureSpec.maxSyntaxRecursion)
	{
		parseErrorf(cursor->parseState, cursor->nextToken, "exceeded maximum recursion depth");
		throw RecoverParseException();
	}
}

static WAVM_FORCENOINLINE void parseBlock(CursorState* cursor, bool isExpr, Uptr depth)
{
	Name branchTargetName;
	ControlStructureImm imm;
	parseControlImm(cursor, branchTargetName, imm);

	ScopedBranchTarget branchTarget(cursor->functionState, branchTargetName);
	cursor->functionState->validatingCodeStream.block(imm);
	parseInstrSequence(cursor, depth);
	cursor->functionState->validatingCodeStream.end();

	if(!isExpr)
	{
		require(cursor, t_end);
		parseAndValidateRedundantBranchTargetName(cursor, branchTargetName, "block", "end");
	}
}

static WAVM_FORCENOINLINE void parseLoop(CursorState* cursor, bool isExpr, Uptr depth)
{
	Name branchTargetName;
	ControlStructureImm imm;
	parseControlImm(cursor, branchTargetName, imm);

	ScopedBranchTarget branchTarget(cursor->functionState, branchTargetName);
	cursor->functionState->validatingCodeStream.loop(imm);
	parseInstrSequence(cursor, depth);
	cursor->functionState->validatingCodeStream.end();

	if(!isExpr)
	{
		require(cursor, t_end);
		parseAndValidateRedundantBranchTargetName(cursor, branchTargetName, "loop", "end");
	}
}

static WAVM_FORCENOINLINE void parseIfInstr(CursorState* cursor, Uptr depth)
{
	Name branchTargetName;
	ControlStructureImm imm;
	parseControlImm(cursor, branchTargetName, imm);

	ScopedBranchTarget branchTarget(cursor->functionState, branchTargetName);
	cursor->functionState->validatingCodeStream.if_(imm);

	// Parse the then clause.
	parseInstrSequence(cursor, depth);

	// Parse the else clause.
	if(cursor->nextToken->type == t_else_)
	{
		++cursor->nextToken;
		parseAndValidateRedundantBranchTargetName(cursor, branchTargetName, "if", "else");

		cursor->functionState->validatingCodeStream.else_();
		parseInstrSequence(cursor, depth);
	}
	cursor->functionState->validatingCodeStream.end();

	require(cursor, t_end);
	parseAndValidateRedundantBranchTargetName(cursor, branchTargetName, "if", "end");
}

static WAVM_FORCENOINLINE void parseIfExpr(CursorState* cursor, Uptr depth)
{
	Name branchTargetName;
	ControlStructureImm imm;
	parseControlImm(cursor, branchTargetName, imm);

	// Parse an optional condition expression.
	if(cursor->nextToken[0].type != t_leftParenthesis || cursor->nextToken[1].type != t_then)
	{ parseExpr(cursor, depth); }

	ScopedBranchTarget branchTarget(cursor->functionState, branchTargetName);
	cursor->functionState->validatingCodeStream.if_(imm);

	// Parse the if clauses.
	if(cursor->nextToken[0].type == t_leftParenthesis && cursor->nextToken[1].type == t_then)
	{
		// First syntax: (then <instr>)* (else <instr>*)?
		parseParenthesized(cursor, [&] {
			require(cursor, t_then);
			parseInstrSequence(cursor, depth);
		});
		if(cursor->nextToken->type == t_leftParenthesis)
		{
			parseParenthesized(cursor, [&] {
				require(cursor, t_else_);
				cursor->functionState->validatingCodeStream.validationErrorToken
					= cursor->nextToken;
				cursor->functionState->validatingCodeStream.else_();
				parseInstrSequence(cursor, depth);
			});
		}
	}
	else
	{
		// Second syntax option: <expr> <expr>?
		parseExpr(cursor, depth);
		if(cursor->nextToken->type != t_rightParenthesis)
		{
			cursor->functionState->validatingCodeStream.else_();
			parseExpr(cursor, depth);
		}
	}
	cursor->functionState->validatingCodeStream.end();
}

static WAVM_FORCENOINLINE void parseTryInstr(CursorState* cursor, Uptr depth)
{
	Name branchTargetName;
	ControlStructureImm imm;
	parseControlImm(cursor, branchTargetName, imm);

	ScopedBranchTarget branchTarget(cursor->functionState, branchTargetName);
	cursor->functionState->validatingCodeStream.try_(imm);

	// Parse the try clause.
	parseInstrSequence(cursor, depth);

	// Parse catch clauses.
	while(cursor->nextToken->type != t_end)
	{
		if(cursor->nextToken->type == t_catch_)
		{
			++cursor->nextToken;
			ExceptionTypeImm exceptionTypeImm;
			parseImm(cursor, exceptionTypeImm);
			cursor->functionState->validatingCodeStream.catch_(exceptionTypeImm);
			parseInstrSequence(cursor, depth);
		}
		else if(cursor->nextToken->type == t_catch_all)
		{
			++cursor->nextToken;
			cursor->functionState->validatingCodeStream.catch_all();
			parseInstrSequence(cursor, depth);
		}
		else
		{
			parseErrorf(cursor->parseState,
						cursor->nextToken,
						"expected 'catch', 'catch_all', or 'end' following 'try'");
			throw RecoverParseException();
		}
	};

	require(cursor, t_end);
	parseAndValidateRedundantBranchTargetName(cursor, branchTargetName, "try", "end");
	cursor->functionState->validatingCodeStream.end();
}

static WAVM_FORCENOINLINE void parseExprSequence(CursorState* cursor, Uptr depth)
{
	while(cursor->nextToken->type != t_rightParenthesis) { parseExpr(cursor, depth); };
}

#define VISIT_OP(opcode, name, nameString, Imm, ...)                                               \
	static WAVM_FORCENOINLINE void parseOp_##name(                                                 \
		CursorState* cursor, bool isExpression, Uptr depth)                                        \
	{                                                                                              \
		const Token* opcodeToken = cursor->nextToken;                                              \
		++cursor->nextToken;                                                                       \
		Imm imm;                                                                                   \
		parseImm(cursor, imm);                                                                     \
		if(isExpression) { parseExprSequence(cursor, depth); }                                     \
		cursor->functionState->validatingCodeStream.validationErrorToken = opcodeToken;            \
		cursor->functionState->validatingCodeStream.name(imm);                                     \
	}
WAVM_ENUM_NONCONTROL_OPERATORS(VISIT_OP)
#undef VISIT_OP

static void parseExpr(CursorState* cursor, Uptr depth)
{
	++depth;
	checkRecursionDepth(cursor, depth);

	parseParenthesized(cursor, [&] {
		cursor->functionState->validatingCodeStream.validationErrorToken = cursor->nextToken;
		try
		{
			switch(cursor->nextToken->type)
			{
			case t_block: {
				++cursor->nextToken;
				parseBlock(cursor, true, depth);
				break;
			}
			case t_loop: {
				++cursor->nextToken;
				parseLoop(cursor, true, depth);
				break;
			}
			case t_if_: {
				++cursor->nextToken;
				parseIfExpr(cursor, depth);
				break;
			}
#define VISIT_OP(opcode, name, nameString, Imm, ...)                                               \
	case t_##name:                                                                                 \
		parseOp_##name(cursor, true, depth);                                                       \
		break;
				WAVM_ENUM_NONCONTROL_OPERATORS(VISIT_OP)
#undef VISIT_OP
			case t_legacyInstructionName:
				parseErrorf(cursor->parseState,
							cursor->nextToken,
							"legacy instruction name: requires the legacy-instr-name feature.");
				throw RecoverParseException();
			default:
				parseErrorf(cursor->parseState, cursor->nextToken, "expected instruction name");
				throw RecoverParseException();
			}
		}
		catch(RecoverParseException const&)
		{
			cursor->functionState->validatingCodeStream.unreachable();
			throw RecoverParseException();
		}
	});
}

static void parseInstrSequence(CursorState* cursor, Uptr depth)
{
	while(true)
	{
		cursor->functionState->validatingCodeStream.validationErrorToken = cursor->nextToken;
		try
		{
			switch(cursor->nextToken->type)
			{
			case t_leftParenthesis: parseExpr(cursor, depth); break;
			case t_rightParenthesis: return;
			case t_else_: return;
			case t_end: return;
			case t_catch_: return;
			case t_catch_all: return;
			case t_block: {
				checkRecursionDepth(cursor, depth + 1);
				++cursor->nextToken;
				parseBlock(cursor, false, depth + 1);
				break;
			}
			case t_loop: {
				checkRecursionDepth(cursor, depth + 1);
				++cursor->nextToken;
				parseLoop(cursor, false, depth + 1);
				break;
			}
			case t_if_: {
				checkRecursionDepth(cursor, depth + 1);
				++cursor->nextToken;
				parseIfInstr(cursor, depth + 1);
				break;
			}
			case t_try_: {
				checkRecursionDepth(cursor, depth + 1);
				++cursor->nextToken;
				parseTryInstr(cursor, depth + 1);
				break;
			}
#define VISIT_OP(opcode, name, nameString, Imm, ...)                                               \
	case t_##name: parseOp_##name(cursor, false, depth); break;
				WAVM_ENUM_NONCONTROL_OPERATORS(VISIT_OP)
#undef VISIT_OP
			case t_legacyInstructionName:
				parseErrorf(cursor->parseState,
							cursor->nextToken,
							"legacy instruction name: requires the legacy-instr-name feature.");
				throw RecoverParseException();
			default:
				parseErrorf(cursor->parseState, cursor->nextToken, "expected instruction name");
				throw RecoverParseException();
			}
		}
		catch(RecoverParseException const&)
		{
			cursor->functionState->validatingCodeStream.unreachable();

			// This is a workaround for rethrowing from a catch handler blowing up the stack.
			// On Windows, the call stack frames between the throw and the catch are not freed until
			// exiting the catch scope. The throw uses substantial stack space, and the catch adds a
			// stack frame on top of that. As a result, unwinding a call stack by recursively
			// throwing exceptions from within catch scopes can overflow the stack.
			// Jumping out of the catch before rethrowing ensures that the stack frames between the
			// original throw and this function are freed before continuing to unwind the stack.
			goto rethrowRecoverParseException;
		}
	};

	WAVM_UNREACHABLE();
rethrowRecoverParseException:
	throw RecoverParseException();
}

FunctionDef WAST::parseFunctionDef(CursorState* cursor, const Token* funcToken)
{
	std::shared_ptr<std::vector<std::string>> localDisassemblyNames
		= std::make_shared<std::vector<std::string>>();
	std::shared_ptr<NameToIndexMap> localNameToIndexMap = std::make_shared<NameToIndexMap>();

	// Parse the function type, as a reference or explicit declaration.
	const UnresolvedFunctionType unresolvedFunctionType
		= parseFunctionTypeRefAndOrDecl(cursor, *localNameToIndexMap, *localDisassemblyNames);

	// Defer resolving the function type until all type declarations have been parsed.
	const Uptr functionIndex = cursor->moduleState->module.functions.size();
	const Uptr functionDefIndex = cursor->moduleState->module.functions.defs.size();
	const Token* firstBodyToken = cursor->nextToken;
	cursor->moduleState->postTypeCallbacks.push_back([functionIndex,
													  functionDefIndex,
													  firstBodyToken,
													  localNameToIndexMap,
													  localDisassemblyNames,
													  unresolvedFunctionType](
														 ModuleState* moduleState) {
		// Resolve the function type and set it on the FunctionDef.
		const IndexedFunctionType functionTypeIndex
			= resolveFunctionType(moduleState, unresolvedFunctionType);
		moduleState->module.functions.defs[functionDefIndex].type = functionTypeIndex;

		// Defer parsing the body of the function until all function types have been resolved.
		moduleState->functionBodyCallbacks.push_back([functionIndex,
													  functionDefIndex,
													  firstBodyToken,
													  localNameToIndexMap,
													  localDisassemblyNames,
													  functionTypeIndex](ModuleState* moduleState) {
			FunctionDef& functionDef = moduleState->module.functions.defs[functionDefIndex];
			FunctionType functionType = functionTypeIndex.index == UINTPTR_MAX
											? FunctionType()
											: moduleState->module.types[functionTypeIndex.index];

			// Parse the function's local variables.
			CursorState functionCursorState(firstBodyToken, moduleState->parseState, moduleState);
			while(tryParseParenthesizedTagged(&functionCursorState, t_local, [&] {
				Name localName;
				if(tryParseName(&functionCursorState, localName))
				{
					bindName(
						moduleState->parseState,
						*localNameToIndexMap,
						localName,
						functionType.params().size() + functionDef.nonParameterLocalTypes.size());
					localDisassemblyNames->push_back(localName.getString());
					functionDef.nonParameterLocalTypes.push_back(
						parseValueType(&functionCursorState));
				}
				else
				{
					while(functionCursorState.nextToken->type != t_rightParenthesis)
					{
						localDisassemblyNames->push_back(std::string());
						functionDef.nonParameterLocalTypes.push_back(
							parseValueType(&functionCursorState));
					};
				}
			}))
			{};

			moduleState->disassemblyNames.functions[functionIndex].locals
				= std::move(*localDisassemblyNames);

			// Parse the function's code.
			const Token* validationErrorToken = firstBodyToken;
			try
			{
				FunctionState functionState(localNameToIndexMap, functionDef, moduleState);
				functionCursorState.functionState = &functionState;
				try
				{
					parseInstrSequence(&functionCursorState, 0);
					if(!moduleState->parseState->unresolvedErrors.size())
					{
						validationErrorToken = functionCursorState.nextToken;
						functionState.validatingCodeStream.end();
						functionState.validatingCodeStream.finishValidation();
					}
				}
				catch(RecoverParseException const&)
				{
				}
				catch(FatalParseException const&)
				{
				}
				functionDef.code = std::move(functionState.codeByteStream.getBytes());
				moduleState->disassemblyNames.functions[functionIndex].labels
					= std::move(functionState.labelDisassemblyNames);
			}
			catch(ValidationException const& exception)
			{
				parseErrorf(moduleState->parseState,
							validationErrorToken,
							"validation error: %s",
							exception.message.c_str());
			}
		});
	});

	// Continue parsing after the closing parenthesis.
	findClosingParenthesis(cursor, funcToken - 1);
	--cursor->nextToken;

	return {{UINTPTR_MAX}, {}, {}, {}};
}
