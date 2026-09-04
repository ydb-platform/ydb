#include <inttypes.h>
#include <stdint.h>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "Lexer.h"
#include "Parse.h"
#include "WAVM/IR/IR.h"
#include "WAVM/IR/Module.h"
#include "WAVM/IR/Types.h"
#include "WAVM/IR/Validate.h"
#include "WAVM/Inline/Assert.h"
#include "WAVM/Inline/BasicTypes.h"
#include "WAVM/Inline/Errors.h"
#include "WAVM/Inline/Hash.h"
#include "WAVM/Inline/HashMap.h"
#include "WAVM/Inline/Timing.h"
#include "WAVM/Logging/Logging.h"
#include "WAVM/WASTParse/WASTParse.h"

using namespace WAVM;
using namespace WAVM::IR;
using namespace WAVM::WAST;

static bool tryParseSizeConstraints(CursorState* cursor,
									U64 maxMax,
									SizeConstraints& outSizeConstraints)
{
	outSizeConstraints.min = 0;
	outSizeConstraints.max = UINT64_MAX;

	// Parse a minimum.
	if(!tryParseU64(cursor, outSizeConstraints.min)) { return false; }
	else
	{
		// Parse an optional maximum.
		if(!tryParseU64(cursor, outSizeConstraints.max)) { outSizeConstraints.max = UINT64_MAX; }
		else
		{
			// Validate that the maximum size is within the limit, and that the size contraints is
			// not disjoint.
			if(outSizeConstraints.max > maxMax)
			{
				parseErrorf(cursor->parseState,
							cursor->nextToken - 1,
							"validation error: maximum size exceeds limit (%" PRIu64 ">%" PRIu64
							")",
							outSizeConstraints.max,
							maxMax);
				outSizeConstraints.max = maxMax;
			}
			else if(outSizeConstraints.max < outSizeConstraints.min)
			{
				parseErrorf(cursor->parseState,
							cursor->nextToken - 1,
							"validation error: maximum size is less than minimum size (%" PRIu64
							"<%" PRIu64 ")",
							outSizeConstraints.max,
							outSizeConstraints.min);
				outSizeConstraints.max = outSizeConstraints.min;
			}
		}

		return true;
	}
}

static IndexType parseOptionalIndexType(CursorState* cursor)
{
	if(cursor->nextToken->type == t_i32)
	{
		++cursor->nextToken;
		return IndexType::i32;
	}
	else if(cursor->nextToken->type == t_i64)
	{
		++cursor->nextToken;
		return IndexType::i64;
	}
	else
	{
		return IndexType::i32;
	}
}

static SizeConstraints parseSizeConstraints(CursorState* cursor, U64 maxMax)
{
	SizeConstraints result;
	if(!tryParseSizeConstraints(cursor, maxMax, result))
	{ parseErrorf(cursor->parseState, cursor->nextToken, "expected size constraints"); }
	return result;
}

static GlobalType parseGlobalType(CursorState* cursor)
{
	GlobalType result;
	result.isMutable = tryParseParenthesizedTagged(
		cursor, t_mut, [&] { result.valueType = parseValueType(cursor); });
	if(!result.isMutable) { result.valueType = parseValueType(cursor); }
	return result;
}

static TypeTuple parseTypeTuple(CursorState* cursor)
{
	std::vector<ValueType> parameters;
	ValueType elementType;
	while(tryParseValueType(cursor, elementType)) { parameters.push_back(elementType); }
	return TypeTuple(parameters);
}

// An unresolved initializer expression: uses a Reference instead of an index for global.get and
// ref.func.
typedef InitializerExpressionBase<Reference> UnresolvedInitializerExpression;

static UnresolvedInitializerExpression parseInitializerInstruction(CursorState* cursor)
{
	UnresolvedInitializerExpression result;
	switch(cursor->nextToken->type)
	{
	case t_i32_const: {
		++cursor->nextToken;
		result = parseI32(cursor);
		break;
	}
	case t_i64_const: {
		++cursor->nextToken;
		result = parseI64(cursor);
		break;
	}
	case t_f32_const: {
		++cursor->nextToken;
		result = parseF32(cursor);
		break;
	}
	case t_f64_const: {
		++cursor->nextToken;
		result = parseF64(cursor);
		break;
	}
	case t_v128_const: {
		++cursor->nextToken;
		result = parseV128(cursor);
		break;
	}
	case t_global_get: {
		++cursor->nextToken;
		Reference globalRef;
		if(!tryParseNameOrIndexRef(cursor, globalRef))
		{
			parseErrorf(cursor->parseState, cursor->nextToken, "expected global name or index");
			throw RecoverParseException();
		}
		result = UnresolvedInitializerExpression(UnresolvedInitializerExpression::Type::global_get,
												 globalRef);
		break;
	}
	case t_ref_null: {
		++cursor->nextToken;
		result = parseReferencedType(cursor);
		break;
	}
	case t_ref_func: {
		++cursor->nextToken;
		Reference funcRef;
		if(!tryParseNameOrIndexRef(cursor, funcRef))
		{
			parseErrorf(cursor->parseState, cursor->nextToken, "expected function name or index");
			throw RecoverParseException();
		}
		result = UnresolvedInitializerExpression(UnresolvedInitializerExpression::Type::ref_func,
												 funcRef);
		break;
	}
	default:
		parseErrorf(cursor->parseState, cursor->nextToken, "expected initializer expression");
		throw RecoverParseException();
	};

	return result;
}

static UnresolvedInitializerExpression parseInitializerExpression(CursorState* cursor)
{
	UnresolvedInitializerExpression result;

	// Parse either a parenthesized or unparenthesized instruction.
	if(cursor->nextToken->type == t_leftParenthesis)
	{
		parseParenthesized(cursor, [&] { result = parseInitializerInstruction(cursor); });
	}
	else
	{
		result = parseInitializerInstruction(cursor);
	}

	return result;
}

static InitializerExpression resolveInitializerExpression(
	ModuleState* moduleState,
	UnresolvedInitializerExpression unresolvedExpression)
{
	switch(unresolvedExpression.type)
	{
	case UnresolvedInitializerExpression::Type::i32_const:
		return InitializerExpression(unresolvedExpression.i32);
	case UnresolvedInitializerExpression::Type::i64_const:
		return InitializerExpression(unresolvedExpression.i64);
	case UnresolvedInitializerExpression::Type::f32_const:
		return InitializerExpression(unresolvedExpression.f32);
	case UnresolvedInitializerExpression::Type::f64_const:
		return InitializerExpression(unresolvedExpression.f64);
	case UnresolvedInitializerExpression::Type::v128_const:
		return InitializerExpression(unresolvedExpression.v128);
	case UnresolvedInitializerExpression::Type::global_get:
		return InitializerExpression(InitializerExpression::Type::global_get,
									 resolveRef(moduleState->parseState,
												moduleState->globalNameToIndexMap,
												moduleState->module.globals.size(),
												unresolvedExpression.ref));
	case UnresolvedInitializerExpression::Type::ref_null:
		return InitializerExpression(unresolvedExpression.nullReferenceType);
	case UnresolvedInitializerExpression::Type::ref_func:
		return InitializerExpression(InitializerExpression::Type::ref_func,
									 resolveRef(moduleState->parseState,
												moduleState->functionNameToIndexMap,
												moduleState->module.functions.size(),
												unresolvedExpression.ref));
	case UnresolvedInitializerExpression::Type::invalid: return InitializerExpression();
	default: WAVM_UNREACHABLE();
	}
}

static void errorIfFollowsDefinitions(CursorState* cursor)
{
	if(cursor->moduleState->module.functions.defs.size()
	   || cursor->moduleState->module.tables.defs.size()
	   || cursor->moduleState->module.memories.defs.size()
	   || cursor->moduleState->module.globals.defs.size())
	{
		parseErrorf(cursor->parseState,
					cursor->nextToken,
					"import declarations must precede all definitions");
	}
}

template<typename Def, typename Type, typename DisassemblyName>
static Uptr createImport(CursorState* cursor,
						 Name name,
						 std::string&& moduleName,
						 std::string&& exportName,
						 NameToIndexMap& nameToIndexMap,
						 IndexSpace<Def, Type>& indexSpace,
						 std::vector<DisassemblyName>& disassemblyNameArray,
						 Type type,
						 ExternKind kind)
{
	const Uptr importIndex = indexSpace.imports.size();
	bindName(cursor->parseState, nameToIndexMap, name, indexSpace.size());
	disassemblyNameArray.push_back({name.getString()});
	indexSpace.imports.push_back({type, std::move(moduleName), std::move(exportName)});
	cursor->moduleState->module.imports.push_back({kind, importIndex});
	return importIndex;
}

static bool parseOptionalSharedDeclaration(CursorState* cursor)
{
	if(cursor->nextToken->type == t_shared)
	{
		++cursor->nextToken;
		return true;
	}
	else
	{
		return false;
	}
}

static void parseImport(CursorState* cursor)
{
	errorIfFollowsDefinitions(cursor);

	require(cursor, t_import);

	std::string moduleName = parseUTF8String(cursor);
	std::string exportName = parseUTF8String(cursor);

	parseParenthesized(cursor, [&] {
		// Parse the import kind.
		const Token* importKindToken = cursor->nextToken;
		switch(importKindToken->type)
		{
		case t_func:
		case t_table:
		case t_memory:
		case t_global:
		case t_exception_type: ++cursor->nextToken; break;
		default:
			parseErrorf(cursor->parseState, cursor->nextToken, "invalid import type");
			throw RecoverParseException();
		}

		// Parse an optional internal name for the import.
		Name name;
		tryParseName(cursor, name);

		// Parse the import type and create the import in the appropriate name/index spaces.
		switch(importKindToken->type)
		{
		case t_func: {
			NameToIndexMap localNameToIndexMap;
			std::vector<std::string> localDissassemblyNames;
			const UnresolvedFunctionType unresolvedFunctionType = parseFunctionTypeRefAndOrDecl(
				cursor, localNameToIndexMap, localDissassemblyNames);
			const Uptr importIndex = createImport(cursor,
												  name,
												  std::move(moduleName),
												  std::move(exportName),
												  cursor->moduleState->functionNameToIndexMap,
												  cursor->moduleState->module.functions,
												  cursor->moduleState->disassemblyNames.functions,
												  {UINTPTR_MAX},
												  ExternKind::function);
			cursor->moduleState->disassemblyNames.functions.back().locals = localDissassemblyNames;

			// Resolve the function import type after all type declarations have been parsed.
			cursor->moduleState->postTypeCallbacks.push_back(
				[unresolvedFunctionType, importIndex](ModuleState* moduleState) {
					moduleState->module.functions.imports[importIndex].type
						= resolveFunctionType(moduleState, unresolvedFunctionType);
				});
			break;
		}
		case t_table: {
			const IndexType indexType = parseOptionalIndexType(cursor);
			const SizeConstraints sizeConstraints = parseSizeConstraints(
				cursor, indexType == IndexType::i32 ? IR::maxTable32Elems : IR::maxTable64Elems);
			const bool isShared = parseOptionalSharedDeclaration(cursor);
			const ReferenceType elemType = parseReferenceType(cursor);
			createImport(cursor,
						 name,
						 std::move(moduleName),
						 std::move(exportName),
						 cursor->moduleState->tableNameToIndexMap,
						 cursor->moduleState->module.tables,
						 cursor->moduleState->disassemblyNames.tables,
						 TableType{elemType, isShared, indexType, sizeConstraints},
						 ExternKind::table);
			break;
		}
		case t_memory: {
			const IndexType indexType = parseOptionalIndexType(cursor);
			const SizeConstraints sizeConstraints = parseSizeConstraints(
				cursor, indexType == IndexType::i32 ? IR::maxTable32Elems : IR::maxTable64Elems);
			const bool isShared = parseOptionalSharedDeclaration(cursor);
			createImport(cursor,
						 name,
						 std::move(moduleName),
						 std::move(exportName),
						 cursor->moduleState->memoryNameToIndexMap,
						 cursor->moduleState->module.memories,
						 cursor->moduleState->disassemblyNames.memories,
						 MemoryType{isShared, indexType, sizeConstraints},
						 ExternKind::memory);
			break;
		}
		case t_global: {
			const GlobalType globalType = parseGlobalType(cursor);
			createImport(cursor,
						 name,
						 std::move(moduleName),
						 std::move(exportName),
						 cursor->moduleState->globalNameToIndexMap,
						 cursor->moduleState->module.globals,
						 cursor->moduleState->disassemblyNames.globals,
						 globalType,
						 ExternKind::global);
			break;
		}
		case t_exception_type: {
			TypeTuple params = parseTypeTuple(cursor);
			createImport(cursor,
						 name,
						 std::move(moduleName),
						 std::move(exportName),
						 cursor->moduleState->exceptionTypeNameToIndexMap,
						 cursor->moduleState->module.exceptionTypes,
						 cursor->moduleState->disassemblyNames.exceptionTypes,
						 ExceptionType{params},
						 ExternKind::exceptionType);
			break;
		}
		default: WAVM_UNREACHABLE();
		};
	});
}

static bool tryParseExternKind(CursorState* cursor, ExternKind& outKind)
{
	switch(cursor->nextToken->type)
	{
	case t_func:
		++cursor->nextToken;
		outKind = ExternKind::function;
		return true;
	case t_table:
		++cursor->nextToken;
		outKind = ExternKind::table;
		return true;
	case t_memory:
		++cursor->nextToken;
		outKind = ExternKind::memory;
		return true;
	case t_global:
		++cursor->nextToken;
		outKind = ExternKind::global;
		return true;
	case t_exception_type:
		++cursor->nextToken;
		outKind = ExternKind::exceptionType;
		return true;
	default: return false;
	};
}

static void parseExport(CursorState* cursor)
{
	require(cursor, t_export);

	std::string exportName = parseUTF8String(cursor);

	parseParenthesized(cursor, [&] {
		ExternKind exportKind;
		if(!tryParseExternKind(cursor, exportKind))
		{
			parseErrorf(cursor->parseState, cursor->nextToken, "expected export kind");
			throw RecoverParseException();
		}

		Reference exportRef;
		if(!tryParseNameOrIndexRef(cursor, exportRef))
		{
			parseErrorf(cursor->parseState, cursor->nextToken, "expected name or index");
			throw RecoverParseException();
		}

		const Uptr exportIndex = cursor->moduleState->module.exports.size();
		cursor->moduleState->module.exports.push_back({std::move(exportName), exportKind, 0});

		cursor->moduleState->postDeclarationCallbacks.push_back([=](ModuleState* moduleState) {
			moduleState->module.exports[exportIndex].index
				= resolveExternRef(moduleState, exportKind, exportRef);
		});
	});
}

static void parseType(CursorState* cursor)
{
	require(cursor, t_type);

	Name name;
	tryParseName(cursor, name);

	parseParenthesized(cursor, [&] {
		require(cursor, t_func);

		NameToIndexMap parameterNameToIndexMap;
		std::vector<std::string> localDisassemblyNames;
		FunctionType functionType
			= parseFunctionType(cursor, parameterNameToIndexMap, localDisassemblyNames);

		const Uptr functionTypeIndex = cursor->moduleState->module.types.size();
		cursor->moduleState->module.types.push_back(functionType);
		cursor->moduleState->functionTypeToIndexMap.set(functionType, functionTypeIndex);

		bindName(
			cursor->parseState, cursor->moduleState->typeNameToIndexMap, name, functionTypeIndex);
		cursor->moduleState->disassemblyNames.types.push_back(name.getString());
	});
}

static void parseData(CursorState* cursor)
{
	const Token* firstToken = cursor->nextToken;
	require(cursor, t_data);

	Name segmentName;
	Reference memoryRef;
	UnresolvedInitializerExpression baseAddress;
	bool isActive = false;

	tryParseName(cursor, segmentName);

	if(cursor->nextToken[0].type == t_leftParenthesis && cursor->nextToken[1].type == t_memory)
	{
		parseParenthesized(cursor, [&]() {
			require(cursor, t_memory);
			memoryRef = parseNameOrIndexRef(cursor, "data memory");
		});
		isActive = true;
	}

	if(isActive || cursor->nextToken[0].type == t_leftParenthesis)
	{
		if(!isActive)
		{
			memoryRef = Reference(0);
			isActive = true;
		}

		if(cursor->nextToken[0].type != t_leftParenthesis || cursor->nextToken[1].type != t_offset)
		{ baseAddress = parseInitializerExpression(cursor); }
		else
		{
			parseParenthesized(cursor, [&]() {
				require(cursor, t_offset);
				baseAddress = parseInitializerExpression(cursor);
			});
		}
	}

	// Parse a list of strings that contains the segment's data.
	std::string dataString;
	while(tryParseString(cursor, dataString)) {};

	// Create the data segment.
	std::vector<U8> dataVector((const U8*)dataString.data(),
							   (const U8*)dataString.data() + dataString.size());
	const Uptr dataSegmentIndex = cursor->moduleState->module.dataSegments.size();
	cursor->moduleState->module.dataSegments.push_back(
		{isActive,
		 UINTPTR_MAX,
		 InitializerExpression(),
		 std::make_shared<std::vector<U8>>(std::move(dataVector))});

	if(segmentName)
	{
		bindName(cursor->parseState,
				 cursor->moduleState->dataNameToIndexMap,
				 segmentName,
				 dataSegmentIndex);
	}
	cursor->moduleState->disassemblyNames.dataSegments.push_back(segmentName.getString());

	// Enqueue a callback that is called after all declarations are parsed to resolve the memory to
	// put the data segment in, and the base offset.
	if(isActive)
	{
		cursor->moduleState->postDeclarationCallbacks.push_back([=](ModuleState* moduleState) {
			if(!moduleState->module.memories.size())
			{
				parseErrorf(
					moduleState->parseState,
					firstToken,
					"validation error: data segments aren't allowed in modules without any memory declarations");
			}
			else
			{
				DataSegment& dataSegment = moduleState->module.dataSegments[dataSegmentIndex];
				dataSegment.memoryIndex = memoryRef
											  ? resolveRef(moduleState->parseState,
														   moduleState->memoryNameToIndexMap,
														   moduleState->module.memories.size(),
														   memoryRef)
											  : 0;
				dataSegment.baseOffset = resolveInitializerExpression(moduleState, baseAddress);
			}
		});
	}
}

struct UnresolvedElem
{
	union
	{
		Reference ref;
		ReferenceType nullReferenceType;
	};
	ElemExpr::Type type;
	UnresolvedElem(Reference&& inRef = Reference(),
				   ElemExpr::Type inType = ElemExpr::Type::ref_null)
	: ref(std::move(inRef)), type(inType)
	{
	}

	UnresolvedElem(ReferenceType inNullReferenceType)
	: nullReferenceType(inNullReferenceType), type(ElemExpr::Type::ref_null)
	{
	}
};

static UnresolvedElem parseElemSegmentInstr(CursorState* cursor)
{
	switch(cursor->nextToken->type)
	{
	case t_ref_null: {
		++cursor->nextToken;
		const ReferenceType nullReferenceType = parseReferencedType(cursor);
		return UnresolvedElem(nullReferenceType);
		break;
	}
	case t_ref_func: {
		++cursor->nextToken;

		Reference elementRef;
		if(!tryParseNameOrIndexRef(cursor, elementRef))
		{
			parseErrorf(cursor->parseState, cursor->nextToken, "expected function name or index");
			throw RecoverParseException();
		}

		return UnresolvedElem(std::move(elementRef), ElemExpr::Type::ref_func);
		break;
	}
	default:
		parseErrorf(cursor->parseState, cursor->nextToken, "expected 'ref.func' or 'ref.null'");
		throw RecoverParseException();
	};
}

static UnresolvedElem parseElemSegmentExpr(CursorState* cursor)
{
	UnresolvedElem result;
	if(cursor->nextToken->type != t_leftParenthesis) { result = parseElemSegmentInstr(cursor); }
	else
	{
		parseParenthesized(cursor, [&] { result = parseElemSegmentInstr(cursor); });
	}
	return result;
}

static UnresolvedElem parseElemSegmentItemExpr(CursorState* cursor)
{
	UnresolvedElem result;
	parseParenthesized(cursor, [&] {
		if(cursor->nextToken->type == t_item)
		{
			++cursor->nextToken;
			result = parseElemSegmentExpr(cursor);
		}
		else
		{
			result = parseElemSegmentExpr(cursor);
		}
	});
	return result;
}

static Uptr parseElemSegmentBody(CursorState* cursor,
								 ElemSegment::Type segmentType,
								 ElemSegment::Encoding encoding,
								 ExternKind externKind,
								 ReferenceType elemType,
								 Name segmentName,
								 Reference tableRef,
								 UnresolvedInitializerExpression baseIndex,
								 const Token* elemToken)
{
	// Allocate the elementReferences array on the heap so it doesn't need to be copied for the
	// post-declaration callback.
	std::shared_ptr<std::vector<UnresolvedElem>> elementReferences
		= std::make_shared<std::vector<UnresolvedElem>>();

	while(cursor->nextToken->type != t_rightParenthesis)
	{
		switch(encoding)
		{
		case ElemSegment::Encoding::expr: {
			elementReferences->push_back(parseElemSegmentItemExpr(cursor));
			break;
		}
		case ElemSegment::Encoding::index: {
			Reference elementRef;
			if(!tryParseNameOrIndexRef(cursor, elementRef))
			{
				parseErrorf(
					cursor->parseState, cursor->nextToken, "expected function name or index");
				throw RecoverParseException();
			}
			elementReferences->push_back(UnresolvedElem(std::move(elementRef)));
			break;
		}

		default: WAVM_UNREACHABLE();
		}
	}

	// Create the elem segment.
	const Uptr elemSegmentIndex = cursor->moduleState->module.elemSegments.size();
	auto contents = std::make_shared<ElemSegment::Contents>();
	contents->encoding = encoding;
	contents->elemType = elemType;
	contents->externKind = externKind;
	cursor->moduleState->module.elemSegments.push_back(
		{segmentType, UINTPTR_MAX, InitializerExpression(), std::move(contents)});

	if(segmentName)
	{
		bindName(cursor->parseState,
				 cursor->moduleState->elemNameToIndexMap,
				 segmentName,
				 elemSegmentIndex);
	}
	cursor->moduleState->disassemblyNames.elemSegments.push_back(segmentName.getString());

	// Enqueue a callback that is called after all declarations are parsed to resolve the table
	// elements' references.
	cursor->moduleState->postDeclarationCallbacks.push_back([segmentType,
															 tableRef,
															 elemSegmentIndex,
															 elementReferences,
															 elemToken,
															 baseIndex,
															 encoding,
															 externKind](ModuleState* moduleState) {
		ElemSegment& elemSegment = moduleState->module.elemSegments[elemSegmentIndex];

		if(segmentType == ElemSegment::Type::active)
		{
			if(!moduleState->module.tables.size())
			{
				parseErrorf(
					moduleState->parseState,
					elemToken,
					"validation error: "
					"elem segments aren't allowed in modules without any table declarations");
			}
			else
			{
				elemSegment.tableIndex = tableRef ? resolveRef(moduleState->parseState,
															   moduleState->tableNameToIndexMap,
															   moduleState->module.tables.size(),
															   tableRef)
												  : 0;
				elemSegment.baseOffset = resolveInitializerExpression(moduleState, baseIndex);
			}
		}

		switch(encoding)
		{
		case ElemSegment::Encoding::expr:
			elemSegment.contents->elemExprs.resize(elementReferences->size());
			for(Uptr elementIndex = 0; elementIndex < elementReferences->size(); ++elementIndex)
			{
				const UnresolvedElem& unresolvedElem = (*elementReferences)[elementIndex];
				switch(unresolvedElem.type)
				{
				case ElemExpr::Type::ref_null:
					elemSegment.contents->elemExprs[elementIndex]
						= ElemExpr(unresolvedElem.nullReferenceType);
					break;
				case ElemExpr::Type::ref_func:
					elemSegment.contents->elemExprs[elementIndex]
						= ElemExpr(ElemExpr::Type::ref_func,
								   resolveRef(moduleState->parseState,
											  moduleState->functionNameToIndexMap,
											  moduleState->module.functions.size(),
											  unresolvedElem.ref));
					break;

				case ElemExpr::Type::invalid:
				default: WAVM_UNREACHABLE();
				}
			}
			break;
		case ElemSegment::Encoding::index:
			elemSegment.contents->elemIndices.resize(elementReferences->size());
			for(Uptr elementIndex = 0; elementIndex < elementReferences->size(); ++elementIndex)
			{
				const UnresolvedElem& unresolvedElem = (*elementReferences)[elementIndex];
				elemSegment.contents->elemIndices[elementIndex]
					= resolveExternRef(moduleState, externKind, unresolvedElem.ref);
			}
			break;

		default: WAVM_UNREACHABLE();
		};
	});

	return elementReferences->size();
}

static void parseElem(CursorState* cursor)
{
	const Token* elemToken = cursor->nextToken;
	require(cursor, t_elem);

	Name segmentName;
	Reference tableRef;
	UnresolvedInitializerExpression baseIndex;
	ElemSegment::Type segmentType = ElemSegment::Type::passive;

	tryParseName(cursor, segmentName);

	if(cursor->nextToken->type == t_declare)
	{
		++cursor->nextToken;
		segmentType = ElemSegment::Type::declared;
	}
	else if(cursor->nextToken[0].type == t_leftParenthesis && cursor->nextToken[1].type == t_table)
	{
		parseParenthesized(cursor, [&]() {
			require(cursor, t_table);
			tableRef = parseNameOrIndexRef(cursor, "elem table");
		});
		segmentType = ElemSegment::Type::active;
	}

	if(segmentType == ElemSegment::Type::active || cursor->nextToken[0].type == t_leftParenthesis)
	{
		if(segmentType != ElemSegment::Type::active)
		{
			tableRef = Reference(0);
			segmentType = ElemSegment::Type::active;
		}

		if(cursor->nextToken[0].type != t_leftParenthesis || cursor->nextToken[1].type != t_offset)
		{ baseIndex = parseInitializerExpression(cursor); }
		else
		{
			parseParenthesized(cursor, [&]() {
				require(cursor, t_offset);
				baseIndex = parseInitializerExpression(cursor);
			});
		}
	}

	ExternKind elemExternKind = ExternKind::invalid;
	ReferenceType elemRefType = ReferenceType::none;
	ElemSegment::Encoding encoding;
	if(tryParseExternKind(cursor, elemExternKind)) { encoding = ElemSegment::Encoding::index; }
	else if(tryParseReferenceType(cursor, elemRefType))
	{
		encoding = ElemSegment::Encoding::expr;
	}
	else
	{
		encoding = ElemSegment::Encoding::index;
		elemExternKind = ExternKind::function;
	}
	parseElemSegmentBody(cursor,
						 segmentType,
						 encoding,
						 elemExternKind,
						 elemRefType,
						 segmentName,
						 tableRef,
						 baseIndex,
						 elemToken);
}

template<typename Def,
		 typename Type,
		 typename ParseImport,
		 typename ParseDef,
		 typename DisassemblyName>
static void parseObjectDefOrImport(CursorState* cursor,
								   NameToIndexMap& nameToIndexMap,
								   IR::IndexSpace<Def, Type>& indexSpace,
								   std::vector<DisassemblyName>& disassemblyNameArray,
								   TokenType declarationTag,
								   IR::ExternKind kind,
								   ParseImport parseImportFunc,
								   ParseDef parseDefFunc)
{
	const Token* declarationTagToken = cursor->nextToken;
	require(cursor, declarationTag);

	Name name;
	tryParseName(cursor, name);

	// Handle inline export declarations.
	while(true)
	{
		const bool isExport = tryParseParenthesizedTagged(cursor, t_export, [&] {
			cursor->moduleState->module.exports.push_back(
				{parseUTF8String(cursor), kind, indexSpace.size()});
		});
		if(!isExport) { break; }
	};

	// Handle an inline import declaration.
	std::string importModuleName;
	std::string exportName;
	const bool isImport = tryParseParenthesizedTagged(cursor, t_import, [&] {
		errorIfFollowsDefinitions(cursor);

		importModuleName = parseUTF8String(cursor);
		exportName = parseUTF8String(cursor);
	});
	if(isImport)
	{
		Type importType = parseImportFunc(cursor);
		createImport(cursor,
					 name,
					 std::move(importModuleName),
					 std::move(exportName),
					 nameToIndexMap,
					 indexSpace,
					 disassemblyNameArray,
					 importType,
					 kind);
	}
	else
	{
		Def def = parseDefFunc(cursor, declarationTagToken);
		bindName(cursor->parseState, nameToIndexMap, name, indexSpace.size());
		indexSpace.defs.push_back(std::move(def));
		disassemblyNameArray.push_back({name.getString()});
	}
}

static void parseFunc(CursorState* cursor)
{
	parseObjectDefOrImport(
		cursor,
		cursor->moduleState->functionNameToIndexMap,
		cursor->moduleState->module.functions,
		cursor->moduleState->disassemblyNames.functions,
		t_func,
		ExternKind::function,
		[&](CursorState* cursor) {
			// Parse the imported function's type.
			NameToIndexMap localNameToIndexMap;
			std::vector<std::string> localDisassemblyNames;
			const UnresolvedFunctionType unresolvedFunctionType
				= parseFunctionTypeRefAndOrDecl(cursor, localNameToIndexMap, localDisassemblyNames);

			// Resolve the function import type after all type declarations have been parsed.
			const Uptr importIndex = cursor->moduleState->module.functions.imports.size();
			cursor->moduleState->postTypeCallbacks.push_back(
				[unresolvedFunctionType, importIndex](ModuleState* moduleState) {
					moduleState->module.functions.imports[importIndex].type
						= resolveFunctionType(moduleState, unresolvedFunctionType);
				});
			return IndexedFunctionType{UINTPTR_MAX};
		},
		parseFunctionDef);
}

static void parseTable(CursorState* cursor)
{
	parseObjectDefOrImport(
		cursor,
		cursor->moduleState->tableNameToIndexMap,
		cursor->moduleState->module.tables,
		cursor->moduleState->disassemblyNames.tables,
		t_table,
		ExternKind::table,
		// Parse a table import.
		[](CursorState* cursor) {
			const IndexType indexType = parseOptionalIndexType(cursor);
			const SizeConstraints sizeConstraints = parseSizeConstraints(
				cursor, indexType == IndexType::i32 ? IR::maxTable32Elems : IR::maxTable64Elems);
			const bool isShared = parseOptionalSharedDeclaration(cursor);
			const ReferenceType elemType = parseReferenceType(cursor);
			return TableType{elemType, isShared, indexType, sizeConstraints};
		},
		// Parse a table definition.
		[](CursorState* cursor, const Token*) {
			// Parse the table type.
			const IndexType indexType = parseOptionalIndexType(cursor);
			SizeConstraints sizeConstraints;
			const bool hasSizeConstraints = tryParseSizeConstraints(
				cursor,
				indexType == IndexType::i32 ? IR::maxTable32Elems : IR::maxTable64Elems,
				sizeConstraints);
			const bool isShared = parseOptionalSharedDeclaration(cursor);

			const ReferenceType elemType = parseReferenceType(cursor);

			// If we couldn't parse an explicit size constraints, the table definition must contain
			// an elem segment that implicitly defines the size.
			if(!hasSizeConstraints)
			{
				parseParenthesized(cursor, [&] {
					require(cursor, t_elem);

					const Uptr tableIndex = cursor->moduleState->module.tables.size();

					ElemSegment::Encoding encoding = ElemSegment::Encoding::index;
					ExternKind elemExternKind = ExternKind::invalid;
					ReferenceType elemRefType = ReferenceType::none;
					if(cursor->nextToken->type != t_leftParenthesis)
					{ elemExternKind = ExternKind::function; }
					else
					{
						encoding = ElemSegment::Encoding::expr;
						elemRefType = elemType;
					}

					const Uptr numElements = parseElemSegmentBody(
						cursor,
						ElemSegment::Type::active,
						encoding,
						elemExternKind,
						elemRefType,
						Name(),
						Reference(tableIndex),
						indexType == IndexType::i32 ? UnresolvedInitializerExpression(I32(0))
													: UnresolvedInitializerExpression(I64(0)),
						cursor->nextToken - 1);
					sizeConstraints.min = sizeConstraints.max = numElements;
				});
			}

			return TableDef{TableType(elemType, isShared, indexType, sizeConstraints)};
		});
}

static void parseMemory(CursorState* cursor)
{
	parseObjectDefOrImport(
		cursor,
		cursor->moduleState->memoryNameToIndexMap,
		cursor->moduleState->module.memories,
		cursor->moduleState->disassemblyNames.memories,
		t_memory,
		ExternKind::memory,
		// Parse a memory import.
		[](CursorState* cursor) {
			const IndexType indexType = parseOptionalIndexType(cursor);
			const SizeConstraints sizeConstraints = parseSizeConstraints(
				cursor, indexType == IndexType::i32 ? IR::maxMemory32Pages : IR::maxMemory64Pages);
			const bool isShared = parseOptionalSharedDeclaration(cursor);
			return MemoryType{isShared, indexType, sizeConstraints};
		},
		// Parse a memory definition
		[](CursorState* cursor, const Token*) {
			const IndexType indexType = parseOptionalIndexType(cursor);
			SizeConstraints sizeConstraints;
			if(!tryParseSizeConstraints(
				   cursor,
				   indexType == IndexType::i32 ? IR::maxMemory32Pages : IR::maxMemory64Pages,
				   sizeConstraints))
			{
				std::string dataString;

				parseParenthesized(cursor, [&] {
					require(cursor, t_data);

					while(tryParseString(cursor, dataString)) {};
				});

				std::vector<U8> dataVector((const U8*)dataString.data(),
										   (const U8*)dataString.data() + dataString.size());
				sizeConstraints.min = sizeConstraints.max
					= (dataVector.size() + IR::numBytesPerPage - 1) / IR::numBytesPerPage;
				cursor->moduleState->module.dataSegments.push_back(
					{true,
					 cursor->moduleState->module.memories.size(),
					 indexType == IndexType::i32 ? InitializerExpression(I32(0))
												 : InitializerExpression(I64(0)),
					 std::make_shared<std::vector<U8>>(std::move(dataVector))});
				cursor->moduleState->disassemblyNames.dataSegments.emplace_back();
			}

			const bool isShared = parseOptionalSharedDeclaration(cursor);
			return MemoryDef{MemoryType(isShared, indexType, sizeConstraints)};
		});
}

static void parseGlobal(CursorState* cursor)
{
	parseObjectDefOrImport(
		cursor,
		cursor->moduleState->globalNameToIndexMap,
		cursor->moduleState->module.globals,
		cursor->moduleState->disassemblyNames.globals,
		t_global,
		ExternKind::global,
		// Parse a global import.
		parseGlobalType,
		// Parse a global definition
		[](CursorState* cursor, const Token*) {
			const GlobalType globalType = parseGlobalType(cursor);

			// Parse the unresolved initializer expression, but defer resolving it until all
			// declarations have been parsed. This allows the initializer to reference
			// function/global names declared after this global.
			const UnresolvedInitializerExpression unresolvedInitializerExpression
				= parseInitializerExpression(cursor);
			const Uptr globalDefIndex = cursor->moduleState->module.globals.defs.size();
			cursor->moduleState->postDeclarationCallbacks.push_back(
				[cursor, globalDefIndex, unresolvedInitializerExpression](
					ModuleState* moduleState) {
					cursor->moduleState->module.globals.defs[globalDefIndex].initializer
						= resolveInitializerExpression(cursor->moduleState,
													   unresolvedInitializerExpression);
				});

			return GlobalDef{globalType, InitializerExpression()};
		});
}

static void parseExceptionType(CursorState* cursor)
{
	parseObjectDefOrImport(
		cursor,
		cursor->moduleState->exceptionTypeNameToIndexMap,
		cursor->moduleState->module.exceptionTypes,
		cursor->moduleState->disassemblyNames.exceptionTypes,
		t_exception_type,
		ExternKind::exceptionType,
		// Parse an exception type import.
		[](CursorState* cursor) {
			TypeTuple params = parseTypeTuple(cursor);
			return ExceptionType{params};
		},
		// Parse an exception type definition
		[](CursorState* cursor, const Token*) {
			TypeTuple params = parseTypeTuple(cursor);
			return ExceptionTypeDef{ExceptionType{params}};
		});
}

static void parseStart(CursorState* cursor)
{
	if(cursor->moduleState->startFieldToken)
	{
		parseErrorf(cursor->parseState,
					cursor->nextToken,
					"module may not have more than one 'start' field.");
		parseErrorf(cursor->parseState,
					cursor->moduleState->startFieldToken,
					"first 'start' field occurred here");
	}
	cursor->moduleState->startFieldToken = cursor->nextToken;

	require(cursor, t_start);

	Reference functionRef;
	if(!tryParseNameOrIndexRef(cursor, functionRef))
	{ parseErrorf(cursor->parseState, cursor->nextToken, "expected function name or index"); }

	cursor->moduleState->postDeclarationCallbacks.push_back([functionRef](
																ModuleState* moduleState) {
		moduleState->module.startFunctionIndex = resolveRef(moduleState->parseState,
															moduleState->functionNameToIndexMap,
															moduleState->module.functions.size(),
															functionRef);
	});
}

static OrderedSectionID parseOrderedSectionID(CursorState* cursor)
{
	OrderedSectionID result;
	switch(cursor->nextToken->type)
	{
	case t_type: result = OrderedSectionID::type; break;
	case t_import: result = OrderedSectionID::import; break;
	case t_func: result = OrderedSectionID::function; break;
	case t_table: result = OrderedSectionID::table; break;
	case t_memory: result = OrderedSectionID::memory; break;
	case t_global: result = OrderedSectionID::global; break;
	case t_exception_type:
		if(!cursor->moduleState->module.featureSpec.exceptionHandling)
		{
			parseErrorf(
				cursor->parseState,
				cursor->nextToken,
				"custom section after 'exception_type' section requires the 'exception-handling' feature.");
		}
		result = OrderedSectionID::exceptionType;
		break;
	case t_export: result = OrderedSectionID::export_; break;
	case t_start: result = OrderedSectionID::start; break;
	case t_elem: result = OrderedSectionID::elem; break;
	case t_data_count:
		if(!cursor->moduleState->module.featureSpec.bulkMemoryOperations)
		{
			parseErrorf(
				cursor->parseState,
				cursor->nextToken,
				"custom section after exception_type section requires the 'bulk-memory-operations' feature.");
		}
		result = OrderedSectionID::dataCount;
		break;
	case t_code: result = OrderedSectionID::code; break;
	case t_data: result = OrderedSectionID::data; break;

	default:
		parseErrorf(cursor->parseState, cursor->nextToken, "expected section ID");
		throw RecoverParseException();
	};
	++cursor->nextToken;
	return result;
}

static void parseCustomSection(CursorState* cursor)
{
	if(!cursor->moduleState->module.featureSpec.customSectionsInTextFormat)
	{
		parseErrorf(
			cursor->parseState,
			cursor->nextToken,
			"custom sections in the text format require the 'wat-custom-sections' feature.");
	}

	const Token* customSectionToken = cursor->nextToken;
	require(cursor, t_custom_section);

	// Parse the section name.
	IR::CustomSection customSection;
	customSection.name = parseUTF8String(cursor);

	// Parse the section order.
	OrderedSectionID afterSection = OrderedSectionID::moduleBeginning;
	if(cursor->nextToken[0].type == t_leftParenthesis && cursor->nextToken[1].type == t_after)
	{
		parseParenthesized(cursor, [&] {
			WAVM_ASSERT(cursor->nextToken->type == t_after);
			++cursor->nextToken;
			afterSection = parseOrderedSectionID(cursor);
		});
	}
	customSection.afterSection = afterSection;

	// Ensure that custom sections do not occur out-of-order w.r.t. afterSection.
	if(cursor->moduleState->module.customSections.size())
	{
		const CustomSection& lastCustomSection = cursor->moduleState->module.customSections.back();
		if(lastCustomSection.afterSection > afterSection)
		{
			WAVM_ASSERT(cursor->moduleState->lastCustomSectionToken);
			parseErrorf(cursor->parseState,
						customSectionToken,
						"out-of-order custom section: this section is declared to be after '%s'...",
						asString(afterSection));
			parseErrorf(cursor->parseState,
						cursor->moduleState->lastCustomSectionToken,
						"...but it occurs after a custom section that is declared to be after '%s'",
						asString(lastCustomSection.afterSection));
		}
	}
	cursor->moduleState->lastCustomSectionToken = customSectionToken;

	// Parse a list of strings that contains the custom section's data.
	std::string dataString;
	while(tryParseString(cursor, dataString)) {};

	customSection.data = std::vector<U8>((const U8*)dataString.data(),
										 (const U8*)dataString.data() + dataString.size());

	// Add the custom section to the module.
	cursor->moduleState->module.customSections.push_back(std::move(customSection));

	// After all declarations have been parsed, validate that the custom section ordering constraint
	// doesn't reference a virtual section that may not be present in the binary encoding of the
	// module. This ensures that the text format cannot express order constraints that may not be
	// encoded in a binary module.
	cursor->moduleState->postDeclarationCallbacks.push_back(
		[customSectionToken, afterSection](ModuleState* moduleState) {
			const IR::Module& module = moduleState->module;
			bool hasPrecedingSection = true;
			switch(afterSection)
			{
			case OrderedSectionID::moduleBeginning: break;
			case OrderedSectionID::type: hasPrecedingSection = hasTypeSection(module); break;
			case OrderedSectionID::import: hasPrecedingSection = hasImportSection(module); break;
			case OrderedSectionID::function:
				hasPrecedingSection = hasFunctionSection(module);
				break;
			case OrderedSectionID::table: hasPrecedingSection = hasTableSection(module); break;
			case OrderedSectionID::memory: hasPrecedingSection = hasMemorySection(module); break;
			case OrderedSectionID::global: hasPrecedingSection = hasGlobalSection(module); break;
			case OrderedSectionID::exceptionType:
				hasPrecedingSection = hasExceptionTypeSection(module);
				break;
			case OrderedSectionID::export_: hasPrecedingSection = hasExportSection(module); break;
			case OrderedSectionID::start: hasPrecedingSection = hasStartSection(module); break;
			case OrderedSectionID::elem: hasPrecedingSection = hasElemSection(module); break;
			case OrderedSectionID::dataCount:
				hasPrecedingSection = hasDataCountSection(module);
				break;
			case OrderedSectionID::code: hasPrecedingSection = hasCodeSection(module); break;
			case OrderedSectionID::data: hasPrecedingSection = hasDataSection(module); break;
			default: WAVM_UNREACHABLE();
			};

			if(!hasPrecedingSection)
			{
				parseErrorf(moduleState->parseState,
							customSectionToken,
							"custom section is after a virtual section that is not present (%s)",
							asString(afterSection));
			}
		});
}

static void parseDeclaration(CursorState* cursor)
{
	parseParenthesized(cursor, [&] {
		switch(cursor->nextToken->type)
		{
		case t_import: parseImport(cursor); return true;
		case t_export: parseExport(cursor); return true;
		case t_exception_type: parseExceptionType(cursor); return true;
		case t_global: parseGlobal(cursor); return true;
		case t_memory: parseMemory(cursor); return true;
		case t_table: parseTable(cursor); return true;
		case t_type: parseType(cursor); return true;
		case t_data: parseData(cursor); return true;
		case t_elem: parseElem(cursor); return true;
		case t_func: parseFunc(cursor); return true;
		case t_start: parseStart(cursor); return true;
		case t_custom_section: parseCustomSection(cursor); return true;
		default:
			parseErrorf(cursor->parseState, cursor->nextToken, "unrecognized definition in module");
			throw RecoverParseException();
		};
	});
}

template<typename Map> void dumpHashMapSpaceAnalysis(const Map& map, const char* description)
{
	if(map.size())
	{
		Uptr totalMemoryBytes = 0;
		Uptr maxProbeCount = 0;
		F32 occupancy = 0.0f;
		F32 averageProbeCount = 0.0f;
		map.analyzeSpaceUsage(totalMemoryBytes, maxProbeCount, occupancy, averageProbeCount);
		Log::printf(
			Log::metrics,
			"%s used %.1fKiB for %" WAVM_PRIuPTR
			" elements (%.0f%% occupancy, %.1f bytes/element). Avg/max probe length: %f/%" WAVM_PRIuPTR
			"\n",
			description,
			totalMemoryBytes / 1024.0,
			map.size(),
			F64(occupancy) * 100.0,
			F64(totalMemoryBytes) / map.size(),
			F64(averageProbeCount),
			maxProbeCount);
	}
}

void WAST::parseModuleBody(CursorState* cursor, IR::Module& outModule)
{
	try
	{
		const Token* firstToken = cursor->nextToken;
		ModuleState moduleState(cursor->parseState, outModule);
		cursor->moduleState = &moduleState;

		// Parse the module's declarations.
		while(cursor->nextToken->type != t_rightParenthesis && cursor->nextToken->type != t_eof)
		{ parseDeclaration(cursor); };

		// Process the callbacks requested after all type declarations have been parsed.
		if(!cursor->parseState->unresolvedErrors.size())
		{
			for(const auto& callback : cursor->moduleState->postTypeCallbacks)
			{ callback(&moduleState); }
		}

		// Process the callbacks requested after all declarations have been parsed.
		if(!cursor->parseState->unresolvedErrors.size())
		{
			for(const auto& callback : cursor->moduleState->postDeclarationCallbacks)
			{ callback(&moduleState); }
		}

		// After all declarations have been parsed, but before function bodies are parsed, validate
		// the parts of the module that correspond to pre-code sections in binary modules.
		if(!cursor->parseState->unresolvedErrors.size())
		{
			try
			{
				IR::validatePreCodeSections(*moduleState.validationState);
			}
			catch(ValidationException const& validationException)
			{
				parseErrorf(cursor->parseState,
							firstToken,
							"validation error: %s",
							validationException.message.c_str());
			}
		}

		// Process the function body parsing callbacks.
		if(!cursor->parseState->unresolvedErrors.size())
		{
			for(const auto& callback : cursor->moduleState->functionBodyCallbacks)
			{ callback(&moduleState); }
		}

		// After function bodies have been parsed, validate the parts of the module that correspond
		// to post-code sections in binary modules.
		if(!cursor->parseState->unresolvedErrors.size())
		{
			try
			{
				IR::validatePostCodeSections(*moduleState.validationState);
			}
			catch(ValidationException const& validationException)
			{
				parseErrorf(cursor->parseState,
							firstToken,
							"validation error: %s",
							validationException.message.c_str());
			}
		}

		// Set the module's disassembly names.
		const DisassemblyNames& disassemblyNames = moduleState.disassemblyNames;
		WAVM_ASSERT(outModule.functions.size() == disassemblyNames.functions.size());
		WAVM_ASSERT(outModule.tables.size() == disassemblyNames.tables.size());
		WAVM_ASSERT(outModule.memories.size() == disassemblyNames.memories.size());
		WAVM_ASSERT(outModule.globals.size() == disassemblyNames.globals.size());
		WAVM_ASSERT(outModule.elemSegments.size() == disassemblyNames.elemSegments.size());
		WAVM_ASSERT(outModule.dataSegments.size() == disassemblyNames.dataSegments.size());
		WAVM_ASSERT(outModule.exceptionTypes.size() == disassemblyNames.exceptionTypes.size());
		IR::setDisassemblyNames(outModule, disassemblyNames);
	}
	catch(RecoverParseException const&)
	{
		cursor->moduleState = nullptr;
		throw RecoverParseException();
	}
	cursor->moduleState = nullptr;
}

bool WAST::parseModule(const char* string,
					   Uptr stringLength,
					   IR::Module& outModule,
					   std::vector<Error>& outErrors)
{
	Timing::Timer timer;

	// Lex the string.
	LineInfo* lineInfo = nullptr;
	Token* tokens
		= lex(string, stringLength, lineInfo, outModule.featureSpec.allowLegacyInstructionNames);
	ParseState parseState(string, lineInfo);
	CursorState cursor(tokens, &parseState);

	try
	{
		if(cursor.nextToken[0].type == t_leftParenthesis && cursor.nextToken[1].type == t_module)
		{
			// Parse (module <module body>)
			parseParenthesized(&cursor, [&] {
				require(&cursor, t_module);
				parseModuleBody(&cursor, outModule);
			});
		}
		else
		{
			// Also allow a module body without any enclosing (module ...).
			parseModuleBody(&cursor, outModule);
		}
		require(&cursor, t_eof);
	}
	catch(RecoverParseException const&)
	{
	}
	catch(FatalParseException const&)
	{
	}

	// Resolve line information for any errors, and write them to outErrors.
	for(auto& unresolvedError : parseState.unresolvedErrors)
	{
		TextFileLocus locus = calcLocusFromOffset(string, lineInfo, unresolvedError.charOffset);
		outErrors.push_back({std::move(locus), std::move(unresolvedError.message)});
	}

	// Free the tokens and line info.
	freeTokens(tokens);
	freeLineInfo(lineInfo);

	Timing::logRatePerSecond("lexed and parsed WAST", timer, stringLength / 1024.0 / 1024.0, "MiB");

	return outErrors.size() == 0;
}
