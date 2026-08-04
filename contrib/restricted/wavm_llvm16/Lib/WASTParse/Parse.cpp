#include "Parse.h"
#include <assert.h>
#include <stdint.h>
#include <climits>
#include <cstdarg>
#include <cstdio>
#include <memory>
#include <string>
#include <utility>
#include "Lexer.h"
#include "WAVM/IR/Module.h"
#include "WAVM/Inline/Assert.h"
#include "WAVM/Inline/BasicTypes.h"
#include "WAVM/Inline/Errors.h"
#include "WAVM/Inline/Hash.h"
#include "WAVM/Inline/HashMap.h"
#include "WAVM/Inline/IsNameChar.h"
#include "WAVM/Inline/Unicode.h"
#include "WAVM/Logging/Logging.h"
#include "WAVM/WASTParse/WASTParse.h"

using namespace WAVM;
using namespace WAVM::IR;
using namespace WAVM::WAST;

void WAST::findClosingParenthesis(CursorState* cursor, const Token* openingParenthesisToken)
{
	// Skip over tokens until the ')' closing the current parentheses nesting depth is found.
	Uptr depth = 1;
	while(depth > 0)
	{
		switch(cursor->nextToken->type)
		{
		default: ++cursor->nextToken; break;
		case t_leftParenthesis:
			++cursor->nextToken;
			++depth;
			break;
		case t_rightParenthesis:
			++cursor->nextToken;
			--depth;
			break;
		case t_eof:
			parseErrorf(cursor->parseState,
						openingParenthesisToken,
						"reached end of input while trying to find closing parenthesis");
			throw FatalParseException();
		}
	}
}

static void parseErrorfImpl(ParseState* parseState,
							Uptr charOffset,
							const char* messageFormat,
							va_list messageArgs)
{
	// Call vsnprintf to determine how many bytes the formatted string will be.
	// vsnprintf consumes the va_list passed to it, so make a copy of it.
	va_list messageArgsProbe;
	va_copy(messageArgsProbe, messageArgs);
	int numFormattedChars = std::vsnprintf(nullptr, 0, messageFormat, messageArgsProbe);
	va_end(messageArgsProbe);

	// Allocate a buffer for the formatted message.
	WAVM_ERROR_UNLESS(numFormattedChars >= 0);
	std::string formattedMessage;
	formattedMessage.resize(numFormattedChars);

	// Print the formatted message
	int numWrittenChars = std::vsnprintf(
		(char*)formattedMessage.data(), numFormattedChars + 1, messageFormat, messageArgs);
	WAVM_ASSERT(numWrittenChars == numFormattedChars);

	// Add the error to the cursor's error list.
	parseState->unresolvedErrors.emplace_back(charOffset, std::move(formattedMessage));
}
void WAST::parseErrorf(ParseState* parseState, Uptr charOffset, const char* messageFormat, ...)
{
	va_list messageArguments;
	va_start(messageArguments, messageFormat);
	parseErrorfImpl(parseState, charOffset, messageFormat, messageArguments);
	va_end(messageArguments);
}
void WAST::parseErrorf(ParseState* parseState, const char* nextChar, const char* messageFormat, ...)
{
	va_list messageArguments;
	va_start(messageArguments, messageFormat);
	parseErrorfImpl(parseState, nextChar - parseState->string, messageFormat, messageArguments);
	va_end(messageArguments);
}
void WAST::parseErrorf(ParseState* parseState,
					   const Token* nextToken,
					   const char* messageFormat,
					   ...)
{
	va_list messageArguments;
	va_start(messageArguments, messageFormat);
	parseErrorfImpl(parseState, nextToken->begin, messageFormat, messageArguments);
	va_end(messageArguments);
}

void WAST::require(CursorState* cursor, TokenType type)
{
	if(cursor->nextToken->type != type)
	{
		parseErrorf(cursor->parseState, cursor->nextToken, "expected %s", describeToken(type));
		throw RecoverParseException();
	}
	++cursor->nextToken;
}

bool WAST::tryParseValueType(CursorState* cursor, ValueType& outValueType)
{
	switch(cursor->nextToken->type)
	{
	case t_i32: outValueType = ValueType::i32; break;
	case t_i64: outValueType = ValueType::i64; break;
	case t_f32: outValueType = ValueType::f32; break;
	case t_f64: outValueType = ValueType::f64; break;
	case t_v128: outValueType = ValueType::v128; break;
	case t_externref: outValueType = ValueType::externref; break;
	case t_funcref: outValueType = ValueType::funcref; break;
	default: outValueType = ValueType::none; return false;
	};

	++cursor->nextToken;
	return true;
}

ValueType WAST::parseValueType(CursorState* cursor)
{
	ValueType result;
	if(!tryParseValueType(cursor, result))
	{
		parseErrorf(cursor->parseState, cursor->nextToken, "expected value type");
		throw RecoverParseException();
	}
	return result;
}

bool WAST::tryParseReferenceType(CursorState* cursor, IR::ReferenceType& outRefType)
{
	switch(cursor->nextToken->type)
	{
	case t_externref:
		++cursor->nextToken;
		outRefType = ReferenceType::externref;
		return true;
	case t_funcref:
		++cursor->nextToken;
		outRefType = ReferenceType::funcref;
		return true;
	default: return false;
	};
}

ReferenceType WAST::parseReferenceType(CursorState* cursor)
{
	ReferenceType result;
	if(!tryParseReferenceType(cursor, result))
	{
		parseErrorf(cursor->parseState, cursor->nextToken, "expected reference type");
		throw RecoverParseException();
	}
	return result;
}

static bool tryParseReferencedType(CursorState* cursor, IR::ReferenceType& outRefType)
{
	switch(cursor->nextToken->type)
	{
	case t_extern:
		++cursor->nextToken;
		outRefType = ReferenceType::externref;
		return true;
	case t_func:
		++cursor->nextToken;
		outRefType = ReferenceType::funcref;
		return true;
	default: return false;
	};
}

ReferenceType WAST::parseReferencedType(CursorState* cursor)
{
	ReferenceType result;
	if(!tryParseReferencedType(cursor, result))
	{
		parseErrorf(cursor->parseState, cursor->nextToken, "expected 'func' or 'extern'");
		throw RecoverParseException();
	}
	return result;
}

FunctionType WAST::parseFunctionType(CursorState* cursor,
									 NameToIndexMap& outLocalNameToIndexMap,
									 std::vector<std::string>& outLocalDisassemblyNames)
{
	std::vector<ValueType> parameters;
	std::vector<ValueType> results;
	CallingConvention callingConvention = CallingConvention::wasm;

	// Parse the function parameters.
	while(tryParseParenthesizedTagged(cursor, t_param, [&] {
		Name parameterName;
		if(tryParseName(cursor, parameterName))
		{
			// (param <name> <type>)
			bindName(cursor->parseState, outLocalNameToIndexMap, parameterName, parameters.size());
			parameters.push_back(parseValueType(cursor));
			outLocalDisassemblyNames.push_back(parameterName.getString());
		}
		else
		{
			// (param <type>*)
			ValueType parameterType;
			while(tryParseValueType(cursor, parameterType))
			{
				parameters.push_back(parameterType);
				outLocalDisassemblyNames.emplace_back();
			};
		}
	}))
	{};

	// Parse the result types: (result <value type>*)*
	while(cursor->nextToken[0].type == t_leftParenthesis && cursor->nextToken[1].type == t_result)
	{
		parseParenthesized(cursor, [&] {
			require(cursor, t_result);

			ValueType result;
			while(tryParseValueType(cursor, result)) { results.push_back(result); };
		});
	};

	// Parse an optional calling convention.
	if(cursor->nextToken[0].type == t_leftParenthesis
	   && cursor->nextToken[1].type == t_calling_conv)
	{
		parseParenthesized(cursor, [&] {
			require(cursor, t_calling_conv);

			switch(cursor->nextToken->type)
			{
			case t_intrinsic: callingConvention = CallingConvention::intrinsic; break;
			case t_intrinsic_with_context_switch:
				callingConvention = CallingConvention::intrinsicWithContextSwitch;
				break;
			case t_c: callingConvention = CallingConvention::c; break;
			case t_c_api_callback: callingConvention = CallingConvention::cAPICallback; break;

			default:
				parseErrorf(cursor->parseState, cursor->nextToken, "expected calling convention");
				throw RecoverParseException();
			};
			++cursor->nextToken;
		});
	}

	return FunctionType(TypeTuple(results), TypeTuple(parameters), callingConvention);
}

UnresolvedFunctionType WAST::parseFunctionTypeRefAndOrDecl(
	CursorState* cursor,
	NameToIndexMap& outLocalNameToIndexMap,
	std::vector<std::string>& outLocalDisassemblyNames)
{
	// Parse an optional function type reference.
	Reference functionTypeRef;
	if(cursor->nextToken[0].type == t_leftParenthesis && cursor->nextToken[1].type == t_type)
	{
		parseParenthesized(cursor, [&] {
			require(cursor, t_type);
			if(!tryParseNameOrIndexRef(cursor, functionTypeRef))
			{
				parseErrorf(cursor->parseState, cursor->nextToken, "expected type name or index");
				throw RecoverParseException();
			}
		});
	}

	// Parse the explicit function parameters and result type.
	FunctionType explicitFunctionType
		= parseFunctionType(cursor, outLocalNameToIndexMap, outLocalDisassemblyNames);

	UnresolvedFunctionType result;
	result.reference = functionTypeRef;
	result.explicitType = explicitFunctionType;
	return result;
}

IndexedFunctionType WAST::resolveFunctionType(ModuleState* moduleState,
											  const UnresolvedFunctionType& unresolvedType)
{
	if(!unresolvedType.reference)
	{ return getUniqueFunctionTypeIndex(moduleState, unresolvedType.explicitType); }
	else
	{
		// Resolve the referenced type.
		const Uptr referencedFunctionTypeIndex = resolveRef(moduleState->parseState,
															moduleState->typeNameToIndexMap,
															moduleState->module.types.size(),
															unresolvedType.reference);

		// Validate that if the function definition has both a type reference and explicit
		// parameter/result type declarations, they match.
		const bool hasExplicitParametersOrResultType
			= unresolvedType.explicitType != FunctionType();
		if(hasExplicitParametersOrResultType)
		{
			if(referencedFunctionTypeIndex != UINTPTR_MAX
			   && moduleState->module.types[referencedFunctionTypeIndex]
					  != unresolvedType.explicitType)
			{
				parseErrorf(
					moduleState->parseState,
					unresolvedType.reference.token,
					"referenced function type (%s) does not match declared parameters and "
					"results (%s)",
					asString(moduleState->module.types[referencedFunctionTypeIndex]).c_str(),
					asString(unresolvedType.explicitType).c_str());
			}
		}

		return {referencedFunctionTypeIndex};
	}
}

IndexedFunctionType WAST::getUniqueFunctionTypeIndex(ModuleState* moduleState,
													 FunctionType functionType)
{
	// If this type is not in the module's type table yet, add it.
	Uptr& functionTypeIndex
		= moduleState->functionTypeToIndexMap.getOrAdd(functionType, UINTPTR_MAX);
	if(functionTypeIndex == UINTPTR_MAX)
	{
		functionTypeIndex = moduleState->module.types.size();
		moduleState->module.types.push_back(functionType);
		moduleState->disassemblyNames.types.emplace_back();
	}
	return IndexedFunctionType{functionTypeIndex};
}

static void parseStringChars(const char*& nextChar, ParseState* parseState, std::string& outString);

bool WAST::tryParseName(CursorState* cursor, Name& outName)
{
	if(cursor->nextToken->type != t_quotedName && cursor->nextToken->type != t_name)
	{ return false; }

	const char* firstChar = cursor->parseState->string + cursor->nextToken->begin;
	const char* nextChar = firstChar;
	WAVM_ASSERT(*nextChar == '$');
	++nextChar;

	if(cursor->nextToken->type == t_quotedName)
	{
		if(!cursor->moduleState->module.featureSpec.quotedNamesInTextFormat)
		{ parseErrorf(cursor->parseState, cursor->nextToken, "quoted names are disabled"); }

		WAVM_ASSERT(*nextChar == '\"');
		++nextChar;
		{
			std::string quotedNameChars;
			parseStringChars(nextChar, cursor->parseState, quotedNameChars);
			if(quotedNameChars.size() == 0)
			{
				parseErrorf(
					cursor->parseState, cursor->nextToken, "quoted names must not be empty");
				outName = Name();
			}
			else
			{
				cursor->parseState->quotedNameStrings.push_back(
					std::unique_ptr<std::string>{new std::string(std::move(quotedNameChars))});
				const std::unique_ptr<std::string>& quotedName
					= cursor->parseState->quotedNameStrings.back();
				WAVM_ASSERT(quotedName->size() <= UINT32_MAX);
				outName
					= Name(quotedName->data(), U32(quotedName->size()), cursor->nextToken->begin);
			}
		}
	}
	else
	{
		// Find the first non-name character.
		while(true)
		{
			const char c = *nextChar;
			if(isNameChar(c)) { ++nextChar; }
			else
			{
				break;
			}
		};

		outName = Name(firstChar + 1, U32(nextChar - firstChar - 1), cursor->nextToken->begin);
	}

	WAVM_ASSERT(U32(nextChar - cursor->parseState->string) > cursor->nextToken->begin + 1);
	++cursor->nextToken;
	WAVM_ASSERT(U32(nextChar - cursor->parseState->string) <= cursor->nextToken->begin);
	WAVM_ASSERT(U32(nextChar - firstChar) <= UINT32_MAX);
	return true;
}

bool WAST::tryParseNameOrIndexRef(CursorState* cursor, Reference& outRef)
{
	outRef.token = cursor->nextToken;
	if(tryParseName(cursor, outRef.name))
	{
		outRef.type = Reference::Type::name;
		return true;
	}
	else if(tryParseUptr(cursor, outRef.index))
	{
		outRef.type = Reference::Type::index;
		return true;
	}
	return false;
}

bool WAST::tryParseAndResolveNameOrIndexRef(CursorState* cursor,
											const NameToIndexMap& nameToIndexMap,
											Uptr maxIndex,
											const char* context,
											Uptr& outIndex)
{
	Reference ref;
	if(!tryParseNameOrIndexRef(cursor, ref)) { return false; }
	outIndex = resolveRef(cursor->parseState, nameToIndexMap, maxIndex, ref);
	return true;
}

Name WAST::parseName(CursorState* cursor, const char* context)
{
	Name result;
	if(!tryParseName(cursor, result))
	{
		parseErrorf(cursor->parseState, cursor->nextToken, "expected %s name", context);
		throw RecoverParseException();
	}
	return result;
}

Reference WAST::parseNameOrIndexRef(CursorState* cursor, const char* context)
{
	Reference result;
	if(!tryParseNameOrIndexRef(cursor, result))
	{
		parseErrorf(cursor->parseState, cursor->nextToken, "expected %s name or index", context);
		throw RecoverParseException();
	}
	return result;
}

Uptr WAST::parseAndResolveNameOrIndexRef(CursorState* cursor,
										 const NameToIndexMap& nameToIndexMap,
										 Uptr maxIndex,
										 const char* context)
{
	Uptr resolvedIndex;
	if(!tryParseAndResolveNameOrIndexRef(cursor, nameToIndexMap, maxIndex, context, resolvedIndex))
	{
		parseErrorf(cursor->parseState, cursor->nextToken, "expected %s name or index", context);
		throw RecoverParseException();
	}
	return resolvedIndex;
}

void WAST::bindName(ParseState* parseState,
					NameToIndexMap& nameToIndexMap,
					const Name& name,
					Uptr index)
{
	if(name)
	{
		if(!nameToIndexMap.add(name, index))
		{
			const HashMapPair<Name, Uptr>* nameIndexPair = nameToIndexMap.getPair(name);
			WAVM_ASSERT(nameIndexPair);
			const TextFileLocus previousDefinitionLocus = calcLocusFromOffset(
				parseState->string, parseState->lineInfo, nameIndexPair->key.getSourceOffset());
			parseErrorf(parseState,
						name.getSourceOffset(),
						"redefinition of name defined at %s",
						previousDefinitionLocus.describe().c_str());
			nameToIndexMap.set(name, index);
		}
	}
}

Uptr WAST::resolveRef(ParseState* parseState,
					  const NameToIndexMap& nameToIndexMap,
					  Uptr maxIndex,
					  const Reference& ref)
{
	switch(ref.type)
	{
	case Reference::Type::index: {
		if(ref.index >= maxIndex)
		{
			parseErrorf(parseState, ref.token, "validation error: invalid index");
			return UINTPTR_MAX;
		}
		return ref.index;
	}
	case Reference::Type::name: {
		const HashMapPair<Name, Uptr>* nameIndexPair = nameToIndexMap.getPair(ref.name);
		if(!nameIndexPair)
		{
			parseErrorf(parseState, ref.token, "unknown name");
			return UINTPTR_MAX;
		}
		else
		{
			return nameIndexPair->value;
		}
	}

	case Reference::Type::invalid:
	default: WAVM_UNREACHABLE();
	};
}

Uptr WAST::resolveExternRef(ModuleState* moduleState, ExternKind externKind, const Reference& ref)
{
	switch(externKind)
	{
	case ExternKind::function:
		return resolveRef(moduleState->parseState,
						  moduleState->functionNameToIndexMap,
						  moduleState->module.functions.size(),
						  ref);
	case ExternKind::table:
		return resolveRef(moduleState->parseState,
						  moduleState->tableNameToIndexMap,
						  moduleState->module.tables.size(),
						  ref);
	case ExternKind::memory:
		return resolveRef(moduleState->parseState,
						  moduleState->memoryNameToIndexMap,
						  moduleState->module.memories.size(),
						  ref);
	case ExternKind::global:
		return resolveRef(moduleState->parseState,
						  moduleState->globalNameToIndexMap,
						  moduleState->module.globals.size(),
						  ref);
	case ExternKind::exceptionType:
		return resolveRef(moduleState->parseState,
						  moduleState->exceptionTypeNameToIndexMap,
						  moduleState->module.exceptionTypes.size(),
						  ref);

	case ExternKind::invalid:
	default: WAVM_UNREACHABLE();
	}
}

bool WAST::tryParseHexit(const char*& nextChar, U8& outValue)
{
	if(*nextChar >= '0' && *nextChar <= '9') { outValue = *nextChar - '0'; }
	else if(*nextChar >= 'a' && *nextChar <= 'f')
	{
		outValue = *nextChar - 'a' + 10;
	}
	else if(*nextChar >= 'A' && *nextChar <= 'F')
	{
		outValue = *nextChar - 'A' + 10;
	}
	else
	{
		outValue = 0;
		return false;
	}
	++nextChar;
	return true;
}

static void parseCharEscapeCode(const char*& nextChar,
								ParseState* parseState,
								std::string& outString)
{
	U8 firstNibble;
	if(tryParseHexit(nextChar, firstNibble))
	{
		// Parse an 8-bit literal from two hexits.
		U8 secondNibble;
		if(!tryParseHexit(nextChar, secondNibble))
		{ parseErrorf(parseState, nextChar, "expected hexit"); }
		outString += char(firstNibble * 16 + secondNibble);
	}
	else
	{
		switch(*nextChar)
		{
		case 't':
			outString += '\t';
			++nextChar;
			break;
		case 'n':
			outString += '\n';
			++nextChar;
			break;
		case 'r':
			outString += '\r';
			++nextChar;
			break;
		case '\"':
			outString += '\"';
			++nextChar;
			break;
		case '\'':
			outString += '\'';
			++nextChar;
			break;
		case '\\':
			outString += '\\';
			++nextChar;
			break;
		case 'u': {
			// \u{...} - Unicode codepoint from hexadecimal number
			if(nextChar[1] != '{') { parseErrorf(parseState, nextChar, "expected '{'"); }
			nextChar += 2;

			// Parse the hexadecimal number.
			const char* firstHexit = nextChar;
			U32 codepoint = 0;
			U8 hexit = 0;
			while(tryParseHexit(nextChar, hexit))
			{
				if(codepoint > (UINT32_MAX - hexit) / 16)
				{
					codepoint = UINT32_MAX;
					while(tryParseHexit(nextChar, hexit)) {};
					break;
				}
				WAVM_ASSERT(codepoint * 16 + hexit >= codepoint);
				codepoint = codepoint * 16 + hexit;
			}

			// Check that it denotes a valid Unicode codepoint.
			if((codepoint >= 0xD800 && codepoint <= 0xDFFF) || codepoint >= 0x110000)
			{
				parseErrorf(parseState, firstHexit, "invalid Unicode codepoint");
				codepoint = 0x1F642;
			}

			// Encode the codepoint as UTF-8.
			Unicode::encodeUTF8CodePoint(codepoint, outString);

			if(*nextChar != '}') { parseErrorf(parseState, nextChar, "expected '}'"); }
			++nextChar;
			break;
		}
		default:
			outString += '\\';
			++nextChar;
			parseErrorf(parseState, nextChar, "invalid escape code");
			break;
		}
	}
}

static void parseStringChars(const char*& nextChar, ParseState* parseState, std::string& outString)
{
	while(true)
	{
		switch(*nextChar)
		{
		case '\\': {
			++nextChar;
			parseCharEscapeCode(nextChar, parseState, outString);
			break;
		}
		case '\"': return;
		default: outString += *nextChar++; break;
		};
	};
}

bool WAST::tryParseString(CursorState* cursor, std::string& outString)
{
	if(cursor->nextToken->type != t_string) { return false; }

	// Parse a string literal; the lexer has already rejected unterminated strings, so this just
	// needs to copy the characters and evaluate escape codes.
	const char* nextChar = cursor->parseState->string + cursor->nextToken->begin;
	WAVM_ASSERT(*nextChar == '\"');
	++nextChar;
	parseStringChars(nextChar, cursor->parseState, outString);
	++cursor->nextToken;
	WAVM_ASSERT(cursor->parseState->string + cursor->nextToken->begin > nextChar);
	return true;
}

std::string WAST::parseUTF8String(CursorState* cursor)
{
	const Token* stringToken = cursor->nextToken;
	std::string result;
	if(!tryParseString(cursor, result))
	{
		parseErrorf(cursor->parseState, stringToken, "expected string literal");
		throw RecoverParseException();
	}

	// Check that the string is a valid UTF-8 encoding.
	const U8* endChar = (const U8*)result.data() + result.size();
	const U8* nextChar = Unicode::validateUTF8String((const U8*)result.data(), endChar);
	if(nextChar != endChar)
	{
		const Uptr charOffset = stringToken->begin + (nextChar - (const U8*)result.data()) + 1;
		parseErrorf(cursor->parseState, charOffset, "invalid UTF-8 encoding");
	}

	return result;
}

void WAST::reportParseErrors(const char* filename,
							 const char* source,
							 const std::vector<WAST::Error>& parseErrors,
							 Log::Category outputCategory)
{
	// Print any parse errors.
	for(auto& error : parseErrors)
	{
		Log::printf(outputCategory,
					"%s:%s: %s\n%.*s\n%*s\n",
					filename,
					error.locus.describe().c_str(),
					error.message.c_str(),
					int(error.locus.lineEndOffset - error.locus.lineStartOffset),
					source + error.locus.lineStartOffset,
					error.locus.column(8),
					"^");
	}
}
