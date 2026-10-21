#pragma once

#include <string.h>
#include <functional>
#include <memory>
#include <string>
#include <vector>
#include "Lexer.h"
#include "WAVM/IR/Module.h"
#include "WAVM/IR/Types.h"
#include "WAVM/IR/Validate.h"
#include "WAVM/Inline/BasicTypes.h"
#include "WAVM/Inline/Hash.h"
#include "WAVM/Inline/HashMap.h"
#include "WAVM/WASTParse/WASTParse.h"

namespace WAVM { namespace WAST {
	struct FatalParseException
	{
	};
	struct RecoverParseException
	{
	};

	// Like WAST::Error, but only has an offset in the input string instead of a full
	// TextFileLocus.
	struct UnresolvedError
	{
		Uptr charOffset;
		std::string message;
		UnresolvedError(Uptr inCharOffset, std::string&& inMessage)
		: charOffset(inCharOffset), message(inMessage)
		{
		}
	};

	struct ParseState
	{
		const char* string;
		const LineInfo* lineInfo;
		std::vector<UnresolvedError> unresolvedErrors;

		std::vector<std::unique_ptr<std::string>> quotedNameStrings;

		ParseState(const char* inString, const LineInfo* inLineInfo)
		: string(inString), lineInfo(inLineInfo)
		{
		}
	};

	// Encapsulates a name ($whatever) parsed from the WAST file.
	// References the characters in the input string the name was parsed from,
	// so it can handle arbitrary length names in a fixed size struct, but only
	// as long as the input string isn't freed.
	// Includes a hash of the name's characters.
	struct Name
	{
		constexpr Name() : begin(nullptr), numChars(0), sourceOffset(0) {}
		Name(const char* inBegin, U32 inNumChars, U32 inSourceOffset)
		: begin(inBegin), numChars(inNumChars), sourceOffset(inSourceOffset)
		{
			WAVM_ASSERT(inNumChars > 0);
		}

		constexpr operator bool() const { return begin != nullptr; }
		std::string getString() const
		{
			return begin ? std::string(begin, numChars) : std::string();
		}
		constexpr Uptr getSourceOffset() const { return sourceOffset; }
		Uptr getHash() const { return XXH<Uptr>(begin, numChars, 0); }

		void reset()
		{
			begin = nullptr;
			numChars = 0;
		}

		friend constexpr bool operator==(const Name& a, const Name& b)
		{
			return a.numChars == b.numChars
				   && (a.numChars == 0 || memcmp(a.begin, b.begin, a.numChars) == 0);
		}
		friend constexpr bool operator!=(const Name& a, const Name& b) { return !(a == b); }

		struct HashPolicy
		{
			static bool areKeysEqual(const Name& left, const Name& right) { return left == right; }
			static Uptr getKeyHash(const Name& name) { return name.getHash(); }
		};

	private:
		const char* begin;
		U32 numChars;
		U32 sourceOffset;
	};

	// A map from Name to index using a hash table.
	typedef HashMap<Name, Uptr, Name::HashPolicy> NameToIndexMap;

	// Represents a yet-to-be-resolved reference, parsed as either a name or an index.
	struct Reference
	{
		enum class Type
		{
			invalid,
			name,
			index
		};
		Type type;
		union
		{
			Name name;
			Uptr index;
		};
		const Token* token;
		Reference(const Name& inName) : type(Type::name), name(inName) {}
		Reference(Uptr inIndex) : type(Type::index), index(inIndex) {}
		Reference() : type(Type::invalid), token(nullptr) {}
		operator bool() const { return type != Type::invalid; }
	};

	// Represents a function type, either as an unresolved name/index, or as an explicit type, or
	// both.
	struct UnresolvedFunctionType
	{
		Reference reference;
		IR::FunctionType explicitType;
	};

	// State associated with parsing a module.
	struct ModuleState
	{
		ParseState* parseState;

		IR::Module& module;
		std::shared_ptr<IR::ModuleValidationState> validationState;

		HashMap<IR::FunctionType, Uptr> functionTypeToIndexMap;
		NameToIndexMap typeNameToIndexMap;

		NameToIndexMap functionNameToIndexMap;
		NameToIndexMap tableNameToIndexMap;
		NameToIndexMap memoryNameToIndexMap;
		NameToIndexMap globalNameToIndexMap;
		NameToIndexMap exceptionTypeNameToIndexMap;
		NameToIndexMap elemNameToIndexMap;
		NameToIndexMap dataNameToIndexMap;

		IR::DisassemblyNames disassemblyNames;

		const Token* startFieldToken{nullptr};
		const Token* lastCustomSectionToken{nullptr};

		// Thunks that are called after parsing all types.
		std::vector<std::function<void(ModuleState*)>> postTypeCallbacks;

		// Thunks that are called after parsing all declarations.
		std::vector<std::function<void(ModuleState*)>> postDeclarationCallbacks;

		// Thunks that are called to parse function bodies.
		std::vector<std::function<void(ModuleState*)>> functionBodyCallbacks;

		ModuleState(ParseState* inParseState, IR::Module& inModule)
		: parseState(inParseState)
		, module(inModule)
		, validationState(IR::createModuleValidationState(inModule))
		{
		}
	};

	// The state that's threaded through the various parsers.
	struct CursorState
	{
		const Token* nextToken;

		ParseState* parseState;
		ModuleState* moduleState;
		struct FunctionState* functionState;

		CursorState(const Token* inNextToken,
					ParseState* inParseState,
					ModuleState* inModuleState = nullptr,
					struct FunctionState* inFunctionState = nullptr)
		: nextToken(inNextToken)
		, parseState(inParseState)
		, moduleState(inModuleState)
		, functionState(inFunctionState)
		{
		}
	};

	// Error handling.
	void parseErrorf(ParseState* parseState, Uptr charOffset, const char* messageFormat, ...)
		WAVM_VALIDATE_AS_PRINTF(3, 4);
	void parseErrorf(ParseState* parseState, const char* nextChar, const char* messageFormat, ...)
		WAVM_VALIDATE_AS_PRINTF(3, 4);
	void parseErrorf(ParseState* parseState, const Token* nextToken, const char* messageFormat, ...)
		WAVM_VALIDATE_AS_PRINTF(3, 4);

	void require(CursorState* cursor, TokenType type);

	// Type parsing and uniqueing
	bool tryParseValueType(CursorState* cursor, IR::ValueType& outValueType);
	IR::ValueType parseValueType(CursorState* cursor);
	bool tryParseReferenceType(CursorState* cursor, IR::ReferenceType& outRefType);
	IR::ReferenceType parseReferenceType(CursorState* cursor);
	IR::ReferenceType parseReferencedType(CursorState* cursor);

	IR::FunctionType parseFunctionType(CursorState* cursor,
									   NameToIndexMap& outLocalNameToIndexMap,
									   std::vector<std::string>& outLocalDisassemblyNames);
	UnresolvedFunctionType parseFunctionTypeRefAndOrDecl(
		CursorState* cursor,
		NameToIndexMap& outLocalNameToIndexMap,
		std::vector<std::string>& outLocalDisassemblyNames);
	IR::IndexedFunctionType resolveFunctionType(ModuleState* moduleState,
												const UnresolvedFunctionType& unresolvedType);
	IR::IndexedFunctionType getUniqueFunctionTypeIndex(ModuleState* moduleState,
													   IR::FunctionType functionType);

	// Literal parsing.
	bool tryParseHexit(const char*& nextChar, U8& outValue);

	bool tryParseU64(CursorState* cursor, U64& outI64);
	bool tryParseUptr(CursorState* cursor, Uptr& outUptr);

	U8 parseU8(CursorState* cursor, bool allowSign = true);
	U32 parseU32(CursorState* cursor);
	U64 parseU64(CursorState* cursor);

	// Uninterpreted integers: may be anywhere in the range INT_MIN to UINT_MAX.
	I8 parseI8(CursorState* cursor);
	I16 parseI16(CursorState* cursor);
	I32 parseI32(CursorState* cursor);
	I64 parseI64(CursorState* cursor);

	F32 parseF32(CursorState* cursor);
	F64 parseF64(CursorState* cursor);
	template<typename Float> Float parseFloat(CursorState* cursor);
	template<> inline F32 parseFloat<F32>(CursorState* cursor) { return parseF32(cursor); }
	template<> inline F64 parseFloat<F64>(CursorState* cursor) { return parseF64(cursor); }

	V128 parseV128(CursorState* cursor);

	bool tryParseString(CursorState* cursor, std::string& outString);

	std::string parseUTF8String(CursorState* cursor);

	// Name parsing and resolution.
	bool tryParseName(CursorState* cursor, Name& outName);
	bool tryParseNameOrIndexRef(CursorState* cursor, Reference& outRef);
	bool tryParseAndResolveNameOrIndexRef(CursorState* cursor,
										  const NameToIndexMap& nameToIndexMap,
										  Uptr maxIndex,
										  const char* context,
										  Uptr& outIndex);
	Name parseName(CursorState* cursor, const char* context);
	Reference parseNameOrIndexRef(CursorState* cursor, const char* context);
	Uptr parseAndResolveNameOrIndexRef(CursorState* cursor,
									   const NameToIndexMap& nameToIndexMap,
									   Uptr maxIndex,
									   const char* context);

	void bindName(ParseState* parseState,
				  NameToIndexMap& nameToIndexMap,
				  const Name& name,
				  Uptr index);
	Uptr resolveRef(ParseState* parseState,
					const NameToIndexMap& nameToIndexMap,
					Uptr maxIndex,
					const Reference& ref);
	Uptr resolveExternRef(ModuleState* moduleState,
						  IR::ExternKind externKind,
						  const Reference& ref);

	// Finds the parenthesis closing the current s-expression.
	void findClosingParenthesis(CursorState* cursor, const Token* openingParenthesisToken);

	// Parses the surrounding parentheses for an inner parser, and handles recovery at the closing
	// parenthesis.
	template<typename ParseInner>
	static void parseParenthesized(CursorState* cursor, ParseInner parseInner)
	{
		const Token* openingParenthesisToken = cursor->nextToken;
		require(cursor, t_leftParenthesis);
		try
		{
			parseInner();
			require(cursor, t_rightParenthesis);
		}
		catch(RecoverParseException const&)
		{
			findClosingParenthesis(cursor, openingParenthesisToken);
		}
	}

	// Tries to parse '(' tagType parseInner ')', handling recovery at the closing parenthesis.
	// Returns true if any tokens were consumed.
	template<typename ParseInner>
	static bool tryParseParenthesizedTagged(CursorState* cursor,
											TokenType tagType,
											ParseInner parseInner)
	{
		const Token* openingParenthesisToken = cursor->nextToken;
		if(cursor->nextToken[0].type != t_leftParenthesis || cursor->nextToken[1].type != tagType)
		{ return false; }
		try
		{
			cursor->nextToken += 2;
			parseInner();
			require(cursor, t_rightParenthesis);
		}
		catch(RecoverParseException const&)
		{
			findClosingParenthesis(cursor, openingParenthesisToken);
		}
		return true;
	}

	// Function parsing.
	IR::FunctionDef parseFunctionDef(CursorState* cursor, const Token* funcToken);

	// Module parsing.
	void parseModuleBody(CursorState* cursor, IR::Module& outModule);
}}
