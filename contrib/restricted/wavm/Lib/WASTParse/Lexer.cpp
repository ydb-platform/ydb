#include "Lexer.h"
#include <inttypes.h>
#include <stdint.h>
#include <stdlib.h>
#include <string>
#include <tuple>
#include <utility>
#include "WAVM/Inline/Assert.h"
#include "WAVM/Inline/BasicTypes.h"
#include "WAVM/Inline/CLI.h"
#include "WAVM/Inline/Errors.h"
#include "WAVM/Inline/Timing.h"
#include "WAVM/Logging/Logging.h"
#include "WAVM/NFA/NFA.h"
#include "WAVM/RegExp/RegExp.h"
#include "WAVM/WASTParse/WASTParse.h"

#define DUMP_NFA_GRAPH 0
#define DUMP_DFA_GRAPH 0

using namespace WAVM;
using namespace WAVM::WAST;

namespace WAVM { namespace WAST {
	struct LineInfo
	{
		U32* lineStarts;
		U32 numLineStarts;

		mutable Uptr lastQueryCharOffset{0};
		mutable U32 lastQueryTabs{0};
		mutable U32 lastQueryCharactersAndTabs{0};

		LineInfo(U32* inLineStarts, U32 inNumLineStarts)
		: lineStarts(inLineStarts), numLineStarts(inNumLineStarts)
		{
		}
	};
}}

const char* WAST::describeToken(TokenType tokenType)
{
	static const char* tokenDescriptions[] = {
// This ENUM_TOKENS must come before the literalTokenPairs definition that redefines
// VISIT_OPERATOR_TOKEN.
#define VISIT_TOKEN(name, description, _) description,
		ENUM_TOKENS()
#undef VISIT_TOKEN
	};
	WAVM_ASSERT(tokenType * sizeof(const char*) < sizeof(tokenDescriptions));
	return tokenDescriptions[tokenType];
}

struct StaticData
{
	NFA::Machine nfaMachine;
	StaticData(bool allowLegacyInstructionNames);

	static StaticData& get(bool allowLegacyInstructionNames);
};

static NFA::StateIndex createTokenSeparatorPeekState(NFA::Builder* builder,
													 NFA::StateIndex finalState)
{
	NFA::CharSet tokenSeparatorCharSet;
	tokenSeparatorCharSet.add(U8(' '));
	tokenSeparatorCharSet.add(U8('\t'));
	tokenSeparatorCharSet.add(U8('\r'));
	tokenSeparatorCharSet.add(U8('\n'));
	tokenSeparatorCharSet.add(U8('='));
	tokenSeparatorCharSet.add(U8('('));
	tokenSeparatorCharSet.add(U8(')'));
	tokenSeparatorCharSet.add(U8(';'));
	tokenSeparatorCharSet.add(0);
	auto separatorState = addState(builder);
	NFA::addEdge(builder,
				 separatorState,
				 tokenSeparatorCharSet,
				 finalState | NFA::edgeDoesntConsumeInputFlag);
	return separatorState;
}

static void addLiteralStringToNFA(const char* string,
								  NFA::Builder* builder,
								  NFA::StateIndex initialState,
								  NFA::StateIndex finalState)
{
	// Add the literal to the NFA, one character at a time, reusing existing states that are
	// reachable by the same string.
	for(const char* nextChar = string; *nextChar; ++nextChar)
	{
		NFA::StateIndex nextState = NFA::getNonTerminalEdge(builder, initialState, *nextChar);
		if(nextState < 0 || nextChar[1] == 0)
		{
			nextState = nextChar[1] == 0 ? finalState : addState(builder);
			NFA::addEdge(builder, initialState, NFA::CharSet(*nextChar), nextState);
		}
		initialState = nextState;
	}
}

static void addLiteralTokenToNFA(const char* literalString,
								 NFA::Builder* builder,
								 TokenType tokenType,
								 bool isTokenSeparator)
{
	NFA::StateIndex finalState = NFA::maximumTerminalStateIndex - (NFA::StateIndex)tokenType;
	if(!isTokenSeparator) { finalState = createTokenSeparatorPeekState(builder, finalState); }

	addLiteralStringToNFA(literalString, builder, 0, finalState);
}

StaticData::StaticData(bool allowLegacyInstructionNames)
{
	// clang-format off
static const std::pair<TokenType, const char*> regexpTokenPairs[] = {
	{t_decimalInt, "[+\\-]?\\d+(_\\d+)*"},
	{t_decimalFloat, "[+\\-]?\\d+(_\\d+)*\\.(\\d+(_\\d+)*)*([eE][+\\-]?\\d+(_\\d+)*)?"},
	{t_decimalFloat, "[+\\-]?\\d+(_\\d+)*[eE][+\\-]?\\d+(_\\d+)*"},

	{t_hexInt, "[+\\-]?0[xX][\\da-fA-F]+(_[\\da-fA-F]+)*"},
	{t_hexFloat, "[+\\-]?0[xX][\\da-fA-F]+(_[\\da-fA-F]+)*\\.([\\da-fA-F]+(_[\\da-fA-F]+)*)*([pP][+\\-]?\\d+(_\\d+)*)?"},
	{t_hexFloat, "[+\\-]?0[xX][\\da-fA-F]+(_[\\da-fA-F]+)*[pP][+\\-]?\\d+(_\\d+)*"},

	{t_floatNaN, "[+\\-]?nan(:0[xX][\\da-fA-F]+(_[\\da-fA-F]+)*)?"},
	{t_floatInf, "[+\\-]?inf"},

	{t_string, "\"([^\"\n\\\\]*(\\\\([^0-9a-fA-Fu]|[0-9a-fA-F][0-9a-fA-F]|u\\{[0-9a-fA-F]+})))*\""},

	{t_name, "\\$[a-zA-Z0-9\'_+*/~=<>!?@#$%&|:`.\\-\\^\\\\]+"},
	{t_quotedName, "\\$\"([^\"\n\\\\]*(\\\\([^0-9a-fA-Fu]|[0-9a-fA-F][0-9a-fA-F]|u\\{[0-9a-fA-F]+})))*\""},
};

static const std::tuple<TokenType, const char*, bool> literalTokenTuples[] = {
	std::make_tuple(t_leftParenthesis, "(", true),
	std::make_tuple(t_rightParenthesis, ")", true),
	std::make_tuple(t_equals, "=", true),
	std::make_tuple(t_canonicalNaN, "nan:canonical", false),
	std::make_tuple(t_arithmeticNaN, "nan:arithmetic", false),

	#define VISIT_TOKEN(name, _, literalString) std::make_tuple(t_##name, literalString, false),
	ENUM_LITERAL_TOKENS()
	#undef VISIT_TOKEN

	#undef VISIT_OPERATOR_TOKEN
	#define VISIT_OPERATOR_TOKEN(_, name, nameString, ...) std::make_tuple(t_##name, nameString, false),
	WAVM_ENUM_OPERATORS(VISIT_OPERATOR_TOKEN)
	#undef VISIT_OPERATOR_TOKEN
};

// Legacy aliases for tokens.
static const std::tuple<TokenType, const char*> legacyOperatorAliasTuples[] = {
	std::make_tuple(t_funcref            , "anyfunc"            ),

	std::make_tuple(t_local_get          , "get_local"          ),
	std::make_tuple(t_local_set          , "set_local"          ),
	std::make_tuple(t_local_tee          , "tee_local"          ),
	std::make_tuple(t_global_get         , "get_global"         ),
	std::make_tuple(t_global_set         , "set_global"         ),

	std::make_tuple(t_i32_wrap_i64       , "i32.wrap/i64"       ),
	std::make_tuple(t_i32_trunc_f32_s    , "i32.trunc_s/f32"    ),
	std::make_tuple(t_i32_trunc_f32_u    , "i32.trunc_u/f32"    ),
	std::make_tuple(t_i32_trunc_f64_s    , "i32.trunc_s/f64"    ),
	std::make_tuple(t_i32_trunc_f64_u    , "i32.trunc_u/f64"    ),
	std::make_tuple(t_i64_extend_i32_s   , "i64.extend_s/i32"   ),
	std::make_tuple(t_i64_extend_i32_u   , "i64.extend_u/i32"   ),
	std::make_tuple(t_i64_trunc_f32_s    , "i64.trunc_s/f32"    ),
	std::make_tuple(t_i64_trunc_f32_u    , "i64.trunc_u/f32"    ),
	std::make_tuple(t_i64_trunc_f64_s    , "i64.trunc_s/f64"    ),
	std::make_tuple(t_i64_trunc_f64_u    , "i64.trunc_u/f64"    ),
	std::make_tuple(t_f32_convert_i32_s  , "f32.convert_s/i32"  ),
	std::make_tuple(t_f32_convert_i32_u  , "f32.convert_u/i32"  ),
	std::make_tuple(t_f32_convert_i64_s  , "f32.convert_s/i64"  ),
	std::make_tuple(t_f32_convert_i64_u  , "f32.convert_u/i64"  ),
	std::make_tuple(t_f32_demote_f64     , "f32.demote/f64"     ),
	std::make_tuple(t_f64_convert_i32_s  , "f64.convert_s/i32"  ),
	std::make_tuple(t_f64_convert_i32_u  , "f64.convert_u/i32"  ),
	std::make_tuple(t_f64_convert_i64_s  , "f64.convert_s/i64"  ),
	std::make_tuple(t_f64_convert_i64_u  , "f64.convert_u/i64"  ),
	std::make_tuple(t_f64_promote_f32    , "f64.promote/f32"    ),
	std::make_tuple(t_i32_reinterpret_f32, "i32.reinterpret/f32"),
	std::make_tuple(t_i64_reinterpret_f64, "i64.reinterpret/f64"),
	std::make_tuple(t_f32_reinterpret_i32, "f32.reinterpret/i32"),
	std::make_tuple(t_f64_reinterpret_i64, "f64.reinterpret/i64")
};
	// clang-format on

	Timing::Timer timer;

	NFA::Builder* nfaBuilder = NFA::createBuilder();

	for(auto regexpTokenPair : regexpTokenPairs)
	{
		NFA::StateIndex finalState
			= NFA::maximumTerminalStateIndex - (NFA::StateIndex)regexpTokenPair.first;
		finalState = createTokenSeparatorPeekState(nfaBuilder, finalState);
		RegExp::addToNFA(regexpTokenPair.second, nfaBuilder, 0, finalState);
	}

	for(auto literalTokenTuple : literalTokenTuples)
	{
		const TokenType tokenType = std::get<0>(literalTokenTuple);
		const char* literalString = std::get<1>(literalTokenTuple);
		const bool isTokenSeparator = std::get<2>(literalTokenTuple);
		addLiteralTokenToNFA(literalString, nfaBuilder, tokenType, isTokenSeparator);
	}

	for(auto legacyOperatorAliasTuple : legacyOperatorAliasTuples)
	{
		const TokenType tokenType = allowLegacyInstructionNames
										? std::get<0>(legacyOperatorAliasTuple)
										: TokenType(t_legacyInstructionName);
		const char* literalString = std::get<1>(legacyOperatorAliasTuple);
		addLiteralTokenToNFA(literalString, nfaBuilder, tokenType, false);
	}

	if(DUMP_NFA_GRAPH)
	{
		std::string nfaGraphVizString = NFA::dumpNFAGraphViz(nfaBuilder);
		WAVM_ERROR_UNLESS(
			saveFile("nfaGraph.dot", nfaGraphVizString.data(), nfaGraphVizString.size()));
	}

	nfaMachine = NFA::Machine(nfaBuilder);

	if(DUMP_DFA_GRAPH)
	{
		std::string dfaGraphVizString = nfaMachine.dumpDFAGraphViz();
		WAVM_ERROR_UNLESS(
			saveFile("dfaGraph.dot", dfaGraphVizString.data(), dfaGraphVizString.size()));
	}

	Timing::logTimer("built lexer tables", timer);
}

StaticData& StaticData::get(bool allowLegacyInstructionNames)
{
	if(allowLegacyInstructionNames)
	{
		static StaticData staticData(true);
		return staticData;
	}
	else
	{
		static StaticData staticData(false);
		return staticData;
	}
}

inline bool isRecoveryPointChar(char c)
{
	switch(c)
	{
	// Recover lexing at the next whitespace or parenthesis.
	case ' ':
	case '\t':
	case '\r':
	case '\n':
	case '\f':
	case '(':
	case ')': return true;
	default: return false;
	};
}

Token* WAST::lex(const char* string,
				 Uptr stringLength,
				 LineInfo*& outLineInfo,
				 bool allowLegacyInstructionNames)
{
	WAVM_ERROR_UNLESS(string);
	WAVM_ERROR_UNLESS(string[stringLength - 1] == 0);

	StaticData& staticData = StaticData::get(allowLegacyInstructionNames);

	Timing::Timer timer;

	if(stringLength > UINT32_MAX)
	{ Errors::fatalf("cannot lex strings with more than %u characters", UINT32_MAX); }

	// Allocate enough memory up front for a token and newline for each character in the input
	// string.
	Token* tokens = (Token*)malloc(sizeof(Token) * (stringLength + 1));
	U32* lineStarts = (U32*)malloc(sizeof(U32) * (stringLength + 2));

	Token* nextToken = tokens;
	U32* nextLineStart = lineStarts;
	*nextLineStart++ = 0;

	const char* nextChar = string;
	while(true)
	{
		// Skip whitespace and comments (keeping track of newlines).
		while(true)
		{
			switch(*nextChar)
			{
			// Single line comments.
			case ';':
				if(nextChar[1] != ';') { goto doneSkippingWhitespace; }
				else
				{
					nextChar += 2;
					while(*nextChar)
					{
						if(*nextChar == '\n')
						{
							// Emit a line start for the newline.
							*nextLineStart++ = U32(nextChar - string + 1);
							++nextChar;
							break;
						}
						++nextChar;
					};
				}
				break;
			// Delimited (possibly multi-line) comments.
			case '(':
				if(nextChar[1] != ';') { goto doneSkippingWhitespace; }
				else
				{
					const char* firstCommentChar = nextChar;
					nextChar += 2;
					U32 commentDepth = 1;
					while(commentDepth)
					{
						if(nextChar[0] == ';' && nextChar[1] == ')')
						{
							--commentDepth;
							nextChar += 2;
						}
						else if(nextChar[0] == '(' && nextChar[1] == ';')
						{
							++commentDepth;
							nextChar += 2;
						}
						else if(nextChar == string + stringLength - 1)
						{
							// Emit an unterminated comment token.
							nextToken->type = t_unterminatedComment;
							nextToken->begin = U32(firstCommentChar - string);
							++nextToken;
							goto doneSkippingWhitespace;
						}
						else
						{
							if(*nextChar == '\n')
							{
								// Emit a line start for the newline.
								*nextLineStart++ = U32(nextChar - string);
							}
							++nextChar;
						}
					};
				}
				break;
			// Whitespace.
			case '\n':
				*nextLineStart++ = U32(nextChar - string + 1);
				++nextChar;
				break;
			case ' ':
			case '\t':
			case '\r':
			case '\f': ++nextChar; break;
			default: goto doneSkippingWhitespace;
			};
		}
	doneSkippingWhitespace:

		// Once we reach a non-whitespace, non-comment character, feed characters into the NFA
		// until it reaches a terminal state.
		nextToken->begin = U32(nextChar - string);
		NFA::StateIndex terminalState = staticData.nfaMachine.feed(nextChar);
		if(terminalState != NFA::unmatchedCharacterTerminal)
		{
			nextToken->type
				= TokenType(NFA::maximumTerminalStateIndex - (NFA::StateIndex)terminalState);
			++nextToken;
		}
		else
		{
			if(nextToken->begin < stringLength - 1)
			{
				// Emit an unrecognized token.
				nextToken->type = t_unrecognized;
				++nextToken;

				// Advance until a recovery point or the end of the string.
				const char* stringEnd = string + stringLength - 1;
				while(nextChar < stringEnd && !isRecoveryPointChar(*nextChar)) { ++nextChar; }
			}
			else
			{
				break;
			}
		}
	}

	// Emit an end token to mark the end of the token stream.
	nextToken->type = t_eof;
	++nextToken;

	// Emit an extra line start for the end of the file, so you can find the end of a line with
	// lineStarts[line + 1].
	*nextLineStart++ = U32(nextChar - string) + 1;

	// Shrink the line start and token arrays to the final number of tokens/lines.
	const Uptr numLineStarts = nextLineStart - lineStarts;
	const Uptr numTokens = nextToken - tokens;
	lineStarts = (U32*)realloc(lineStarts, sizeof(U32) * numLineStarts);
	tokens = (Token*)realloc(tokens, sizeof(Token) * numTokens);

	// Create the LineInfo object that encapsulates the line start information.
	outLineInfo = new LineInfo{lineStarts, U32(numLineStarts)};

	Timing::logRatePerSecond("lexed WAST file", timer, stringLength / 1024.0 / 1024.0, "MiB");
	Log::printf(Log::metrics,
				"lexer produced %" WAVM_PRIuPTR " tokens (%.1fMiB)\n",
				numTokens,
				numTokens * sizeof(Token) / 1024.0 / 1024.0);

	return tokens;
}

void WAST::freeTokens(Token* tokens) { free(tokens); }

void WAST::freeLineInfo(LineInfo* lineInfo)
{
	free(lineInfo->lineStarts);
	delete lineInfo;
}

static Uptr getLineOffset(const LineInfo* lineInfo, Uptr lineIndex)
{
	WAVM_ERROR_UNLESS(lineIndex < lineInfo->numLineStarts);
	return lineInfo->lineStarts[lineIndex];
}

TextFileLocus WAST::calcLocusFromOffset(const char* string,
										const LineInfo* lineInfo,
										Uptr charOffset)
{
	// The last line start is at the end of the string, so use it to sanity check that the
	// charOffset isn't past the end of the string.
	const Uptr numChars = lineInfo->lineStarts[lineInfo->numLineStarts - 1];
	WAVM_ASSERT(charOffset <= numChars);

	// Binary search the line starts for the last one before charIndex.
	Uptr minLineIndex = 0;
	Uptr maxLineIndex = lineInfo->numLineStarts - 1;
	while(maxLineIndex > minLineIndex)
	{
		const Uptr medianLineIndex = (minLineIndex + maxLineIndex + 1) / 2;
		if(charOffset < lineInfo->lineStarts[medianLineIndex])
		{ maxLineIndex = medianLineIndex - 1; }
		else if(charOffset > lineInfo->lineStarts[medianLineIndex])
		{
			minLineIndex = medianLineIndex;
		}
		else
		{
			minLineIndex = maxLineIndex = medianLineIndex;
		}
	};
	TextFileLocus result;
	result.newlines = (U32)minLineIndex;

	// Calculate the offsets to the start and end of the line containing the locus.
	result.lineStartOffset = getLineOffset(lineInfo, result.newlines);
	result.lineEndOffset = getLineOffset(lineInfo, result.newlines + 1) - 1;
	WAVM_ASSERT(charOffset >= result.lineStartOffset);
	WAVM_ASSERT(charOffset <= result.lineEndOffset);

	// Try to reuse work done by the last query counting characters on this line. Without this
	// reuse, it's possible to craft an input that takes O(numChars^2) time to parse: an input with
	// a single line with O(numChars) errors will take O(numChars) time to count the tabs+characters
	// preceding the error on the line *for each error*.
	const char* nextChar = string + result.lineStartOffset;
	if(lineInfo->lastQueryCharOffset < charOffset
	   && lineInfo->lastQueryCharOffset > result.lineStartOffset)
	{
		nextChar = string + lineInfo->lastQueryCharOffset;
		result.tabs = lineInfo->lastQueryTabs;
	}

	// Count tabs from the beginning of the line to locus.
	while(nextChar < string + charOffset)
	{
		result.tabs += *nextChar == '\t' ? 1 : 0;
		++nextChar;
	}

	// Derive the number of non-tab characters from the offset within the line and the number of
	// tabs preceding this locus in the line.
	result.characters = U32(charOffset - result.lineStartOffset - result.tabs);

	lineInfo->lastQueryCharOffset = charOffset;
	lineInfo->lastQueryTabs = result.tabs;

	return result;
}
