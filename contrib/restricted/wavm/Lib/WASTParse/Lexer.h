#pragma once

#include "WAVM/IR/Operators.h"
#include "WAVM/Inline/BasicTypes.h"
#include "WAVM/Platform/Defines.h"
#include "WAVM/WASTParse/WASTParse.h"

#define VISIT_OPERATOR_TOKEN(opcode, name, nameString, ...)                                        \
	VISIT_TOKEN(name, "'" #nameString "'", #nameString)

#define VISIT_LITERAL_TOKEN(name) VISIT_TOKEN(name, "'" #name "'", #name)
#define ENUM_LITERAL_TOKENS()                                                                      \
	VISIT_LITERAL_TOKEN(module)                                                                    \
	VISIT_LITERAL_TOKEN(func)                                                                      \
	VISIT_LITERAL_TOKEN(type)                                                                      \
	VISIT_LITERAL_TOKEN(table)                                                                     \
	VISIT_LITERAL_TOKEN(export)                                                                    \
	VISIT_LITERAL_TOKEN(import)                                                                    \
	VISIT_LITERAL_TOKEN(memory)                                                                    \
	VISIT_LITERAL_TOKEN(data)                                                                      \
	VISIT_LITERAL_TOKEN(elem)                                                                      \
	VISIT_LITERAL_TOKEN(start)                                                                     \
	VISIT_LITERAL_TOKEN(param)                                                                     \
	VISIT_LITERAL_TOKEN(result)                                                                    \
	VISIT_LITERAL_TOKEN(local)                                                                     \
	VISIT_LITERAL_TOKEN(global)                                                                    \
	VISIT_LITERAL_TOKEN(assert_return)                                                             \
	VISIT_LITERAL_TOKEN(assert_return_arithmetic_nan)                                              \
	VISIT_LITERAL_TOKEN(assert_return_canonical_nan)                                               \
	VISIT_LITERAL_TOKEN(assert_return_arithmetic_nan_f32x4)                                        \
	VISIT_LITERAL_TOKEN(assert_return_canonical_nan_f32x4)                                         \
	VISIT_LITERAL_TOKEN(assert_return_arithmetic_nan_f64x2)                                        \
	VISIT_LITERAL_TOKEN(assert_return_canonical_nan_f64x2)                                         \
	VISIT_LITERAL_TOKEN(assert_return_func)                                                        \
	VISIT_LITERAL_TOKEN(assert_trap)                                                               \
	VISIT_LITERAL_TOKEN(assert_throws)                                                             \
	VISIT_LITERAL_TOKEN(assert_invalid)                                                            \
	VISIT_LITERAL_TOKEN(assert_unlinkable)                                                         \
	VISIT_LITERAL_TOKEN(assert_malformed)                                                          \
	VISIT_LITERAL_TOKEN(assert_exhaustion)                                                         \
	VISIT_LITERAL_TOKEN(benchmark)                                                                 \
	VISIT_LITERAL_TOKEN(thread)                                                                    \
	VISIT_LITERAL_TOKEN(wait)                                                                      \
	VISIT_LITERAL_TOKEN(either)                                                                    \
	VISIT_LITERAL_TOKEN(invoke)                                                                    \
	VISIT_LITERAL_TOKEN(get)                                                                       \
	VISIT_LITERAL_TOKEN(align)                                                                     \
	VISIT_LITERAL_TOKEN(offset)                                                                    \
	VISIT_LITERAL_TOKEN(item)                                                                      \
	VISIT_LITERAL_TOKEN(then)                                                                      \
	VISIT_LITERAL_TOKEN(register)                                                                  \
	VISIT_LITERAL_TOKEN(mut)                                                                       \
	VISIT_LITERAL_TOKEN(i32)                                                                       \
	VISIT_LITERAL_TOKEN(i64)                                                                       \
	VISIT_LITERAL_TOKEN(f32)                                                                       \
	VISIT_LITERAL_TOKEN(f64)                                                                       \
	VISIT_LITERAL_TOKEN(i8x16)                                                                     \
	VISIT_LITERAL_TOKEN(i16x8)                                                                     \
	VISIT_LITERAL_TOKEN(i32x4)                                                                     \
	VISIT_LITERAL_TOKEN(i64x2)                                                                     \
	VISIT_LITERAL_TOKEN(f32x4)                                                                     \
	VISIT_LITERAL_TOKEN(f64x2)                                                                     \
	VISIT_LITERAL_TOKEN(externref)                                                                 \
	VISIT_LITERAL_TOKEN(funcref)                                                                   \
	VISIT_LITERAL_TOKEN(extern)                                                                    \
	VISIT_LITERAL_TOKEN(declare)                                                                   \
	VISIT_LITERAL_TOKEN(shared)                                                                    \
	VISIT_LITERAL_TOKEN(quote)                                                                     \
	VISIT_LITERAL_TOKEN(binary)                                                                    \
	VISIT_LITERAL_TOKEN(v128)                                                                      \
	VISIT_LITERAL_TOKEN(exception_type)                                                            \
	VISIT_LITERAL_TOKEN(custom_section)                                                            \
	VISIT_LITERAL_TOKEN(after)                                                                     \
	VISIT_LITERAL_TOKEN(before)                                                                    \
	VISIT_LITERAL_TOKEN(data_count)                                                                \
	VISIT_LITERAL_TOKEN(code)                                                                      \
	VISIT_LITERAL_TOKEN(calling_conv)                                                              \
	VISIT_LITERAL_TOKEN(intrinsic)                                                                 \
	VISIT_LITERAL_TOKEN(intrinsic_with_context_switch)                                             \
	VISIT_LITERAL_TOKEN(c)                                                                         \
	VISIT_LITERAL_TOKEN(c_api_callback)                                                            \
	VISIT_TOKEN(ref_extern, "'ref.extern'", "ref.extern")

#define ENUM_TOKENS()                                                                              \
	VISIT_TOKEN(eof, "eof", _)                                                                     \
                                                                                                   \
	VISIT_TOKEN(unterminatedComment, "unterminated comment", _)                                    \
	VISIT_TOKEN(unrecognized, "unrecognized token", _)                                             \
	VISIT_TOKEN(legacyInstructionName, "legacy operator name", _)                                  \
                                                                                                   \
	VISIT_TOKEN(decimalFloat, "decimal float literal", _)                                          \
	VISIT_TOKEN(decimalInt, "decimal int literal", _)                                              \
	VISIT_TOKEN(hexFloat, "hexadecimal float literal", _)                                          \
	VISIT_TOKEN(hexInt, "hexadecimal int literal", _)                                              \
	VISIT_TOKEN(floatNaN, "float NaN literal", _)                                                  \
	VISIT_TOKEN(floatInf, "float infinity literal", _)                                             \
	VISIT_TOKEN(canonicalNaN, "float canonical NaN literal", _)                                    \
	VISIT_TOKEN(arithmeticNaN, "float arithmetic NaN literal", _)                                  \
	VISIT_TOKEN(string, "string literal", _)                                                       \
	VISIT_TOKEN(name, "name literal", _)                                                           \
	VISIT_TOKEN(quotedName, "quoted name literal", _)                                              \
                                                                                                   \
	VISIT_TOKEN(leftParenthesis, "'('", _)                                                         \
	VISIT_TOKEN(rightParenthesis, "')'", _)                                                        \
	VISIT_TOKEN(equals, "'='", _)                                                                  \
                                                                                                   \
	ENUM_LITERAL_TOKENS()                                                                          \
                                                                                                   \
	WAVM_ENUM_OPERATORS(VISIT_OPERATOR_TOKEN)

namespace WAVM { namespace WAST {
	typedef U16 TokenType;
	enum : U16
	{
#define VISIT_TOKEN(name, description, _) t_##name,
		ENUM_TOKENS()
#undef VISIT_TOKEN
	};

	WAVM_PACKED_STRUCT(struct Token {
		TokenType type;
		U32 begin;
	});

	struct LineInfo;

	// Lexes a string and returns an array of tokens.
	// Also returns a pointer in outLineInfo to the information necessary to resolve line/column
	// numbers for the tokens. The caller should pass the tokens and line info to
	// freeTokens/freeLineInfo, respectively, when it is done with them.
	Token* lex(const char* string,
			   Uptr stringLength,
			   LineInfo*& outLineInfo,
			   bool allowLegacyInstructionNames);

	void freeTokens(Token* tokens);
	void freeLineInfo(LineInfo* lineInfo);

	const char* describeToken(TokenType tokenType);

	TextFileLocus calcLocusFromOffset(const char* string,
									  const LineInfo* lineInfo,
									  Uptr charOffset);
}}
