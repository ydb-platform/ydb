#include <string.h>
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "Lexer.h"
#include "Parse.h"
#include "WAVM/IR/FeatureSpec.h"
#include "WAVM/IR/IR.h"
#include "WAVM/IR/Module.h"
#include "WAVM/IR/Types.h"
#include "WAVM/IR/Validate.h"
#include "WAVM/IR/Value.h"
#include "WAVM/Inline/BasicTypes.h"
#include "WAVM/Platform/Diagnostics.h"
#include "WAVM/Platform/Mutex.h"
#include "WAVM/RuntimeABI/RuntimeABI.h"
#include "WAVM/WASM/WASM.h"
#include "WAVM/WASTParse/TestScript.h"
#include "WAVM/WASTParse/WASTParse.h"

using namespace WAVM;
using namespace WAVM::IR;
using namespace WAVM::WAST;

struct HostRef
{
	Runtime::Function* function;
	HostRef() : function(nullptr) {}
	~HostRef()
	{
		if(function)
		{
			delete function->mutableData;
			delete function;
		}
	}

	HostRef(HostRef&& movee) noexcept
	{
		function = movee.function;
		movee.function = nullptr;
	}
	void operator=(HostRef&& movee) noexcept
	{
		function = movee.function;
		movee.function = nullptr;
	}
};

static Runtime::Function* makeHostRef(Uptr index)
{
	static Platform::Mutex indexToHostRefMapMutex;
	static HashMap<Uptr, HostRef> indexToHostRefMap;
	Platform::Mutex::Lock lock(indexToHostRefMapMutex);
	HostRef& hostRef = indexToHostRefMap.getOrAdd(index, HostRef());
	if(!hostRef.function)
	{
		Runtime::FunctionMutableData* functionMutableData
			= new Runtime::FunctionMutableData("test!ref.host!" + std::to_string(index));
		hostRef.function
			= new Runtime::Function(functionMutableData, UINTPTR_MAX, FunctionType::Encoding{0});
		functionMutableData->function = hostRef.function;
	}
	return hostRef.function;
}

static IR::Value parseConstExpression(CursorState* cursor)
{
	IR::Value result;
	parseParenthesized(cursor, [&] {
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
			result.type = ValueType::v128;
			result.v128 = parseV128(cursor);
			break;
		}
		case t_ref_extern: {
			++cursor->nextToken;
			result.type = ValueType::externref;
			result.function = makeHostRef(parseU32(cursor));
			break;
		}
		case t_ref_null: {
			++cursor->nextToken;
			result.type = asValueType(parseReferencedType(cursor));
			result.object = nullptr;
			break;
		}
		default:
			parseErrorf(cursor->parseState, cursor->nextToken, "expected const expression");
			throw RecoverParseException();
		};
	});
	return result;
}

static std::vector<IR::Value> parseConstExpressionTuple(CursorState* cursor)
{
	std::vector<IR::Value> values;
	while(cursor->nextToken->type == t_leftParenthesis)
	{ values.push_back(parseConstExpression(cursor)); };
	return values;
}

template<typename Float> FloatResultSet<Float> parseFloatResultSet(CursorState* cursor)
{
	FloatResultSet<Float> result;
	switch(cursor->nextToken->type)
	{
	case t_canonicalNaN:
		++cursor->nextToken;
		result.type = FloatResultSet<Float>::Type::canonicalNaN;
		result.literal = Float(0);
		break;
	case t_arithmeticNaN:
		++cursor->nextToken;
		result.type = FloatResultSet<Float>::Type::arithmeticNaN;
		result.literal = Float(0);
		break;

	default:
		result.type = FloatResultSet<Float>::Type::literal;
		result.literal = parseFloat<Float>(cursor);
		break;
	};
	return result;
}

static ResultSet parseV128ResultSet(CursorState* cursor)
{
	ResultSet result;
	switch(cursor->nextToken->type)
	{
	case t_i8x16:
		++cursor->nextToken;
		result.type = ResultSet::Type::i8x16_const;
		for(Uptr laneIndex = 0; laneIndex < 16; ++laneIndex)
		{ result.i8x16[laneIndex] = parseI8(cursor); }
		break;
	case t_i16x8:
		++cursor->nextToken;
		result.type = ResultSet::Type::i16x8_const;
		for(Uptr laneIndex = 0; laneIndex < 8; ++laneIndex)
		{ result.i16x8[laneIndex] = parseI16(cursor); }
		break;
	case t_i32x4:
		++cursor->nextToken;
		result.type = ResultSet::Type::i32x4_const;
		for(Uptr laneIndex = 0; laneIndex < 4; ++laneIndex)
		{ result.i32x4[laneIndex] = parseI32(cursor); }
		break;
	case t_i64x2:
		++cursor->nextToken;
		result.type = ResultSet::Type::i64x2_const;
		for(Uptr laneIndex = 0; laneIndex < 2; ++laneIndex)
		{ result.i64x2[laneIndex] = parseI64(cursor); }
		break;
	case t_f32x4:
		++cursor->nextToken;
		result.type = ResultSet::Type::f32x4_const;
		for(Uptr laneIndex = 0; laneIndex < 4; ++laneIndex)
		{ result.f32x4[laneIndex] = parseFloatResultSet<F32>(cursor); }
		break;
	case t_f64x2:
		++cursor->nextToken;
		result.type = ResultSet::Type::f64x2_const;
		for(Uptr laneIndex = 0; laneIndex < 2; ++laneIndex)
		{ result.f64x2[laneIndex] = parseFloatResultSet<F64>(cursor); }
		break;
	default:
		parseErrorf(cursor->parseState,
					cursor->nextToken,
					"expected 'i8x6', 'i16x8', 'i32x4', 'i64x2', 'f32x4', or 'f64x2'");
		throw RecoverParseException();
	};

	return result;
}

static ResultSet parseResultSet(CursorState* cursor)
{
	ResultSet result;
	parseParenthesized(cursor, [&] {
		switch(cursor->nextToken->type)
		{
		case t_i32_const: {
			++cursor->nextToken;
			result.type = ResultSet::Type::i32_const;
			result.i32 = parseI32(cursor);
			break;
		}
		case t_i64_const: {
			++cursor->nextToken;
			result.type = ResultSet::Type::i64_const;
			result.i64 = parseI64(cursor);
			break;
		}
		case t_f32_const: {
			++cursor->nextToken;
			result.type = ResultSet::Type::f32_const;
			result.f32 = parseFloatResultSet<F32>(cursor);
			break;
		}
		case t_f64_const: {
			++cursor->nextToken;
			result.type = ResultSet::Type::f64_const;
			result.f64 = parseFloatResultSet<F64>(cursor);
			break;
		}
		case t_v128_const: {
			++cursor->nextToken;
			result = parseV128ResultSet(cursor);
			break;
		}
		case t_ref_extern: {
			++cursor->nextToken;
			result.type = ResultSet::Type::ref_extern;
			result.object = &makeHostRef(parseU32(cursor))->object;
			break;
		}
		case t_ref_func: {
			++cursor->nextToken;
			result.type = ResultSet::Type::ref_func;
			break;
		}
		case t_ref_null: {
			++cursor->nextToken;
			result.type = ResultSet::Type::ref_null;
			result.nullReferenceType = parseReferencedType(cursor);
			break;
		}
		case t_either: {
			++cursor->nextToken;
			result.type = ResultSet::Type::either;
			new(&result.alternatives) std::vector<std::shared_ptr<ResultSet>>();
			while(cursor->nextToken->type != t_rightParenthesis)
			{
				result.alternatives.emplace_back(
					std::make_shared<ResultSet>(parseResultSet(cursor)));
			};
			break;
		}
		default:
			parseErrorf(cursor->parseState, cursor->nextToken, "expected const expression");
			throw RecoverParseException();
		};
	});
	return result;
}

static std::vector<ResultSet> parseResultSetTuple(CursorState* cursor)
{
	std::vector<ResultSet> results;
	while(cursor->nextToken->type == t_leftParenthesis)
	{ results.push_back(parseResultSet(cursor)); };
	return results;
}

static std::string parseOptionalNameAsString(CursorState* cursor)
{
	Name name;
	return tryParseName(cursor, name) ? name.getString() : std::string();
}

static void parseTestScriptModule(CursorState* cursor,
								  IR::Module& outModule,
								  std::string& outInternalModuleName,
								  QuotedModuleType& outQuotedModuleType,
								  std::string& outQuotedModuleString)
{
	outInternalModuleName = parseOptionalNameAsString(cursor);

	if(cursor->nextToken->type == t_quote || cursor->nextToken->type == t_binary)
	{
		// Parse a quoted module: (module quote|binary "...")
		const Token* quoteToken = cursor->nextToken;
		++cursor->nextToken;

		if(!tryParseString(cursor, outQuotedModuleString))
		{ parseErrorf(cursor->parseState, cursor->nextToken, "expected string"); }
		else
		{
			while(tryParseString(cursor, outQuotedModuleString)) {};
		}

		if(quoteToken->type == t_quote)
		{
			outQuotedModuleType = QuotedModuleType::text;

			std::vector<Error> quotedErrors;
			parseModule(outQuotedModuleString.c_str(),
						outQuotedModuleString.size() + 1,
						outModule,
						quotedErrors);
			for(auto&& error : quotedErrors)
			{
				cursor->parseState->unresolvedErrors.push_back(
					{quoteToken->begin, std::move(error.message)});
			}
		}
		else
		{
			outQuotedModuleType = QuotedModuleType::binary;

			WASM::LoadError loadError;
			if(!WASM::loadBinaryModule((const U8*)outQuotedModuleString.data(),
									   outQuotedModuleString.size(),
									   outModule,
									   &loadError))
			{
				switch(loadError.type)
				{
				case WASM::LoadError::Type::malformed:
					parseErrorf(cursor->parseState,
								quoteToken,
								"error deserializing binary module: %s",
								loadError.message.c_str());
					break;
				case WASM::LoadError::Type::invalid:
					parseErrorf(cursor->parseState,
								quoteToken,
								"validation error: %s",
								loadError.message.c_str());
					break;
				default: WAVM_UNREACHABLE();
				};
			}
		}
	}
	else
	{
		const U32 startCharOffset = cursor->nextToken->begin;
		parseModuleBody(cursor, outModule);
		const U32 endCharOffset = cursor->nextToken->begin;

		outQuotedModuleType = QuotedModuleType::text;
		outQuotedModuleString = std::string(cursor->parseState->string + startCharOffset,
											cursor->parseState->string + endCharOffset);
	}
}

static std::unique_ptr<Action> parseAction(CursorState* cursor, const IR::FeatureSpec& featureSpec)
{
	std::unique_ptr<Action> result;
	parseParenthesized(cursor, [&] {
		TextFileLocus locus = calcLocusFromOffset(
			cursor->parseState->string, cursor->parseState->lineInfo, cursor->nextToken->begin);

		switch(cursor->nextToken->type)
		{
		case t_get: {
			++cursor->nextToken;

			std::string nameString = parseOptionalNameAsString(cursor);
			std::string exportName = parseUTF8String(cursor);

			result = std::unique_ptr<Action>(
				new GetAction(std::move(locus), std::move(nameString), std::move(exportName)));
			break;
		}
		case t_invoke: {
			++cursor->nextToken;

			std::string nameString = parseOptionalNameAsString(cursor);
			std::string exportName = parseUTF8String(cursor);

			std::vector<IR::Value> arguments = parseConstExpressionTuple(cursor);
			result = std::unique_ptr<Action>(new InvokeAction(std::move(locus),
															  std::move(nameString),
															  std::move(exportName),
															  std::move(arguments)));
			break;
		}
		case t_module: {
			++cursor->nextToken;

			std::string internalModuleName;
			std::unique_ptr<Module> module{new Module(featureSpec)};

			QuotedModuleType quotedModuleType = QuotedModuleType::none;
			std::string quotedModuleString;
			parseTestScriptModule(
				cursor, *module, internalModuleName, quotedModuleType, quotedModuleString);

			result = std::unique_ptr<Action>(new ModuleAction(
				std::move(locus), std::move(internalModuleName), std::move(module)));
			break;
		}
		default:
			parseErrorf(cursor->parseState, cursor->nextToken, "expected 'get' or 'invoke'");
			throw RecoverParseException();
		};
	});

	return result;
}

template<Uptr numPrefixChars>
static bool stringStartsWith(const char* string, const char (&prefix)[numPrefixChars])
{
	return !strncmp(string, prefix, numPrefixChars - 1);
}

static std::unique_ptr<Command> parseCommand(CursorState* cursor,
											 const IR::FeatureSpec& featureSpec)
{
	std::unique_ptr<Command> result;

	if(cursor->nextToken[0].type == t_leftParenthesis
	   && (cursor->nextToken[1].type == t_module || cursor->nextToken[1].type == t_invoke
		   || cursor->nextToken[1].type == t_get))
	{
		std::unique_ptr<Action> action = parseAction(cursor, featureSpec);
		if(action)
		{
			TextFileLocus locus = action->locus;
			result
				= std::unique_ptr<Command>(new ActionCommand(std::move(locus), std::move(action)));
		}
	}
	else
	{
		parseParenthesized(cursor, [&] {
			TextFileLocus locus = calcLocusFromOffset(
				cursor->parseState->string, cursor->parseState->lineInfo, cursor->nextToken->begin);

			switch(cursor->nextToken->type)
			{
			case t_register: {
				++cursor->nextToken;

				std::string moduleName = parseUTF8String(cursor);
				std::string nameString = parseOptionalNameAsString(cursor);

				result = std::unique_ptr<Command>(new RegisterCommand(
					std::move(locus), std::move(moduleName), std::move(nameString)));
				break;
			}
			case t_assert_return: {
				++cursor->nextToken;

				std::unique_ptr<Action> action = parseAction(cursor, featureSpec);
				std::vector<ResultSet> expectedResults = parseResultSetTuple(cursor);
				result = std::unique_ptr<Command>(new AssertReturnCommand(
					std::move(locus), std::move(action), std::move(expectedResults)));
				break;
			}
			case t_assert_return_arithmetic_nan:
			case t_assert_return_canonical_nan:
			case t_assert_return_arithmetic_nan_f32x4:
			case t_assert_return_canonical_nan_f32x4:
			case t_assert_return_arithmetic_nan_f64x2:
			case t_assert_return_canonical_nan_f64x2: {
				// Translate the token to a command type.
				Command::Type commandType;
				switch(cursor->nextToken->type)
				{
				case t_assert_return_arithmetic_nan:
					commandType = Command::assert_return_arithmetic_nan;
					break;
				case t_assert_return_canonical_nan:
					commandType = Command::assert_return_canonical_nan;
					break;
				case t_assert_return_arithmetic_nan_f32x4:
					commandType = Command::assert_return_arithmetic_nan_f32x4;
					break;
				case t_assert_return_canonical_nan_f32x4:
					commandType = Command::assert_return_canonical_nan_f32x4;
					break;
				case t_assert_return_arithmetic_nan_f64x2:
					commandType = Command::assert_return_arithmetic_nan_f64x2;
					break;
				case t_assert_return_canonical_nan_f64x2:
					commandType = Command::assert_return_canonical_nan_f64x2;
					break;

				default: WAVM_UNREACHABLE();
				}
				++cursor->nextToken;

				std::unique_ptr<Action> action = parseAction(cursor, featureSpec);
				result = std::unique_ptr<Command>(
					new AssertReturnNaNCommand(commandType, std::move(locus), std::move(action)));
				break;
			}
			case t_assert_return_func: {
				++cursor->nextToken;

				std::unique_ptr<Action> action = parseAction(cursor, featureSpec);
				result = std::unique_ptr<Command>(
					new AssertReturnFuncCommand(std::move(locus), std::move(action)));
				break;
			}
			case t_assert_exhaustion:
			case t_assert_trap: {
				++cursor->nextToken;

				std::unique_ptr<Action> action = parseAction(cursor, featureSpec);

				const Token* errorToken = cursor->nextToken;
				std::string expectedErrorMessage;
				if(!tryParseString(cursor, expectedErrorMessage))
				{
					parseErrorf(cursor->parseState, cursor->nextToken, "expected string literal");
					throw RecoverParseException();
				}
				ExpectedTrapType expectedType;
				if(!strcmp(expectedErrorMessage.c_str(), "out of bounds memory access"))
				{ expectedType = ExpectedTrapType::outOfBoundsMemoryAccess; }
				else if(stringStartsWith(expectedErrorMessage.c_str(),
										 "out of bounds data segment access"))
				{
					expectedType = ExpectedTrapType::outOfBoundsDataSegmentAccess;
				}
				else if(stringStartsWith(expectedErrorMessage.c_str(),
										 "out of bounds elem segment access"))
				{
					expectedType = ExpectedTrapType::outOfBoundsElemSegmentAccess;
				}
				else if(stringStartsWith(expectedErrorMessage.c_str(), "out of bounds"))
				{
					expectedType = ExpectedTrapType::outOfBounds;
				}
				else if(!strcmp(expectedErrorMessage.c_str(), "call stack exhausted"))
				{
					expectedType = ExpectedTrapType::stackOverflow;
				}
				else if(!strcmp(expectedErrorMessage.c_str(), "integer overflow"))
				{
					expectedType = ExpectedTrapType::integerDivideByZeroOrIntegerOverflow;
				}
				else if(!strcmp(expectedErrorMessage.c_str(), "integer divide by zero"))
				{
					expectedType = ExpectedTrapType::integerDivideByZeroOrIntegerOverflow;
				}
				else if(!strcmp(expectedErrorMessage.c_str(), "invalid conversion to integer"))
				{
					expectedType = ExpectedTrapType::invalidFloatOperation;
				}
				else if(!strcmp(expectedErrorMessage.c_str(), "unaligned atomic"))
				{
					expectedType = ExpectedTrapType::misalignedAtomicMemoryAccess;
				}
				else if(stringStartsWith(expectedErrorMessage.c_str(), "unreachable"))
				{
					expectedType = ExpectedTrapType::reachedUnreachable;
				}
				else if(stringStartsWith(expectedErrorMessage.c_str(), "indirect call"))
				{
					expectedType = ExpectedTrapType::indirectCallSignatureMismatch;
				}
				else if(stringStartsWith(expectedErrorMessage.c_str(), "undefined"))
				{
					expectedType = ExpectedTrapType::outOfBoundsTableAccess;
				}
				else if(stringStartsWith(expectedErrorMessage.c_str(), "uninitialized"))
				{
					expectedType = ExpectedTrapType::uninitializedTableElement;
				}
				else if(stringStartsWith(expectedErrorMessage.c_str(), "invalid argument"))
				{
					expectedType = ExpectedTrapType::invalidArgument;
				}
				else if(!strcmp(expectedErrorMessage.c_str(), "element segment dropped"))
				{
					expectedType = ExpectedTrapType::invalidArgument;
				}
				else if(!strcmp(expectedErrorMessage.c_str(), "data segment dropped"))
				{
					expectedType = ExpectedTrapType::invalidArgument;
				}
				else
				{
					parseErrorf(cursor->parseState, errorToken, "unrecognized trap type");
					throw RecoverParseException();
				}

				result = std::unique_ptr<Command>(
					new AssertTrapCommand(std::move(locus), std::move(action), expectedType));
				break;
			}
			case t_assert_throws: {
				++cursor->nextToken;

				std::unique_ptr<Action> action = parseAction(cursor, featureSpec);

				std::string exceptionTypeInternalModuleName = parseOptionalNameAsString(cursor);
				std::string exceptionTypeExportName = parseUTF8String(cursor);

				std::vector<IR::Value> expectedArguments = parseConstExpressionTuple(cursor);
				result = std::unique_ptr<Command>(
					new AssertThrowsCommand(std::move(locus),
											std::move(action),
											std::move(exceptionTypeInternalModuleName),
											std::move(exceptionTypeExportName),
											std::move(expectedArguments)));
				break;
			}
			case t_assert_unlinkable: {
				++cursor->nextToken;

				if(cursor->nextToken[0].type != t_leftParenthesis
				   || cursor->nextToken[1].type != t_module)
				{
					parseErrorf(cursor->parseState, cursor->nextToken, "expected module");
					throw RecoverParseException();
				}

				std::unique_ptr<ModuleAction> moduleAction(
					(ModuleAction*)parseAction(cursor, featureSpec).release());

				std::string expectedErrorMessage;
				if(!tryParseString(cursor, expectedErrorMessage))
				{
					parseErrorf(cursor->parseState, cursor->nextToken, "expected string literal");
					throw RecoverParseException();
				}

				result = std::unique_ptr<Command>(
					new AssertUnlinkableCommand(std::move(locus), std::move(moduleAction)));
				break;
			}
			case t_assert_invalid:
			case t_assert_malformed: {
				const Command::Type commandType = cursor->nextToken->type == t_assert_invalid
													  ? Command::assert_invalid
													  : Command::assert_malformed;
				++cursor->nextToken;

				std::string internalModuleName;
				Module module(featureSpec);
				ParseState* outerParseState = cursor->parseState;
				ParseState malformedModuleParseState(outerParseState->string,
													 outerParseState->lineInfo);

				if(commandType == Command::assert_malformed
				   && (cursor->nextToken[0].type != t_leftParenthesis
					   || cursor->nextToken[1].type != t_module
					   || (cursor->nextToken[2].type != t_quote
						   && cursor->nextToken[2].type != t_binary)))
				{
					parseErrorf(
						cursor->parseState, cursor->nextToken, "expected quoted or binary module");
					throw RecoverParseException();
				}

				QuotedModuleType quotedModuleType = QuotedModuleType::none;
				std::string quotedModuleString;
				try
				{
					cursor->parseState = &malformedModuleParseState;
					parseParenthesized(cursor, [&] {
						require(cursor, t_module);

						parseTestScriptModule(cursor,
											  module,
											  internalModuleName,
											  quotedModuleType,
											  quotedModuleString);
					});
				}
				catch(RecoverParseException const&)
				{
					cursor->parseState = outerParseState;
					throw RecoverParseException();
				}
				cursor->parseState = outerParseState;

				std::string expectedErrorMessage;
				if(!tryParseString(cursor, expectedErrorMessage))
				{
					parseErrorf(cursor->parseState, cursor->nextToken, "expected string literal");
					throw RecoverParseException();
				}

				// Determine whether the module was invalid or malformed. If there are any syntax
				// errors, the module is malformed. If there are only validation errors, the module
				// is invalid.
				InvalidOrMalformed invalidOrMalformed = InvalidOrMalformed::wellFormedAndValid;
				for(const UnresolvedError& error : malformedModuleParseState.unresolvedErrors)
				{
					if(stringStartsWith(error.message.c_str(), "validation error"))
					{ invalidOrMalformed = InvalidOrMalformed::invalid; }
					else
					{
						invalidOrMalformed = InvalidOrMalformed::malformed;
						break;
					}
				}

				result = std::unique_ptr<Command>(
					new AssertInvalidOrMalformedCommand(commandType,
														std::move(locus),
														invalidOrMalformed,
														quotedModuleType,
														std::move(quotedModuleString)));
				break;
			}
			case t_benchmark: {
				++cursor->nextToken;

				std::string name;
				if(!tryParseString(cursor, name))
				{
					parseErrorf(
						cursor->parseState, cursor->nextToken, "expected benchmark name string");
					throw RecoverParseException();
				}

				if(cursor->nextToken[0].type != t_leftParenthesis
				   || cursor->nextToken[1].type != t_invoke)
				{
					parseErrorf(cursor->parseState, cursor->nextToken, "expected invoke");
					throw RecoverParseException();
				}

				std::unique_ptr<InvokeAction> invokeAction(
					(InvokeAction*)parseAction(cursor, featureSpec).release());

				result = std::unique_ptr<Command>(new BenchmarkCommand(
					std::move(locus), std::move(name), std::move(invokeAction)));

				break;
			}
			case t_thread: {
				++cursor->nextToken;

				Name threadName;
				if(!tryParseName(cursor, threadName))
				{
					parseErrorf(cursor->parseState, cursor->nextToken, "expected thread name");
					throw RecoverParseException();
				}

				std::vector<std::string> sharedModuleInternalNames;
				if(cursor->nextToken[0].type == t_leftParenthesis
				   && cursor->nextToken[1].type == t_shared)
				{
					parseParenthesized(cursor, [&] {
						++cursor->nextToken;
						while(cursor->nextToken->type == t_leftParenthesis)
						{
							parseParenthesized(cursor, [&] {
								require(cursor, t_module);
								Name internalModuleName;
								if(!tryParseName(cursor, internalModuleName))
								{
									parseErrorf(cursor->parseState,
												cursor->nextToken,
												"expected module name");
									throw RecoverParseException();
								}
								sharedModuleInternalNames.push_back(internalModuleName.getString());
							});
						};
					});
				}

				std::vector<std::unique_ptr<Command>> commands;
				while(cursor->nextToken->type == t_leftParenthesis)
				{ commands.emplace_back(parseCommand(cursor, featureSpec)); };

				result = std::unique_ptr<Command>(
					new ThreadCommand(std::move(locus),
									  threadName.getString(),
									  std::move(sharedModuleInternalNames),
									  std::move(commands)));

				break;
			}
			case t_wait: {
				++cursor->nextToken;

				Name threadName;
				if(!tryParseName(cursor, threadName))
				{
					parseErrorf(cursor->parseState, cursor->nextToken, "expected thread name");
					throw RecoverParseException();
				}

				result = std::unique_ptr<Command>(
					new WaitCommand(std::move(locus), threadName.getString()));

				break;
			}
			default:
				parseErrorf(cursor->parseState, cursor->nextToken, "unknown script command");
				throw RecoverParseException();
			};
		});
	}

	return result;
}

void WAST::parseTestCommands(const char* string,
							 Uptr stringLength,
							 const FeatureSpec& featureSpec,
							 std::vector<std::unique_ptr<Command>>& outTestCommands,
							 std::vector<Error>& outErrors)
{
	// Lex the input string.
	LineInfo* lineInfo = nullptr;
	Token* tokens = lex(string, stringLength, lineInfo, featureSpec.allowLegacyInstructionNames);
	ParseState parseState(string, lineInfo);
	CursorState cursor(tokens, &parseState);

	try
	{
		// Support test scripts that are just an inline module.
		if(cursor.nextToken[0].type == t_leftParenthesis
		   && (cursor.nextToken[1].type == t_import || cursor.nextToken[1].type == t_export
			   || cursor.nextToken[1].type == t_exception_type
			   || cursor.nextToken[1].type == t_global || cursor.nextToken[1].type == t_memory
			   || cursor.nextToken[1].type == t_table || cursor.nextToken[1].type == t_type
			   || cursor.nextToken[1].type == t_data || cursor.nextToken[1].type == t_elem
			   || cursor.nextToken[1].type == t_func || cursor.nextToken[1].type == t_start))
		{
			const TextFileLocus locus
				= calcLocusFromOffset(string, lineInfo, cursor.nextToken[0].begin);
			std::unique_ptr<Module> module{new Module(featureSpec)};
			parseModuleBody(&cursor, *module);
			std::unique_ptr<ModuleAction> moduleAction{
				new ModuleAction(TextFileLocus(locus), "", std::move(module))};
			auto actionCommand = new ActionCommand(TextFileLocus(locus), std::move(moduleAction));
			outTestCommands.emplace_back(actionCommand);
		}
		else
		{
			// (command)*<eof>
			while(cursor.nextToken->type == t_leftParenthesis)
			{ outTestCommands.emplace_back(parseCommand(&cursor, featureSpec)); };
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
}
