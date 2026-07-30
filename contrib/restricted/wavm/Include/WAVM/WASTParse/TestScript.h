#pragma once

#include <memory>
#include <vector>
#include "WAVM/IR/FeatureSpec.h"
#include "WAVM/IR/Module.h"
#include "WAVM/IR/Value.h"
#include "WAVM/Inline/BasicTypes.h"
#include "WAVM/WASTParse/WASTParse.h"

namespace WAVM { namespace WAST {
	struct Command
	{
		enum Type
		{
			_register,
			action,
			assert_return,
			assert_return_arithmetic_nan,
			assert_return_canonical_nan,
			assert_return_arithmetic_nan_f32x4,
			assert_return_canonical_nan_f32x4,
			assert_return_arithmetic_nan_f64x2,
			assert_return_canonical_nan_f64x2,
			assert_return_func,
			assert_trap,
			assert_throws,
			assert_invalid,
			assert_malformed,
			assert_unlinkable,
			benchmark,
			thread,
			wait,
		};
		const Type type;
		const TextFileLocus locus;

		Command(Type inType, TextFileLocus&& inLocus) : type(inType), locus(inLocus) {}

		virtual ~Command() {}
	};

	// Parse a test script from a string. Returns true if it succeeds, and writes the test commands
	// to outTestCommands.
	WAVM_API void parseTestCommands(const char* string,
									Uptr stringLength,
									const IR::FeatureSpec& featureSpec,
									std::vector<std::unique_ptr<Command>>& outTestCommands,
									std::vector<Error>& outErrors);

	// Actions

	enum class ActionType
	{
		_module,
		invoke,
		get,
	};

	enum class ExpectedTrapType
	{
		outOfBounds,
		outOfBoundsMemoryAccess,
		outOfBoundsTableAccess,
		outOfBoundsDataSegmentAccess,
		outOfBoundsElemSegmentAccess,
		stackOverflow,
		integerDivideByZeroOrIntegerOverflow,
		invalidFloatOperation,
		invokeSignatureMismatch,
		reachedUnreachable,
		indirectCallSignatureMismatch,
		uninitializedTableElement,
		outOfMemory,
		misalignedAtomicMemoryAccess,
		invalidArgument
	};

	struct Action
	{
		const ActionType type;
		const TextFileLocus locus;

		Action(ActionType inType, TextFileLocus&& inLocus) : type(inType), locus(inLocus) {}

		virtual ~Action() {}
	};

	struct ModuleAction : Action
	{
		std::string internalModuleName;
		std::unique_ptr<IR::Module> module;
		ModuleAction(TextFileLocus&& inLocus,
					 std::string&& inInternalModuleName,
					 std::unique_ptr<IR::Module>&& inModule)
		: Action(ActionType::_module, std::move(inLocus))
		, internalModuleName(inInternalModuleName)
		, module(std::move(inModule))
		{
		}
	};

	struct InvokeAction : Action
	{
		std::string internalModuleName;
		std::string exportName;
		std::vector<IR::Value> arguments;
		InvokeAction(TextFileLocus&& inLocus,
					 std::string&& inInternalModuleName,
					 std::string&& inExportName,
					 std::vector<IR::Value>&& inArguments)
		: Action(ActionType::invoke, std::move(inLocus))
		, internalModuleName(inInternalModuleName)
		, exportName(inExportName)
		, arguments(inArguments)
		{
		}
	};

	struct GetAction : Action
	{
		std::string internalModuleName;
		std::string exportName;
		GetAction(TextFileLocus&& inLocus,
				  std::string&& inInternalModuleName,
				  std::string&& inExportName)
		: Action(ActionType::get, std::move(inLocus))
		, internalModuleName(inInternalModuleName)
		, exportName(inExportName)
		{
		}
	};

	// Commands

	struct RegisterCommand : Command
	{
		std::string moduleName;
		std::string internalModuleName;
		RegisterCommand(TextFileLocus&& inLocus,
						std::string&& inModuleName,
						std::string&& inInternalModuleName)
		: Command(Command::_register, std::move(inLocus))
		, moduleName(inModuleName)
		, internalModuleName(inInternalModuleName)
		{
		}
	};

	struct ActionCommand : Command
	{
		std::unique_ptr<Action> action;
		ActionCommand(TextFileLocus&& inLocus, std::unique_ptr<Action>&& inAction)
		: Command(Command::action, std::move(inLocus)), action(std::move(inAction))
		{
		}
	};

	template<typename Float> struct FloatResultSet
	{
		enum class Type
		{
			literal,
			canonicalNaN,
			arithmeticNaN,
		};

		Type type;
		Float literal;
	};

	struct ResultSet
	{
		enum class Type
		{
			i32_const,
			i64_const,
			i8x16_const,
			i16x8_const,
			i32x4_const,
			i64x2_const,

			f32_const,
			f64_const,
			f32x4_const,
			f64x2_const,

			ref_extern,
			ref_func,
			ref_null,

			either,
		};

		Type type;

		union
		{
			I32 i32;
			I64 i64;
			I8 i8x16[16];
			I16 i16x8[8];
			I32 i32x4[4];
			I64 i64x2[2];

			FloatResultSet<F32> f32;
			FloatResultSet<F64> f64;
			FloatResultSet<F32> f32x4[4];
			FloatResultSet<F64> f64x2[2];

			Runtime::Object* object;
			IR::ReferenceType nullReferenceType;

			std::vector<std::shared_ptr<ResultSet>> alternatives;
		};

		ResultSet() : type(Type::i32_const), i32(0) {}
		ResultSet(const ResultSet& copyee) { copyFrom(copyee); }
		ResultSet(ResultSet&& movee) { moveFrom(std::move(movee)); }

		ResultSet& operator=(const ResultSet& copyee)
		{
			this->~ResultSet();
			copyFrom(copyee);
			return *this;
		}
		ResultSet& operator=(ResultSet&& movee)
		{
			this->~ResultSet();
			moveFrom(std::move(movee));
			return *this;
		}

		~ResultSet()
		{
			if(type == Type::either) { alternatives.~vector<std::shared_ptr<ResultSet>>(); }
		}

	private:
		void copyFrom(const ResultSet& copyee)
		{
			type = copyee.type;
			switch(type)
			{
			case Type::i32_const: i32 = copyee.i32; break;
			case Type::i64_const: i64 = copyee.i64; break;
			case Type::i8x16_const: memcpy(i8x16, copyee.i8x16, sizeof(i8x16)); break;
			case Type::i16x8_const: memcpy(i16x8, copyee.i16x8, sizeof(i16x8)); break;
			case Type::i32x4_const: memcpy(i32x4, copyee.i32x4, sizeof(i32x4)); break;
			case Type::i64x2_const: memcpy(i64x2, copyee.i64x2, sizeof(i64x2)); break;
			case Type::f32_const: f32 = copyee.f32; break;
			case Type::f64_const: f64 = copyee.f64; break;
			case Type::f32x4_const: memcpy(f32x4, copyee.f32x4, sizeof(f32x4)); break;
			case Type::f64x2_const: memcpy(f64x2, copyee.f64x2, sizeof(f64x2)); break;
			case Type::ref_extern: object = copyee.object; break;
			case Type::ref_func: break;
			case Type::ref_null: nullReferenceType = copyee.nullReferenceType; break;
			case Type::either:
				new(&alternatives) std::vector<std::shared_ptr<ResultSet>>(copyee.alternatives);
				break;
			default: WAVM_UNREACHABLE();
			};
		}
		void moveFrom(ResultSet&& movee)
		{
			type = movee.type;
			switch(type)
			{
			case Type::i32_const: i32 = movee.i32; break;
			case Type::i64_const: i64 = movee.i64; break;
			case Type::i8x16_const: memcpy(i8x16, movee.i8x16, sizeof(i8x16)); break;
			case Type::i16x8_const: memcpy(i16x8, movee.i16x8, sizeof(i16x8)); break;
			case Type::i32x4_const: memcpy(i32x4, movee.i32x4, sizeof(i32x4)); break;
			case Type::i64x2_const: memcpy(i64x2, movee.i64x2, sizeof(i64x2)); break;
			case Type::f32_const: f32 = movee.f32; break;
			case Type::f64_const: f64 = movee.f64; break;
			case Type::f32x4_const: memcpy(f32x4, movee.f32x4, sizeof(f32x4)); break;
			case Type::f64x2_const: memcpy(f64x2, movee.f64x2, sizeof(f64x2)); break;
			case Type::ref_extern: object = movee.object; break;
			case Type::ref_func: break;
			case Type::ref_null: nullReferenceType = movee.nullReferenceType; break;
			case Type::either:
				new(&alternatives)
					std::vector<std::shared_ptr<ResultSet>>(std::move(movee.alternatives));
				break;
			default: WAVM_UNREACHABLE();
			};
		}
	};

	struct AssertReturnCommand : Command
	{
		std::unique_ptr<Action> action;
		std::vector<ResultSet> expectedResultSets;
		AssertReturnCommand(TextFileLocus&& inLocus,
							std::unique_ptr<Action>&& inAction,
							std::vector<ResultSet>&& inExpectedResultSets)
		: Command(Command::assert_return, std::move(inLocus))
		, action(std::move(inAction))
		, expectedResultSets(inExpectedResultSets)
		{
		}
	};

	struct AssertReturnNaNCommand : Command
	{
		std::unique_ptr<Action> action;
		AssertReturnNaNCommand(Command::Type inType,
							   TextFileLocus&& inLocus,
							   std::unique_ptr<Action>&& inAction)
		: Command(inType, std::move(inLocus)), action(std::move(inAction))
		{
		}
	};

	struct AssertReturnFuncCommand : Command
	{
		std::unique_ptr<Action> action;
		AssertReturnFuncCommand(TextFileLocus&& inLocus, std::unique_ptr<Action>&& inAction)
		: Command(Command::assert_return_func, std::move(inLocus)), action(std::move(inAction))
		{
		}
	};

	struct AssertTrapCommand : Command
	{
		std::unique_ptr<Action> action;
		ExpectedTrapType expectedType;
		AssertTrapCommand(TextFileLocus&& inLocus,
						  std::unique_ptr<Action>&& inAction,
						  ExpectedTrapType inExpectedType)
		: Command(Command::assert_trap, std::move(inLocus))
		, action(std::move(inAction))
		, expectedType(inExpectedType)
		{
		}
	};

	struct AssertThrowsCommand : Command
	{
		std::unique_ptr<Action> action;
		std::string exceptionTypeInternalModuleName;
		std::string exceptionTypeExportName;
		std::vector<IR::Value> expectedArguments;
		AssertThrowsCommand(TextFileLocus&& inLocus,
							std::unique_ptr<Action>&& inAction,
							std::string&& inExceptionTypeInternalModuleName,
							std::string&& inExceptionTypeExportName,
							std::vector<IR::Value>&& inExpectedArguments)
		: Command(Command::assert_throws, std::move(inLocus))
		, action(std::move(inAction))
		, exceptionTypeInternalModuleName(inExceptionTypeInternalModuleName)
		, exceptionTypeExportName(inExceptionTypeExportName)
		, expectedArguments(inExpectedArguments)
		{
		}
	};

	enum class QuotedModuleType
	{
		none,
		text,
		binary
	};

	enum class InvalidOrMalformed
	{
		wellFormedAndValid,
		invalid,
		malformed
	};

	struct AssertInvalidOrMalformedCommand : Command
	{
		InvalidOrMalformed wasInvalidOrMalformed;
		QuotedModuleType quotedModuleType;
		std::string quotedModuleString;
		AssertInvalidOrMalformedCommand(Command::Type inType,
										TextFileLocus&& inLocus,
										InvalidOrMalformed inWasInvalidOrMalformed,
										QuotedModuleType inQuotedModuleType,
										std::string&& inQuotedModuleString)
		: Command(inType, std::move(inLocus))
		, wasInvalidOrMalformed(inWasInvalidOrMalformed)
		, quotedModuleType(inQuotedModuleType)
		, quotedModuleString(inQuotedModuleString)
		{
		}
	};

	struct AssertUnlinkableCommand : Command
	{
		std::unique_ptr<ModuleAction> moduleAction;
		AssertUnlinkableCommand(TextFileLocus&& inLocus,
								std::unique_ptr<ModuleAction> inModuleAction)
		: Command(Command::assert_unlinkable, std::move(inLocus))
		, moduleAction(std::move(inModuleAction))
		{
		}
	};

	struct BenchmarkCommand : Command
	{
		std::string name;
		std::unique_ptr<InvokeAction> invokeAction;

		BenchmarkCommand(TextFileLocus&& inLocus,
						 std::string&& inName,
						 std::unique_ptr<InvokeAction>&& inInvokeAction)
		: Command(Command::benchmark, std::move(inLocus))
		, name(std::move(inName))
		, invokeAction(std::move(inInvokeAction))
		{
		}
	};

	struct ThreadCommand : Command
	{
		std::string threadName;
		std::vector<std::string> sharedModuleInternalNames;
		std::vector<std::unique_ptr<Command>> commands;

		ThreadCommand(TextFileLocus&& inLocus,
					  std::string&& inThreadName,
					  std::vector<std::string> inSharedModuleInternalNames,
					  std::vector<std::unique_ptr<Command>>&& inCommands)
		: Command(Command::thread, std::move(inLocus))
		, threadName(inThreadName)
		, sharedModuleInternalNames(inSharedModuleInternalNames)
		, commands(std::move(inCommands))
		{
		}
	};

	struct WaitCommand : Command
	{
		std::string threadName;

		WaitCommand(TextFileLocus&& inLocus, std::string&& inThreadName)
		: Command(Command::wait, std::move(inLocus)), threadName(inThreadName)
		{
		}
	};
}}
