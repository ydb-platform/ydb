#pragma once

#include <memory>
#include <string>
#include "WAVM/IR/IR.h"
#include "WAVM/IR/Operators.h"

namespace WAVM { namespace IR {
	struct FunctionDef;
	struct Module;

	struct ValidationException
	{
		std::string message;
		ValidationException(std::string&& inMessage) : message(inMessage) {}
	};

	struct ModuleValidationState;
	struct CodeValidationStreamImpl;

	struct CodeValidationStream
	{
		typedef void Result;

		WAVM_API CodeValidationStream(ModuleValidationState& moduleValidationState,
									  const FunctionDef& function);
		WAVM_API ~CodeValidationStream();

		WAVM_API void finish();

#define VISIT_OPCODE(_, name, nameString, Imm, ...) WAVM_API void name(Imm imm = {});
		WAVM_ENUM_OPERATORS(VISIT_OPCODE)
#undef VISIT_OPCODE

	private:
		CodeValidationStreamImpl* impl;
	};

	template<typename InnerStream> struct CodeValidationProxyStream
	{
		CodeValidationProxyStream(ModuleValidationState& moduleValidationState,
								  const FunctionDef& function,
								  InnerStream& inInnerStream)
		: codeValidationStream(moduleValidationState, function), innerStream(inInnerStream)
		{
		}

		void finishValidation() { codeValidationStream.finish(); }

#define VISIT_OPCODE(_, name, nameString, Imm, ...)                                                \
	void name(Imm imm = {})                                                                        \
	{                                                                                              \
		codeValidationStream.name(imm);                                                            \
		innerStream.name(imm);                                                                     \
	}
		WAVM_ENUM_OPERATORS(VISIT_OPCODE)
#undef VISIT_OPCODE

	private:
		CodeValidationStream codeValidationStream;
		InnerStream& innerStream;
	};

	WAVM_API std::shared_ptr<ModuleValidationState> createModuleValidationState(
		const Module& module);

	WAVM_API void validateTypes(ModuleValidationState& state);
	WAVM_API void validateImports(ModuleValidationState& state);
	WAVM_API void validateFunctionDeclarations(ModuleValidationState& state);
	WAVM_API void validateTableDefs(ModuleValidationState& state);
	WAVM_API void validateMemoryDefs(ModuleValidationState& state);
	WAVM_API void validateGlobalDefs(ModuleValidationState& state);
	WAVM_API void validateExceptionTypeDefs(ModuleValidationState& state);
	WAVM_API void validateExports(ModuleValidationState& state);
	WAVM_API void validateStartFunction(ModuleValidationState& state);
	WAVM_API void validateElemSegments(ModuleValidationState& state);
	WAVM_API void validateDataSegments(ModuleValidationState& state);

	inline void validatePreCodeSections(ModuleValidationState& state)
	{
		validateTypes(state);
		validateImports(state);
		validateFunctionDeclarations(state);
		validateTableDefs(state);
		validateMemoryDefs(state);
		validateGlobalDefs(state);
		validateExceptionTypeDefs(state);
		validateExports(state);
		validateStartFunction(state);
		validateElemSegments(state);
	}

	WAVM_API void validateCodeSection(ModuleValidationState& state);

	inline void validatePostCodeSections(ModuleValidationState& state)
	{
		validateDataSegments(state);
	}
}}
