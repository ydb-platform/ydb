#pragma once

#include <functional>
#include <string>
#include <utility>
#include <vector>
#include "WAVM/IR/Module.h"
#include "WAVM/IR/Types.h"
#include "WAVM/Inline/BasicTypes.h"
#include "WAVM/Runtime/Runtime.h"

namespace WAVM { namespace Runtime {
	// An abstract resolver: maps module+export name pairs to a Runtime::Object.
	struct WAVM_API Resolver
	{
		virtual ~Resolver() {}
		virtual bool resolve(const std::string& moduleName,
							 const std::string& exportName,
							 IR::ExternType type,
							 Object*& outObject)
			= 0;
	};

	// A resolver that always returns failure.
	struct NullResolver : Resolver
	{
		bool resolve(const std::string& moduleName,
					 const std::string& exportName,
					 IR::ExternType type,
					 Runtime::Object*& outObject) override
		{
			return false;
		}
	};

	// Generates stub objects that conform to the given ExternType.
	// Returns true if successful, false if stub creation failed due to resource exhaustion.
	// Upon successful return, outObject will contain a pointer to the stub object.
	enum class StubFunctionBehavior
	{
		zero,
		trap,
	};
	WAVM_API bool generateStub(const std::string& moduleName,
							   const std::string& exportName,
							   IR::ExternType type,
							   Runtime::Object*& outObject,
							   Compartment* compartment,
							   StubFunctionBehavior functionBehavior = StubFunctionBehavior::trap,
							   ResourceQuotaRefParam resourceQuota = ResourceQuotaRef());

	// A resolver that generates stubs for objects that the inner resolver can't find.
	struct WAVM_API StubResolver : Resolver
	{
		StubResolver(Compartment* inCompartment,
					 StubFunctionBehavior inFunctionBehavior = StubFunctionBehavior::trap,
					 bool inLogErrorOnStubGeneration = true,
					 ResourceQuotaRefParam resourceQuota = ResourceQuotaRef());

		virtual bool resolve(const std::string& moduleName,
							 const std::string& exportName,
							 IR::ExternType type,
							 Runtime::Object*& outObject) override;

	private:
		GCPointer<Compartment> compartment;
		ResourceQuotaRef resourceQuota;
		StubFunctionBehavior functionBehavior;
		bool logErrorOnStubGeneration;
	};

	// Links a module using the given resolver, returning an array mapping import indices to
	// objects. If the resolver fails to resolve any imports, throws a LinkException.
	struct LinkResult
	{
		struct MissingImport
		{
			std::string moduleName;
			std::string exportName;
			IR::ExternType type;
		};

		std::vector<MissingImport> missingImports;
		ImportBindings resolvedImports;
		bool success{false};
	};

	WAVM_API LinkResult linkModule(const IR::Module& module, Resolver& resolver);
}}
