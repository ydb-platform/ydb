#include "WAVM/Runtime/Intrinsics.h"
#include <initializer_list>
#include <string>
#include <utility>
#include <vector>
#include "RuntimePrivate.h"
#include "WAVM/IR/FeatureSpec.h"
#include "WAVM/IR/Module.h"
#include "WAVM/IR/Operators.h"
#include "WAVM/IR/Types.h"
#include "WAVM/IR/Validate.h"
#include "WAVM/Inline/Errors.h"
#include "WAVM/Inline/Hash.h"
#include "WAVM/Inline/HashMap.h"
#include "WAVM/Inline/Serialization.h"
#include "WAVM/Inline/Timing.h"
#include "WAVM/Platform/RWMutex.h"
#include "WAVM/Runtime/Runtime.h"

namespace WAVM { namespace Intrinsics {
	struct ModuleImpl
	{
		HashMap<std::string, Intrinsics::Function*> functionMap;
		HashMap<std::string, Intrinsics::Table*> tableMap;
		HashMap<std::string, Intrinsics::Memory*> memoryMap;
		HashMap<std::string, Intrinsics::Global*> globalMap;
	};
}}

using namespace WAVM;
using namespace WAVM::Runtime;
using namespace WAVM::IR;

Intrinsics::Module::~Module()
{
	if(impl) { delete impl; }
}

static void initializeModule(Intrinsics::Module* moduleRef)
{
	if(!moduleRef->impl) { moduleRef->impl = new Intrinsics::ModuleImpl; }
}

Intrinsics::Function::Function(Intrinsics::Module* moduleRef,
							   const char* inName,
							   void* inNativeFunction,
							   FunctionType inType)
: name(inName), type(inType), nativeFunction(inNativeFunction)
{
	initializeModule(moduleRef);

	if(moduleRef->impl->functionMap.contains(name))
	{ Errors::fatalf("Intrinsic function already registered: %s", name); }
	moduleRef->impl->functionMap.set(name, this);
}

Intrinsics::Global::Global(Intrinsics::Module* moduleRef,
						   const char* inName,
						   ValueType inType,
						   Value inValue)
: name(inName), type(inType), value(inValue)
{
	initializeModule(moduleRef);

	if(moduleRef->impl->globalMap.contains(name))
	{ Errors::fatalf("Intrinsic global already registered: %s", name); }
	moduleRef->impl->globalMap.set(name, this);
}

Intrinsics::Table::Table(Intrinsics::Module* moduleRef, const char* inName, const TableType& inType)
: name(inName), type(inType)
{
	initializeModule(moduleRef);

	if(moduleRef->impl->tableMap.contains(name))
	{ Errors::fatalf("Intrinsic table already registered: %s", name); }
	moduleRef->impl->tableMap.set(name, this);
}

Intrinsics::Memory::Memory(Intrinsics::Module* moduleRef,
						   const char* inName,
						   const MemoryType& inType)
: name(inName), type(inType)
{
	initializeModule(moduleRef);

	if(moduleRef->impl->memoryMap.contains(name))
	{ Errors::fatalf("Intrinsic memory already registered: %s", name); }
	moduleRef->impl->memoryMap.set(name, this);
}

Instance* Intrinsics::instantiateModule(
	Compartment* compartment,
	const std::initializer_list<const Intrinsics::Module*>& moduleRefs,
	std::string&& debugName)
{
	Timing::Timer timer;

	IR::Module irModule(FeatureLevel::wavm);
	irModule.featureSpec.nonWASMFunctionTypes = true;
	DisassemblyNames names;

	std::vector<FunctionImportBinding> functionImportBindings;
	for(const Intrinsics::Module* moduleRef : moduleRefs)
	{
		if(moduleRef->impl)
		{
			for(const auto& pair : moduleRef->impl->functionMap)
			{
				functionImportBindings.push_back({pair.value->getNativeFunction()});
				const Uptr typeIndex = irModule.types.size();
				const Uptr functionIndex = irModule.functions.size();
				irModule.types.push_back(pair.value->getType());
				irModule.functions.imports.push_back({{typeIndex}, "", pair.value->getName()});
				irModule.imports.push_back({ExternKind::function, functionIndex});
				names.functions.push_back({pair.value->getName(), {}, {}});
			}

			for(const auto& pair : moduleRef->impl->tableMap)
			{
				const Uptr tableIndex = irModule.tables.size();
				irModule.tables.defs.push_back({pair.value->getType()});
				names.tables.push_back(pair.value->getName());
				irModule.exports.push_back({pair.value->getName(), ExternKind::table, tableIndex});
			}

			for(const auto& pair : moduleRef->impl->memoryMap)
			{
				const Uptr memoryIndex = irModule.memories.size();
				irModule.memories.defs.push_back({pair.value->getType()});
				names.memories.push_back(pair.value->getName());
				irModule.exports.push_back(
					{pair.value->getName(), ExternKind::memory, memoryIndex});
			}

			for(const auto& pair : moduleRef->impl->globalMap)
			{
				InitializerExpression initializerExpression;
				switch(pair.value->getType())
				{
				case ValueType::i32:
					initializerExpression.type = InitializerExpression::Type::i32_const;
					initializerExpression.i32 = pair.value->getValue().i32;
					break;
				case ValueType::i64:
					initializerExpression.type = InitializerExpression::Type::i64_const;
					initializerExpression.i64 = pair.value->getValue().i64;
					break;
				case ValueType::f32:
					initializerExpression.type = InitializerExpression::Type::f32_const;
					initializerExpression.f32 = pair.value->getValue().f32;
					break;
				case ValueType::f64:
					initializerExpression.type = InitializerExpression::Type::f64_const;
					initializerExpression.f64 = pair.value->getValue().f64;
					break;
				case ValueType::v128:
					initializerExpression.type = InitializerExpression::Type::v128_const;
					initializerExpression.v128 = pair.value->getValue().v128;
					break;

				case ValueType::externref:
				case ValueType::funcref:
					Errors::fatal("Intrinsic reference-typed globals are not supported");

				case ValueType::none:
				case ValueType::any:
				default: WAVM_UNREACHABLE();
				};

				const Uptr globalIndex = irModule.globals.size();
				irModule.globals.defs.push_back(
					{GlobalType(pair.value->getType(), false), initializerExpression});
				names.globals.push_back(pair.value->getName());
				irModule.exports.push_back(
					{pair.value->getName(), ExternKind::global, globalIndex});
			}
		}
	}

	// Generate thunks for the intrinsic functions.
	for(Uptr functionImportIndex = 0; functionImportIndex < irModule.functions.imports.size();
		++functionImportIndex)
	{
		const FunctionImport& functionImport = irModule.functions.imports[functionImportIndex];
		const FunctionType intrinsicFunctionType = irModule.types[functionImport.type.index];
		const FunctionType wasmFunctionType(intrinsicFunctionType.results(),
											intrinsicFunctionType.params(),
											CallingConvention::wasm);

		const Uptr wasmFunctionTypeIndex = irModule.types.size();
		irModule.types.push_back(wasmFunctionType);

		Serialization::ArrayOutputStream codeStream;
		OperatorEncoderStream opEncoder(codeStream);
		for(Uptr paramIndex = 0; paramIndex < intrinsicFunctionType.params().size(); ++paramIndex)
		{ opEncoder.local_get({paramIndex}); }
		opEncoder.call({functionImportIndex});
		opEncoder.end();

		const Uptr wasmFunctionIndex = irModule.functions.size();
		irModule.functions.defs.push_back({{wasmFunctionTypeIndex}, {}, codeStream.getBytes(), {}});
		names.functions.push_back({"thunk:" + functionImport.exportName, {}, {}});
		irModule.exports.push_back(
			{functionImport.exportName, ExternKind::function, wasmFunctionIndex});
	}

	setDisassemblyNames(irModule, names);

	if(WAVM_ENABLE_ASSERTS)
	{
		try
		{
			std::shared_ptr<IR::ModuleValidationState> moduleValidationState
				= IR::createModuleValidationState(irModule);
			validatePreCodeSections(*moduleValidationState);
			validateCodeSection(*moduleValidationState);
			validatePostCodeSections(*moduleValidationState);
		}
		catch(ValidationException const& exception)
		{
			Errors::fatalf("Validation exception in intrinsic module: %s",
						   exception.message.c_str());
		}
	}

	ModuleRef module = compileModule(irModule);
	Instance* instance = instantiateModuleInternal(compartment,
												   module,
												   std::move(functionImportBindings),
												   {},
												   {},
												   {},
												   {},
												   {},
												   std::move(debugName));

	Timing::logTimer("Instantiated intrinsic module", timer);
	return instance;
}

HashMap<std::string, Intrinsics::Function*> Intrinsics::getUninstantiatedFunctions(
	const std::initializer_list<const Intrinsics::Module*>& moduleRefs)
{
	HashMap<std::string, Intrinsics::Function*> result;

	for(const Intrinsics::Module* moduleRef : moduleRefs)
	{
		if(moduleRef->impl)
		{
			for(const auto& pair : moduleRef->impl->functionMap)
			{ result.addOrFail(pair.key, pair.value); }
		}
	}

	return result;
}

namespace WAVM { namespace Runtime {

#ifdef __APPLE__
static constexpr auto WAVM_CLOCK_TYPE = _CLOCK_MONOTONIC_RAW;
#else
static constexpr auto WAVM_CLOCK_TYPE = CLOCK_MONOTONIC_COARSE;
#endif

class Deadline {
public:
	bool isDeadlineReached() const {
		if (!deadline.has_value()) {
			return false;
		}
	
		struct timespec current = {};
		if (clock_gettime(WAVM_CLOCK_TYPE, &current) != 0) {
			// The simplest thing to do here is to assume that the deadline just hasn't arrived.
			return false;
		}
	
		return current.tv_sec > deadline->tv_sec ||
			(current.tv_sec == deadline->tv_sec && current.tv_nsec >= deadline->tv_nsec);
	}
	
	void setDeadline(std::optional<struct timespec> newDeadline) {
		deadline = newDeadline;
	}
	
private:
	std::optional<struct timespec> deadline;
};

static thread_local Deadline currentDeadline;

__attribute__((__noinline__)) void setCurrentDeadline(std::optional<struct timespec> deadline)
{
	currentDeadline.setDeadline(deadline);
}

__attribute__((__noinline__)) bool isCurrentDeadlineReached()
{
	return currentDeadline.isDeadlineReached();
}

struct timespec getInstant()
{
	struct timespec current = {};
	WAVM_ASSERT(clock_gettime(WAVM_CLOCK_TYPE, &current) == 0);
	return current;
}
	
}}
