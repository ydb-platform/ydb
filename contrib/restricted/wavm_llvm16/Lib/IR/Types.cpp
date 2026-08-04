#include "WAVM/IR/Types.h"
#include <stdlib.h>
#include <string.h>
#include <new>
#include <utility>
#include "WAVM/Inline/Hash.h"
#include "WAVM/Inline/HashSet.h"
#include "WAVM/Platform/Diagnostics.h"
#include "WAVM/Platform/Mutex.h"

using namespace WAVM;
using namespace WAVM::IR;

struct TypeTupleHashPolicy
{
	static bool areKeysEqual(TypeTuple left, TypeTuple right)
	{
		if(left.size() != right.size()) { return false; }
		for(Uptr elemIndex = 0; elemIndex < left.size(); ++elemIndex)
		{
			if(left[elemIndex] != right[elemIndex]) { return false; }
		}
		return true;
	}
	static Uptr getKeyHash(TypeTuple typeTuple) { return typeTuple.getHash(); }
};

struct FunctionTypeHashPolicy
{
	static bool areKeysEqual(FunctionType left, FunctionType right)
	{
		return left.params() == right.params() && left.results() == right.results();
	}
	static Uptr getKeyHash(FunctionType functionType) { return functionType.getHash(); }
};

IR::TypeTuple::Impl::Impl(Uptr inNumElems, const ValueType* inElems) : numElems(inNumElems)
{
	if(numElems) { memcpy(elems, inElems, sizeof(ValueType) * numElems); }
	hash = XXH<Uptr>(elems, numElems * sizeof(ValueType), 0);
}

IR::TypeTuple::Impl::Impl(const Impl& inCopy) : hash(inCopy.hash), numElems(inCopy.numElems)
{
	if(numElems) { memcpy(elems, inCopy.elems, numElems * sizeof(ValueType)); }
}

IR::TypeTuple::TypeTuple(ValueType inElem) { impl = getUniqueImpl(1, &inElem); }

IR::TypeTuple::TypeTuple(const std::initializer_list<ValueType>& inElems)
{
	impl = getUniqueImpl(inElems.size(), inElems.begin());
}

IR::TypeTuple::TypeTuple(const std::vector<ValueType>& inElems)
{
	impl = getUniqueImpl(inElems.size(), inElems.data());
}

IR::TypeTuple::TypeTuple(const ValueType* inElems, Uptr numElems)
{
	impl = getUniqueImpl(numElems, inElems);
}

struct GlobalUniqueTypeTuples
{
	Platform::Mutex mutex;
	HashSet<TypeTuple, TypeTupleHashPolicy> set;
	std::vector<void*> impls;

	~GlobalUniqueTypeTuples()
	{
		Platform::Mutex::Lock lock(mutex);
		for(void* impl : impls) { free(impl); }
	}

	static GlobalUniqueTypeTuples& get()
	{
		static GlobalUniqueTypeTuples singleton;
		return singleton;
	}

private:
	GlobalUniqueTypeTuples() = default;
};

const TypeTuple::Impl* IR::TypeTuple::getUniqueImpl(Uptr numElems, const ValueType* inElems)
{
	if(numElems == 0)
	{
		static Impl emptyImpl(0, nullptr);
		return &emptyImpl;
	}
	else
	{
		const Uptr numImplBytes = Impl::calcNumBytes(numElems);
		Impl* localImpl = new(alloca(numImplBytes)) Impl(numElems, inElems);

		GlobalUniqueTypeTuples& globalUniqueTypeTuples = GlobalUniqueTypeTuples::get();
		Platform::Mutex::Lock lock(globalUniqueTypeTuples.mutex);

		const TypeTuple* typeTuple = globalUniqueTypeTuples.set.get(TypeTuple(localImpl));
		if(typeTuple) { return typeTuple->impl; }
		else
		{
			Impl* globalImpl = new(malloc(numImplBytes)) Impl(*localImpl);
			globalUniqueTypeTuples.set.addOrFail(TypeTuple(globalImpl));
			globalUniqueTypeTuples.impls.push_back(globalImpl);
			return globalImpl;
		}
	}
}

struct GlobalUniqueFunctionTypes
{
	Platform::Mutex mutex;
	HashSet<FunctionType, FunctionTypeHashPolicy> set;
	std::vector<void*> impls;

	~GlobalUniqueFunctionTypes()
	{
		Platform::Mutex::Lock lock(mutex);
		for(void* impl : impls) { free(impl); }
	}

	static GlobalUniqueFunctionTypes& get()
	{
		static GlobalUniqueFunctionTypes singleton;
		return singleton;
	}

private:
	GlobalUniqueFunctionTypes() = default;
};

IR::FunctionType::Impl::Impl(TypeTuple inResults,
							 TypeTuple inParams,
							 CallingConvention inCallingConvention)
: results(inResults), params(inParams), callingConvention(inCallingConvention)
{
	hash = Hash<Uptr>()(results.getHash(), params.getHash());
	hash = Hash<Uptr>()(hash, Uptr(callingConvention));
}

const FunctionType::Impl* IR::FunctionType::getUniqueImpl(TypeTuple results,
														  TypeTuple params,
														  CallingConvention callingConvention)
{
	if(results.size() == 0 && params.size() == 0 && callingConvention == CallingConvention::wasm)
	{
		static Impl emptyImpl{TypeTuple(), TypeTuple(), CallingConvention::wasm};
		return &emptyImpl;
	}
	else if(results.size() == 0 && params.size() == 0
			&& callingConvention == CallingConvention::intrinsic)
	{
		static Impl emptyImpl{TypeTuple(), TypeTuple(), CallingConvention::intrinsic};
		return &emptyImpl;
	}
	else
	{
		Impl localImpl(results, params, callingConvention);

		GlobalUniqueFunctionTypes& globalUniqueFunctionTypes = GlobalUniqueFunctionTypes::get();
		Platform::Mutex::Lock lock(globalUniqueFunctionTypes.mutex);

		const FunctionType* functionType
			= globalUniqueFunctionTypes.set.get(FunctionType(&localImpl));
		if(functionType) { return functionType->impl; }
		else
		{
			Impl* globalImpl = new(malloc(sizeof(Impl))) Impl(localImpl);
			globalUniqueFunctionTypes.set.addOrFail(FunctionType(globalImpl));
			globalUniqueFunctionTypes.impls.push_back(globalImpl);
			return globalImpl;
		}
	}
}
