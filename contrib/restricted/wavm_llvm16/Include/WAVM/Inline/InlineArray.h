#pragma once

#include "Impl/OptionalStorage.h"
#include "WAVM/Inline/Assert.h"

namespace WAVM {
	template<typename Element, Uptr maxElements> struct InlineArray
	{
		InlineArray() : numElements(0) {}
		~InlineArray()
		{
			if(!std::is_trivially_destructible<Element>::value)
			{
				for(Uptr index = 0; index < numElements; ++index) { elements[index].destruct(); }
			}
		}

		template<typename... ElementArgs> void push_back(ElementArgs&&... elementArgs)
		{
			WAVM_ERROR_UNLESS(numElements < maxElements);
			elements[numElements++].construct(std::forward<ElementArgs>(elementArgs)...);
		}

		template<typename... ElementArgs>
		void resize(Uptr newNumElements, ElementArgs&&... elementArgs)
		{
			WAVM_ERROR_UNLESS(newNumElements < maxElements);
			if(newNumElements < numElements)
			{
				for(Uptr index = newNumElements; index < numElements; ++index)
				{ elements[index].destruct(); }
			}
			else if(newNumElements > numElements)
			{
				for(Uptr index = numElements; index < newNumElements; ++index)
				{ elements[index].construct(std::forward<ElementArgs>(elementArgs)...); }
			}
			numElements = newNumElements;
		}

		Uptr size() const { return numElements; }
		Uptr maxSize() const { return maxElements; }
		bool isFull() const { return numElements == maxElements; }

		const Element& operator[](Uptr index) const
		{
			WAVM_ASSERT(index < numElements);
			return elements[index].get();
		}
		Element& operator[](Uptr index)
		{
			WAVM_ASSERT(index < numElements);
			return elements[index].get();
		}

	private:
		Uptr numElements;
		WAVM::OptionalStorage<Element> elements[maxElements];
	};
}
