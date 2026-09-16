#pragma once

#include <initializer_list>
#include "WAVM/Inline/Assert.h"
#include "WAVM/Inline/BasicTypes.h"
#include "WAVM/Inline/Hash.h"
#include "WAVM/Inline/HashTable.h"

namespace WAVM {
	template<typename Element> struct HashSetIterator
	{
		template<typename, typename> friend struct HashSet;

		bool operator!=(const HashSetIterator& other) const;
		bool operator==(const HashSetIterator& other) const;
		operator bool() const;

		void operator++();

		const Element& operator*() const;
		const Element* operator->() const;

	private:
		const HashTableBucket<Element>* bucket;
		const HashTableBucket<Element>* endBucket;

		HashSetIterator(const HashTableBucket<Element>* inBucket,
						const HashTableBucket<Element>* inEndBucket);
	};

	template<typename Element, typename ElementHashPolicy = DefaultHashPolicy<Element>>
	struct HashSet
	{
		HashSet(Uptr reserveNumElements = 0);
		HashSet(const std::initializer_list<Element>& initializerList);

		// If the set contains the element already, returns false.
		// If the set didn't contain the element, adds it and returns true.
		bool add(const Element& element);

		// Assuming the set doesn't contain the element, add it. Asserts if the set contained the
		// element, or silently does nothing if assertions are disabled.
		void addOrFail(const Element& element);

		// If the set contains the element, removes it and returns true.
		// If the set doesn't contain the element, returns false.
		bool remove(const Element& element);

		// Assuming the set contains the element, remove it. Asserts if the set didn't contain the
		// element, or silently does nothing if assertions are disabled.
		void removeOrFail(const Element& element);

		// Returns a reference to the element in the set matching the given element. Assumes that
		// the map contains the key. This is useful if the hash policy allows distinct elements to
		// compare as equal; e.g. for deduplicating equivalent values.
		const Element& operator[](const Element& element) const;

		// Returns true if the set contains the element.
		bool contains(const Element& element) const;

		// If the set contains the element, returns a pointer to it. This is useful if the hash
		// policy allows distinct elements to compare as equal; e.g. for deduplicating equivalent
		// values.
		const Element* get(const Element& element) const;

		// Removes all elements from the set.
		void clear();

		HashSetIterator<Element> begin() const;
		HashSetIterator<Element> end() const;

		Uptr size() const;

		// Compute some statistics about the space usage of this set.
		void analyzeSpaceUsage(Uptr& outTotalMemoryBytes,
							   Uptr& outMaxProbeCount,
							   F32& outOccupancy,
							   F32& outAverageProbeCount) const;

	private:
		struct HashTablePolicy
		{
			WAVM_FORCEINLINE static const Element& getKey(const Element& element)
			{
				return element;
			}
			WAVM_FORCEINLINE static bool areKeysEqual(const Element& left, const Element& right)
			{
				return ElementHashPolicy::areKeysEqual(left, right);
			}
		};

		HashTable<Element, Element, HashTablePolicy> table;
	};

// The implementation is defined in a separate file.
#include "Impl/HashSetImpl.h"
}
