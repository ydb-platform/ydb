#pragma once

#include <initializer_list>
#include "WAVM/Inline/Assert.h"
#include "WAVM/Inline/BasicTypes.h"
#include "WAVM/Inline/Hash.h"
#include "WAVM/Inline/HashTable.h"

namespace WAVM {
	template<typename Key, typename Value> struct HashMapPair
	{
		Key key;
		Value value;

		template<typename... ValueArgs> HashMapPair(const Key& inKey, ValueArgs&&... valueArgs);

		template<typename... ValueArgs> HashMapPair(Key&& inKey, ValueArgs&&... valueArgs);
	};

	template<typename Key, typename Value> struct HashMapIterator
	{
		template<typename, typename, typename> friend struct HashMap;

		typedef HashMapPair<Key, Value> Pair;

		bool operator!=(const HashMapIterator& other) const;
		bool operator==(const HashMapIterator& other) const;
		operator bool() const;
		void operator++();

		const Pair& operator*() const;
		const Pair* operator->() const;

	private:
		const HashTableBucket<Pair>* bucket;
		const HashTableBucket<Pair>* endBucket;

		HashMapIterator(const HashTableBucket<Pair>* inBucket,
						const HashTableBucket<Pair>* inEndBucket);
	};

	template<typename Key, typename Value, typename KeyHashPolicy = DefaultHashPolicy<Key>>
	struct HashMap
	{
		typedef HashMapPair<Key, Value> Pair;
		typedef HashMapIterator<Key, Value> Iterator;

		HashMap(Uptr reserveNumPairs = 0);
		HashMap(const std::initializer_list<Pair>& initializerList);

		// If the map contains the key already, returns the value bound to that key.
		// If the map doesn't contain the key, adds it to the map bound to a value constructed from
		// the provided arguments.
		template<typename... ValueArgs> Value& getOrAdd(const Key& key, ValueArgs&&... valueArgs);

		// If the map contains the key already, returns false.
		// If the map doesn't contain the key, adds it to the map bound to a value constructed from
		// the provided arguments, and returns true.
		template<typename... ValueArgs> bool add(const Key& key, ValueArgs&&... valueArgs);

		// Assuming the map doesn't contain the key, add it. Asserts if the map contained the key,
		// or silently does nothing if assertions are disabled.
		template<typename... ValueArgs> void addOrFail(const Key& key, ValueArgs&&... valueArgs);

		// If the map contains the key already, replaces the value bound to it with a value
		// constructed from the provided arguments. If the map doesn't contain the key, adds it to
		// the map bound to a value constructed from the provided arguments. In both cases, a
		// reference to the value bound to the key is returned.
		template<typename... ValueArgs> Value& set(const Key& key, ValueArgs&&... valueArgs);

		// If the map contains the key, removes it and returns true.
		// If the map doesn't contain the key, returns false.
		bool remove(const Key& key);

		// Assuming the map contains the key, remove it. Asserts if the map didn't contain the key,
		// or silently does nothing if assertions are disabled.
		void removeOrFail(const Key& key);

		// Returns true if the map contains the key.
		bool contains(const Key& key) const;

		// Returns a reference to the value bound to the key. Assumes that the map contains the key.
		const Value& operator[](const Key& key) const;
		Value& operator[](const Key& key);

		// Returns a pointer to the value bound to the key, or null if the map doesn't contain the
		// key.
		const Value* get(const Key& key) const;
		Value* get(const Key& key);

		// Returns a pointer to the key-value pair for a key, or null if the map doesn't contain the
		// key.
		const Pair* getPair(const Key& key) const;

		// Removes all pairs from the map.
		void clear();

		Iterator begin() const;
		Iterator end() const;

		Uptr size() const;

		// Compute some statistics about the space usage of this map.
		void analyzeSpaceUsage(Uptr& outTotalMemoryBytes,
							   Uptr& outMaxProbeCount,
							   F32& outOccupancy,
							   F32& outAverageProbeCount) const;

	private:
		struct HashTablePolicy
		{
			WAVM_FORCEINLINE static const Key& getKey(const Pair& pair) { return pair.key; }
			WAVM_FORCEINLINE static bool areKeysEqual(const Key& left, const Key& right)
			{
				return KeyHashPolicy::areKeysEqual(left, right);
			}
		};

		HashTable<Key, Pair, HashTablePolicy> table;
	};

// The implementation is defined in a separate file.
#include "Impl/HashMapImpl.h"
}
