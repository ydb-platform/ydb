// IWYU pragma: private, include "WAVM/Inline/HashMap.h"
// You should only include this file indirectly by including HashMap.h.

// Use these macros to compress the boilerplate template declarations in a non-inline member
// function definition for HashMap.
#define HASHMAP_PARAMETERS typename Key, typename Value, typename KeyHashPolicy
#define HASHMAP_ARGUMENTS Key, Value, KeyHashPolicy

template<HASHMAP_PARAMETERS>
HashMap<HASHMAP_ARGUMENTS>::HashMap(Uptr reserveNumPairs) : table(reserveNumPairs)
{
}

template<HASHMAP_PARAMETERS>
HashMap<HASHMAP_ARGUMENTS>::HashMap(const std::initializer_list<Pair>& initializerList)
: table(initializerList.size())
{
	for(const Pair& pair : initializerList)
	{
		const bool result = add(pair.key, pair.value);
		WAVM_ASSERT(result);
	}
}

template<HASHMAP_PARAMETERS>
template<typename... ValueArgs>
Value& HashMap<HASHMAP_ARGUMENTS>::getOrAdd(const Key& key, ValueArgs&&... valueArgs)
{
	const Uptr hashAndOccupancy
		= KeyHashPolicy::getKeyHash(key) | HashTableBucket<Pair>::isOccupiedMask;
	HashTableBucket<Pair>& bucket = table.getBucketForAdd(hashAndOccupancy, key);
	if(!bucket.hashAndOccupancy)
	{
		bucket.hashAndOccupancy = hashAndOccupancy;
		bucket.storage.construct(key, std::forward<ValueArgs>(valueArgs)...);
	}
	return bucket.storage.get().value;
}

template<HASHMAP_PARAMETERS>
template<typename... ValueArgs>
bool HashMap<HASHMAP_ARGUMENTS>::add(const Key& key, ValueArgs&&... valueArgs)
{
	const Uptr hashAndOccupancy
		= KeyHashPolicy::getKeyHash(key) | HashTableBucket<Pair>::isOccupiedMask;
	HashTableBucket<Pair>& bucket = table.getBucketForAdd(hashAndOccupancy, key);
	if(!bucket.hashAndOccupancy)
	{
		bucket.hashAndOccupancy = hashAndOccupancy;
		bucket.storage.construct(key, std::forward<ValueArgs>(valueArgs)...);
		return true;
	}
	else
	{
		return false;
	}
}

template<HASHMAP_PARAMETERS>
template<typename... ValueArgs>
void HashMap<HASHMAP_ARGUMENTS>::addOrFail(const Key& key, ValueArgs&&... valueArgs)
{
	const Uptr hashAndOccupancy
		= KeyHashPolicy::getKeyHash(key) | HashTableBucket<Pair>::isOccupiedMask;
	HashTableBucket<Pair>& bucket = table.getBucketForAdd(hashAndOccupancy, key);
	WAVM_ASSERT(!bucket.hashAndOccupancy);
	bucket.hashAndOccupancy = hashAndOccupancy;
	bucket.storage.construct(key, std::forward<ValueArgs>(valueArgs)...);
}

template<HASHMAP_PARAMETERS>
template<typename... ValueArgs>
Value& HashMap<HASHMAP_ARGUMENTS>::set(const Key& key, ValueArgs&&... valueArgs)
{
	const Uptr hashAndOccupancy
		= KeyHashPolicy::getKeyHash(key) | HashTableBucket<Pair>::isOccupiedMask;
	HashTableBucket<Pair>& bucket = table.getBucketForAdd(hashAndOccupancy, key);
	if(!bucket.hashAndOccupancy)
	{
		bucket.hashAndOccupancy = hashAndOccupancy;
		bucket.storage.construct(key, std::forward<ValueArgs>(valueArgs)...);
	}
	else
	{
		bucket.storage.get().value = Value(std::forward<ValueArgs>(valueArgs)...);
	}
	return bucket.storage.get().value;
}

template<HASHMAP_PARAMETERS> bool HashMap<HASHMAP_ARGUMENTS>::remove(const Key& key)
{
	return table.remove(KeyHashPolicy::getKeyHash(key), key);
}

template<HASHMAP_PARAMETERS> void HashMap<HASHMAP_ARGUMENTS>::removeOrFail(const Key& key)
{
	const bool removed = table.remove(KeyHashPolicy::getKeyHash(key), key);
	WAVM_ASSERT(removed);
}

template<HASHMAP_PARAMETERS> bool HashMap<HASHMAP_ARGUMENTS>::contains(const Key& key) const
{
	const Uptr hash = KeyHashPolicy::getKeyHash(key);
	const HashTableBucket<Pair>* bucket = table.getBucketForRead(hash, key);
	WAVM_ASSERT(!bucket
				|| bucket->hashAndOccupancy == (hash | HashTableBucket<Pair>::isOccupiedMask));
	return bucket != nullptr;
}

template<HASHMAP_PARAMETERS>
const Value& HashMap<HASHMAP_ARGUMENTS>::operator[](const Key& key) const
{
	const Uptr hash = KeyHashPolicy::getKeyHash(key);
	const HashTableBucket<Pair>* bucket = table.getBucketForRead(hash, key);
	WAVM_ASSERT(bucket);
	if(!bucket)
	{
		// In addition to the assert, use WAVM_UNREACHABLE to ensure that GCC's -Wnull-dereference
		// warning isn't triggered because it thinks this function might return nullptr.
		WAVM_UNREACHABLE();
	}
	WAVM_ASSERT(bucket->hashAndOccupancy == (hash | HashTableBucket<Pair>::isOccupiedMask));
	return bucket->storage.get().value;
}

template<HASHMAP_PARAMETERS> Value& HashMap<HASHMAP_ARGUMENTS>::operator[](const Key& key)
{
	const Uptr hash = KeyHashPolicy::getKeyHash(key);
	HashTableBucket<Pair>* bucket = table.getBucketForModify(hash, key);
	WAVM_ASSERT(bucket);
	if(!bucket)
	{
		// In addition to the assert, use WAVM_UNREACHABLE to ensure that GCC's -Wnull-dereference
		// warning isn't triggered because it thinks this function might return nullptr.
		WAVM_UNREACHABLE();
	}
	WAVM_ASSERT(bucket->hashAndOccupancy == (hash | HashTableBucket<Pair>::isOccupiedMask));
	return bucket->storage.get().value;
}

template<HASHMAP_PARAMETERS> const Value* HashMap<HASHMAP_ARGUMENTS>::get(const Key& key) const
{
	const Uptr hash = KeyHashPolicy::getKeyHash(key);
	const HashTableBucket<Pair>* bucket = table.getBucketForRead(hash, key);
	if(!bucket) { return nullptr; }
	else
	{
		WAVM_ASSERT(bucket->hashAndOccupancy == (hash | HashTableBucket<Pair>::isOccupiedMask));
		return &bucket->storage.get().value;
	}
}

template<HASHMAP_PARAMETERS> Value* HashMap<HASHMAP_ARGUMENTS>::get(const Key& key)
{
	const Uptr hash = KeyHashPolicy::getKeyHash(key);
	HashTableBucket<Pair>* bucket = table.getBucketForModify(hash, key);
	if(!bucket) { return nullptr; }
	else
	{
		WAVM_ASSERT(bucket->hashAndOccupancy == (hash | HashTableBucket<Pair>::isOccupiedMask));
		return &bucket->storage.get().value;
	}
}

template<HASHMAP_PARAMETERS>
const HashMapPair<Key, Value>* HashMap<HASHMAP_ARGUMENTS>::getPair(const Key& key) const
{
	const Uptr hash = KeyHashPolicy::getKeyHash(key);
	const HashTableBucket<Pair>* bucket = table.getBucketForRead(hash, key);
	if(!bucket) { return nullptr; }
	else
	{
		WAVM_ASSERT(bucket->hashAndOccupancy == (hash | HashTableBucket<Pair>::isOccupiedMask));
		return &bucket->storage.get();
	}
}

template<HASHMAP_PARAMETERS> void HashMap<HASHMAP_ARGUMENTS>::clear() { table.clear(); }

template<HASHMAP_PARAMETERS> HashMapIterator<Key, Value> HashMap<HASHMAP_ARGUMENTS>::begin() const
{
	// Find the first occupied bucket.
	HashTableBucket<Pair>* beginBucket = table.getBuckets();
	HashTableBucket<Pair>* endBucket = table.getBuckets() + table.numBuckets();
	while(beginBucket < endBucket && !beginBucket->hashAndOccupancy) { ++beginBucket; };
	return HashMapIterator<Key, Value>(beginBucket, endBucket);
}

template<HASHMAP_PARAMETERS> HashMapIterator<Key, Value> HashMap<HASHMAP_ARGUMENTS>::end() const
{
	return HashMapIterator<Key, Value>(table.getBuckets() + table.numBuckets(),
									   table.getBuckets() + table.numBuckets());
}

template<HASHMAP_PARAMETERS> Uptr HashMap<HASHMAP_ARGUMENTS>::size() const { return table.size(); }

template<HASHMAP_PARAMETERS>
void HashMap<HASHMAP_ARGUMENTS>::analyzeSpaceUsage(Uptr& outTotalMemoryBytes,
												   Uptr& outMaxProbeCount,
												   F32& outOccupancy,
												   F32& outAverageProbeCount) const
{
	return table.analyzeSpaceUsage(
		outTotalMemoryBytes, outMaxProbeCount, outOccupancy, outAverageProbeCount);
}

template<typename Key, typename Value>
template<typename... ValueArgs>
HashMapPair<Key, Value>::HashMapPair(const Key& inKey, ValueArgs&&... valueArgs)
: key(inKey), value(std::forward<ValueArgs>(valueArgs)...)
{
}

template<typename Key, typename Value>
template<typename... ValueArgs>
HashMapPair<Key, Value>::HashMapPair(Key&& inKey, ValueArgs&&... valueArgs)
: key(std::move(inKey)), value(std::forward<ValueArgs>(valueArgs)...)
{
}

template<typename Key, typename Value>
bool HashMapIterator<Key, Value>::operator!=(const HashMapIterator& other) const
{
	return bucket != other.bucket;
}

template<typename Key, typename Value>
bool HashMapIterator<Key, Value>::operator==(const HashMapIterator& other) const
{
	return bucket == other.bucket;
}

template<typename Key, typename Value> HashMapIterator<Key, Value>::operator bool() const
{
	return bucket < endBucket && bucket->hashAndOccupancy;
}

template<typename Key, typename Value> void HashMapIterator<Key, Value>::operator++()
{
	do
	{
		++bucket;
	} while(bucket < endBucket && !bucket->hashAndOccupancy);
}

template<typename Key, typename Value>
const HashMapPair<Key, Value>& HashMapIterator<Key, Value>::operator*() const
{
	WAVM_ASSERT(bucket->hashAndOccupancy);
	return bucket->storage.get();
}

template<typename Key, typename Value>
const HashMapPair<Key, Value>* HashMapIterator<Key, Value>::operator->() const
{
	WAVM_ASSERT(bucket->hashAndOccupancy);
	return &bucket->storage.get();
}

template<typename Key, typename Value>
HashMapIterator<Key, Value>::HashMapIterator(
	const HashTableBucket<HashMapPair<Key, Value>>* inBucket,
	const HashTableBucket<HashMapPair<Key, Value>>* inEndBucket)
: bucket(inBucket), endBucket(inEndBucket)
{
}

#undef HASHMAP_PARAMETERS
#undef HASHMAP_ARGUMENTS
