// IWYU pragma: private, include "WAVM/Inline/HashTable.h"
// You should only include this file indirectly by including HashMap.h.

// Use these macros to compress the boilerplate template declarations in a non-inline member
// function definition for HashTable.
#define HASHTABLE_PARAMETERS                                                                       \
	typename Key, typename Element, typename HashTablePolicy, typename AllocPolicy
#define HASHTABLE_ARGUMENTS Key, Element, HashTablePolicy, AllocPolicy

template<HASHTABLE_PARAMETERS>
Uptr HashTable<HASHTABLE_ARGUMENTS>::calcProbeCount(Uptr bucketIndex) const
{
	WAVM_ASSERT(buckets[bucketIndex].hashAndOccupancy);
	WAVM_ASSERT(!(hashToBucketIndexMask & Bucket::isOccupiedMask));
	const Uptr idealBucketIndex = buckets[bucketIndex].hashAndOccupancy & hashToBucketIndexMask;
	if(idealBucketIndex <= bucketIndex) { return bucketIndex - idealBucketIndex; }
	else
	{
		return numBuckets() - idealBucketIndex + bucketIndex;
	}
}

template<HASHTABLE_PARAMETERS> void HashTable<HASHTABLE_ARGUMENTS>::clear()
{
	destruct();
	buckets = nullptr;
	numElements = 0;
	hashToBucketIndexMask = UINTPTR_MAX;
}

template<HASHTABLE_PARAMETERS> void HashTable<HASHTABLE_ARGUMENTS>::resize(Uptr newNumBuckets)
{
	WAVM_ASSERT(!(newNumBuckets & (newNumBuckets - 1)));

	const Uptr oldNumBuckets = numBuckets();
	Bucket* oldBuckets = buckets;

	if(!newNumBuckets)
	{
		WAVM_ASSERT(!numElements);
		buckets = nullptr;
	}
	else
	{
		// Allocate the new buckets.
		buckets = new Bucket[newNumBuckets]();
	}

	hashToBucketIndexMask = newNumBuckets - 1;

	if(numElements)
	{
		WAVM_ASSERT(oldBuckets);
		WAVM_ASSERT(buckets);

		// Iterate over the old buckets, and reinsert their contents in the new buckets.
		for(Uptr bucketIndex = 0; bucketIndex < oldNumBuckets; ++bucketIndex)
		{
			if(oldBuckets[bucketIndex].hashAndOccupancy)
			{
				Bucket& oldBucket = oldBuckets[bucketIndex];
				const Key& oldKey = HashTablePolicy::getKey(oldBucket.storage.get());

				// Find the new bucket to write the element to.
				Bucket& newBucket = getBucketForWrite(oldBucket.hashAndOccupancy, oldKey);

				// Move the element from the old bucket to the new.
				newBucket.storage.construct(std::move(oldBucket.storage.get()));
				newBucket.hashAndOccupancy = oldBucket.hashAndOccupancy;
				oldBucket.storage.destruct();
				oldBucket.hashAndOccupancy = 0;
			}
		}

		// Free the old buckets.
		delete[] oldBuckets;
	}
}

template<HASHTABLE_PARAMETERS>
bool HashTable<HASHTABLE_ARGUMENTS>::remove(Uptr hash, const Key& key)
{
	// Find the bucket (if any) holding the key.
	const Uptr hashAndOccupancy = hash | Bucket::isOccupiedMask;
	const HashTableBucket<Element>* bucket = getBucketForRead(hashAndOccupancy, key);
	if(!bucket) { return false; }
	else
	{
		// Remove the element in the bucket.
		eraseHashBucket(bucket - getBuckets());

		// Decrease the number of elements and resize the table if the occupancy is too low.
		--numElements;
		const Uptr maxDesiredBuckets = AllocPolicy::getMaxDesiredBuckets(numElements);
		if(numBuckets() > maxDesiredBuckets) { resize(maxDesiredBuckets); }

		return true;
	}
}

template<HASHTABLE_PARAMETERS>
const HashTableBucket<Element>* HashTable<HASHTABLE_ARGUMENTS>::getBucketForRead(
	Uptr hash,
	const Key& key) const
{
	if(!buckets) { return nullptr; }

	// Start at the bucket indexed by the lower bits of the hash.
	const Uptr hashAndOccupancy = hash | Bucket::isOccupiedMask;
	Uptr probeCount = 0;
	while(true)
	{
		const Uptr bucketIndex = (hashAndOccupancy + probeCount) & hashToBucketIndexMask;
		const Bucket& bucket = buckets[bucketIndex];

		if(!bucket.hashAndOccupancy)
		{
			// If the bucket is empty, return null.
			return nullptr;
		}
		else if(bucket.hashAndOccupancy == hashAndOccupancy
				&& HashTablePolicy::areKeysEqual(
					HashTablePolicy::getKey(buckets[bucketIndex].storage.get()), key))
		{
			// If the bucket holds the specified key, return null.
			return &bucket;
		}
		else
		{
			const Uptr bucketProbeCount = calcProbeCount(bucketIndex);
			if(probeCount > bucketProbeCount)
			{
				// If the bucket holds a key whose ideal bucket follows the specified key's ideal
				// bucket, then the following buckets cannot hold the key, so return null.
				return nullptr;
			}
			else
			{
				// Otherwise, continue to the next bucket.
				++probeCount;
				WAVM_ASSERT(probeCount < numBuckets());
			}
		}
	};
}

template<HASHTABLE_PARAMETERS>
HashTableBucket<Element>* HashTable<HASHTABLE_ARGUMENTS>::getBucketForModify(Uptr hash,
																			 const Key& key)
{
	return const_cast<Bucket*>(getBucketForRead(hash, key));
}

template<HASHTABLE_PARAMETERS>
HashTableBucket<Element>& HashTable<HASHTABLE_ARGUMENTS>::getBucketForAdd(Uptr hash, const Key& key)
{
	// Make sure there's enough space to add a new key to the table.
	const Uptr minDesiredBuckets = AllocPolicy::getMinDesiredBuckets(numElements + 1);
	if(numBuckets() < minDesiredBuckets) { resize(minDesiredBuckets); }

	// Find the bucket to write the new key to.
	Bucket& bucket = getBucketForWrite(hash, key);

	// If the bucket is empty, increment the number of elements in the table.
	// The caller is expected to fill the bucket once this function returns.
	if(!bucket.hashAndOccupancy) { ++numElements; }
	else
	{
		WAVM_ASSERT(bucket.hashAndOccupancy == (hash | Bucket::isOccupiedMask));
	}

	return bucket;
}

template<HASHTABLE_PARAMETERS>
HashTableBucket<Element>& HashTable<HASHTABLE_ARGUMENTS>::getBucketForWrite(Uptr hash,
																			const Key& key)
{
	WAVM_ASSERT(buckets);

	// Start at the bucket indexed by the lower bits of the hash.
	const Uptr hashAndOccupancy = hash | Bucket::isOccupiedMask;
	Uptr probeCount = 0;
	while(true)
	{
		const Uptr bucketIndex = (hashAndOccupancy + probeCount) & hashToBucketIndexMask;
		Bucket& bucket = buckets[bucketIndex];

		if(!bucket.hashAndOccupancy)
		{
			// If the bucket is empty, return it.
			return bucket;
		}
		else if(bucket.hashAndOccupancy == hashAndOccupancy
				&& HashTablePolicy::areKeysEqual(HashTablePolicy::getKey(bucket.storage.get()),
												 key))
		{
			// If the bucket already holds the specified key, return it.
			return bucket;
		}
		else
		{
			const Uptr bucketProbeCount = calcProbeCount(bucketIndex);
			if(probeCount > bucketProbeCount)
			{
				// If the bucket holds an element with a lower probe count (i.e. its ideal
				// bucket is after this key's ideal bucket), then evict the element and return
				// this bucket.
				evictHashBucket(bucketIndex);
				return bucket;
			}
			else
			{
				// Otherwise, continue to the next bucket.
				++probeCount;
				WAVM_ASSERT(probeCount < numBuckets());
			}
		}
	};
}

template<HASHTABLE_PARAMETERS>
void HashTable<HASHTABLE_ARGUMENTS>::evictHashBucket(Uptr bucketIndex)
{
	WAVM_ASSERT(buckets);

	// Move the bucket's element into a local variable and empty the bucket.
	Bucket& evictedBucket = buckets[bucketIndex];
	WAVM_ASSERT(evictedBucket.hashAndOccupancy);
	Element evictedElement{std::move(evictedBucket.storage.get())};
	Uptr evictedHashAndOccupancy = evictedBucket.hashAndOccupancy;
	evictedBucket.hashAndOccupancy = 0;
	evictedBucket.storage.destruct();

	while(true)
	{
		// Consider the next bucket
		bucketIndex = (bucketIndex + 1) & hashToBucketIndexMask;
		Bucket& bucket = buckets[bucketIndex];

		if(!bucket.hashAndOccupancy)
		{
			// If the bucket is empty, then fill it with the evicted element and return.
			bucket.storage.construct(std::move(evictedElement));
			bucket.hashAndOccupancy = evictedHashAndOccupancy;
			return;
		}
		else
		{
			// Otherwise, swap the evicted element with the bucket's element and continue
			// searching for an empty bucket.
			std::swap(evictedElement, bucket.storage.get());
			std::swap(evictedHashAndOccupancy, bucket.hashAndOccupancy);
		}
	};
}

template<HASHTABLE_PARAMETERS>
void HashTable<HASHTABLE_ARGUMENTS>::eraseHashBucket(Uptr eraseBucketIndex)
{
	WAVM_ASSERT(buckets);

	while(true)
	{
		// Consider the bucket following the erase bucket
		Bucket& bucketToErase = buckets[eraseBucketIndex];
		const Uptr bucketIndex = (eraseBucketIndex + 1) & hashToBucketIndexMask;
		Bucket& bucket = buckets[bucketIndex];

		if(!bucket.hashAndOccupancy)
		{
			// If the following bucket is empty, empty the erase bucket and return.
			WAVM_ASSERT(bucketToErase.hashAndOccupancy);
			bucketToErase.hashAndOccupancy = 0;
			bucketToErase.storage.destruct();
			return;
		}
		else
		{
			const Uptr bucketProbeCount = calcProbeCount(bucketIndex);
			if(bucketProbeCount == 0)
			{
				// If the bucket contains an element in its ideal bucket, it and its successors
				// don't need to be shifted into the erase bucket, so just empty the erase
				// bucket and return.
				WAVM_ASSERT(bucketToErase.hashAndOccupancy);
				bucketToErase.hashAndOccupancy = 0;
				bucketToErase.storage.destruct();
				return;
			}
			else
			{
				// Otherwise, shift the contents of the following bucket into the erase bucket
				// and continue with the following bucket as the new erase bucket.
				bucketToErase.storage.get() = std::move(bucket.storage.get());
				bucketToErase.hashAndOccupancy = bucket.hashAndOccupancy;
				eraseBucketIndex = bucketIndex;
			}
		}
	};
}

template<HASHTABLE_PARAMETERS>
void HashTable<HASHTABLE_ARGUMENTS>::analyzeSpaceUsage(Uptr& outTotalMemoryBytes,
													   Uptr& outMaxProbeCount,
													   F32& outOccupancy,
													   F32& outAverageProbeCount) const
{
	outTotalMemoryBytes = sizeof(Bucket) * numBuckets() + sizeof(*this);
	outOccupancy = size() / F32(numBuckets());

	outMaxProbeCount = 0;
	outAverageProbeCount = 0.0f;
	for(Uptr idealBucketIndex = 0; idealBucketIndex < numBuckets(); ++idealBucketIndex)
	{
		Uptr probeCount = 0;
		while(true)
		{
			const Uptr bucketIndex = (idealBucketIndex + probeCount) & hashToBucketIndexMask;
			if(!buckets[bucketIndex].hashAndOccupancy || probeCount > calcProbeCount(bucketIndex))
			{ break; }
			++probeCount;
		};

		outMaxProbeCount = probeCount > outMaxProbeCount ? probeCount : outMaxProbeCount;
		outAverageProbeCount += probeCount / F32(numBuckets());
	}
}

template<HASHTABLE_PARAMETERS>
HashTable<HASHTABLE_ARGUMENTS>::HashTable(Uptr estimatedNumElements)
: buckets(nullptr), numElements(0), hashToBucketIndexMask(UINTPTR_MAX)
{
	const Uptr numBuckets = AllocPolicy::getMinDesiredBuckets(estimatedNumElements);
	if(numBuckets)
	{
		// Allocate the initial buckets.
		hashToBucketIndexMask = numBuckets - 1;
		WAVM_ASSERT((numBuckets & hashToBucketIndexMask) == 0);
		buckets = new Bucket[numBuckets]();
	}
}

template<HASHTABLE_PARAMETERS> HashTable<HASHTABLE_ARGUMENTS>::HashTable(const HashTable& copy)
{
	copyFrom(copy);
}

template<HASHTABLE_PARAMETERS> HashTable<HASHTABLE_ARGUMENTS>::HashTable(HashTable&& movee) noexcept
{
	moveFrom(std::move(movee));
}

template<HASHTABLE_PARAMETERS> HashTable<HASHTABLE_ARGUMENTS>::~HashTable() { destruct(); }

template<HASHTABLE_PARAMETERS>
HashTable<HASHTABLE_ARGUMENTS>& HashTable<HASHTABLE_ARGUMENTS>::operator=(
	const HashTable<HASHTABLE_ARGUMENTS>& copyee)
{
	// Do nothing if copying from this.
	if(this != &copyee)
	{
		destruct();
		copyFrom(copyee);
	}
	return *this;
}

template<HASHTABLE_PARAMETERS>
HashTable<HASHTABLE_ARGUMENTS>& HashTable<HASHTABLE_ARGUMENTS>::operator=(
	HashTable<HASHTABLE_ARGUMENTS>&& movee) noexcept
{
	// Do nothing if moving from this.
	if(this != &movee)
	{
		destruct();
		moveFrom(std::move(movee));
	}
	return *this;
}

template<HASHTABLE_PARAMETERS> void HashTable<HASHTABLE_ARGUMENTS>::destruct()
{
	if(buckets)
	{
		for(Uptr bucketIndex = 0; bucketIndex < numBuckets(); ++bucketIndex)
		{
			if(buckets[bucketIndex].hashAndOccupancy) { buckets[bucketIndex].storage.destruct(); }
		}

		delete[] buckets;
		buckets = nullptr;
	}
}

template<HASHTABLE_PARAMETERS> void HashTable<HASHTABLE_ARGUMENTS>::copyFrom(const HashTable& copy)
{
	numElements = copy.numElements;
	hashToBucketIndexMask = copy.hashToBucketIndexMask;

	if(!copy.buckets) { buckets = nullptr; }
	else
	{
		buckets = new Bucket[copy.numBuckets()];
		for(Uptr bucketIndex = 0; bucketIndex < numBuckets(); ++bucketIndex)
		{
			buckets[bucketIndex].hashAndOccupancy = copy.buckets[bucketIndex].hashAndOccupancy;
			if(buckets[bucketIndex].hashAndOccupancy)
			{ buckets[bucketIndex].storage.construct(copy.buckets[bucketIndex].storage.get()); }
		}
	}
}

template<HASHTABLE_PARAMETERS>
void HashTable<HASHTABLE_ARGUMENTS>::moveFrom(HashTable&& movee) noexcept
{
	numElements = movee.numElements;
	hashToBucketIndexMask = movee.hashToBucketIndexMask;

	if(!movee.buckets) { buckets = nullptr; }
	else
	{
		buckets = movee.buckets;

		movee.numElements = 0;
		movee.hashToBucketIndexMask = 0;
		movee.buckets = nullptr;
	}
}

#undef HASHTABLE_PARAMETERS
#undef HASHTABLE_ARGUMENTS
