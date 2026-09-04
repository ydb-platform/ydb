#pragma once

#include "Assert.h"
#include "BasicTypes.h"
#include "Impl/OptionalStorage.h"
#include "WAVM/Platform/Intrinsic.h"

namespace WAVM {
	struct DefaultHashTableAllocPolicy
	{
		static constexpr Uptr minBuckets = 8;

		static Uptr divideAndRoundUp(Uptr numerator, Uptr denominator)
		{
			return (numerator + denominator - 1) / denominator;
		}

		static Uptr getMaxDesiredBuckets(Uptr numDesiredElements)
		{
			const Uptr maxDesiredBuckets
				= Uptr(1) << WAVM::ceilLogTwo(divideAndRoundUp(numDesiredElements * 20, 7));
			return maxDesiredBuckets < minBuckets ? minBuckets : maxDesiredBuckets;
		}

		static Uptr getMinDesiredBuckets(Uptr numDesiredElements)
		{
			if(numDesiredElements == 0) { return 0; }
			else
			{
				const Uptr minDesiredBuckets
					= Uptr(1) << WAVM::ceilLogTwo(divideAndRoundUp(numDesiredElements * 20, 16));
				return minDesiredBuckets < minBuckets ? minBuckets : minDesiredBuckets;
			}
		}
	};

	/*
	struct HashTablePolicy
	{
		static const Key& getKey(const Element&);
		static bool areKeysEqual(const Key&, const Key&);
	};
	*/

	template<typename Element> struct HashTableBucket
	{
		OptionalStorage<Element> storage;
		Uptr hashAndOccupancy;

		static constexpr Uptr isOccupiedMask = Uptr(1) << (sizeof(Uptr) * 8 - 1);
		static constexpr Uptr hashMask = ~isOccupiedMask;
	};

	// A lightly encapsulated hash table, used internally by HashMap and HashSet.
	//
	//   It has a concept of both elements and keys: the key is something derivable from an element
	// that is used to find an element in the table. For a set, Key=Element, but for a map,
	// Key!=Element, and HashTablePolicy::getKey(Element) is used to derive the key of an element in
	// the hash table.
	//
	//   The implementation is a Robin Hood hash table.
	//
	//   The buckets are a linear array of elements paired with pointer-sized metadata that uses 1
	// bit to indicate whether the bucket is occupied, and the rest of the bits to store the hash of
	// the occupying element. The array is indexed by a hash of the element's key, modulo the number
	// of buckets, which is always a power of two.
	//
	//   Storing the hash and occupancy of a bucket imposes some overhead on the simplest cases
	// where the key is small/easy to hash and has a null value, but avoids pathological performance
	// in the general case. It also makes it easier to write good debugger visualizers for the hash
	// tables.
	//
	//   If more than one element hashes to the same bucket, the extra elements will be stored in
	// the buckets following the "ideal bucket" in the table. There is an abstract concept of how
	// "rich" an element is by how close it is stored to its ideal bucket.
	//   When inserting new elements, they are stored in the leftmost bucket, starting from their
	// ideal bucket, that is empty or holds an element that hashes to a higher bucket than their
	// ideal bucket. If the bucket contains an element, that element, and the elements from any
	// subsequent consecutively occupied buckets are moved one bucket right to make space for it.
	//   When removing an element, the chain of elements in following buckets are shifted left into
	// the hole until an empty bucket is found, or an element that is in its ideal bucket.
	//   This insert/remove strategy maintains the invariant that all elements that hash to the same
	// ideal bucket are stored in consecutive buckets at or following the ideal bucket, and elements
	// that hash to different buckets are sorted w.r.t. their ideal bucket. This invariant allows
	// searches to stop once they reach an element whose ideal bucket is to the right of the search
	// key's ideal bucket, limiting the worst case search time.
	//
	//   This is more complex than an externally chained hash table, which stored a list of elements
	// in each hash bucket, but has far better cache coherency as searching for a key is a linear
	// walk through memory, and less allocator overhead as the entire table uses a single
	// allocation.
	//
	//   For the table to function correctly, it must contain at least one empty bucket. As the
	// table approaches 100% occupancy, the collision rate increases, and the worst case search time
	// increases quickly.
	//   The table is dynamically resized as dictated by the AllocPolicy. The default
	// policy keeps the table between 35% and 80% occupied. This gives an average occupancy of 65%,
	// or about 2x space usage compared to just storing an array of the elements.
	template<typename Key,
			 typename Element,
			 typename HashTablePolicy,
			 typename AllocPolicy = DefaultHashTableAllocPolicy>
	struct HashTable
	{
		typedef HashTableBucket<Element> Bucket;

		HashTable(Uptr estimatedNumElements = 0);
		HashTable(const HashTable& copy);
		HashTable(HashTable&& movee) noexcept;
		~HashTable();

		HashTable& operator=(const HashTable& copyee);
		HashTable& operator=(HashTable&& movee) noexcept;

		void clear();

		void resize(Uptr newNumBuckets);

		bool remove(Uptr hash, const Key& key);

		const Bucket* getBucketForRead(Uptr hash, const Key& key) const;
		Bucket* getBucketForModify(Uptr hash, const Key& key);
		Bucket& getBucketForAdd(Uptr hash, const Key& key);

		Uptr size() const { return numElements; }
		Uptr numBuckets() const { return hashToBucketIndexMask + 1; }

		Bucket* getBuckets() const { return buckets; }

		// Compute some statistics about the space usage of this hash table.
		void analyzeSpaceUsage(Uptr& outTotalMemoryBytes,
							   Uptr& outMaxProbeCount,
							   F32& outOccupancy,
							   F32& outAverageProbeCount) const;

	private:
		Bucket* buckets;
		Uptr numElements;
		Uptr hashToBucketIndexMask;

		// Calculates the distance of the element in the bucket from its ideal bucket.
		Uptr calcProbeCount(Uptr bucketIndex) const;

		Bucket& getBucketForWrite(Uptr hash, const Key& key);
		void evictHashBucket(Uptr bucketIndex);
		void eraseHashBucket(Uptr eraseBucketIndex);

		void destruct();
		void copyFrom(const HashTable& copy);
		void moveFrom(HashTable&& movee) noexcept;
	};

// The implementation is defined in a separate file.
#include "Impl/HashTableImpl.h"
}
