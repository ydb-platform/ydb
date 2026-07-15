#include <stdlib.h>
#include <initializer_list>
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "WAVM/Inline/Assert.h"
#include "WAVM/Inline/BasicTypes.h"
#include "WAVM/Inline/Hash.h"
#include "WAVM/Inline/HashMap.h"
#include "WAVM/Inline/Timing.h"
#include "wavm-test.h"

using namespace WAVM;

static std::string generateRandomString()
{
	static constexpr Uptr maxChars = 16;

	const Uptr numChars = rand() % maxChars;
	char* buffer = (char*)alloca(numChars + 1);
	for(Uptr charIndex = 0; charIndex < numChars; ++charIndex)
	{ buffer[charIndex] = 0x20 + (rand() % (0x7E - 0x20)); }
	buffer[numChars] = 0;
	return std::string(buffer);
}

static void testStringMap()
{
	static constexpr Uptr numStrings = 1000;

	HashMap<std::string, U32> map;
	std::vector<HashMapPair<std::string, U32>> pairs;

	srand(0);

	for(Uptr i = 0; i < numStrings; ++i)
	{
		while(true)
		{
			std::string randomString = generateRandomString();

			bool alreadySawString = false;
			for(const HashMapPair<std::string, U32>& pair : pairs)
			{
				if(pair.key == randomString)
				{
					alreadySawString = true;
					break;
				}
			}

			if(!alreadySawString)
			{
				pairs.emplace_back(std::move(randomString), rand());
				break;
			}
		};
	}

	for(Uptr i = 0; i < pairs.size(); ++i)
	{
		WAVM_ERROR_UNLESS(map.add(pairs[i].key, pairs[i].value));
		WAVM_ERROR_UNLESS(!map.add(pairs[i].key, pairs[i].value));

		for(Uptr j = 0; j < pairs.size(); ++j)
		{
			const U32* valuePtr = map.get(pairs[j].key);
			if(j <= i) { WAVM_ERROR_UNLESS(valuePtr && *valuePtr == pairs[j].value); }
			else
			{
				WAVM_ERROR_UNLESS(!valuePtr);
			}
		}
	}

	for(Uptr i = 0; i < pairs.size(); ++i)
	{
		WAVM_ERROR_UNLESS(map.remove(pairs[i].key));
		WAVM_ERROR_UNLESS(!map.remove(pairs[i].key));

		for(Uptr j = 0; j < pairs.size(); ++j)
		{
			const U32* valuePtr = map.get(pairs[j].key);
			if(j > i) { WAVM_ERROR_UNLESS(valuePtr && *valuePtr == pairs[j].value); }
			else
			{
				WAVM_ERROR_UNLESS(!valuePtr);
			}
		}
	}
}

static void testU32Map()
{
	HashMap<U32, U32> map;

	static constexpr Uptr maxI = 1024 * 1024;

	for(Uptr i = 0; i < maxI; ++i) { WAVM_ERROR_UNLESS(!map.contains(U32(i))); }

	WAVM_ERROR_UNLESS(map.size() == 0);
	for(Uptr i = 0; i < maxI; ++i)
	{
		WAVM_ERROR_UNLESS(!map.contains(U32(i)));
		WAVM_ERROR_UNLESS(!map.get(U32(i)));
		WAVM_ERROR_UNLESS(map.add(U32(i), U32(i * 2)));
		WAVM_ERROR_UNLESS(map.contains(U32(i)));
		WAVM_ERROR_UNLESS(map[U32(i)] == U32(i * 2));
		WAVM_ERROR_UNLESS(map.size() == i + 1);
	}

	for(Uptr i = 0; i < maxI; ++i)
	{
		WAVM_ERROR_UNLESS(map.contains(U32(i)));
		WAVM_ERROR_UNLESS(map.remove(U32(i)));
		WAVM_ERROR_UNLESS(!map.contains(U32(i)));
		WAVM_ERROR_UNLESS(map.size() == maxI - i - 1);
	}

	for(Uptr i = 0; i < maxI; ++i) { WAVM_ERROR_UNLESS(!map.contains(U32(i))); }
}

#if defined(__clang__)
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wself-assign"
#endif
static void testMapCopy()
{
	// Add 1000..1999 to a HashMap.
	HashMap<Uptr, Uptr> a;
	for(Uptr i = 0; i < 1000; ++i) { a.add(i + 1000, i); }

	// Copy the map to a new HashMap.
	HashMap<Uptr, Uptr> b{a};

	// Test that both the new and old HashMap contain the expected numbers.
	for(Uptr i = 0; i < 1000; ++i)
	{
		WAVM_ERROR_UNLESS(!a.get(i));
		WAVM_ERROR_UNLESS(a[i + 1000] == i);
		WAVM_ERROR_UNLESS(!a.get(i + 2000));

		WAVM_ERROR_UNLESS(!b.get(i));
		WAVM_ERROR_UNLESS(b[i + 1000] == i);
		WAVM_ERROR_UNLESS(!b.get(i + 2000));
	}

	// Test copying a map from itself.
	b = b;

	// Test that the map wasn't changed by the copy-to-self.
	for(Uptr i = 0; i < 1000; ++i)
	{
		WAVM_ERROR_UNLESS(!b.get(i));
		WAVM_ERROR_UNLESS(b[i + 1000] == i);
		WAVM_ERROR_UNLESS(!b.get(i + 2000));
	}

	// Test removing an element from the map.
	b.remove(1000);
	WAVM_ERROR_UNLESS(a[1000] == 0);
	WAVM_ERROR_UNLESS(!b.get(1000));
}
#if defined(__clang__)
#pragma clang diagnostic pop
#endif

#if defined(__clang__)
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wself-move"
#endif
static void testMapMove()
{
	// Add 1000..1999 to a HashMap.
	HashMap<Uptr, Uptr> a;
	for(Uptr i = 0; i < 1000; ++i) { a.add(i + 1000, i); }

	// Move the map to a new HashMap.
	HashMap<Uptr, Uptr> b{std::move(a)};

	// Test that the new HashMap contains the expected numbers.
	for(Uptr i = 0; i < 1000; ++i)
	{
		WAVM_ERROR_UNLESS(!b.get(i));
		WAVM_ERROR_UNLESS(b[i + 1000] == i);
		WAVM_ERROR_UNLESS(!b.get(i + 2000));
	}

	// Test moving the map to itself.
	b = std::move(b);

	// Test that the map wasn't changed by the move-to-self.
	for(Uptr i = 0; i < 1000; ++i)
	{
		WAVM_ERROR_UNLESS(!b.get(i));
		WAVM_ERROR_UNLESS(b[i + 1000] == i);
		WAVM_ERROR_UNLESS(!b.get(i + 2000));
	}
}
#if defined(__clang__)
#pragma clang diagnostic pop
#endif

static void testMapInitializerList()
{
	HashMap<Uptr, Uptr> map{{1, 1}, {3, 2}, {5, 3}, {7, 4}, {11, 5}, {13, 6}, {17, 7}};
	WAVM_ERROR_UNLESS(!map.get(0));
	WAVM_ERROR_UNLESS(map[1] == 1);
	WAVM_ERROR_UNLESS(!map.get(2));
	WAVM_ERROR_UNLESS(map[3] == 2);
	WAVM_ERROR_UNLESS(!map.get(4));
	WAVM_ERROR_UNLESS(map[5] == 3);
	WAVM_ERROR_UNLESS(!map.get(6));
	WAVM_ERROR_UNLESS(map[7] == 4);
	WAVM_ERROR_UNLESS(!map.get(8));
	WAVM_ERROR_UNLESS(!map.get(9));
	WAVM_ERROR_UNLESS(!map.get(10));
	WAVM_ERROR_UNLESS(map[11] == 5);
	WAVM_ERROR_UNLESS(!map.get(12));
	WAVM_ERROR_UNLESS(map[13] == 6);
	WAVM_ERROR_UNLESS(!map.get(14));
	WAVM_ERROR_UNLESS(!map.get(15));
	WAVM_ERROR_UNLESS(!map.get(16));
	WAVM_ERROR_UNLESS(map[17] == 7);
}

static void testMapIterator()
{
	// Add 1..9 to a HashMap.
	HashMap<Uptr, Uptr> a;
	for(Uptr i = 1; i < 10; ++i) { a.add(i, i * 2); }

	// 1 + 2 + 3 + 4 + 5 + 6 + 7 + 8 + 9 = 45
	{
		Uptr keySum = 0;
		Uptr valueSum = 0;
		for(const auto& pair : a)
		{
			keySum += pair.key;
			valueSum += pair.value;
		}
		WAVM_ERROR_UNLESS(keySum == 45);
		WAVM_ERROR_UNLESS(valueSum == 90);
	}

	// Remove 5.
	a.remove(5);

	// 1 + 2 + 3 + 4 + 6 + 7 + 8 + 9 = 40
	{
		Uptr keySum = 0;
		Uptr valueSum = 0;
		for(const auto& pair : a)
		{
			keySum += pair.key;
			valueSum += pair.value;
		}
		WAVM_ERROR_UNLESS(keySum == 40);
		WAVM_ERROR_UNLESS(valueSum == 80);
	}
}

static void testMapGetOrAdd()
{
	HashMap<Uptr, Uptr> map;

	WAVM_ERROR_UNLESS(!map.get(0));
	WAVM_ERROR_UNLESS(map.getOrAdd(0, 1) == 1);
	WAVM_ERROR_UNLESS(map[0] == 1);
	WAVM_ERROR_UNLESS(map.getOrAdd(0, 3) == 1);
	WAVM_ERROR_UNLESS(map[0] == 1);
	WAVM_ERROR_UNLESS((map.getOrAdd(0, 5) += 7) == 8);
	WAVM_ERROR_UNLESS(map[0] == 8);
}

static void testMapSet()
{
	HashMap<Uptr, Uptr> map;

	WAVM_ERROR_UNLESS(!map.get(0));
	WAVM_ERROR_UNLESS(map.set(0, 1) == 1);
	WAVM_ERROR_UNLESS(map[0] == 1);
	WAVM_ERROR_UNLESS(map.set(0, 3) == 3);
	WAVM_ERROR_UNLESS(map[0] == 3);
}

struct EmplacedValue
{
	std::string a;
	std::string b;

	EmplacedValue(const std::string& inA, const std::string& inB) : a(inA), b(inB) {}
};

static void testMapEmplace()
{
	HashMap<Uptr, EmplacedValue> map;

	EmplacedValue& a = map.getOrAdd(0, "a", "b");
	WAVM_ERROR_UNLESS(a.a == "a");
	WAVM_ERROR_UNLESS(a.b == "b");

	WAVM_ERROR_UNLESS(map.add(1, "c", "d"));
	const EmplacedValue& b = *map.get(1);
	WAVM_ERROR_UNLESS(b.a == "c");
	WAVM_ERROR_UNLESS(b.b == "d");

	EmplacedValue& c = map.set(2, "e", "f");
	WAVM_ERROR_UNLESS(c.a == "e");
	WAVM_ERROR_UNLESS(c.b == "f");

	EmplacedValue& d = map.getOrAdd(2, "g", "h");
	WAVM_ERROR_UNLESS(d.a == "e");
	WAVM_ERROR_UNLESS(d.b == "f");
}

static void testMapBracketOperator()
{
	HashMap<Uptr, Uptr> map{{1, 1}, {3, 2}, {5, 3}, {7, 4}, {11, 5}, {13, 6}, {17, 7}};
	WAVM_ERROR_UNLESS(map[1] == 1);
	WAVM_ERROR_UNLESS(map[3] == 2);
	WAVM_ERROR_UNLESS(map[5] == 3);
	WAVM_ERROR_UNLESS(map[7] == 4);
	WAVM_ERROR_UNLESS(map[11] == 5);
	WAVM_ERROR_UNLESS(map[13] == 6);
	WAVM_ERROR_UNLESS(map[17] == 7);
}

I32 execHashMapTest(int argc, char** argv)
{
	Timing::Timer timer;
	testStringMap();
	testU32Map();
	testMapCopy();
	testMapMove();
	testMapInitializerList();
	testMapIterator();
	testMapGetOrAdd();
	testMapSet();
	testMapEmplace();
	testMapBracketOperator();
	Timing::logTimer("HashMapTest", timer);
	return 0;
}
