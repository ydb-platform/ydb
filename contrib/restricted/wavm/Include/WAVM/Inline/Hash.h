#pragma once

#include <functional>
#include <string>
#include <type_traits>
#include <vector>
#include "BasicTypes.h"

#if defined(__GNUC__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wswitch-default"
#endif

#define XXH_FORCE_NATIVE_FORMAT 1
#define XXH_INLINE_ALL
#include <contrib/libs/xxhash/xxhash.h>

#if defined(__GNUC__)
#pragma GCC diagnostic pop
#undef Long
#endif

namespace WAVM {
	template<typename Key> struct Hash : std::hash<Key>
	{
	};

	inline U32 XXH32_fixed(U32 data, U32 seed)
	{
		const U32 a = data * I32(0x3D4D51C3);
		const U32 b = seed + 0x165667B5 - a;
		const U32 c = (b << 17) | (b >> 15);
		const U32 d = c * I32(0x27D4EB2F);
		const U32 e = d >> 15;
		const U32 f = d ^ e;
		const U32 g = f * I32(0x85EBCA77);
		const U32 h = g >> 13;
		const U32 i = g ^ h;
		const U32 j = i * I32(0xC2B2AE3D);
		const U32 k = j << 16;
		const U32 l = j ^ k;
		return l;
	}

	inline U64 XXH64_fixed(U64 data, U64 seed)
	{
		const U64 a = data * I64(0xC2B2AE3D27D4EB4F);
		const U64 b = (a << 31) | (a >> 33);
		const U64 c = b * I64(0x9E3779B185EBCA87);
		const U64 d = c ^ (seed + 0x27D4EB2F165667CD);
		const U64 e = (d << 27) | (d >> 37);
		const U64 f = e * I64(0x9E3779B185EBCA87);
		const U64 g = f - 0x7A1435883D4D519D;
		const U64 h = g >> 33;
		const U64 i = g ^ h;
		const U64 j = i * I64(0xC2B2AE3D27D4EB4F);
		const U64 k = j >> 29;
		const U64 l = j ^ k;
		const U64 m = l * I64(0x165667B19E3779F9);
		const U64 n = m >> 32;
		const U64 o = m ^ n;
		return o;
	}

	template<typename Hash> inline Hash XXH(const void* data, Uptr numBytes, Hash seed);
	template<> inline U32 XXH<U32>(const void* data, Uptr numBytes, U32 seed)
	{
		return ((U32)XXH3_64bits(data, numBytes)) ^ seed;
	}
	template<> inline U64 XXH<U64>(const void* data, Uptr numBytes, U64 seed)
	{
		return XXH3_64bits(data, numBytes) ^ seed;
	}

	template<> struct Hash<U8>
	{
		Uptr operator()(U8 i, Uptr seed = 0) const { return Uptr(XXH32_fixed(U32(i), U32(seed))); }
	};
	template<> struct Hash<I8>
	{
		Uptr operator()(I8 i, Uptr seed = 0) const { return Uptr(XXH32_fixed(I32(i), U32(seed))); }
	};
	template<> struct Hash<U16>
	{
		Uptr operator()(U16 i, Uptr seed = 0) const { return Uptr(XXH32_fixed(U32(i), U32(seed))); }
	};
	template<> struct Hash<I16>
	{
		Uptr operator()(I16 i, Uptr seed = 0) const { return Uptr(XXH32_fixed(I32(i), U32(seed))); }
	};
	template<> struct Hash<U32>
	{
		Uptr operator()(U32 i, Uptr seed = 0) const { return Uptr(XXH32_fixed(U32(i), U32(seed))); }
	};
	template<> struct Hash<I32>
	{
		Uptr operator()(I32 i, Uptr seed = 0) const { return Uptr(XXH32_fixed(I32(i), U32(seed))); }
	};
	template<> struct Hash<U64>
	{
		Uptr operator()(U64 i, Uptr seed = 0) const { return Uptr(XXH64_fixed(U64(i), U64(seed))); }
	};
	template<> struct Hash<I64>
	{
		Uptr operator()(I64 i, Uptr seed = 0) const { return Uptr(XXH64_fixed(I64(i), U64(seed))); }
	};

	template<> struct Hash<std::string>
	{
		Uptr operator()(const std::string& string, Uptr seed = 0) const
		{
			return Uptr(XXH64(string.data(), string.size(), seed));
		}
	};

	template<typename Element> struct Hash<std::vector<Element>>
	{
		Uptr operator()(const std::vector<Element>& vector, Uptr seed = 0) const
		{
			Uptr hash = seed;
			for(const Element& element : vector) { hash = Hash<Element>()(element, hash); }
			return hash;
		}
	};

	template<typename Key> struct DefaultHashPolicy
	{
		static bool areKeysEqual(const Key& left, const Key& right) { return left == right; }
		static Uptr getKeyHash(const Key& key) { return Hash<Key>()(key); }
	};
}
