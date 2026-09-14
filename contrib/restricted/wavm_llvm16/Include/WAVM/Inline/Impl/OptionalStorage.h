#pragma once

#include <new>
#include <type_traits>
#include "WAVM/Inline/BasicTypes.h"

namespace WAVM {
	// A type that holds an optional instance of another type. The lifetime of the contained
	// instance is defined by the user of this type, and the user is responsible for calling
	// construct and destruct.
	template<typename Contents,
			 bool hasTrivialDestructor = std::is_trivially_destructible<Contents>::value>
	struct OptionalStorage
	{
		template<typename... Args> void construct(Args&&... args)
		{
			new(&contents) Contents(std::forward<Args>(args)...);
		}

		void destruct() { get().~Contents(); }

#if __cplusplus >= 201703L
		Contents& get() { return *std::launder(reinterpret_cast<Contents*>(&contents)); }
		const Contents& get() const
		{
			return *std::launder(reinterpret_cast<const Contents*>(&contents));
		}
#else
		Contents& get() { return *reinterpret_cast<Contents*>(&contents); }
		const Contents& get() const { return *reinterpret_cast<const Contents*>(&contents); }
#endif

	private:
		typename std::aligned_storage<sizeof(Contents), alignof(Contents)>::type contents;
	};

	// Partial specialization for types with trivial destructors.
	template<typename Contents> struct OptionalStorage<Contents, true>
	{
		template<typename... Args> void construct(Args&&... args)
		{
			new(&contents) Contents(std::forward<Args>(args)...);
		}

		void destruct() {}

#if __cplusplus >= 201703L
		Contents& get() { return *std::launder(reinterpret_cast<Contents*>(&contents)); }
		const Contents& get() const
		{
			return *std::launder(reinterpret_cast<const Contents*>(&contents));
		}
#else
		Contents& get() { return *reinterpret_cast<Contents*>(&contents); }
		const Contents& get() const { return *reinterpret_cast<const Contents*>(&contents); }
#endif

	private:
		typename std::aligned_storage<sizeof(Contents), alignof(Contents)>::type contents;
	};

	namespace OptionalStorageAssertions {
		struct NonTrivialType
		{
			NonTrivialType();
		};
		static_assert(std::is_trivial<OptionalStorage<NonTrivialType>>::value,
					  "OptionalStorage<NonTrivialType> is non-trivial");
	};
}
