#pragma once

#include <library/cpp/yt/memory/erased_storage.h>

#include <library/cpp/yt/misc/strong_typedef.h>

#include <atomic>

namespace NYT::NGlobal {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

class TGlobalVariablesRegistry;

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

inline constexpr size_t GlobalVariableMaxByteSize = 32;
using TErasedStorage = NYT::TErasedStorage<GlobalVariableMaxByteSize>;

// NB(arkady-e1ppa): Accessor must ensure thread-safety on its own.
using TAccessor = TErasedStorage(*)() noexcept;

////////////////////////////////////////////////////////////////////////////////

// Usage:
/*
 * // public.h file:
 * // NB: It's important to mark it inline for linker to deduplicate
 * // addresses accross different UT's.
 * inline constexpr NGlobal::TVariableTag MyGlobalVarTag = {};
 *
 *
 *
 * // some_stuff.cpp file
 *
 * TErasedStorage GetMyVar()
 * {
 * // definition here
 * }
 * NGlobal::Variable<int> MyGlobalVar{MyGlobalVarTag, &GetMyVar};
 *
 *
 *
 * // other_stuff.cpp file
 *
 *
 * int ReadMyVar()
 * {
 *   auto erased = NGlobal::GetErasedVariable(MyGlobalVarTag);
 *   return erased->AsConcrete<int>();
 * }
 */
class TVariableTag
{
public:
    TVariableTag() = default;

    TVariableTag(const TVariableTag& other) = delete;
    TVariableTag& operator=(const TVariableTag& other) = delete;

private:
    friend class ::NYT::NGlobal::NDetail::TGlobalVariablesRegistry;

    mutable std::atomic<bool> Initialized_ = false;
    mutable std::atomic<int> Key_ = -1;
};

////////////////////////////////////////////////////////////////////////////////

// Defined in impl.cpp.
// |std::nullopt| iff global variable with a given tag is not present.
std::optional<TErasedStorage> GetErasedVariable(const TVariableTag& tag);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NGlobal
