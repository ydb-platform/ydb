#pragma once

#include <util/generic/string.h>
#include <util/generic/noncopyable.h>
#include <util/generic/vector.h>

/*
  Common registry for tagging memory profiler.
  Register a new tag with MakeTag using a unique string.
  Use registered tags with SetThreadAllocTag function in allocator API.
*/

namespace NProfiling {
    ui32 MakeTag(const char* s);

    // Make only unique tags. Y_VERIFY inside.
    ui32 MakeTags(const TVector<const char*>& ss);

    const char* GetTag(ui32 tag);
    size_t GetTagsCount();

    using TSetThreadAllocTag = ui32(ui32 tag);
    extern TSetThreadAllocTag* SetThreadAllocTag;

    class TMemoryTagScope {
    public:
        explicit TMemoryTagScope(ui32 tag)
            : RestoreTag(SetThreadAllocTag(tag))
        {
        }

        explicit TMemoryTagScope(const char* tagName) {
            ui32 newTag = MakeTag(tagName);
            RestoreTag = SetThreadAllocTag(newTag);
        }

        TMemoryTagScope(TMemoryTagScope&& move)
            : RestoreTag(move.RestoreTag)
            , Released(move.Released)
        {
            move.Released = true;
        }

        TMemoryTagScope& operator=(TMemoryTagScope&& move) {
            RestoreTag = move.RestoreTag;
            Released = move.Released;
            move.Released = true;
            return *this;
        }

        static void Reset(ui32 tag) {
            SetThreadAllocTag(tag);
        }

        void Release() {
            if (!Released) {
                SetThreadAllocTag(RestoreTag);
                Released = true;
            }
        }

        ~TMemoryTagScope() {
            if (!Released) {
                SetThreadAllocTag(RestoreTag);
            }
        }

    protected:
        TMemoryTagScope(const TMemoryTagScope&) = delete;
        void operator=(const TMemoryTagScope&) = delete;

        ui32 RestoreTag = 0;
        bool Released = false;
    };
}

class TMemoryProfileGuard: TNonCopyable {
private:
    const TString Id;
public:
    TMemoryProfileGuard(const TString& id);
    ~TMemoryProfileGuard();

};
