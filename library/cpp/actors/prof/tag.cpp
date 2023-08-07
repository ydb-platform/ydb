#include "tag.h"
#include "tcmalloc.h"

#include <library/cpp/charset/ci_string.h>
#include <library/cpp/containers/atomizer/atomizer.h>
#include <library/cpp/malloc/api/malloc.h>

#if defined(PROFILE_MEMORY_ALLOCATIONS)
#include <library/cpp/lfalloc/dbg_info/dbg_info.h>
#include <library/cpp/ytalloc/api/ytalloc.h>
#include <library/cpp/yt/memory/memory_tag.h>
#endif

#include <util/generic/singleton.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/system/mutex.h>
#include <library/cpp/actors/util/local_process_key.h>
#include <library/cpp/actors/actor_type/index_constructor.h>

namespace NProfiling {
    class TStringAtoms {
    private:
        TMutex Mutex;
        atomizer<ci_hash, ci_equal_to> Tags;

    public:
        static TStringAtoms& Instance() {
            return *Singleton<TStringAtoms>();
        }

        ui32 MakeTag(const char* s) {
            Y_VERIFY(s);
            with_lock (Mutex) {
                return Tags.string_to_atom(s);
            }
        }

        ui32 MakeTags(const TVector<const char*>& ss) {
            Y_VERIFY(ss);
            with_lock (Mutex) {
                ui32 baseTag = Tags.string_to_atom(ss[0]);
                ui32 nextTag = baseTag + 1;
                for (auto i = ss.begin() + 1; i != ss.end(); ++i, ++nextTag) {
                    Y_VERIFY(*i);
                    ui32 ctag = Tags.string_to_atom(*i);
                    Y_VERIFY(ctag == nextTag);
                }
                return baseTag;
            }
        }

        const char* GetTag(ui32 tag) const {
            with_lock (Mutex) {
                return Tags.get_atom_name(tag);
            }
        }

        size_t GetTagsCount() const {
            with_lock (Mutex) {
                return Tags.size();
            }
        }
    };

    ui32 MakeTag(const char* s) {
        return TStringAtoms::Instance().MakeTag(s);
    }

    ui32 MakeTags(const TVector<const char*>& ss) {
        return TStringAtoms::Instance().MakeTags(ss);
    }

    const char* GetTag(ui32 tag) {
        return TStringAtoms::Instance().GetTag(tag);
    }

    size_t GetTagsCount() {
        return TStringAtoms::Instance().GetTagsCount();
    }

    static ui32 SetThreadAllocTag_Default(ui32 tag) {
        Y_UNUSED(tag);
        return 0;
    }

#if defined(PROFILE_MEMORY_ALLOCATIONS)
    static ui32 SetThreadAllocTag_YT(ui32 tag) {
        auto prev = NYT::GetCurrentMemoryTag();
        NYT::SetCurrentMemoryTag(tag);
        return prev;
    }

    static TSetThreadAllocTag* SetThreadAllocTagFn() {
        const auto& info = NMalloc::MallocInfo();

        TStringBuf name(info.Name);
        if (name.StartsWith("lf")) {
            return (TSetThreadAllocTag*)NAllocDbg::SetThreadAllocTag;
        } else if (name.StartsWith("yt")) {
            return SetThreadAllocTag_YT;
        } else if (name.StartsWith("tc")) {
            return SetTCMallocThreadAllocTag;
        } else {
            return SetThreadAllocTag_Default;
        }
    }
#else
    static TSetThreadAllocTag* SetThreadAllocTagFn() {
        const auto& info = NMalloc::MallocInfo();

        TStringBuf name(info.Name);
        if (name.StartsWith("tc")) {
            return SetTCMallocThreadAllocTag;
        } else {
            return SetThreadAllocTag_Default;
        }
    }
#endif

    TSetThreadAllocTag* SetThreadAllocTag = SetThreadAllocTagFn();
}

TMemoryProfileGuard::TMemoryProfileGuard(const TString& id)
    : Id(id)
{
    NProfiling::TMemoryTagScope::Reset(TLocalProcessKeyState<NActors::TActorActivityTag>::GetInstance().Register(Id + "-Start"));
}

TMemoryProfileGuard::~TMemoryProfileGuard() {
    NProfiling::TMemoryTagScope::Reset(TLocalProcessKeyState<NActors::TActorActivityTag>::GetInstance().Register(Id + "-Finish"));
}
