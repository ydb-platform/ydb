#include "api.h"

namespace NYT::NYTProf {

////////////////////////////////////////////////////////////////////////////////

DEFINE_REFCOUNTED_TYPE(TProfilerTag)

struct TCpuProfilerTags;

// This variable is referenced from signal handler.
constinit thread_local std::atomic<TCpuProfilerTags*> CpuProfilerTagsPtr = nullptr;

struct TCpuProfilerTags
{
    TCpuProfilerTags()
    {
        CpuProfilerTagsPtr = this;
    }

    ~TCpuProfilerTags()
    {
        CpuProfilerTagsPtr = nullptr;
    }

    std::array<TAtomicSignalPtr<TProfilerTag>, MaxActiveTags> Tags;
};

// We can't reference CpuProfilerTags from signal handler,
// since it may trigger lazy initialization.
thread_local TCpuProfilerTags CpuProfilerTags;

std::array<TAtomicSignalPtr<TProfilerTag>, MaxActiveTags>* GetCpuProfilerTags()
{
    auto tags = CpuProfilerTagsPtr.load();
    if (tags) {
        return &(tags->Tags);
    }

    return nullptr;
}

////////////////////////////////////////////////////////////////////////////////

TCpuProfilerTagGuard::TCpuProfilerTagGuard(TProfilerTagPtr tag)
{
    for (int i = 0; i < MaxActiveTags; i++) {
        if (!CpuProfilerTags.Tags[i].IsSetFromThread()) {
            CpuProfilerTags.Tags[i].StoreFromThread(std::move(tag));
            TagIndex_ = i;
            return;
        }
    }
}

TCpuProfilerTagGuard::~TCpuProfilerTagGuard()
{
    if (TagIndex_ != -1) {
        CpuProfilerTags.Tags[TagIndex_].StoreFromThread(nullptr);
    }
}

TCpuProfilerTagGuard::TCpuProfilerTagGuard(TCpuProfilerTagGuard&& other)
    : TagIndex_(other.TagIndex_)
{
    other.TagIndex_ = -1;
}

TCpuProfilerTagGuard& TCpuProfilerTagGuard::operator = (TCpuProfilerTagGuard&& other)
{
    if (this == &other) {
        return *this;
    }

    if (TagIndex_ != -1) {
        CpuProfilerTags.Tags[TagIndex_].StoreFromThread(nullptr);
    }

    TagIndex_ = other.TagIndex_;
    other.TagIndex_ = -1;
    return *this;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTProf
