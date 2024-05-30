#include "api.h"

#include <library/cpp/yt/misc/tls.h>

namespace NYT::NYTProf {

////////////////////////////////////////////////////////////////////////////////

DEFINE_REFCOUNTED_TYPE(TProfilerTag)

struct TCpuProfilerTags;

// This variable is referenced from signal handler.
YT_DEFINE_THREAD_LOCAL(std::atomic<TCpuProfilerTags*>, CpuProfilerTagsPtr, nullptr);

struct TCpuProfilerTags
{
    TCpuProfilerTags()
    {
        CpuProfilerTagsPtr() = this;
    }

    ~TCpuProfilerTags()
    {
        CpuProfilerTagsPtr() = nullptr;
    }

    std::array<TAtomicSignalPtr<TProfilerTag>, MaxActiveTags> Tags;
};

// We can't reference CpuProfilerTags from signal handler,
// since it may trigger lazy initialization.
YT_DEFINE_THREAD_LOCAL(TCpuProfilerTags, CpuProfilerTags);

std::array<TAtomicSignalPtr<TProfilerTag>, MaxActiveTags>* GetCpuProfilerTags()
{
    auto tags = CpuProfilerTagsPtr().load();
    if (tags) {
        return &(tags->Tags);
    }

    return nullptr;
}

////////////////////////////////////////////////////////////////////////////////

TCpuProfilerTagGuard::TCpuProfilerTagGuard(TProfilerTagPtr tag)
{
    auto& cpuProfilerTags = CpuProfilerTags();

    for (int i = 0; i < MaxActiveTags; i++) {
        if (!cpuProfilerTags.Tags[i].IsSetFromThread()) {
            cpuProfilerTags.Tags[i].StoreFromThread(std::move(tag));
            TagIndex_ = i;
            return;
        }
    }
}

TCpuProfilerTagGuard::~TCpuProfilerTagGuard()
{
    auto& cpuProfilerTags = CpuProfilerTags();

    if (TagIndex_ != -1) {
        cpuProfilerTags.Tags[TagIndex_].StoreFromThread(nullptr);
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
        auto& cpuProfilerTags = CpuProfilerTags();
        cpuProfilerTags.Tags[TagIndex_].StoreFromThread(nullptr);
    }

    TagIndex_ = other.TagIndex_;
    other.TagIndex_ = -1;
    return *this;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTProf
