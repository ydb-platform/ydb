#include "iterator.h"

#include <library/cpp/streams/growing_file_input/growing_file_input.h>

#include <util/string/cast.h>
#include <util/string/split.h>
#include <util/string/type.h>
#include <util/stream/file.h>

using namespace NEventLog;

namespace {
    inline TIntrusivePtr<TEventFilter> ConstructEventFilter(bool enableEvents, const TString& evList, IEventFactory* fac) {
        if (evList.empty()) {
            return nullptr;
        }

        TVector<TString> events;

        StringSplitter(evList).Split(',').SkipEmpty().Collect(&events);
        if (events.empty()) {
            return nullptr;
        }

        TIntrusivePtr<TEventFilter> filter(new TEventFilter(enableEvents));

        for (const auto& event : events) {
            if (IsNumber(event))
                filter->AddEventClass(FromString<size_t>(event));
            else
                filter->AddEventClass(fac->ClassByName(event));
        }

        return filter;
    }

    struct TIterator: public IIterator {
        inline TIterator(const TOptions& o, IEventFactory* fac)
            : First(true)
        {
            if (o.FileName.size()) {
                if (o.ForceStreamMode || o.TailFMode) {
                    FileInput.Reset(o.TailFMode ? (IInputStream*)new TGrowingFileInput(o.FileName) : (IInputStream*)new TUnbufferedFileInput(o.FileName));
                    FrameStream.Reset(new TFrameStreamer(*FileInput, fac, o.FrameFilter));
                } else {
                    FrameStream.Reset(new TFrameStreamer(o.FileName, o.StartTime, o.EndTime, o.MaxRequestDuration, fac, o.FrameFilter));
                }
            } else {
                FrameStream.Reset(new TFrameStreamer(*o.Input, fac, o.FrameFilter));
            }

            EvFilter = ConstructEventFilter(o.EnableEvents, o.EvList, fac);
            EventStream.Reset(new TEventStreamer(*FrameStream, o.StartTime, o.EndTime, o.ForceStrongOrdering, EvFilter, o.ForceLosslessStrongOrdering));
        }

        TConstEventPtr Next() override {
            if (First) {
                First = false;

                if (!EventStream->Avail()) {
                    return nullptr;
                }
            } else {
                if (!EventStream->Next()) {
                    return nullptr;
                }
            }

            return **EventStream;
        }

        THolder<IInputStream> FileInput;
        THolder<TFrameStreamer> FrameStream;
        TIntrusivePtr<TEventFilter> EvFilter;
        THolder<TEventStreamer> EventStream;
        bool First;
    };
}

IIterator::~IIterator() = default;

THolder<IIterator> NEventLog::CreateIterator(const TOptions& o, IEventFactory* fac) {
    return MakeHolder<TIterator>(o, fac);
}

THolder<IIterator> NEventLog::CreateIterator(const TOptions& o) {
    return MakeHolder<TIterator>(o, NEvClass::Factory());
}
