#pragma once

#include <util/stream/input.h>
#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/generic/iterator.h>

#include "eventlog.h"
#include "logparser.h"

namespace NEventLog {
    struct TOptions {
        inline TOptions& SetFileName(const TString& fileName) {
            FileName = fileName;

            return *this;
        }

        inline TOptions& SetForceStrongOrdering(bool v) {
            if(!ForceLosslessStrongOrdering) {
                ForceStrongOrdering = v;
            }

            return *this;
        }

        ui64 StartTime = MIN_START_TIME;
        ui64 EndTime = MAX_END_TIME;
        ui64 MaxRequestDuration = MAX_REQUEST_DURATION;
        TString FileName;
        bool ForceStrongOrdering = false;
        bool ForceWeakOrdering = false;
        bool EnableEvents = true;
        TString EvList;
        bool ForceStreamMode = false;
        bool ForceLosslessStrongOrdering = false;
        bool TailFMode = false;
        IInputStream* Input = &Cin;
        IFrameFilterRef FrameFilter;
    };

    class IIterator: public TInputRangeAdaptor<IIterator> {
    public:
        virtual ~IIterator();

        virtual TConstEventPtr Next() = 0;
    };

    THolder<IIterator> CreateIterator(const TOptions& o);
    THolder<IIterator> CreateIterator(const TOptions& o, IEventFactory* fac);
}
