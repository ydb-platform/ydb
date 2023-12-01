#pragma once

#include <util/generic/ptr.h>
#include <util/generic/yexception.h>
#include <util/generic/vector.h>
#include <util/generic/set.h>
#include <util/generic/maybe.h>
#include <util/memory/blob.h>
#include <util/stream/length.h>
#include <util/stream/mem.h>

#include "eventlog_int.h"
#include "eventlog.h"
#include "common.h"

class IInputStream;

static const ui64 MAX_REQUEST_DURATION = 60'000'000;
static const ui64 MIN_START_TIME = MAX_REQUEST_DURATION;
static const ui64 MAX_END_TIME = ((ui64)-1) - MAX_REQUEST_DURATION;

class TEventFilter: public TSet<TEventClass>, public TSimpleRefCount<TEventFilter> {
public:
    TEventFilter(bool enableEvents)
        : Enable_(enableEvents)
    {
    }

    void AddEventClass(TEventClass cls) {
        insert(cls);
    }

    bool EventAllowed(TEventClass cls) const {
        bool found = (find(cls) != end());

        return Enable_ == found;
    }

private:
    bool Enable_;
};

using TEventStream = TPacketInputStream<TConstEventPtr>;

struct TFrameHeader {
    // Reads header from the stream. The caller must make sure that the
    // sync data is present just befor the stream position.
    explicit TFrameHeader(IInputStream& in);

    ui64 StartTime() const {
        return Framehdr.StartTimestamp;
    }

    ui64 EndTime() const {
        return Framehdr.EndTimestamp;
    }

    ui32 FrameId() const {
        return Basehdr.FrameId;
    }

    ui64 Duration() const {
        return EndTime() - StartTime();
    }

    TEventLogFormat ContentFormat() const {
        return Basehdr.Format & 0xffffff;
    }

    TEventLogFormat LogFormat() const {
        return Basehdr.Format >> 24;
    }

    ui64 FrameLength() const {
        return Basehdr.Length - sizeof(TCompressedFrameHeader2);
    }

    // Length including the header
    ui64 FullLength() const {
        return sizeof(*this) + FrameLength();
    }

    TCompressedFrameBaseHeader Basehdr;
    TCompressedFrameHeader2 Framehdr;
};

struct TFrameLoadError: public yexception {
    explicit TFrameLoadError(size_t skipAfter)
        : SkipAfter(skipAfter)
    {}

    size_t SkipAfter;
};

class TFrame : public TFrameHeader {
public:
    // Reads the frame after the header has been read.
    TFrame(IInputStream& in, TFrameHeader header, IEventFactory*);

    TString GetRawFrame() const;
    TString GetCompressedFrame() const;

    ui64 Address() const { return Address_; }

private:
    const TConstEventPtr& GetEvent(size_t index) const {
        return EventsCache_[index];
    }

    void ClearEventsCache() const;

    THolder<TLengthLimitedInput> Limiter_;
    mutable TVector<TConstEventPtr> EventsCache_;

    IEventFactory* Fac_;
    ui64 Address_;

    friend class TFrameDecoder;
    friend class TFrameStreamer;

private:
    class TIterator: TEventStream {
    public:
        TIterator(const TFrame& frame, TIntrusiveConstPtr<TEventFilter> filter);
        ~TIterator() override = default;

        bool Avail() const override {
            return Index_ < Size_;
        }

        TConstEventPtr operator*() const override;
        bool Next() override;

    private:
        void SkipToValidEvent();

        const TFrame& Frame_;
        size_t Size_;
        TIntrusiveConstPtr<TEventFilter> Filter_;
        size_t Index_;
    };

public:
    TFrame::TIterator GetIterator(TIntrusiveConstPtr<TEventFilter> eventFilter = nullptr) const;
};

// If `in` is derived from TCountingInput, Frame's address will
// be set accorting to the in->Counter(). Otherwise it will be zeroO
TMaybe<TFrame> FindNextFrame(IInputStream* in, IEventFactory*);

using TFrameStream = TPacketInputStream<const TFrame&>;

class IFrameFilter: public TSimpleRefCount<IFrameFilter> {
public:
    IFrameFilter() {
    }

    virtual ~IFrameFilter() = default;

    virtual bool FrameAllowed(const TFrame& frame) const = 0;
};

using IFrameFilterRef = TIntrusivePtr<IFrameFilter>;

class TDurationFrameFilter: public IFrameFilter {
public:
    TDurationFrameFilter(ui64 minFrameDuration, ui64 maxFrameDuration = Max<ui64>())
        : MinDuration_(minFrameDuration)
        , MaxDuration_(maxFrameDuration)
    {
    }

    bool FrameAllowed(const TFrame& frame) const override {
        return frame.Duration() >= MinDuration_ && frame.Duration() <= MaxDuration_;
    }

private:
    const ui64 MinDuration_;
    const ui64 MaxDuration_;
};

class TFrameIdFrameFilter: public IFrameFilter {
public:
    TFrameIdFrameFilter(ui32 frameId)
        : FrameId_(frameId)
    {
    }

    bool FrameAllowed(const TFrame& frame) const override {
        return frame.FrameId() == FrameId_;
    }

private:
    const ui32 FrameId_;
};

class TContainsEventFrameFilter: public IFrameFilter {
public:
    TContainsEventFrameFilter(const TString& args, const IEventFactory* fac);

    bool FrameAllowed(const TFrame& frame) const override;

private:
    struct TMatchGroup {
        TEventClass EventID;
        TString FieldName;
        TString ValueToMatch;
    };

    TVector<TMatchGroup> MatchGroups;
};

void SplitWithEscaping(TVector<TStringBuf>& tokens, const TStringBuf& stringToSplit, const TStringBuf& externalCharacterSet);

TString UnescapeCharacters(const TStringBuf& stringToUnescape, const TStringBuf& characterSet);

TString GetEventFieldAsString(const NProtoBuf::Message* message, const google::protobuf::FieldDescriptor* fieldDescriptor, const google::protobuf::Reflection* reflection);

class TFrameStreamer: public TFrameStream {
public:
    TFrameStreamer(IInputStream&, IEventFactory* fac, IFrameFilterRef ff = nullptr);
    TFrameStreamer(
            const TString& fileName,
            ui64 startTime,
            ui64 endTime,
            ui64 maxRequestDuration,
            IEventFactory* fac,
            IFrameFilterRef ff = nullptr);
    ~TFrameStreamer() override;

    bool Avail() const override;
    const TFrame& operator*() const override;
    bool Next() override;

private:
    bool DoNext();
    bool AllowedTimeRange(const TFrame& frame) const;

    bool AllowedFrame(const TFrame& frame) const {
        return AllowedTimeRange(frame) && (!FrameFilter_ || FrameFilter_->FrameAllowed(frame));
    }

    void SkipToAllowedFrame() {
        if (Frame_) {
            while (!AllowedFrame(*Frame_) && DoNext()) {
                //do nothing
            }
        }
    }

    TBlob File_;
    TMemoryInput MemoryIn_;
    TCountingInput In_;
    THolder<IInputStream> Stream_;
    ui64 StartTime_ = 0;
    ui64 EndTime_ = 0;
    ui64 CutoffTime_ = 0;
    TMaybe<TFrame> Frame_;
    IFrameFilterRef FrameFilter_;
    IEventFactory* EventFactory_;
};

class TFrameDecoder: TEventStream {
public:
    TFrameDecoder(const TFrame&, const TEventFilter* const filter, bool strict = false, bool withRawData = false);
    ~TFrameDecoder() override;

    bool Avail() const override;

    TConstEventPtr operator*() const override;
    bool Next() override;

    const TStringBuf GetRawEvent() const;

private:
    TFrameDecoder(const TFrameDecoder&);
    void operator=(const TFrameDecoder&);

    inline bool HaveData() const {
        return Event_ != nullptr;
    }

    void Decode();

private:
    const TFrame& Frame_;
    THolder<IInputStream> Decompressor_;
    THolder<TLengthLimitedInput> Limiter_;
    TEventPtr Event_;
    const TEventFilter* const Flt_;
    IEventFactory* Fac_;
    THolder<TEvent> EndOfFrame_;
    bool Strict_;
    TBuffer UncompressedData_;
    TStringBuf RawEventData_;
    bool WithRawData_;
};

class TEventStreamer: public TEventStream {
public:
    TEventStreamer(TFrameStream&, ui64 start, ui64 end, bool strongOrdering, TIntrusivePtr<TEventFilter> filter, bool losslessStrongOrdering = false);
    ~TEventStreamer() override;

    bool Avail() const override;
    TConstEventPtr operator*() const override;
    bool Next() override;

private:
    class TEventBuffer: public TEventStream {
    public:
        void SetCheckpoint();
        void Rollback();
        void Reorder(bool strongOrdering);
        void Append(TConstEventPtr event, bool strongOrdering);

        bool Avail() const override;
        TConstEventPtr operator*() const override;
        bool Next() override;

    private:
        TVector<TConstEventPtr> Buffer_;
        size_t BufLen_ = 0;
        ui64 LastTimestamp_ = 0;
    };

private:
    struct TInvalidEventTimestamps: public yexception {
    };

    bool LoadMoreEvents();
    void TransferEvents(const TFrame&);

private:
    TFrameStream& Frames_;
    TEventBuffer Events_;

    ui64 Start_, End_;
    ui64 MaxEndTimestamp_;
    ui64 Frontier_;
    bool StrongOrdering_;
    bool LosslessStrongOrdering_;
    TIntrusivePtr<TEventFilter> EventFilter_;
};
