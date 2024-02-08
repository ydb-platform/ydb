#pragma once

#include <ydb/library/actors/util/rope.h>

namespace NKikimr::NEvWrite {

class IPayloadReader {
public:
    virtual TString GetDataFromPayload(const ui64 index) const = 0;
    virtual ~IPayloadReader() {
    }
};

class IPayloadWriter {
public:
    virtual ui64 AddDataToPayload(TString&& blobData) = 0;
    virtual ~IPayloadWriter() {
    }
};

template <class TEvent>
class TPayloadReader: public IPayloadReader {
    const TEvent& Event;

public:
    TPayloadReader(const TEvent& ev)
        : Event(ev)
    {
    }

    TString GetDataFromPayload(const ui64 index) const override {
        TRope rope = Event.GetPayload(index);
        TString data = TString::Uninitialized(rope.GetSize());
        rope.Begin().ExtractPlainDataAndAdvance(data.Detach(), data.size());
        return data;
    }
};

template <class TEvent>
class TPayloadWriter: public IPayloadWriter {
    TEvent& Event;

public:
    TPayloadWriter(TEvent& ev)
        : Event(ev)
    {
    }

    ui64 AddDataToPayload(TString&& blobData) override {
        TRope rope;
        rope.Insert(rope.End(), TRope(blobData));
        return Event.AddPayload(std::move(rope));
    }
};
}
