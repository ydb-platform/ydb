#pragma once

#include <library/cpp/actors/util/rope.h>

namespace NKikimr::NEvWrite {

class IPayloadData {
public:
    virtual TString GetDataFromPayload(const ui64 index) const = 0;
    virtual ui64 AddDataToPayload(TString&& blobData) = 0;
    virtual ~IPayloadData() {
    }
};

template <class TEvent>
class TPayloadHelper: public IPayloadData {
    TEvent& Event;

public:
    TPayloadHelper(TEvent& ev)
        : Event(ev)
    {
    }

    TString GetDataFromPayload(const ui64 index) const override {
        TRope rope = Event.GetPayload(index);
        TString data = TString::Uninitialized(rope.GetSize());
        rope.Begin().ExtractPlainDataAndAdvance(data.Detach(), data.size());
        return data;
    }

    ui64 AddDataToPayload(TString&& blobData) override {
        TRope rope;
        rope.Insert(rope.End(), TRope(blobData));
        return Event.AddPayload(std::move(rope));
    }
};

}
