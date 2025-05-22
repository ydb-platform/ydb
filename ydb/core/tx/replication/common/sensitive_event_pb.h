#pragma once

#include <ydb/library/actors/core/event_pb.h>
#include <ydb/library/protobuf_printer/security_printer.h>

#include <util/string/builder.h>

namespace NKikimr::NReplication {

template <typename TEv, typename TRecord, ui32 EventType>
struct TSensitiveEventPB: public NActors::TEventPB<TEv, TRecord, EventType> {
    TString ToString() const override {
        return TStringBuilder() << this->ToStringHeader() << " " << SecureDebugString<TRecord>(this->Record);
    }
};

}
