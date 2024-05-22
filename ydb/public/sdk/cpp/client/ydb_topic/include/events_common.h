#pragma once

#include <ydb/public/sdk/cpp/client/ydb_types/status/status.h>

#include <util/generic/hash.h>
#include <util/generic/ptr.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>

namespace NYdb::NTopic {

template <typename TEvent>
class TPrintable {
public:
    TString DebugString(bool printData = false) const {
        TStringBuilder b;
        static_cast<const TEvent*>(this)->DebugString(b, printData);
        return b;
    }

    // implemented in template specializations
    void DebugString(TStringBuilder& ret, bool printData = false) const = delete;
};

//! Session metainformation.
struct TWriteSessionMeta: public TThrRefBase {
    using TPtr = TIntrusivePtr<TWriteSessionMeta>;

    //! User defined fields.
    THashMap<TString, TString> Fields;
};

struct TMessageMeta: public TThrRefBase {
    using TPtr = TIntrusivePtr<TMessageMeta>;

    //! User defined fields.
    TVector<std::pair<TString, TString>> Fields;
};

//! Event that is sent to client during session destruction.
struct TSessionClosedEvent: public TStatus, public TPrintable<TSessionClosedEvent> {
    using TStatus::TStatus;
};

template<>
void TPrintable<TSessionClosedEvent>::DebugString(TStringBuilder& ret, bool) const;

using TSessionClosedHandler = std::function<void(const TSessionClosedEvent&)>;

}
