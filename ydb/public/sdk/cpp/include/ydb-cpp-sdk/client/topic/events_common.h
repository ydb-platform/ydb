#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/status/status.h>

#include <util/generic/ptr.h>
#include <util/string/builder.h>

namespace NYdb::inline Dev::NTopic {

template <typename TEvent>
class TPrintable {
public:
    std::string DebugString(bool printData = false) const {
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
    std::unordered_map<std::string, std::string> Fields;
};

struct TMessageMeta: public TThrRefBase {
    using TPtr = TIntrusivePtr<TMessageMeta>;

    //! User defined fields.
    std::vector<std::pair<std::string, std::string>> Fields;
};

//! Event that is sent to client during session destruction.
struct TSessionClosedEvent: public TStatus, public TPrintable<TSessionClosedEvent> {
    using TStatus::TStatus;
};

template<>
void TPrintable<TSessionClosedEvent>::DebugString(TStringBuilder& res, bool) const;

using TSessionClosedHandler = std::function<void(const TSessionClosedEvent&)>;

}
