#include "trace_data.h"

#include <util/generic/buffer.h>
#include <util/generic/hash.h>
#include <util/generic/strbuf.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NActors::NTracing {
    namespace {

        void Serialize(ui32 val, TBuffer& buffer) {
            buffer.Append(reinterpret_cast<const char*>(&val), sizeof(val));
        }

        void Serialize(const TStringBuf& str, TBuffer& buffer) {
            auto size = str.size();
            buffer.Append(reinterpret_cast<const char*>(&size), sizeof(size));
            buffer.Append(str.data(), str.size());
        }

        void Serialize(const TString& str, TBuffer& buffer) {
            Serialize(static_cast<const TStringBuf>(str), buffer);
        }

        template <typename T>
        void Serialize(const TVector<T>& container, TBuffer& buffer) {
            auto size = container.size();
            buffer.Append(reinterpret_cast<const char*>(&size), sizeof(size));
            for (const auto& element: container) {
                Serialize(element, buffer);
            }
        }

        template <typename K, typename V>
        void Serialize(const THashMap<K, V>& container, TBuffer& buffer) {
            auto size = container.size();
            buffer.Append(reinterpret_cast<const char*>(&size), sizeof(size));
            for (const auto& [k, v]: container) {
                Serialize(k, buffer);
                Serialize(v, buffer);
            }
        }
    }

    TBuffer SerializeHeader(TActivityDict&& activityDict, TEventNamesDict&& eventNamesDict) {
        TBuffer res;
        Serialize(activityDict, res);
        Serialize(eventNamesDict, res);
        return res;
    }

    TBuffer SerializeEvents(TTraceChunk::TEvents&& events) {
        TBuffer res;
#define WRITE_TO_BUFF(buff, MEMBER_NAME) \
        buff.Append(reinterpret_cast<const char*>(&item.MEMBER_NAME), sizeof(item.MEMBER_NAME));

        auto writer = [&res](auto&& item) {
            WRITE_TO_BUFF(res, Timestamp);
            WRITE_TO_BUFF(res, Type);
            switch (item.Type) {
                case TEvent::EType::SendInterconnect:
                    WRITE_TO_BUFF(res, Event.SendInterconnectEvent);
                    break;
                case TEvent::EType::RecieveInterconnect:
                    WRITE_TO_BUFF(res, Event.RecieveInterconnectEvent);
                    break;
                case TEvent::EType::SendLocal:
                    WRITE_TO_BUFF(res, Event.SendLocalEvent);
                    break;
                case TEvent::EType::RecieveLocal:
                    WRITE_TO_BUFF(res, Event.RecieveLocalEvent);
                    break;
                case TEvent::EType::New:
                    WRITE_TO_BUFF(res, Event.NewEvent);
                    break;
                case TEvent::EType::Die:
                    WRITE_TO_BUFF(res, Event.DieEvent);
                    break;
            }
        };
#undef WRITE_TO_BUFF
        for (auto&& event: events) {
            writer(std::move(event));
        }
        return res;
    }
}
