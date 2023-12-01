#pragma once

#include "event_field_output.h"

#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>

#include <library/cpp/threading/atomic/bool.h>
#include <library/cpp/string_utils/base64/base64.h>

#include <util/generic/map.h>
#include <util/generic/deque.h>
#include <util/generic/singleton.h>
#include <util/string/hex.h>
#include <util/system/guard.h>
#include <util/system/mutex.h>

namespace NProtoBuf {
    class TEventFactory {
    public:
        typedef ::google::protobuf::Message Message;
        typedef void (*TEventSerializer)(const Message* event, IOutputStream& output, EFieldOutputFlags flags);
        typedef void (*TRegistrationFunc)();

    private:
        class TFactoryItem {
        public:
            TFactoryItem(const Message* prototype, const TEventSerializer serializer)
                : Prototype_(prototype)
                , Serializer_(serializer)
            {
            }

            TStringBuf GetName() const {
                return Prototype_->GetDescriptor()->name();
            }

            Message* Create() const {
                return Prototype_->New();
            }

            void PrintEvent(const Message* event, IOutputStream& out, EFieldOutputFlags flags) const {
                (*Serializer_)(event, out, flags);
            }

        private:
            const Message* Prototype_;
            const TEventSerializer Serializer_;
        };

        typedef TMap<size_t, TFactoryItem> TFactoryMap;

    public:
        TEventFactory()
            : FactoryItems_()
        {
        }

        void ScheduleRegistration(TRegistrationFunc func) {
            EventRegistrators_.push_back(func);
        }

        void RegisterEvent(size_t eventId, const Message* prototype, const TEventSerializer serializer) {
            FactoryItems_.insert(std::make_pair(eventId, TFactoryItem(prototype, serializer)));
        }

        size_t IdByName(TStringBuf eventname) {
            DelayedRegistration();
            for (TFactoryMap::const_iterator it = FactoryItems_.begin(); it != FactoryItems_.end(); ++it) {
                if (it->second.GetName() == eventname)
                    return it->first;
            }

            ythrow yexception() << "do not know event '" << eventname << "'";
        }

        TStringBuf NameById(size_t id) {
            DelayedRegistration();
            TFactoryMap::const_iterator it = FactoryItems_.find(id);
            return it != FactoryItems_.end() ? it->second.GetName() : TStringBuf();
        }

        Message* CreateEvent(size_t eventId) {
            DelayedRegistration();
            TFactoryMap::const_iterator it = FactoryItems_.find(eventId);

            if (it != FactoryItems_.end()) {
                return it->second.Create();
            }

            return nullptr;
        }

        const TMap<size_t, TFactoryItem>& FactoryItems() {
            DelayedRegistration();
            return FactoryItems_;
        }

        void PrintEvent(
            size_t eventId,
            const Message* event,
            IOutputStream& output,
            EFieldOutputFlags flags = {}) {
            DelayedRegistration();
            TFactoryMap::const_iterator it = FactoryItems_.find(eventId);

            if (it != FactoryItems_.end()) {
                it->second.PrintEvent(event, output, flags);
            }
        }

        static TEventFactory* Instance() {
            return Singleton<TEventFactory>();
        }

    private:
        void DelayedRegistration() {
            if (!DelayedRegistrationDone_) {
                TGuard<TMutex> guard(MutexEventRegistrators_);
                Y_UNUSED(guard);
                while (!EventRegistrators_.empty()) {
                    EventRegistrators_.front()();
                    EventRegistrators_.pop_front();
                }
                DelayedRegistrationDone_ = true;
            }
        }

    private:
        TMap<size_t, TFactoryItem> FactoryItems_;
        TDeque<TRegistrationFunc> EventRegistrators_;
        NAtomic::TBool DelayedRegistrationDone_ = false;
        TMutex MutexEventRegistrators_;
    };

    template <typename T>
    void PrintAsBytes(const T& obj, IOutputStream& output) {
        const ui8* b = reinterpret_cast<const ui8*>(&obj);
        const ui8* e = b + sizeof(T);
        const char* delim = "";

        while (b != e) {
            output << delim;
            output << (int)*b++;
            delim = ".";
        }
    }

    template <typename T>
    void PrintAsHex(const T& obj, IOutputStream& output) {
        output << "0x";
        output << HexEncode(&obj, sizeof(T));
    }

    inline void PrintAsBase64(TStringBuf data, IOutputStream& output) {
        if (!data.empty()) {
            output << Base64Encode(data);
        }
    }

}
