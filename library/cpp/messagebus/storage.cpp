#include "storage.h"

#include <typeinfo>

namespace NBus {
    namespace NPrivate {
        TTimedMessages::TTimedMessages() {
        }

        TTimedMessages::~TTimedMessages() {
            Y_ABORT_UNLESS(Items.empty());
        }

        void TTimedMessages::PushBack(TNonDestroyingAutoPtr<TBusMessage> m) {
            TItem i;
            i.Message.Reset(m.Release());
            Items.push_back(i);
        }

        TNonDestroyingAutoPtr<TBusMessage> TTimedMessages::PopFront() {
            TBusMessage* r = nullptr;
            if (!Items.empty()) {
                r = Items.front()->Message.Release();
                Items.pop_front();
            }
            return r;
        }

        bool TTimedMessages::Empty() const {
            return Items.empty();
        }

        size_t TTimedMessages::Size() const {
            return Items.size();
        }

        void TTimedMessages::Timeout(TInstant before, TMessagesPtrs* r) {
            // shortcut
            if (before == TInstant::Max()) {
                Clear(r);
                return;
            }

            while (!Items.empty()) {
                TItem& i = *Items.front();
                if (TInstant::MilliSeconds(i.Message->GetHeader()->SendTime) > before) {
                    break;
                }
                r->push_back(i.Message.Release());
                Items.pop_front();
            }
        }

        void TTimedMessages::Clear(TMessagesPtrs* r) {
            while (!Items.empty()) {
                r->push_back(Items.front()->Message.Release());
                Items.pop_front();
            }
        }

        TSyncAckMessages::TSyncAckMessages() {
            KeyToMessage.set_empty_key(0);
            KeyToMessage.set_deleted_key(1);
        }

        TSyncAckMessages::~TSyncAckMessages() {
            Y_ABORT_UNLESS(KeyToMessage.empty());
            Y_ABORT_UNLESS(TimedItems.empty());
        }

        void TSyncAckMessages::Push(TBusMessagePtrAndHeader& m) {
            // Perform garbage collection if `TimedMessages` contain too many junk data
            if (TimedItems.size() > 1000 && TimedItems.size() > KeyToMessage.size() * 4) {
                Gc();
            }

            TValue value = {m.MessagePtr.Release()};

            std::pair<TKeyToMessage::iterator, bool> p = KeyToMessage.insert(TKeyToMessage::value_type(m.Header.Id, value));
            Y_ABORT_UNLESS(p.second, "non-unique id; %s", value.Message->Describe().data());

            TTimedItem item = {m.Header.Id, m.Header.SendTime};
            TimedItems.push_back(item);
        }

        TBusMessage* TSyncAckMessages::Pop(TBusKey id) {
            TKeyToMessage::iterator it = KeyToMessage.find(id);
            if (it == KeyToMessage.end()) {
                return nullptr;
            }
            TValue v = it->second;
            KeyToMessage.erase(it);

            // `TimedMessages` still contain record about this message

            return v.Message;
        }

        void TSyncAckMessages::Timeout(TInstant before, TMessagesPtrs* r) {
            // shortcut
            if (before == TInstant::Max()) {
                Clear(r);
                return;
            }

            Y_ASSERT(r->empty());

            while (!TimedItems.empty()) {
                TTimedItem i = TimedItems.front();
                if (TInstant::MilliSeconds(i.SendTime) > before) {
                    break;
                }

                TKeyToMessage::iterator itMessage = KeyToMessage.find(i.Key);

                if (itMessage != KeyToMessage.end()) {
                    r->push_back(itMessage->second.Message);
                    KeyToMessage.erase(itMessage);
                }

                TimedItems.pop_front();
            }
        }

        void TSyncAckMessages::Clear(TMessagesPtrs* r) {
            for (TKeyToMessage::const_iterator i = KeyToMessage.begin(); i != KeyToMessage.end(); ++i) {
                r->push_back(i->second.Message);
            }

            KeyToMessage.clear();
            TimedItems.clear();
        }

        void TSyncAckMessages::Gc() {
            TDeque<TTimedItem> tmp;

            for (auto& timedItem : TimedItems) {
                if (KeyToMessage.find(timedItem.Key) == KeyToMessage.end()) {
                    continue;
                }
                tmp.push_back(timedItem);
            }

            TimedItems.swap(tmp);
        }

        void TSyncAckMessages::RemoveAll(const TMessagesPtrs& messages) {
            for (auto message : messages) {
                TKeyToMessage::iterator it = KeyToMessage.find(message->GetHeader()->Id);
                Y_ABORT_UNLESS(it != KeyToMessage.end(), "delete non-existent message");
                KeyToMessage.erase(it);
            }
        }

        void TSyncAckMessages::DumpState() {
            Cerr << TimedItems.size() << Endl;
            Cerr << KeyToMessage.size() << Endl;
        }

    }
}
