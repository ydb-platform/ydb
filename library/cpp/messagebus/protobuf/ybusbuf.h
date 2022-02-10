#pragma once

#include <library/cpp/messagebus/ybus.h>

#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>

#include <util/generic/cast.h>
#include <util/generic/vector.h>
#include <util/stream/mem.h>

#include <array>

namespace NBus {
    using TBusBufferRecord = ::google::protobuf::Message;

    template <class TBufferMessage>
    class TBusBufferMessagePtr;
    template <class TBufferMessage>
    class TBusBufferMessageAutoPtr;

    class TBusBufferBase: public TBusMessage {
    public:
        TBusBufferBase(int type)
            : TBusMessage((ui16)type)
        {
        }
        TBusBufferBase(ECreateUninitialized)
            : TBusMessage(MESSAGE_CREATE_UNINITIALIZED)
        {
        }

        ui16 GetType() const {
            return GetHeader()->Type;
        }

        virtual TBusBufferRecord* GetRecord() const = 0;
        virtual TBusBufferBase* New() = 0;
    };

    ///////////////////////////////////////////////////////////////////
    /// \brief Template for all messages that have protobuf description

    /// @param TBufferRecord is record described in .proto file with namespace
    /// @param MessageFile is offset for .proto file message ids

    /// \attention If you want one protocol NBus::TBusBufferProtocol to handle
    /// messageges described in different .proto files, make sure that they have
    /// unique values for MessageFile

    template <class TBufferRecord, int MType>
    class TBusBufferMessage: public TBusBufferBase {
    public:
        static const int MessageType = MType;

        typedef TBusBufferMessagePtr<TBusBufferMessage<TBufferRecord, MType>> TPtr;
        typedef TBusBufferMessageAutoPtr<TBusBufferMessage<TBufferRecord, MType>> TAutoPtr;

    public:
        typedef TBufferRecord RecordType;
        TBufferRecord Record;

    public:
        TBusBufferMessage()
            : TBusBufferBase(MessageType)
        {
        }
        TBusBufferMessage(ECreateUninitialized)
            : TBusBufferBase(MESSAGE_CREATE_UNINITIALIZED)
        {
        }
        explicit TBusBufferMessage(const TBufferRecord& record)
            : TBusBufferBase(MessageType)
            , Record(record)
        {
        }
        explicit TBusBufferMessage(TBufferRecord&& record)
            : TBusBufferBase(MessageType)
            , Record(std::move(record))
        {
        }

    public:
        TBusBufferRecord* GetRecord() const override {
            return (TBusBufferRecord*)&Record;
        }
        TBusBufferBase* New() override {
            return new TBusBufferMessage<TBufferRecord, MessageType>();
        }
    };

    template <class TSelf, class TBufferMessage>
    class TBusBufferMessagePtrBase {
    public:
        typedef typename TBufferMessage::RecordType RecordType;

    private:
        TSelf* GetSelf() {
            return static_cast<TSelf*>(this);
        }
        const TSelf* GetSelf() const {
            return static_cast<const TSelf*>(this);
        }

    public:
        RecordType* operator->() {
            Y_ASSERT(GetSelf()->Get());
            return &(GetSelf()->Get()->Record);
        }
        const RecordType* operator->() const {
            Y_ASSERT(GetSelf()->Get());
            return &(GetSelf()->Get()->Record);
        }
        RecordType& operator*() {
            Y_ASSERT(GetSelf()->Get());
            return GetSelf()->Get()->Record;
        }
        const RecordType& operator*() const {
            Y_ASSERT(GetSelf()->Get());
            return GetSelf()->Get()->Record;
        }

        TBusHeader* GetHeader() {
            return GetSelf()->Get()->GetHeader();
        }
        const TBusHeader* GetHeader() const {
            return GetSelf()->Get()->GetHeader();
        }
    };

    template <class TBufferMessage>
    class TBusBufferMessagePtr: public TBusBufferMessagePtrBase<TBusBufferMessagePtr<TBufferMessage>, TBufferMessage> {
    protected:
        TBufferMessage* Holder;

    public:
        TBusBufferMessagePtr(TBufferMessage* mess)
            : Holder(mess)
        {
        }
        static TBusBufferMessagePtr<TBufferMessage> DynamicCast(TBusMessage* message) {
            return dynamic_cast<TBufferMessage*>(message);
        }
        TBufferMessage* Get() {
            return Holder;
        }
        const TBufferMessage* Get() const {
            return Holder;
        }

        operator TBufferMessage*() {
            return Holder;
        }
        operator const TBufferMessage*() const {
            return Holder;
        }

        operator TAutoPtr<TBusMessage>() {
            TAutoPtr<TBusMessage> r(Holder);
            Holder = 0;
            return r;
        }
        operator TBusMessageAutoPtr() {
            TBusMessageAutoPtr r(Holder);
            Holder = nullptr;
            return r;
        }
    };

    template <class TBufferMessage>
    class TBusBufferMessageAutoPtr: public TBusBufferMessagePtrBase<TBusBufferMessageAutoPtr<TBufferMessage>, TBufferMessage> {
    public:
        TAutoPtr<TBufferMessage> AutoPtr;

    public:
        TBusBufferMessageAutoPtr() {
        }
        TBusBufferMessageAutoPtr(TBufferMessage* message)
            : AutoPtr(message)
        {
        }

        TBufferMessage* Get() {
            return AutoPtr.Get();
        }
        const TBufferMessage* Get() const {
            return AutoPtr.Get();
        }

        TBufferMessage* Release() const {
            return AutoPtr.Release();
        }

        operator TAutoPtr<TBusMessage>() {
            return AutoPtr.Release();
        }
        operator TBusMessageAutoPtr() {
            return AutoPtr.Release();
        }
    };

    /////////////////////////////////////////////
    /// \brief Generic protocol object for messages descibed with protobuf

    /// \attention If you mix messages in the same protocol from more than
    /// .proto file make sure that they have different MessageFile parameter
    ///  in the NBus::TBusBufferMessage template

    class TBusBufferProtocol: public TBusProtocol {
    private:
        TVector<TBusBufferBase*> Types;
        std::array<ui32, ((1 << 16) >> 5)> TypeMask;

        TBusBufferBase* FindType(int type);
        bool IsRegisteredType(unsigned type);

    public:
        TBusBufferProtocol(TBusService name, int port);

        ~TBusBufferProtocol() override;

        /// register all the message that this protocol should handle
        void RegisterType(TAutoPtr<TBusBufferBase> mess);

        TArrayRef<TBusBufferBase* const> GetTypes() const;

        /// serialized protocol specific data into TBusData
        void Serialize(const TBusMessage* mess, TBuffer& data) override;

        TAutoPtr<TBusMessage> Deserialize(ui16 messageType, TArrayRef<const char> payload) override;
    };

}
