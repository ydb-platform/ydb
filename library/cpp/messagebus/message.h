#pragma once

#include "base.h"
#include "local_flags.h"
#include "message_status.h"
#include "netaddr.h"
#include "socket_addr.h"

#include <util/generic/array_ref.h>
#include <util/generic/noncopyable.h>
#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/system/defaults.h>
#include <util/system/type_name.h>
#include <util/system/yassert.h>

#include <optional>
#include <typeinfo>

namespace NBus {
    ///////////////////////////////////////////////////////////////////
    /// \brief Structure to preserve identity from message to reply
    struct TBusIdentity : TNonCopyable {
        friend class TBusMessage;
        friend class NPrivate::TRemoteServerSession;
        friend struct NPrivate::TClientRequestImpl;
        friend class TOnMessageContext;

        // TODO: make private
        TBusKey MessageId;

    private:
        ui32 Size;
        TIntrusivePtr<NPrivate::TRemoteServerConnection> Connection;
        ui16 Flags;
        ui32 LocalFlags;
        TInstant RecvTime;

#ifndef NDEBUG
        std::optional<TString> MessageType;
#endif

    private:
        // TODO: drop
        TNetAddr GetNetAddr() const;

    public:
        void Pack(char* dest);
        void Unpack(const char* src);

        bool IsInWork() const {
            return LocalFlags & NPrivate::MESSAGE_IN_WORK;
        }

        // for internal use only
        void BeginWork() {
            SetInWork(true);
        }

        // for internal use only
        void EndWork() {
            SetInWork(false);
        }

        TBusIdentity();
        ~TBusIdentity();

        void Swap(TBusIdentity& that) {
            DoSwap(MessageId, that.MessageId);
            DoSwap(Size, that.Size);
            DoSwap(Connection, that.Connection);
            DoSwap(Flags, that.Flags);
            DoSwap(LocalFlags, that.LocalFlags);
            DoSwap(RecvTime, that.RecvTime);
#ifndef NDEBUG
            DoSwap(MessageType, that.MessageType);
#endif
        }

        TString ToString() const;

    private:
        void SetInWork(bool inWork) {
            if (LocalFlags == 0 && inWork) {
                LocalFlags = NPrivate::MESSAGE_IN_WORK;
            } else if (LocalFlags == NPrivate::MESSAGE_IN_WORK && !inWork) {
                LocalFlags = 0;
            } else {
                Y_ABORT("impossible combination of flag and parameter: %s %d",
                       inWork ? "true" : "false", unsigned(LocalFlags));
            }
        }

        void SetMessageType(const std::type_info& messageTypeInfo) {
#ifndef NDEBUG
            Y_ABORT_UNLESS(!MessageType, "state check");
            MessageType = TypeName(messageTypeInfo);
#else
            Y_UNUSED(messageTypeInfo);
#endif
        }
    };

    static const size_t BUS_IDENTITY_PACKED_SIZE = sizeof(TBusIdentity);

    ///////////////////////////////////////////////////////////////
    /// \brief Message flags in TBusHeader.Flags
    enum EMessageFlags {
        MESSAGE_COMPRESS_INTERNAL = 0x8000, ///< message is compressed
        MESSAGE_COMPRESS_RESPONSE = 0x4000, ///< message prefers compressed response
        MESSAGE_VERSION_INTERNAL = 0x00F0,  ///< these bits are used as version
    };

//////////////////////////////////////////////////////////
/// \brief Message header present in all message send and received

/// This header is send into the wire.
/// \todo fix for low/high end, 32/64bit some day
#pragma pack(1)
    struct TBusHeader {
        friend class TBusMessage;

        TBusKey Id = 0;           ///< unique message ID
        ui32 Size = 0;            ///< total size of the message
        TBusInstant SendTime = 0; ///< time the message was sent
        ui16 FlagsInternal = 0;   ///< TRACE is one of the flags
        ui16 Type = 0;            ///< to be used by TBusProtocol

        int GetVersionInternal() {
            return (FlagsInternal & MESSAGE_VERSION_INTERNAL) >> 4;
        }
        void SetVersionInternal(unsigned ver = YBUS_VERSION) {
            FlagsInternal |= (ver << 4);
        }

    public:
        TBusHeader() {
        }
        TBusHeader(TArrayRef<const char> data) {
            ReadHeader(data);
        }

    private:
        /// function for serialization/deserialization of the header
        /// returns number of bytes written/read
        int ReadHeader(TArrayRef<const char> data);

        void GenerateId();
    };
#pragma pack()

#define TBUSMAX_MESSAGE 26 * 1024 * 1024 + sizeof(NBus::TBusHeader) ///< is't it enough?
#define TBUSMIN_MESSAGE sizeof(NBus::TBusHeader)                    ///< can't be less then header

    inline bool IsVersionNegotiation(const NBus::TBusHeader& header) {
        return header.Id == 0 && header.Size == sizeof(TBusHeader);
    }

    //////////////////////////////////////////////////////////
    /// \brief Base class for all messages passed in the system

    enum ECreateUninitialized {
        MESSAGE_CREATE_UNINITIALIZED,
    };

    class TBusMessage
       : protected  TBusHeader,
          public TRefCounted<TBusMessage, TAtomicCounter, TDelete>,
          private TNonCopyable {
        friend class TLocalSession;
        friend struct ::NBus::NPrivate::TBusSessionImpl;
        friend class ::NBus::NPrivate::TRemoteServerSession;
        friend class ::NBus::NPrivate::TRemoteClientSession;
        friend class ::NBus::NPrivate::TRemoteConnection;
        friend class ::NBus::NPrivate::TRemoteClientConnection;
        friend class ::NBus::NPrivate::TRemoteServerConnection;
        friend struct ::NBus::NPrivate::TBusMessagePtrAndHeader;

    private:
        ui32 LocalFlags;

        /// connection identity for reply set by PushMessage()
        NPrivate::TBusSocketAddr ReplyTo;
        // server-side response only, hack
        ui32 RequestSize;

        TInstant RecvTime;

    public:
        /// constructor to create messages on sending end
        TBusMessage(ui16 type, int approxsize = sizeof(TBusHeader));

        /// constructor with serialzed data to examine the header
        TBusMessage(ECreateUninitialized);

        // slow, for diagnostics only
        virtual TString Describe() const;

        // must be called if this message object needs to be reused
        void Reset();

        void CheckClean() const;

        void SetCompressed(bool);
        void SetCompressedResponse(bool);

    private:
        bool IsCompressed() const {
            return FlagsInternal & MESSAGE_COMPRESS_INTERNAL;
        }
        bool IsCompressedResponse() const {
            return FlagsInternal & MESSAGE_COMPRESS_RESPONSE;
        }

    public:
        /// can have private data to destroy
        virtual ~TBusMessage();

        /// returns header of the message
        TBusHeader* GetHeader() {
            return this;
        }
        const TBusHeader* GetHeader() const {
            return this;
        }

        /// helper to return type for protocol object to unpack object
        static ui16 GetType(TArrayRef<const char> data) {
            return TBusHeader(data).Type;
        }

        /// returns payload data
        static TArrayRef<const char> GetPayload(TArrayRef<const char> data) {
            return data.Slice(sizeof(TBusHeader));
        }

    private:
        void DoReset();

        /// serialize message identity to be used to construct reply message
        void GetIdentity(TBusIdentity& ident) const;

        /// set message identity from serialized form
        void SetIdentity(const TBusIdentity& ident);

    public:
        TNetAddr GetReplyTo() const {
            return ReplyTo.ToNetAddr();
        }

        /// store of application specific data, never serialized into wire
        void* Data;
    };

    class TBusMessageAutoPtr: public TAutoPtr<TBusMessage> {
    public:
        TBusMessageAutoPtr() {
        }

        TBusMessageAutoPtr(TBusMessage* message)
            : TAutoPtr<TBusMessage>(message)
        {
        }

        template <typename T1>
        TBusMessageAutoPtr(const TAutoPtr<T1>& that)
            : TAutoPtr<TBusMessage>(that.Release())
        {
        }
    };

}
