#include "remote_server_connection.h"
#include "ybus.h"

#include <util/random/random.h>
#include <util/string/printf.h>
#include <library/cpp/deprecated/atomic/atomic.h>

#include <string.h>

using namespace NBus;

namespace NBus {
    using namespace NBus::NPrivate;

    TBusIdentity::TBusIdentity()
        : MessageId(0)
        , Size(0)
        , Flags(0)
        , LocalFlags(0)
    {
    }

    TBusIdentity::~TBusIdentity() {
    // TODO: print local flags
#ifndef NDEBUG
        Y_ABORT_UNLESS(LocalFlags == 0, "local flags must be zero at this point; message type is %s",
                 MessageType.value_or("unknown").c_str());
#else
        Y_ABORT_UNLESS(LocalFlags == 0, "local flags must be zero at this point");
#endif
    }

    TNetAddr TBusIdentity::GetNetAddr() const {
        if (!!Connection) {
            return Connection->GetAddr();
        } else {
            Y_ABORT();
        }
    }

    void TBusIdentity::Pack(char* dest) {
        memcpy(dest, this, sizeof(TBusIdentity));
        LocalFlags = 0;

        // prevent decref
        new (&Connection) TIntrusivePtr<TRemoteServerConnection>;
    }

    void TBusIdentity::Unpack(const char* src) {
        Y_ABORT_UNLESS(LocalFlags == 0);
        Y_ABORT_UNLESS(!Connection);

        memcpy(this, src, sizeof(TBusIdentity));
    }

    void TBusHeader::GenerateId() {
        for (;;) {
            Id = RandomNumber<TBusKey>();
            // Skip reserved ids
            if (IsBusKeyValid(Id))
                return;
        }
    }

    TBusMessage::TBusMessage(ui16 type, int approxsize)
        //: TCtr("BusMessage")
        : TRefCounted<TBusMessage, TAtomicCounter, TDelete>(1)
        , LocalFlags(0)
        , RequestSize(0)
        , Data(nullptr)
    {
        Y_UNUSED(approxsize);
        GetHeader()->Type = type;
        DoReset();
    }

    TBusMessage::TBusMessage(ECreateUninitialized)
        //: TCtr("BusMessage")
        : TRefCounted<TBusMessage, TAtomicCounter, TDelete>(1)
        , LocalFlags(0)
        , Data(nullptr)
    {
    }

    TString TBusMessage::Describe() const {
        return Sprintf("object type: %s, message type: %d", TypeName(*this).data(), int(GetHeader()->Type));
    }

    TBusMessage::~TBusMessage() {
#ifndef NDEBUG
        Y_ABORT_UNLESS(GetHeader()->Id != YBUS_KEYINVALID, "must not be invalid key, message type: %d, ", int(Type));
        GetHeader()->Id = YBUS_KEYINVALID;
        Data = (void*)17;
        CheckClean();
#endif
    }

    void TBusMessage::DoReset() {
        GetHeader()->SendTime = 0;
        GetHeader()->Size = 0;
        GetHeader()->FlagsInternal = 0;
        GetHeader()->GenerateId();
        GetHeader()->SetVersionInternal();
    }

    void TBusMessage::Reset() {
        CheckClean();
        DoReset();
    }

    void TBusMessage::CheckClean() const {
        if (Y_UNLIKELY(LocalFlags != 0)) {
            TString describe = Describe();
            TString localFlags = LocalFlagSetToString(LocalFlags);
            Y_ABORT("message local flags must be zero, got: %s, message: %s", localFlags.data(), describe.data());
        }
    }

    ///////////////////////////////////////////////////////
    /// \brief Unpacks header from network order

    /// \todo ntoh instead of memcpy
    int TBusHeader::ReadHeader(TArrayRef<const char> data) {
        Y_ASSERT(data.size() >= sizeof(TBusHeader));
        memcpy(this, data.data(), sizeof(TBusHeader));
        return sizeof(TBusHeader);
    }

    ///////////////////////////////////////////////////////
    /// \brief Packs header to network order

    //////////////////////////////////////////////////////////
    /// \brief serialize message identity to be used to construct reply message

    /// function stores messageid, flags and connection reply address into the buffer
    /// that can later be used to construct a reply to the message
    void TBusMessage::GetIdentity(TBusIdentity& data) const {
        data.MessageId = GetHeader()->Id;
        data.Size = GetHeader()->Size;
        data.Flags = GetHeader()->FlagsInternal;
        //data.LocalFlags = LocalFlags;
    }

    ////////////////////////////////////////////////////////////
    /// \brief set message identity from serialized form

    /// function restores messageid, flags and connection reply address from the buffer
    /// into the reply message
    void TBusMessage::SetIdentity(const TBusIdentity& data) {
        // TODO: wrong assertion: YBUS_KEYMIN is 0
        Y_ASSERT(data.MessageId != 0);
        bool compressed = IsCompressed();
        GetHeader()->Id = data.MessageId;
        GetHeader()->FlagsInternal = data.Flags;
        LocalFlags = data.LocalFlags & ~MESSAGE_IN_WORK;
        ReplyTo = data.Connection->PeerAddrSocketAddr;
        SetCompressed(compressed || IsCompressedResponse());
    }

    void TBusMessage::SetCompressed(bool v) {
        if (v) {
            GetHeader()->FlagsInternal |= MESSAGE_COMPRESS_INTERNAL;
        } else {
            GetHeader()->FlagsInternal &= ~(MESSAGE_COMPRESS_INTERNAL);
        }
    }

    void TBusMessage::SetCompressedResponse(bool v) {
        if (v) {
            GetHeader()->FlagsInternal |= MESSAGE_COMPRESS_RESPONSE;
        } else {
            GetHeader()->FlagsInternal &= ~(MESSAGE_COMPRESS_RESPONSE);
        }
    }

    TString TBusIdentity::ToString() const {
        TStringStream ss;
        ss << "msg-id=" << MessageId
           << " size=" << Size;
        if (!!Connection) {
            ss << " conn=" << Connection->GetAddr();
        }
        ss
            << " flags=" << Flags
            << " local-flags=" << LocalFlags
#ifndef NDEBUG
            << " msg-type= " << MessageType.value_or("unknown").c_str()
#endif
            ;
        return ss.Str();
    }

}

template <>
void Out<TBusIdentity>(IOutputStream& os, TTypeTraits<TBusIdentity>::TFuncParam ident) {
    os << ident.ToString();
}
