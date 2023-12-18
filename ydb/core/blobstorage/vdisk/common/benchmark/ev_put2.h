#pragma once

#include "google/protobuf/stubs/port.h"
#include "library/cpp/actors/core/event_load.h"
#include "util/generic/vector.h"
#include "util/generic/yexception.h"
#include "util/stream/output.h"
#include "util/system/types.h"
#include "util/system/yassert.h"
#include "ydb/core/protos/base.pb.h"
#include <util/stream/str.h>
#include <ydb/core/protos/blobstorage.pb.h>
#include <ydb/core/base/blobstorage.h>
#include <ydb/core/blobstorage/vdisk/common/benchmark/fast_buf.h>

struct TEvVPut2 : public NActors::TEventBase<TEvVPut2, NKikimr::TEvBlobStorage::EvVPut> {
    struct TRecord : NYdb::NFastBuf::TFastBuf {
        constexpr static ui32 version = 0x56FE'0001;

        void Save(IOutputStream* out) const {
            out->Write(&version, sizeof(version));

            NYdb::NFastBuf::Save(BlobId, out);
            NYdb::NFastBuf::Save(Buffer, out);
            NYdb::NFastBuf::Save(VDiskID, out);
            NYdb::NFastBuf::Save(FullDataSize, out);
            NYdb::NFastBuf::Save(IgnoreBlock, out);
            NYdb::NFastBuf::Save(NotifyIfNotReady, out);
            NYdb::NFastBuf::Save(Cookie, out);
            NYdb::NFastBuf::Save(HandleClass, out);
            NYdb::NFastBuf::Save(MsgQoS, out);
            NYdb::NFastBuf::Save(Timestamps, out);
            NYdb::NFastBuf::Save(ExtraBlockChecks, out);
        }

        void Load(IInputStream *in) {
            ui32 current_version;
            in->LoadOrFail(&current_version, sizeof(version));
            switch (current_version) {
            case 0x56FE'0001: {
                Load0x56FE0001(in);
                return;
            };
            default: {
                throw yexception();
            }
            }
        }

        ui32 SerializedSize() const {
            return sizeof(ui32)
            + NYdb::NFastBuf::SerializedSize(BlobId)
            + NYdb::NFastBuf::SerializedSize(Buffer)
            + NYdb::NFastBuf::SerializedSize(VDiskID)
            + NYdb::NFastBuf::SerializedSize(FullDataSize)
            + NYdb::NFastBuf::SerializedSize(IgnoreBlock)
            + NYdb::NFastBuf::SerializedSize(NotifyIfNotReady)
            + NYdb::NFastBuf::SerializedSize(Cookie)
            + NYdb::NFastBuf::SerializedSize(HandleClass)
            + NYdb::NFastBuf::SerializedSize(MsgQoS)
            + NYdb::NFastBuf::SerializedSize(Timestamps)
            + NYdb::NFastBuf::SerializedSize(ExtraBlockChecks)
            ;
        }

    private:
        void Load0x56FE0001(IInputStream* in) {
            NYdb::NFastBuf::Load(BlobId, in);
            NYdb::NFastBuf::Load(Buffer, in);
            NYdb::NFastBuf::Load(VDiskID, in);
            NYdb::NFastBuf::Load(FullDataSize, in);
            NYdb::NFastBuf::Load(IgnoreBlock, in);
            NYdb::NFastBuf::Load(NotifyIfNotReady, in);
            NYdb::NFastBuf::Load(Cookie, in);
            NYdb::NFastBuf::Load(HandleClass, in);
            NYdb::NFastBuf::Load(MsgQoS, in);
            NYdb::NFastBuf::Load(Timestamps, in);
            NYdb::NFastBuf::Load(ExtraBlockChecks, in);
        }

    private:
        std::optional<::NKikimrProto::TLogoBlobID> BlobId;
        std::optional<TProtoStringType> Buffer;
        std::optional<::NKikimrBlobStorage::TVDiskID> VDiskID;
        std::optional<arc_ui64> FullDataSize;
        std::optional<bool> IgnoreBlock;
        std::optional<bool> NotifyIfNotReady;
        std::optional<arc_ui64> Cookie;
        std::optional<::NKikimrBlobStorage::EPutHandleClass> HandleClass;
        std::optional<::NKikimrBlobStorage::TMsgQoS> MsgQoS;
        std::optional<::NKikimrBlobStorage::TTimestamps> Timestamps;
        TVector<::NKikimrBlobStorage::TEvVPut_TExtraBlockCheck> ExtraBlockChecks;

    public:
        inline bool HasBlobID() const { return BlobId.has_value(); }
        inline void ClearBlobID() { BlobId.reset(); }
        inline const ::NKikimrProto::TLogoBlobID& GetBlobID() const { return *BlobId; }
        inline ::NKikimrProto::TLogoBlobID* MutableBlobID() { return &*BlobId; }

        inline bool HasBuffer() const { return Buffer.has_value(); }
        inline void ClearBuffer() { Buffer.reset(); }
        inline const TProtoStringType& GetBuffer() const { return *Buffer; }
        inline void SetBuffer(const TProtoStringType& value) { Buffer.emplace(value); }
        inline void SetBuffer(TProtoStringType&& value) { Buffer.emplace(std::move(value)); }
        inline void SetBuffer(const char* value) { Buffer.emplace(value); }
        inline void SetBuffer(const void* value, size_t size) { Buffer.emplace(static_cast<const char*>(value), size); }
        inline TProtoStringType* MutableBuffer() { return &*Buffer; }

        inline bool HasVDiskID() const { return VDiskID.has_value(); }
        inline void ClearVDiskID() { VDiskID.reset(); }
        inline const ::NKikimrBlobStorage::TVDiskID& GetVDiskID() const { return *VDiskID; }
        inline ::NKikimrBlobStorage::TVDiskID* MutableVDiskID() { return &*VDiskID; }

        inline bool HasFullDataSize() const { return FullDataSize.has_value(); }
        inline void ClearFullDataSize() { FullDataSize.reset(); }
        inline arc_ui64 GetFullDataSize() const { return *FullDataSize;}
        inline void SetFullDataSize(arc_ui64 value) { FullDataSize.emplace(value); }

        inline bool HasIgnoreBlock() const { return IgnoreBlock.has_value(); }
        inline void ClearIgnoreBlock() { IgnoreBlock.reset(); }
        inline bool GetIgnoreBlock() const { return *IgnoreBlock;}
        inline void SetIgnoreBlock(bool value) { IgnoreBlock.emplace(value); }

        inline bool HasNotifyIfNotReady() const { return NotifyIfNotReady.has_value(); }
        inline void ClearNotifyIfNotReady() { NotifyIfNotReady.reset(); }
        inline bool GetNotifyIfNotReady() const { return *NotifyIfNotReady;}
        inline void SetNotifyIfNotReady(bool value) { NotifyIfNotReady.emplace(value); }

        inline bool HasCookie() const { return Cookie.has_value(); }
        inline void ClearCookie() { Cookie.reset(); }
        inline arc_ui64 GetCookie() const { return *Cookie;}
        inline void SetCookie(arc_ui64 value) { Cookie.emplace(value); }

        inline bool HasHandleClass() const { return HandleClass.has_value(); }
        inline void ClearHandleClass() { HandleClass.reset(); }
        inline ::NKikimrBlobStorage::EPutHandleClass GetHandleClass() const { return *HandleClass; }
        inline void SetHandleClass(::NKikimrBlobStorage::EPutHandleClass value) { HandleClass.emplace(value); }

        inline bool HasMsgQoS() const { return MsgQoS.has_value(); }
        inline void ClearMsgQoS() { MsgQoS.reset(); }
        inline const ::NKikimrBlobStorage::TMsgQoS& GetMsgQoS() const { return *MsgQoS; }
        inline ::NKikimrBlobStorage::TMsgQoS* MutableMsgQoS() { return &*MsgQoS; }

        inline bool HasTimestamps() const { return Timestamps.has_value(); }
        inline void ClearTimestamps() { Timestamps.reset(); }
        inline const ::NKikimrBlobStorage::TTimestamps& GetTimestamps() const { return *Timestamps; }
        inline ::NKikimrBlobStorage::TTimestamps* MutableTimestamps() { return &*Timestamps; }

        inline size_t ExtraBlockChecksSize() const { return ExtraBlockChecks.size(); }
        inline void ClearExtraBlockChecks() { ExtraBlockChecks.clear(); }
        inline const ::NKikimrBlobStorage::TEvVPut_TExtraBlockCheck& GetExtraBlockChecks(size_t _index) const {Y_ASSERT(_index < static_cast<size_t>(::Max<int>())); return ExtraBlockChecks[_index]; }
        inline ::NKikimrBlobStorage::TEvVPut_TExtraBlockCheck* MutableExtraBlockChecks(size_t _index) {Y_ASSERT(_index < static_cast<size_t>(::Max<int>())); return &ExtraBlockChecks[_index]; }
        inline ::NKikimrBlobStorage::TEvVPut_TExtraBlockCheck* AddExtraBlockChecks() { return &ExtraBlockChecks.emplace_back(); }
        inline const auto& GetExtraBlockChecks() const { return ExtraBlockChecks; }
        inline auto* MutableExtraBlockChecks() { return &ExtraBlockChecks; }

    } Record;

    TString ToStringHeader() const override { return TString("VPut2"); }

    ui32 CalculateSerializedSize() const override {
        return Record.SerializedSize();
    }

    bool SerializeToArcadiaStream(NActors::TChunkSerializer* serializer) const override {
        if (serializer == nullptr) {
            return false;
        }
        TStringStream ss;
        NYdb::NFastBuf::Save(Record, &ss);
        ss.Finish();
        auto s = ss.ReadAll();
        Y_ASSERT(!s.Empty());
        serializer->WriteString(&s);
        return serializer->ByteCount();
    }

    static IEventBase* Load(NActors::TEventSerializedData* data) {
        if (data == nullptr) {
            return nullptr;
        }
        auto ptr = new TEvVPut2();
        auto s = data->GetString();
        Y_ASSERT(!s.Empty());
        TStringStream ss;
        ss.Write(s);
        NYdb::NFastBuf::Load(ptr->Record, &ss);
        return ptr;
    }

    bool IsSerializable() const override {
        return true;
    }

    // NActors::TEventSerializationInfo CreateSerializationInfo() const override {
    //     return {};
    // }
};
