#pragma once
#include "defs.h"
#include "vdisk_events.h"

#include <ydb/core/blobstorage/base/blobstorage_syncstate.h>
#include <ydb/core/blobstorage/base/utility.h>
#include <ydb/core/blobstorage/vdisk/common/disk_part.h>
#include <ydb/core/base/blobstorage.h>
#include <ydb/core/base/event_filter.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/events.h>

#include <util/digest/murmur.h>
#include <util/generic/algorithm.h>
#include <util/stream/mem.h>
#include <util/system/byteorder.h>

namespace NKikimr {

    struct TEvBlobStorage::TEvVPutBinary
        : public TEventBase<TEvBlobStorage::TEvVPutBinary, TEvBlobStorage::EvVPutBinary>
        , TEventWithRelevanceTracker
    {
        // Constants for binary format identification
        static constexpr ui32 MAGIC_NUMBER = 0x565042; // "VPB" in ASCII
        static constexpr ui8 VERSION = 1;

        TLogoBlobID BlobId;
        TString Buffer;
        TVDiskID VDiskId;
        bool IgnoreBlock = false;
        ui64 Cookie = 0;
        TInstant Deadline = TInstant::Max();
        NKikimrBlobStorage::EPutHandleClass HandleClass = NKikimrBlobStorage::TabletLog;
        mutable NLWTrace::TOrbit Orbit; // No active usage
        bool RewriteBlob = false;
        bool IsInternal = false;
        std::vector<std::pair<ui64, ui32>> ExtraBlockChecks; // (TabletId, Generation) pairs
        bool HasCookie = false;

        TEvVPutBinary() = default;

        // Новый конструктор для создания напрямую без использования TEvVPut
        TEvVPutBinary(
            const TLogoBlobID& blobId,
            const TString& buffer,
            const TVDiskID& vdiskId,
            bool ignoreBlock = false,
            ui64* cookie = nullptr,
            TInstant deadline = TInstant::Max(),
            NKikimrBlobStorage::EPutHandleClass handleClass = NKikimrBlobStorage::TabletLog,
            bool rewriteBlob = false,
            bool isInternal = false
        ) {
            BlobId = blobId;
            Buffer = buffer;
            VDiskId = vdiskId;
            IgnoreBlock = ignoreBlock;
            HasCookie = cookie != nullptr;
            if (HasCookie) {
                Cookie = *cookie;
            }
            Deadline = deadline;
            HandleClass = handleClass;
            RewriteBlob = rewriteBlob;
            IsInternal = isInternal;
        }

        TEvVPutBinary(const TEvVPut& original) {
            BlobId = LogoBlobIDFromLogoBlobID(original.Record.GetBlobID());
            VDiskId = VDiskIDFromVDiskID(original.Record.GetVDiskID());
            Buffer = original.Record.GetBuffer();
            IgnoreBlock = original.GetIgnoreBlock();
            HasCookie = original.Record.HasCookie();
            if (HasCookie) {
                Cookie = original.Record.GetCookie();
            }
            
            if (original.Record.GetMsgQoS().HasDeadlineSeconds()) {
                Deadline = TInstant::Seconds(original.Record.GetMsgQoS().GetDeadlineSeconds());
            }
            
            HandleClass = original.Record.GetHandleClass();
            
            for (const auto& check : original.Record.GetExtraBlockChecks()) {
                ExtraBlockChecks.emplace_back(check.GetTabletId(), check.GetGeneration());
            }
            
            // Копируем дополнительные флаги
            RewriteBlob = original.RewriteBlob;
            IsInternal = original.IsInternal;
        }

        TEvVPut* ToOriginal() const {
            auto result = new TEvVPut();
            
            LogoBlobIDFromLogoBlobID(BlobId, result->Record.MutableBlobID());
            result->Record.SetFullDataSize(BlobId.BlobSize());
            VDiskIDFromVDiskID(VDiskId, result->Record.MutableVDiskID());
            
            if (IgnoreBlock) {
                result->Record.SetIgnoreBlock(IgnoreBlock);
            }
            
            if (HasCookie) {
                result->Record.SetCookie(Cookie);
            }
            
            if (Deadline != TInstant::Max()) {
                result->Record.MutableMsgQoS()->SetDeadlineSeconds((ui32)Deadline.Seconds());
            }
            
            result->Record.SetHandleClass(HandleClass);
            result->Record.MutableMsgQoS()->SetExtQueueId(HandleClassToQueueId(HandleClass));
            
            for (const auto& check : ExtraBlockChecks) {
                auto* checkProto = result->Record.AddExtraBlockChecks();
                checkProto->SetTabletId(check.first);
                checkProto->SetGeneration(check.second);
            }
            
            result->Record.SetBuffer(Buffer);
            
            // Устанавливаем дополнительные флаги
            result->RewriteBlob = RewriteBlob;
            result->IsInternal = IsInternal;
            
            return result;
        }

        void Save(TEventSerializedData* data) const {
            TString serialized;
            Serialize(*this, serialized);
            
            data->Append(serialized);
        }
        
        static IEventBase* Load(TEventSerializedData* data) {
            if (data->GetSize() == 0) {
                return nullptr;
            }
            
            TString serialized = data->GetString();
            return Deserialize(serialized);
        }
        
        static void Serialize(const TEvVPutBinary& msg, TString& out) {
            ui64 size = EstimateSerializeSize(msg);
            out.resize(size);
            TMemoryOutput mo(out.begin(), size);
            
            WriteToStream(mo, HostToLittle(MAGIC_NUMBER));
            WriteToStream(mo, VERSION);
            
            const TLogoBlobID& blobId = msg.BlobId;
            WriteToStream(mo, HostToLittle(blobId.TabletID()));
            WriteToStream(mo, HostToLittle(blobId.Channel()));
            WriteToStream(mo, HostToLittle(blobId.Generation()));
            WriteToStream(mo, HostToLittle(blobId.Step()));
            WriteToStream(mo, HostToLittle(blobId.BlobSize()));
            WriteToStream(mo, HostToLittle(blobId.PartId()));
            
            const TVDiskID& vdiskId = msg.VDiskId;
            WriteToStream(mo, HostToLittle(vdiskId.GroupID.GetRawId()));
            WriteToStream(mo, HostToLittle(vdiskId.GroupGeneration));
            WriteToStream(mo, HostToLittle(vdiskId.FailRealm));
            WriteToStream(mo, HostToLittle(vdiskId.FailDomain));
            WriteToStream(mo, HostToLittle(vdiskId.VDisk));
            
            ui8 flags = 0;
            if (msg.IgnoreBlock) flags |= 1;
            if (msg.HasCookie) flags |= 2;
            if (msg.RewriteBlob) flags |= 4;
            if (msg.IsInternal) flags |= 8;
            if (msg.Deadline != TInstant::Max()) flags |= 16;
            WriteToStream(mo, flags);
            
            if (msg.HasCookie) {
                WriteToStream(mo, HostToLittle(msg.Cookie));
            }
            
            if (msg.Deadline != TInstant::Max()) {
                WriteToStream(mo, HostToLittle((ui64)msg.Deadline.MilliSeconds()));
            }
            
            WriteToStream(mo, HostToLittle((ui32)msg.HandleClass));
            
            WriteToStream(mo, HostToLittle((ui32)msg.ExtraBlockChecks.size()));
            for (const auto& check : msg.ExtraBlockChecks) {
                WriteToStream(mo, HostToLittle(check.first));  // TabletId
                WriteToStream(mo, HostToLittle(check.second)); // Generation
            }
            
            WriteToStream(mo, HostToLittle((ui64)msg.Buffer.size()));
            if (!msg.Buffer.empty()) {
                mo.Write(msg.Buffer.data(), msg.Buffer.size());
            }
        }
        
        static TEvVPutBinary* Deserialize(TStringBuf data) {
            try {
                TMemoryInput mi(data.data(), data.size());
                
                ui32 magic;
                ReadFromStream(mi, magic);
                magic = LittleToHost(magic);
                if (magic != MAGIC_NUMBER) {
                    return nullptr;
                }
                
                ui8 version;
                ReadFromStream(mi, version);
                if (version != VERSION) {
                    return nullptr;
                }
                
                THolder<TEvVPutBinary> result = MakeHolder<TEvVPutBinary>();
                
                ui64 tabletId;
                ui32 channel;
                ui32 generation;
                ui32 step;
                ui32 blobSize;
                ui8 partId;
                
                ReadFromStream(mi, tabletId);
                ReadFromStream(mi, channel);
                ReadFromStream(mi, generation);
                ReadFromStream(mi, step);
                ReadFromStream(mi, blobSize);
                ReadFromStream(mi, partId);
                
                result->BlobId = TLogoBlobID(
                    LittleToHost(tabletId),
                    LittleToHost(generation),
                    LittleToHost(step),
                    LittleToHost(channel),
                    LittleToHost(blobSize),
                    LittleToHost(partId)
                );
                
                ui32 groupId;
                ui32 groupGeneration;
                ui8 failRealm;
                ui8 failDomain;
                ui8 vDisk;
                
                ReadFromStream(mi, groupId);
                ReadFromStream(mi, groupGeneration);
                ReadFromStream(mi, failRealm);
                ReadFromStream(mi, failDomain);
                ReadFromStream(mi, vDisk);
                
                result->VDiskId = TVDiskID(
                    TGroupId::FromValue(LittleToHost(groupId)),
                    LittleToHost(groupGeneration),
                    LittleToHost(failRealm),
                    LittleToHost(failDomain),
                    LittleToHost(vDisk)
                );
                
                ui8 flags;
                ReadFromStream(mi, flags);
                
                result->IgnoreBlock = (flags & 1) != 0;
                result->HasCookie = (flags & 2) != 0;
                result->RewriteBlob = (flags & 4) != 0;
                result->IsInternal = (flags & 8) != 0;
                bool hasDeadline = (flags & 16) != 0;
                
                if (result->HasCookie) {
                    ui64 cookie;
                    ReadFromStream(mi, cookie);
                    result->Cookie = LittleToHost(cookie);
                }
                
                if (hasDeadline) {
                    ui64 deadline;
                    ReadFromStream(mi, deadline);
                    result->Deadline = TInstant::MilliSeconds(LittleToHost(deadline));
                }
                
                ui32 handleClass;
                ReadFromStream(mi, handleClass);
                result->HandleClass = static_cast<NKikimrBlobStorage::EPutHandleClass>(LittleToHost(handleClass));
                
                ui32 extraChecksCount;
                ReadFromStream(mi, extraChecksCount);
                extraChecksCount = LittleToHost(extraChecksCount);
                
                result->ExtraBlockChecks.clear();
                result->ExtraBlockChecks.reserve(extraChecksCount);
                
                for (ui32 i = 0; i < extraChecksCount; ++i) {
                    ui64 tabletId;
                    ui32 generation;
                    ReadFromStream(mi, tabletId);
                    ReadFromStream(mi, generation);
                    result->ExtraBlockChecks.emplace_back(LittleToHost(tabletId), LittleToHost(generation));
                }
                
                ui64 bufferSize;
                ReadFromStream(mi, bufferSize);
                bufferSize = LittleToHost(bufferSize);
                
                if (bufferSize > 0) {
                    result->Buffer.resize(bufferSize);
                    mi.Read(result->Buffer.begin(), bufferSize);
                } else {
                    result->Buffer.clear();
                }
                
                return result.Release();
            } catch (...) {
                return nullptr;
            }
        }
        
        static ui64 EstimateSerializeSize(const TEvVPutBinary& msg) {
            ui64 size = 0;
            
            // Header (Magic + Version)
            size += sizeof(ui32) + sizeof(ui8);
            
            // TLogoBlobID
            size += sizeof(ui64) + sizeof(ui32) * 4 + sizeof(ui8);
            
            // TVDiskID
            size += sizeof(ui32) * 3 + sizeof(ui8) * 2;
            
            // Флаги
            size += sizeof(ui8);
            
            // Optional fields
            if (msg.HasCookie) {
                size += sizeof(ui64);
            }
            
            if (msg.Deadline != TInstant::Max()) {
                size += sizeof(ui64);
            }
            
            // Handle class
            size += sizeof(ui32);
            
            // ExtraBlockChecks
            size += sizeof(ui32); // Array size
            size += (sizeof(ui64) + sizeof(ui32)) * msg.ExtraBlockChecks.size();
            
            // Buffer size
            size += sizeof(ui64);
            size += msg.Buffer.size();
            
            return size;
        }
        
        // Template functions for primitive types serialization/deserialization
        template<typename T>
        static void WriteToStream(IOutputStream& output, const T& value) {
            output.Write(&value, sizeof(T));
        }
        
        template<typename T>
        static void ReadFromStream(IInputStream& input, T& value) {
            input.Load(&value, sizeof(T));
        }
        
        TString ToString() const override {
            TStringStream str;
            str << "TEvVPutBinary {ID# " << BlobId.ToString();
            str << " VDiskID# " << VDiskId.ToString();
            if (IgnoreBlock) {
                str << " IgnoreBlock";
            }
            if (HasCookie) {
                str << " Cookie# " << Cookie;
            }
            if (Deadline != TInstant::Max()) {
                str << " Deadline# " << Deadline.ToString();
            }
            str << " HandleClass# " << (ui32)HandleClass;
            if (RewriteBlob) {
                str << " RewriteBlob";
            }
            if (IsInternal) {
                str << " IsInternal";
            }
            if (!ExtraBlockChecks.empty()) {
                str << " ExtraBlockChecks# " << ExtraBlockChecks.size();
            }
            str << " BufferSize# " << Buffer.size();
            str << "}";
            return str.Str();
        }

        TString ToStringHeader() const override {
            return TStringBuilder() << "TEvVPutBinary: " << BlobId.ToString() << " to " << VDiskId.ToString();
        }

        bool SerializeToArcadiaStream(TChunkSerializer* serializer) const override {
            TString serialized;
            Serialize(*this, serialized);
            serializer->WriteAliasedRaw(serialized.data(), serialized.size());
            return true;
        }

        bool IsSerializable() const override {
            return true;
        }
    };

} // namespace NKikimr
