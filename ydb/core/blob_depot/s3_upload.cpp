#include "s3.h"

#include <ydb/core/wrappers/abstract.h>

namespace NKikimr::NBlobDepot {

    using TS3Manager = TBlobDepot::TS3Manager;
    using TEvExternalStorage = NWrappers::TEvExternalStorage;

    struct TS3Manager::TEvUploadResult : TEventLocal<TEvUploadResult, TEvPrivate::EvUploadResult> {
    };

    class TS3Manager::TUploaderActor : public TActorBootstrapped<TUploaderActor> {
        const TActorId WrapperId;
        const TString BasePath;
        const TString Bucket;
        const TIntrusivePtr<TTabletStorageInfo> Info;
        const TData::TKey Key;
        const TValueChain ValueChain;
        ui32 RepliesPending = 0;
        TString Buffer;

    public:
        TUploaderActor(TActorId wrapperId, TString basePath, TString bucket, TIntrusivePtr<TTabletStorageInfo> info,
                TData::TKey key, TValueChain valueChain)
            : WrapperId(wrapperId)
            , BasePath(std::move(basePath))
            , Bucket(std::move(bucket))
            , Info(std::move(info))
            , Key(std::move(key))
            , ValueChain(std::move(valueChain))
        {}

        void Bootstrap() {
            STLOG(PRI_DEBUG, BLOB_DEPOT, BDTS00, "TUploaderActor::Bootstrap", (Key, Key), (ValueChain, ValueChain));
            size_t targetOffset = 0;
            EnumerateBlobsForValueChain(ValueChain, Info->TabletID, TOverloaded{
                [&](TLogoBlobID id, ui32 shift, ui32 size) {
                    const ui32 groupId = Info->GroupFor(id.Channel(), id.Generation());
                    SendToBSProxy(SelfId(), groupId, new TEvBlobStorage::TEvGet(id, shift, size, TInstant::Max(),
                        NKikimrBlobStorage::EGetHandleClass::FastRead), targetOffset);
                    ++RepliesPending;
                    targetOffset += size;
                },
                [&](TS3Locator) {
                    // already stored in S3, nothing to do
                }
            });
            Buffer = TString::Uninitialized(targetOffset);
            if (!RepliesPending) {
                // don't have to read anything?
            }
            Become(&TThis::StateFunc);
        }

        void Handle(TEvBlobStorage::TEvGetResult::TPtr ev) {
            auto *msg = ev->Get();
            STLOG(PRI_DEBUG, BLOB_DEPOT, BDTS01, "TUploaderActor::Handle(TEvGetResult)", (Msg, *msg));
            if (msg->Status == NKikimrProto::OK && msg->ResponseSz == 1 && msg->Responses->Status == NKikimrProto::OK) {
                TRope& rope = msg->Responses->Buffer;
                char *ptr = Buffer.Detach() + ev->Cookie;
                for (auto it = rope.Begin(); it.Valid(); it.AdvanceToNextContiguousBlock()) {
                    memcpy(ptr, it.ContiguousData(), it.ContiguousSize());
                    ptr += it.ContiguousSize();
                }
                if (!--RepliesPending) {
                    auto request = Aws::S3::Model::PutObjectRequest()
                        .WithBucket(Bucket)
                        .WithKey(TStringBuilder() << BasePath << '/' << Key.MakeTextualKey());
                    Send(WrapperId, new TEvExternalStorage::TEvPutObjectRequest(request, std::move(Buffer)));
                }
            } else {
            }
        }

        void Handle(TEvExternalStorage::TEvPutObjectResponse::TPtr ev) {
            STLOG(PRI_DEBUG, BLOB_DEPOT, BDTS02, "TUploaderActor::Handle(TEvPutObjectResponse)", (Result, ev->Get()->Result));
            if (auto& result = ev->Get()->Result; result.IsSuccess()) {
            } else {
            }
            PassAway();
        }

        STRICT_STFUNC(StateFunc,
            hFunc(TEvBlobStorage::TEvGetResult, Handle)
            hFunc(TEvExternalStorage::TEvPutObjectResponse, Handle)
            cFunc(TEvents::TSystem::Poison, PassAway)
        )
    };

    void TS3Manager::OnKeyWritten(const TData::TKey& key, const TValueChain& valueChain) {
        if (AsyncMode) {
            ActiveUploaders.insert(Self->Register(new TUploaderActor(WrapperId, BasePath, Bucket, Self->Info(), key,
                valueChain)));
        }
    }

} // NKikimr::NBlobDepot
