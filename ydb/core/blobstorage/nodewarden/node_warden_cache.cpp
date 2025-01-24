#include "node_warden.h"
#include "node_warden_impl.h"

using namespace NKikimr;
using namespace NStorage;

void TNodeWarden::InvokeSyncOp(std::unique_ptr<IActor> actor) {
    Y_ABORT_UNLESS(!SyncActorId);
    SyncActorId = Register(actor.release(), TMailboxType::Simple, AppData()->IOPoolId);
}

void TNodeWarden::Handle(TEvents::TEvInvokeResult::TPtr ev) {
    // reset actor
    Y_ABORT_UNLESS(SyncActorId == ev->Sender);
    SyncActorId = {};

    // process message
    try {
        ev->Get()->Process(ActorContext());
    } catch (const std::exception&) {
        Y_ABORT("Exception while executing sync callback: %s", CurrentExceptionMessage().data());
    }

    // issue other operation if pending (and if not already issued)
    if (!SyncActorId && !SyncOpQ.empty()) {
        InvokeSyncOp(std::move(SyncOpQ.front()));
        SyncOpQ.pop();
    }
}

void TNodeWarden::EnqueueSyncOp(std::function<std::function<void()>(const TActorContext&)> callback) {
    auto complete = [](auto&& resGetter, const TActorContext&) { resGetter()(); };
    auto actor = CreateInvokeActor(std::move(callback), std::move(complete), NKikimrServices::TActivity::NODE_WARDEN);
    if (SyncActorId) {
        // there is other operation currently going on, we have to wait for a while
        SyncOpQ.push(std::move(actor));
    } else {
        // execute operation right now
        InvokeSyncOp(std::move(actor));
    }
}

std::function<std::function<void()>(const TActorContext&)> TNodeWarden::WrapCacheOp(TWrappedCacheOp operation) {
    return [cfg = Cfg, op = std::move(operation), instanceId = InstanceId, availDomainId = AvailDomainId](const TActorContext&) {
        std::function<void()> res;
        try {
            cfg->CacheAccessor->Update([&](TString data) {
                // TODO(alexvru): special handling needed when data is empty -- this means cache file was deleted due to
                // some error and needs to be rebuilt from scratch; unlikely case

                NKikimrBlobStorage::TNodeWardenCache proto;
                if (!google::protobuf::TextFormat::ParseFromString(data, &proto)) {
                    proto.Clear();
                }

                // find matching availability domain in cache protobuf; create new, if not found
                if (!proto.HasAvailDomain() || proto.GetAvailDomain() != availDomainId ||
                        !proto.HasInstanceId() || proto.GetInstanceId() != instanceId) {
                    proto.Clear();
                    proto.SetAvailDomain(availDomainId);
                    proto.SetInstanceId(*instanceId);
                }

                // execute operation over the service set
                res = op(proto.MutableServiceSet());

                // format updated cache
                data.clear();
                google::protobuf::TextFormat::PrintToString(proto, &data);
                return data;
            });
        } catch (...) {
            STLOG(PRI_WARN, BS_NODE, NW06, "WrapCacheOp failed to update cache", (Error, CurrentExceptionMessage()));
        }
        if (!res) { // no processor was actually called due to error
            res = op(nullptr);
        }
        return res;
    };
}

TNodeWarden::TWrappedCacheOp TNodeWarden::UpdateGroupInCache(const NKikimrBlobStorage::TGroupInfo& group) {
    return [group](NKikimrBlobStorage::TNodeWardenServiceSet *services) -> std::function<void()> {
        if (services) {
            auto *protoGroups = services->MutableGroups();
            for (int i = 0; i < protoGroups->size(); ++i) {
                const NKikimrBlobStorage::TGroupInfo& pb = protoGroups->at(i);
                if (pb.GetGroupID() == group.GetGroupID()) {
                    if (pb.GetGroupGeneration() <= group.GetGroupGeneration()) {
                        protoGroups->Mutable(i)->CopyFrom(group);
                    }
                    return [] {};
                }
            }
            protoGroups->Add()->CopyFrom(group);
        }
        return [] {};
    };
}

TNodeWarden::TWrappedCacheOp TNodeWarden::UpdateServiceSet(const NKikimrBlobStorage::TNodeWardenServiceSet& newServices,
        bool comprehensive, std::function<void()> tail) {
    return [newServices, comprehensive, tail = std::move(tail)](NKikimrBlobStorage::TNodeWardenServiceSet *services) {
        if (!services) {
            return tail;
        }

        auto applyDiff = [](auto *cache, auto& map) {
            using TMap = std::decay_t<decltype(map)>;
            using TItem = std::remove_cv_t<std::remove_pointer_t<typename TMap::mapped_type>>;
            static constexpr bool isGroupInfo = std::is_same_v<TItem, NKikimrBlobStorage::TGroupInfo>;
            static constexpr bool isVDisk = std::is_same_v<TItem, NKikimrBlobStorage::TNodeWardenServiceSet::TVDisk>;
            auto extractKey = [](const auto& item) -> typename TMap::key_type {
                if constexpr (isGroupInfo) {
                    return item.GetGroupID();
                } else if constexpr (isVDisk) {
                    return TVSlotId(item.GetVDiskLocation());
                } else {
                    return item;
                }
            };
            for (int i = 0; i < cache->size(); ++i) {
                const auto& item = cache->at(i);
                if (const auto it = map.find(extractKey(item)); it != map.end()) {
                    if (it->second) {
                        bool pred = true;
                        if constexpr (isGroupInfo) {
                            pred = item.GetGroupGeneration() <= it->second->GetGroupGeneration();
                        }
                        if (pred) {
                            cache->Mutable(i)->CopyFrom(item);
                        }
                    } else {
                        cache->DeleteSubrange(i--, 1);
                    }
                    map.erase(it);
                }
            }
            for (const auto& [key, value] : map) {
                if (value) {
                    cache->Add()->CopyFrom(*value);
                }
            }
        };

        if (comprehensive) {
            // when we receive comprehensive configuration, we just overwrite corresponding values
            services->MutablePDisks()->CopyFrom(newServices.GetPDisks());
            services->MutableVDisks()->CopyFrom(newServices.GetVDisks());
        } else {
            // otherwise we have to calculate diffs and apply only changed entities
            std::set<TPDiskKey> pdisksToDestroy;
            std::map<TPDiskKey, const NKikimrBlobStorage::TNodeWardenServiceSet::TPDisk*> pdisks;
            for (const auto& x : newServices.GetPDisks()) {
                const auto *ptr = x.HasEntityStatus() && x.GetEntityStatus() == NKikimrBlobStorage::DESTROY ? nullptr : &x;
                pdisks.emplace(x, ptr);
                if (!ptr) {
                    pdisksToDestroy.insert(x);
                }
            }
            applyDiff(services->MutablePDisks(), pdisks);

            // do the same thing for vdisks
            std::map<TVSlotId, const NKikimrBlobStorage::TNodeWardenServiceSet::TVDisk*> vdisks;
            for (const auto& x : newServices.GetVDisks()) {
                const TVSlotId key(x.GetVDiskLocation());
                Y_ABORT_UNLESS(!pdisksToDestroy.count({key.NodeId, key.PDiskId}) || x.GetDoDestroy(), "inconsistent ServiceSet");
                vdisks.emplace(key, pdisksToDestroy.count({key.NodeId, key.PDiskId}) ? nullptr : &x);
            }
            applyDiff(services->MutableVDisks(), vdisks);
        }

        // update groups configuration
        std::unordered_map<ui32, const NKikimrBlobStorage::TGroupInfo*> groups;
        for (const auto& x : newServices.GetGroups()) {
            groups.emplace(x.GetGroupID(), x.HasEntityStatus() && x.GetEntityStatus() == NKikimrBlobStorage::DESTROY ? nullptr : &x);
        }
        applyDiff(services->MutableGroups(), groups);

        return tail;
    };
}

std::unique_ptr<ICacheAccessor> NKikimr::CreateFileCacheAccessor(const TString& templ, const std::unordered_map<char, TString>& vars) {
    class TAccessor : public ICacheAccessor {
        TFsPath Path;

    public:
        TAccessor(TFsPath path)
            : Path(std::move(path))
        {}

        TString Read() override {
            return TFileInput(Path).ReadAll();
        }

        void Update(std::function<TString(TString)> processor) override {
            try {
                TString data;
                std::optional<TFile> inFile;
                inFile.emplace(Path, OpenAlways | RdWr | Sync);
                inFile->Flock(LOCK_EX);

                // read the file unless it is way too big to fit into memory
                if (i64 len = inFile->GetLength(); len <= 32 * 1024 * 1024) {
                    data = TString::Uninitialized(len);
                    try {
                        data.resize(inFile->Read(data.Detach(), len));
                    } catch (const TFileError&) {
                        data.clear(); // consider empty cache on read error
                    }
                }

                data = processor(data);

                const TString suffix = Sprintf(".%08" PRIx32, RandomNumber<ui32>());
                TFsPath temp = Path.Parent() / (Path.GetName() + suffix);
                try {
                    TFile outFile(temp, CreateNew | WrOnly);
                    outFile.Write(data.data(), data.size());
                    outFile.Close();
                    temp.RenameTo(Path);
                } catch (...) {
                    temp.DeleteIfExists();
                    throw;
                }
            } catch (...) {
                Path.DeleteIfExists();
                throw;
            }
        }
    };

    TStringStream ss;
    for (size_t i = 0; i < templ.size(); ++i) {
        if (templ[i] == '%') {
            ++i;
            if (i != templ.size()) {
                if (const auto it = vars.find(templ[i]); it != vars.end()) {
                    ss << it->second;
                } else {
                    ss << templ[i];
                }
            } else {
                ss << '%';
            }
        } else {
            ss << templ[i];
        }
    }

    return std::make_unique<TAccessor>(ss.Str());
}
