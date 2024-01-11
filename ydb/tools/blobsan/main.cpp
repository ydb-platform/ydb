#include <stdio.h>
#include <stdlib.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_partlayout.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_sets.h>
#include <ydb/core/blobstorage/vdisk/query/query_stream.h>
#include <library/cpp/containers/absl_flat_hash/flat_hash_map.h>
#include <library/cpp/streams/bzip2/bzip2.h>
#include <library/cpp/pop_count/popcount.h>
#include <util/generic/buffer.h>
#include <util/generic/hash.h>
#include <util/stream/buffer.h>
#include <util/stream/input.h>
#include <util/stream/output.h>
#include <util/stream/str.h>
#include <util/string/builder.h>
#include <util/system/thread.h>
#include <util/system/mutex.h>
#include <util/system/condvar.h>
#include <deque>
#include <map>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <list>

using namespace NKikimr;

ui32 TotalPartCount = 0;
ui32 BlobSubgroupSize = 0;

bool OutputCout = true;

struct TRecordHeader {
    ui32 GroupId;
    ui32 Index;
    ui32 Length;
};

using TStreamId = std::tuple<ui32, ui32>;

struct TParseState {
    std::optional<TString> ActorId;
    ui64 SequenceId = 1;
};

std::unordered_map<TStreamId, TParseState, THash<TStreamId>> ParseState;

using TBarrierKey = std::tuple<ui64, ui32>; // tablet:channel

struct TBarrierRecord {
    bool Hard = false;
    ui32 CollectGen = 0;
    ui32 CollectStep = 0;
    ui32 Ingress = 0;
};

struct TBarrierInfo {
    std::map<std::tuple<ui32, ui32>, TBarrierRecord> Records;
    std::tuple<ui32, ui32> SoftKey, HardKey;
    std::optional<std::tuple<ui32, ui32>> Soft, Hard;

    TString ToString(ui32 blobGen, ui32 blobStep, bool hasKeepFlag) const {
        TStringStream str;
        if (Soft) {
            auto [rec, ctr] = SoftKey;
            auto [gen, step] = *Soft;
            str << "soft[" << rec << ":" << ctr << "=>" << gen << ":" << step << "]";
            if (std::make_tuple(blobGen, blobStep) <= std::make_tuple(gen, step)) {
                str << "(collect)";
                if (hasKeepFlag) {
                    str << "(keep)";
                }
            }
        }
        if (Hard) {
            if (Soft) {
                str << " ";
            }
            auto [rec, ctr] = HardKey;
            auto [gen, step] = *Hard;
            str << "hard[" << rec << ":" << ctr << "=>" << gen << ":" << step << "]";
            if (std::make_tuple(blobGen, blobStep) <= std::make_tuple(gen, step)) {
                str << "(collect)";
            }
        }
        return str.Str();
    }
};

enum EKeepMode {
    DEFAULT,
    KEEP,
    DO_NOT_KEEP,
};

struct TBlobInfo {
    EKeepMode KeepMode = DEFAULT;
    ui32 Nodes = 0;
    TSubgroupPartLayout Layout;
    TSubgroupPartLayout LocalLayout;
    TSubgroupPartLayout ReflectionLayout;

    void Merge(const TBlobInfo& other, const TBlobStorageGroupType& type) {
        KeepMode = Max(KeepMode, other.KeepMode);
        Nodes |= other.Nodes;
        Layout.Merge(other.Layout, type);
        LocalLayout.Merge(other.LocalLayout, type);
        ReflectionLayout.Merge(other.ReflectionLayout, type);
    }
};

struct TVDiskState {
    THashMap<TBarrierKey, TBarrierInfo> Barriers;
    std::unordered_map<TLogoBlobID, TBlobInfo> Blobs;
};

struct TGroupState {
    std::unordered_map<ui32, TVDiskState> States;
};

std::unordered_map<ui32, TGroupState> Groups;

void ParseBlock(TBuffer& buffer, TVDiskState& vs, const TBlobStorageGroupInfo& info) {
    TBufferInput stream(buffer);
    TRecordHeader header;
    const size_t read = stream.Load(&header, sizeof(header));
    Y_ABORT_UNLESS(read == sizeof(header));
    TBZipDecompress y(&stream);

    const TStreamId streamId(header.GroupId, header.Index);
    TParseState& ps = ParseState[streamId];

    ui64 rawX1, rawX2, sequenceId, numBlocks;
    Load(&y, rawX1);
    Load(&y, rawX2);
    Load(&y, sequenceId);
    Load(&y, numBlocks);

    if (sequenceId == 1) {
        vs = {};
        ps = {};
    }

    const TString actorId(TStringBuilder() << rawX1 << "," << rawX2);
    Y_ABORT_UNLESS(actorId == ps.ActorId.value_or(actorId));
    Y_ABORT_UNLESS(sequenceId == ps.SequenceId, "SequenceId# %" PRIu64 " Expected# %" PRIu64, sequenceId, ps.SequenceId);
    ps.ActorId = actorId;
    ++ps.SequenceId;

    TLevelIndexStreamActor::TOutputBlockHeader blockHeader;

    const auto& topology = info.GetTopology();
    const auto& vdiskId = topology.GetVDiskId(header.Index);
    auto ingressCache = TIngressCache::Create(info.PickTopology(), vdiskId);

    TBufferedOutput lines(&Cout, 1_MB);

    while (const size_t read = y.Load(&blockHeader, sizeof(blockHeader))) {
        Y_ABORT_UNLESS(read == sizeof(blockHeader));

        TString linePrefix;
        if (OutputCout) {
            linePrefix = TStringBuilder() << header.GroupId << " " << header.Index << " " << static_cast<char>('A' + blockHeader.Database) << " ";
            TStringOutput pfx(linePrefix);
            switch (blockHeader.TableType) {
                case static_cast<ui32>(TLevelIndexStreamActor::ETableType::FCUR):
                    pfx << "FCur ";
                    break;

                case static_cast<ui32>(TLevelIndexStreamActor::ETableType::FDREG):
                    pfx << "FDreg ";
                    break;

                case static_cast<ui32>(TLevelIndexStreamActor::ETableType::FOLD):
                    pfx << "FOld ";
                    break;

                case static_cast<ui32>(TLevelIndexStreamActor::ETableType::LEVEL):
                    pfx << "L" << blockHeader.Level << " ID# " << blockHeader.SstId << " ";
                    break;

                default:
                    Y_ABORT();
            }
        }

        auto processLogoBlob = [&](const TLevelIndexStreamActor::TOutputLogoBlob& record) {
            const TLogoBlobID& blobId = record.Key.LogoBlobID();
            const TIngress ingress(record.Ingress);
            if (OutputCout) {
                lines << "Key# " << blobId << " Ingress# " << ingress.ToString(&topology, vdiskId, blobId);
            }

            EKeepMode keepMode;
            switch (ingress.GetCollectMode(TIngress::IngressMode(info.Type))) {
                case CollectModeDefault:
                    keepMode = DEFAULT;
                    break;

                case CollectModeKeep:
                    keepMode = KEEP;
                    break;

                case CollectModeDoNotKeep:
                case CollectModeDoNotKeep | CollectModeKeep:
                    keepMode = DO_NOT_KEEP;
                    break;
            }

            const ui32 nodeId = topology.GetIdxInSubgroup(vdiskId, blobId.Hash());

            const auto layout = TSubgroupPartLayout::CreateFromIngress(ingress, info.Type);
            TSubgroupPartLayout localLayout;
            const auto& local = ingress.LocalParts(info.Type);
            for (ui32 i = local.FirstPosition(); i != local.GetSize(); i = local.NextPosition(i)) {
                localLayout.AddItem(nodeId, i, info.Type);
            }

            TSubgroupPartLayout reflectionLayout = layout;
            for (ui32 i = 0; i < TotalPartCount; ++i) {
                reflectionLayout.Mask(i, 1 << nodeId);
            }

            const TBlobInfo blob{
                .KeepMode = keepMode,
                .Nodes = (ui32)1 << nodeId,
                .Layout = layout,
                .LocalLayout = localLayout,
                .ReflectionLayout = reflectionLayout,
            };
            vs.Blobs[blobId].Merge(blob, info.Type);
        };

        auto processBlock = [&](const TLevelIndexStreamActor::TOutputBlock& record) {
            if (OutputCout) {
                lines << "Key# " << record.Key.ToString() << " MemRec# " << record.MemRec.ToString(ingressCache.Get());
            }
        };

        auto processBarrier = [&](const TLevelIndexStreamActor::TOutputBarrier& record) {
            if (OutputCout) {
                lines << "Key# " << record.Key.ToString() << " MemRec# " << record.MemRec.ToString(ingressCache.Get());
            }

            auto& barrier = vs.Barriers[TBarrierKey(record.Key.TabletId, record.Key.Channel)];

            const auto [it, inserted] = barrier.Records.try_emplace({record.Key.Gen, record.Key.GenCounter});
            auto& r = it->second;
            if (inserted) {
                r = {
                    .Hard = static_cast<bool>(record.Key.Hard),
                    .CollectGen = record.MemRec.CollectGen,
                    .CollectStep = record.MemRec.CollectStep,
                    .Ingress = record.MemRec.Ingress.Raw(),
                };
            } else {
                Y_ABORT_UNLESS(r.Hard == record.Key.Hard);
                Y_ABORT_UNLESS(r.CollectGen == record.MemRec.CollectGen);
                Y_ABORT_UNLESS(r.CollectStep == record.MemRec.CollectStep);
                r.Ingress |= record.MemRec.Ingress.Raw();
            }

            auto ingress = TBarrierIngress::CreateFromRaw(r.Ingress);
            if (ingress.IsQuorum(ingressCache.Get())) {
                auto& key = r.Hard ? barrier.HardKey : barrier.SoftKey;
                auto& value = r.Hard ? barrier.Hard : barrier.Soft;
                if (!value || key < it->first) { // newer barrier with quorum wins
                    key = it->first;
                    value.emplace(r.CollectGen, r.CollectStep);
                }
            }
        };

        auto processRecord = [&](auto& record, const auto& callback) {
            if (OutputCout) {
                lines << linePrefix;
            }
            callback(record);
            if (OutputCout) {
                lines << Endl;
            }
        };

        auto processRecordArray = [&](const auto& v, const auto& callback) {
            using T = std::decay_t<decltype(v)>;
            for (ui32 i = 0; i < blockHeader.NumRecs; ++i) {
                T record;
                const size_t n = y.Load(&record, sizeof(record));
                Y_ABORT_UNLESS(n == sizeof(record));
                processRecord(record, callback);
            }
        };

        switch (blockHeader.Database) {
            case static_cast<ui32>(TLevelIndexStreamActor::EDatabase::LOGOBLOBS):
                processRecordArray(TLevelIndexStreamActor::TOutputLogoBlob(), processLogoBlob);
                break;

            case static_cast<ui32>(TLevelIndexStreamActor::EDatabase::BLOCKS):
                processRecordArray(TLevelIndexStreamActor::TOutputBlock(), processBlock);
                break;

            case static_cast<ui32>(TLevelIndexStreamActor::EDatabase::BARRIERS):
                processRecordArray(TLevelIndexStreamActor::TOutputBarrier(), processBarrier);
                break;

            default:
                Y_ABORT();
        }
    }
}

void PushBlock(TStreamId streamId, TBuffer&& buffer, const TBlobStorageGroupInfo& info) {
    auto [groupId, index] = streamId;
    ParseBlock(buffer, Groups[groupId].States[index], info);
}

TString FormatBitMask(ui64 mask, ui32 numBits) {
    TStringStream str;
    for (ui32 i = 0; i < numBits; ++i) {
        str << (mask >> i & 1);
    }
    return str.Str();
}

TString FormatLayout(const TSubgroupPartLayout& layout, TBlobStorageGroupType type) {
    TStringStream s;
    bool first = true;
    s << "{";

    switch (type.GetErasure()) {
        case TBlobStorageGroupType::ErasureMirror3dc:
            for (ui32 realm = 0; realm < 3; ++realm) {
                for (ui32 domain = 0; domain < 3; ++domain) {
                    const ui32 nodeId = realm + domain * 3;
                    ui32 parts = 0;
                    for (ui32 i = 0; i < TotalPartCount; ++i) {
                        if (layout.GetDisksWithPart(i) >> nodeId & 1) {
                            parts |= 1 << i;
                        }
                    }
                    if (parts) {
                        s << (std::exchange(first, false) ? "" : " ") << "R" << realm << "D" << domain;
                        if (parts != (1 << realm)) {
                            s << "!";
                            for (ui32 i = 0; parts; ++i, parts >>= 1) {
                                if (parts & 1) {
                                    if (i == nodeId % 3) {
                                        s << "+";
                                    } else {
                                        s << (i + 1);
                                    }
                                } else if (i == nodeId % 3) {
                                    s << "?";
                                }
                            }
                        }
                    }
                }
            }
            break;

        default:
            return layout.ToString(type);
    }

    s << "}";
    return s.Str();
}

int main(int argc, char **argv) {
    if (argc != 2) {
        Cerr << "usage: " << argv[0] << " <erasure>" << Endl;
        return 1;
    }

    char link[256];
    ssize_t rv = readlink("/proc/self/fd/1", link, sizeof(link));
    if (rv == 9 && strncmp(link, "/dev/null", rv) == 0) {
        OutputCout = false;
    }

    TBlobStorageGroupType type(TBlobStorageGroupType::ErasureSpeciesByName(argv[1]));
    TotalPartCount = type.TotalPartCount();
    BlobSubgroupSize = type.BlobSubgroupSize();

    const ui32 numRealms = type.GetErasure() == TBlobStorageGroupType::ErasureMirror3dc ? 3 : 1;
    const ui32 numDomains = type.GetErasure() == TBlobStorageGroupType::ErasureMirror3dc ? 3 : BlobSubgroupSize;
    TBlobStorageGroupInfo info(type, 1, numDomains, numRealms);
    auto& checker = info.GetQuorumChecker();
    const TBlobStorageGroupInfo::TSubgroupVDisks failed(&info.GetTopology());
    ui64 trashSize = 0;

    IInputStream& stream = Cin;
    for (;;) {
        TRecordHeader header;
        if (const size_t read = stream.Load(&header, sizeof(header))) {
            Y_ABORT_UNLESS(read == sizeof(header));
            TBuffer buffer(read + header.Length);
            buffer.Append((const char*)&header, sizeof(header));
            const size_t n = stream.Load(buffer.Pos(), header.Length);
            buffer.Advance(n);
            Y_ABORT_UNLESS(n == header.Length);
            PushBlock(TStreamId(header.GroupId, header.Index), std::move(buffer), info);
        } else {
            break;
        }
    }

    TBufferedOutput err(&Cerr, 1_MB);

    for (auto& [groupId, group] : Groups) {
        struct TBlobRecord {
            TBlobInfo Info;
            bool Hard = false;
            ui32 UnderBarrierMask = 0;
            ui32 DiscardMask = 0;
            ui32 KeepMask = 0;
            ui32 DoNotKeepMask = 0;
            std::vector<TBlobInfo> PerIndexInfo;
        };

        std::vector<ui32> indexes;
        for (const auto& [index, vdisk] : group.States) {
            indexes.push_back(index);
        }
        std::sort(indexes.begin(), indexes.end());

        for (;;) {
            TLogoBlobID blobId;

            // collect record information from the disks of the group
            TBlobRecord record;
            for (auto& [index, vdisk] : group.States) {
                auto it = blobId == TLogoBlobID() ? vdisk.Blobs.begin() : vdisk.Blobs.find(blobId);
                if (it == vdisk.Blobs.end()) {
                    continue;
                }
                auto nh = vdisk.Blobs.extract(it);
                blobId = nh.key();
                TBlobInfo& blob = nh.mapped();

                bool underBarrier = false;
                bool hard = false;
                bool discard = false;

                const auto& barrierKey = std::make_tuple(blobId.TabletID(), blobId.Channel());
                if (const auto barrierIt = vdisk.Barriers.find(barrierKey); barrierIt != vdisk.Barriers.end()) {
                    const auto& barrier = barrierIt->second;
                    const auto& blobBarrierKey = std::make_tuple(blobId.Generation(), blobId.Step());
                    if (barrier.Hard && blobBarrierKey <= *barrier.Hard) {
                        underBarrier = true;
                        discard = true;
                        hard = true;
                    } else if (barrier.Soft && blobBarrierKey <= *barrier.Soft) {
                        underBarrier = true;
                        discard = blob.KeepMode != KEEP;
                        hard = false;
                    }
                }

                if (discard) { // no local data
                    blob.LocalLayout = {};
                }

                record.Info.Merge(blob, type);
                record.Hard |= hard;
                if (underBarrier) {
                    record.UnderBarrierMask |= blob.Nodes;
                }
                if (discard) {
                    record.DiscardMask |= blob.Nodes;
                }
                if (blob.KeepMode == KEEP) {
                    record.KeepMask |= blob.Nodes;
                } else if (blob.KeepMode == DO_NOT_KEEP) {
                    record.DoNotKeepMask |= blob.Nodes;
                }

                if (record.PerIndexInfo.empty()) {
                    record.PerIndexInfo.resize(BlobSubgroupSize);
                }
                record.PerIndexInfo[index].Merge(blob, type);
            }
            if (blobId == TLogoBlobID()) {
                break;
            }

            // process the blob
            TBlobRecord& blob = record;
            TBlobStorageGroupInfo::TOrderNums nums;
            info.GetTopology().PickSubgroup(blobId.Hash(), nums);

            auto originalLocalLayout = blob.Info.LocalLayout;

            for (ui32 i = 0; i < BlobSubgroupSize; ++i) {
                if (!group.States.contains(nums[i])) {
                    TSubgroupPartLayout temp = blob.Info.Layout;
                    for (ui32 j = 0; j < TotalPartCount; ++j) {
                        temp.Mask(j, 1 << i);
                    }
                    blob.Info.LocalLayout.Merge(temp, type);
                    blob.Info.ReflectionLayout.Merge(temp, type);
                }
            }

            auto getNumParts = [&](const TSubgroupPartLayout& layout) {
                ui32 res = 0;
                for (ui32 i = 0; i < TotalPartCount; ++i) {
                    res += layout.GetDisksWithPart(i) != 0;
                }
                return res;
            };

            const ui32 numParts = getNumParts(blob.Info.Layout);
            const ui32 numLocalParts = getNumParts(blob.Info.LocalLayout);
            const ui32 numNodes = PopCount(blob.Info.Nodes);

            auto isFull = [&](const TSubgroupPartLayout& layout) {
                return checker.GetBlobState(layout, failed) == TBlobStorageGroupInfo::EBS_FULL;
            };

            bool misplacedParts = false;
            if (type.GetErasure() == TBlobStorageGroupType::ErasureMirror3dc) {
                for (ui32 partId = 0; partId < TotalPartCount; ++partId) {
                    const ui32 disks = blob.Info.Layout.GetDisksWithPart(partId) | blob.Info.LocalLayout.GetDisksWithPart(partId);
                    if ((disks & (0b001001001 << partId)) != disks) {
                        for (ui32 nodeId = 0; nodeId < BlobSubgroupSize; ++nodeId) {
                            if (disks >> nodeId & 1 && nodeId % 3 != partId) {
                                trashSize += blobId.BlobSize();
                                misplacedParts = true;
                            }
                        }
                    }
                    // mask out misplaced parts
                    blob.Info.LocalLayout.Mask(partId, 0b001001001 << partId);
                }
            }

            if (!blob.UnderBarrierMask || blob.Hard || blob.Info.KeepMode != KEEP ||
                    (isFull(blob.Info.Layout) && isFull(blob.Info.ReflectionLayout) && isFull(blob.Info.LocalLayout))) {
                // looks like correct blob -- either it is not under barrier (and may be being written right now), or
                // it is with keep flags and fully written with correct ingress bits on every disk
                continue;
            }

            ui32 seenDisks = 0;
            for (ui32 i = 0; i < BlobSubgroupSize; ++i) {
                if (group.States.contains(nums[i])) {
                    seenDisks |= 1 << i;
                }
            }

            const bool phantomLike = seenDisks != blob.Info.Nodes;

            if (phantomLike || numParts < type.MinimalRestorablePartCount() || numLocalParts < type.MinimalRestorablePartCount() ||
                    !info.GetQuorumChecker().CheckFailModelForSubgroup(~TBlobStorageGroupInfo::TSubgroupVDisks::CreateFromMask(
                    &info.GetTopology(), blob.Info.Nodes))) {
                // looks like phantom blob -- count found parts as trash
                for (ui32 partIdx = 0; partIdx < TotalPartCount; ++partIdx) {
                    if (const ui32 disks = blob.Info.LocalLayout.GetDisksWithPart(partIdx)) {
                        trashSize += type.PartSize(blobId) * PopCount(disks);
                    }
                }
                continue;
            }

            TString keepMode;
            switch (blob.Info.KeepMode) {
                case DEFAULT: keepMode = "default"; break;
                case KEEP: keepMode = "keep"; break;
                case DO_NOT_KEEP: keepMode = "doNotKeep"; break;
            }

            err << "GroupId# " << groupId << " BlobId# " << blobId
                << "\n\tOrderNums#          " << FormatList(nums)
                << "\n\tKeepMode#           " << keepMode
                << "\n\tLayout#             " << FormatLayout(blob.Info.Layout, type)
                << "\n\tLocalLayout#        " << FormatLayout(blob.Info.LocalLayout, type)
                << "\n\tOrigLocalLayout#    " << FormatLayout(originalLocalLayout, type)
                << "\n\tReflectionLayout#   " << FormatLayout(blob.Info.ReflectionLayout, type)
                << "\n\tNumParts#           " << numParts
                << "\n\tNumLocalParts#      " << numLocalParts
                << "\n\tNumReflectionParts# " << getNumParts(blob.Info.ReflectionLayout)
                << "\n\tNumNodes#           " << numNodes
                << "\n\tNodes#              " << FormatBitMask(blob.Info.Nodes, BlobSubgroupSize)
                << "\n\tSeenDisks#          " << FormatBitMask(seenDisks, BlobSubgroupSize)
                << "\n\tNumUnderBarrier#    " << PopCount(blob.UnderBarrierMask)
                << "\n\tUnderBarrierMask#   " << FormatBitMask(blob.UnderBarrierMask, BlobSubgroupSize)
                << "\n\tNumDiscard#         " << PopCount(blob.DiscardMask)
                << "\n\tDiscardMask#        " << FormatBitMask(blob.DiscardMask, BlobSubgroupSize)
                << "\n\tKeepMask#           " << FormatBitMask(blob.KeepMask, BlobSubgroupSize)
                << "\n\tDoNotKeepMask#      " << FormatBitMask(blob.DoNotKeepMask, BlobSubgroupSize)
                << "\n\tMisplaced#          " << (misplacedParts ? "true" : "false");

            for (const ui32 index : indexes) {
                const auto& vdisk = group.States.at(index);
                const auto& k = std::make_tuple(blobId.TabletID(), blobId.Channel());
                if (const auto it = vdisk.Barriers.find(k); it != vdisk.Barriers.end()) {
                    const bool keep = blob.KeepMask >> index & 1;
                    err << "\n\tBarrier[" << index << "]#         " << it->second.ToString(blobId.Generation(), blobId.Channel(), keep);
                }
            }

            err << Endl;
        }
    }

    if (trashSize) {
        err << "TrashSize# " << trashSize << Endl;
    }

    return 0;
}
