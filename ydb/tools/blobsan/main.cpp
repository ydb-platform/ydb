#include <stdio.h>
#include <stdlib.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_partlayout.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_sets.h>
#include <ydb/core/blobstorage/vdisk/query/query_stream.h>
#include <library/cpp/containers/absl/flat_hash_map.h>
#include <library/cpp/getopt/last_getopt.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/writer/json.h>
#include <library/cpp/streams/bzip2/bzip2.h>
#include <util/generic/buffer.h>
#include <util/generic/hash.h>
#include <util/stream/buffer.h>
#include <util/stream/input.h>
#include <util/stream/file.h>
#include <util/stream/output.h>
#include <util/stream/str.h>
#include <util/string/builder.h>
#include <util/system/thread.h>
#include <util/system/mutex.h>
#include <util/system/condvar.h>
#include <bit>
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
bool Write2x2Candidates = false;
TString ScanManifestPath;
TString ScanReportPath;

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
            if (Write2x2Candidates || ingress.IsQuorum(ingressCache.Get())) {
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

struct TScanCounters {
    ui64 Blobs = 0;
    ui64 LogicalBytes = 0;
    ui64 ObservedReplicaBytes = 0;
    ui64 MissingMetadataBlobs = 0;

    NJson::TJsonValue ToJson() const {
        NJson::TJsonValue result(NJson::JSON_MAP);
        result["blobs"] = Blobs;
        result["logical_bytes"] = LogicalBytes;
        result["observed_replica_bytes"] = ObservedReplicaBytes;
        result["missing_metadata_blobs"] = MissingMetadataBlobs;
        return result;
    }
};

// Counts are in group failrealm order, not the blob-dependent subgroup order.
TString ScanLayout(const TSubgroupPartLayout& layout, const TBlobStorageGroupInfo::TOrderNums& nums,
        const TBlobStorageGroupInfo& info, const std::unordered_set<ui32>* completed, ui32* replicas = nullptr) {
    ui32 counts[3] = {};
    bool known[3] = {true, true, true};
    for (ui32 node = 0; node < nums.size(); ++node) {
        const ui32 realm = info.GetTopology().GetVDiskId(nums[node]).FailRealm;
        if (completed && !completed->contains(nums[node])) {
            known[realm] = false;
        }
        if (layout.GetDisksWithPart(node % 3) >> node & 1) {
            ++counts[realm];
            if (replicas) {
                ++*replicas;
            }
        }
    }
    TStringStream result;
    for (ui32 realm = 0; realm < 3; ++realm) {
        if (realm) {
            result << "+";
        }
        if (known[realm]) {
            result << counts[realm];
        } else {
            result << "?";
        }
    }
    return result.Str();
}

int main(int argc, char **argv) {
    NLastGetopt::TOpts opts = NLastGetopt::TOpts::Default();
    opts.SetTitle("Analyze BlobStorage VDisk index snapshots for incomplete blob layouts.");
    opts.SetFreeArgsNum(1);
    opts.SetFreeArgTitle(0, "erasure", "Erasure species of all input groups, e.g. mirror-3-dc or block-4-2");
    opts.AddLongOption("write2x2-candidates", "Find mirror-3-dc write2x2 recovery candidates using local parts from the snapshot")
        .StoreTrue(&Write2x2Candidates);
    opts.AddLongOption("scan-manifest", "JSON file with group_id and completed_vdisks, including successfully captured empty VDisks")
        .RequiredArgument("PATH").StoreResult(&ScanManifestPath);
    opts.AddLongOption("scan-report", "Write a JSON histogram of all observed mirror-3-dc blobs before candidate filtering")
        .RequiredArgument("PATH").StoreResult(&ScanReportPath);
    opts.AddSection("Input",
        "Read a binary snapshot from stdin, as produced by `ydb-dstool group take-snapshot`."
        " It contains group/VDisk headers and bzip2-compressed LogoBlobs, Blocks and"
        " Barriers index records. Blob payloads are not included. All input groups"
        " must use the specified erasure species.");
    opts.AddSection("Analysis",
        "By default, check Keep blobs below quorum-confirmed GC barriers for"
        " incomplete local data or ingress layouts. Parts on missing VDisks may"
        " be inferred from ingress metadata."
        " --write2x2-candidates requires mirror-3-dc. Judge"
        " completeness using only local parts from captured VDisks, include blobs"
        " above barriers and accept barriers without quorum. Skip hard-collected"
        " and DoNotKeep blobs; blobs below soft barriers still require Keep."
        " Both modes filter out blobs with missing records on captured VDisks,"
        " too few restorable parts or insufficient subgroup coverage.");
    opts.AddSection("Output",
        "stdout: text dump of the input index records. Redirect to /dev/null"
        " to skip generating the dump.\n"
        "stderr: GroupId#/BlobId# reports with part layouts, Keep flags, VDisk"
        " coverage masks and GC barriers. An optional TrashSize# line"
        " estimates the size of misplaced and phantom-like parts."
        " In write2x2 mode this estimate may include blobs still being written"
        " above GC barriers; it does not establish that these parts can be deleted."
        " FULL marks a write2x2 candidate whose merged ingress layout is full"
        " while its local-part layout from captured VDisks is incomplete.");
    opts.AddSection("Exit status",
        "0: analysis completed, including when findings are reported."
        " Nonzero: argument or input error.");
    opts.AddSection("Examples",
        "ydb-dstool -e HOST:2135 group take-snapshot --group-ids 42 --output snapshot.bin"
        " blobsan mirror-3-dc < snapshot.bin > dump.txt 2> report.txt"
        " blobsan --write2x2-candidates mirror-3-dc < snapshot.bin > /dev/null 2> candidates.txt");
    NLastGetopt::TOptsParseResult args(&opts, argc, argv);
    const TString erasure = args.GetFreeArgs().front();

    char link[256];
    ssize_t rv = readlink("/proc/self/fd/1", link, sizeof(link));
    if (rv == 9 && strncmp(link, "/dev/null", rv) == 0) {
        OutputCout = false;
    }

    TBlobStorageGroupType::EErasureSpecies species;
    if (!TBlobStorageGroupType::ParseErasureName(species, erasure)) {
        Cerr << "invalid erasure species name: " << erasure << Endl;
        return 1;
    }
    if (Write2x2Candidates && species != TBlobStorageGroupType::ErasureMirror3dc) {
        Cerr << "--write2x2-candidates requires mirror-3-dc" << Endl;
        return 1;
    }
    const bool scanReport = !ScanReportPath.empty();
    if (scanReport != !ScanManifestPath.empty() || (scanReport && species != TBlobStorageGroupType::ErasureMirror3dc)) {
        Cerr << "--scan-report and --scan-manifest must be used together with mirror-3-dc" << Endl;
        return 1;
    }
    ui32 scanGroupId = 0;
    std::unordered_set<ui32> completed;
    if (scanReport) {
        TFileInput input(ScanManifestPath);
        NJson::TJsonValue manifest;
        if (!NJson::ReadJsonTree(&input, &manifest) || !manifest["group_id"].IsUInteger()
                || manifest["group_id"].GetUInteger() > Max<ui32>() || !manifest["completed_vdisks"].IsArray()) {
            Cerr << "Invalid scan manifest" << Endl;
            return 1;
        }
        scanGroupId = manifest["group_id"].GetUInteger();
        for (const auto& index : manifest["completed_vdisks"].GetArray()) {
            if (!index.IsUInteger() || index.GetUInteger() >= 9 || !completed.insert(index.GetUInteger()).second) {
                Cerr << "Invalid or duplicate VDisk in scan manifest" << Endl;
                return 1;
            }
        }
        // A complete stream may contain no blocks. Its empty index must still
        // participate in coverage and in the existing candidate checks.
        for (const ui32 index : completed) {
            Groups[scanGroupId].States[index];
        }
        Groups[scanGroupId];
    }
    TBlobStorageGroupType type(species);
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
            if (scanReport && (header.GroupId != scanGroupId || !completed.contains(header.Index))) {
                Cerr << "Snapshot block is outside the scan manifest" << Endl;
                return 1;
            }
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
    std::map<std::tuple<TString, TString, TString, bool, bool>, TScanCounters> scanBuckets;
    TScanCounters scanTotals;

    for (auto& [groupId, group] : Groups) {
        struct TBlobRecord {
            TBlobInfo Info;
            TSubgroupPartLayout ObservedLocalLayout;
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

                record.ObservedLocalLayout.Merge(blob.LocalLayout, type);
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
            const ui32 numNodes = std::popcount(blob.Info.Nodes);

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

            ui32 seenDisks = 0;
            for (ui32 i = 0; i < BlobSubgroupSize; ++i) {
                if (group.States.contains(nums[i])) {
                    seenDisks |= 1 << i;
                }
            }
            const bool phantomLike = seenDisks != blob.Info.Nodes;
            const bool gcEligible = !blob.Hard && blob.Info.KeepMode != DO_NOT_KEEP
                && (!blob.UnderBarrierMask || blob.Info.KeepMode == KEEP);
            const bool restorable = !phantomLike && numParts >= type.MinimalRestorablePartCount()
                && numLocalParts >= type.MinimalRestorablePartCount()
                && checker.CheckFailModelForSubgroup(~TBlobStorageGroupInfo::TSubgroupVDisks::CreateFromMask(
                    &info.GetTopology(), blob.Info.Nodes));
            const bool fullLocalLayout = isFull(originalLocalLayout);
            const bool write2x2Candidate = gcEligible && !fullLocalLayout && restorable;

            if (scanReport) {
                const TString gc = blob.Hard ? "hard_barrier"
                    : blob.Info.KeepMode == DO_NOT_KEEP ? "do_not_keep"
                    : blob.UnderBarrierMask ? (blob.Info.KeepMode == KEEP ? "soft_with_keep" : "soft_without_keep")
                    : "no_covering_barrier";
                ui32 replicas = 0;
                const TString local = ScanLayout(blob.ObservedLocalLayout, nums, info, &completed, &replicas);
                const TString ingress = ScanLayout(blob.Info.Layout, nums, info, nullptr);
                auto add = [&](TScanCounters& counters) {
                    ++counters.Blobs;
                    counters.LogicalBytes += blobId.BlobSize();
                    counters.ObservedReplicaBytes += ui64(replicas) * blobId.BlobSize();
                    counters.MissingMetadataBlobs += phantomLike;
                };
                add(scanBuckets[{local, ingress, gc, phantomLike, write2x2Candidate}]);
                add(scanTotals);
            }
            if (!gcEligible) {
                continue;
            }
            if (!Write2x2Candidates && (!blob.UnderBarrierMask || blob.Info.KeepMode != KEEP)) {
                continue;
            }

            if (Write2x2Candidates ? fullLocalLayout :
                    (isFull(blob.Info.Layout) && isFull(blob.Info.ReflectionLayout) && isFull(blob.Info.LocalLayout))) {
                // looks like correct blob -- either it is not under barrier (and may be being written right now), or
                // it is with keep flags and fully written with correct ingress bits on every disk
                continue;
            }

            if (!restorable) {
                // looks like phantom blob -- count found parts as trash
                for (ui32 partIdx = 0; partIdx < TotalPartCount; ++partIdx) {
                    if (const ui32 disks = blob.Info.LocalLayout.GetDisksWithPart(partIdx)) {
                        trashSize += type.PartSize(blobId) * std::popcount(disks);
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

            err << "GroupId# " << groupId << " BlobId# " << blobId << (Write2x2Candidates && isFull(blob.Info.Layout) ? " FULL" : "")
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
                << "\n\tNumUnderBarrier#    " << std::popcount(blob.UnderBarrierMask)
                << "\n\tUnderBarrierMask#   " << FormatBitMask(blob.UnderBarrierMask, BlobSubgroupSize)
                << "\n\tNumDiscard#         " << std::popcount(blob.DiscardMask)
                << "\n\tDiscardMask#        " << FormatBitMask(blob.DiscardMask, BlobSubgroupSize)
                << "\n\tKeepMask#           " << FormatBitMask(blob.KeepMask, BlobSubgroupSize)
                << "\n\tDoNotKeepMask#      " << FormatBitMask(blob.DoNotKeepMask, BlobSubgroupSize)
                << "\n\tMisplaced#          " << (misplacedParts ? "true" : "false");

            for (const ui32 index : indexes) {
                const auto& vdisk = group.States.at(index);
                const auto& k = std::make_tuple(blobId.TabletID(), blobId.Channel());
                if (const auto it = vdisk.Barriers.find(k); it != vdisk.Barriers.end()) {
                    const auto nodeId = std::find(nums.begin(), nums.end(), index) - nums.begin();
                    const bool keep = blob.KeepMask >> nodeId & 1;
                    err << "\n\tBarrier[" << index << "]#         " << it->second.ToString(blobId.Generation(), blobId.Step(), keep);
                }
            }

            err << Endl;
        }
    }

    if (trashSize) {
        err << "TrashSize# " << trashSize << Endl;
    }

    if (scanReport) {
        NJson::TJsonValue report(NJson::JSON_MAP);
        report["version"] = 1;
        report["group_id"] = scanGroupId;
        report["completed_vdisks"] = NJson::TJsonValue(NJson::JSON_ARRAY);
        for (ui32 index = 0; index < 9; ++index) {
            if (completed.contains(index)) {
                report["completed_vdisks"].AppendValue(index);
            }
        }
        report["totals"] = scanTotals.ToJson();
        report["buckets"] = NJson::TJsonValue(NJson::JSON_ARRAY);
        for (const auto& [key, counters] : scanBuckets) {
            auto bucket = counters.ToJson();
            const auto& [local, ingress, gc, phantomLike, write2x2Candidate] = key;
            bucket["local"] = local;
            bucket["ingress"] = ingress;
            bucket["gc"] = gc;
            bucket["phantom_like"] = phantomLike;
            bucket["write2x2_candidate"] = Write2x2Candidates
                ? NJson::TJsonValue(write2x2Candidate) : NJson::TJsonValue(NJson::JSON_NULL);
            report["buckets"].AppendValue(std::move(bucket));
        }
        TFileOutput output(ScanReportPath);
        NJsonWriter::TBuf json;
        json.WriteJsonValue(&report);
        output << json.Str() << Endl;
    }

    return 0;
}
