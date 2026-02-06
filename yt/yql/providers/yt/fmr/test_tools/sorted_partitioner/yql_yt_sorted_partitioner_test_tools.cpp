#include "yql_yt_sorted_partitioner_test_tools.h"

#include <yt/yql/providers/yt/fmr/job/impl/yql_yt_sorted_merge_reader.h>
#include <yt/yql/providers/yt/fmr/utils/yql_yt_parser_fragment_list_index.h>
#include <yt/yql/providers/yt/fmr/utils/yson_block_iterator/impl/yql_yt_yson_yt_block_iterator.h>
#include <yt/yql/providers/yt/fmr/yt_job_service/file/yql_yt_file_yt_job_service.h>

#include <util/stream/file.h>
#include <util/string/builder.h>

namespace NYql::NFmr::NTestTools {

namespace {

NYT::TNode KeyNode(ui64 v) {
    NYT::TNode m = NYT::TNode::CreateMap();
    m["k"] = static_cast<i64>(v);
    return m;
}

TChunkStats MakeSortedChunk(ui64 weight, ui64 firstK, ui64 lastK) {
    TSortedChunkStats s;
    s.IsSorted = true;
    s.FirstRowKeys = KeyNode(firstK);
    s.LastRowKeys = KeyNode(lastK);
    return TChunkStats{.Rows = 1, .DataWeight = weight, .SortedChunkStats = s};
}

TString MakeRowTextYson(ui64 k, ui64 id) {
    return TStringBuilder() << "{\"k\"=" << static_cast<i64>(k) << ";\"id\"=" << static_cast<i64>(id) << "};\n";
}

TTempFileHandle WriteTempFile(const TString& content) {
    TTempFileHandle file;
    TFileOutput out(file.Name());
    out.Write(content.data(), content.size());
    return file;
}

TStringBuf RowBytesFromMarkup(const TString& blob, const TRowIndexMarkup& markup) {
    auto boundary = markup.back();
    if (boundary.EndOffset < blob.size() && blob[boundary.EndOffset] == ';') {
        ++boundary.EndOffset;
    }
    return SliceRange(blob, boundary);
}

void AddFingerprintsFromBinaryYson(const TString& binaryYson, const TVector<TString>& keyColumns, TFingerprintCounts& out) {
    TParserFragmentListIndex parser(binaryYson, keyColumns);
    parser.Parse();
    const auto& rows = parser.GetRows();
    for (const auto& r : rows) {
        TString fp(RowBytesFromMarkup(binaryYson, r));
        ++out[fp];
    }
}

TMaybe<size_t> FindTableIndexById(const TVector<TGeneratedSortedTable>& tables, const TString& id) {
    for (size_t i = 0; i < tables.size(); ++i) {
        if (tables[i].TableId.Id == id) {
            return i;
        }
    }
    return Nothing();
}

} // namespace

TGeneratedSortedTable GenerateBoundaryKeySpanningMultipleChunksTable(const TBoundaryKeyTableSpec& spec, ui64 idBase) {
    TGeneratedSortedTable t;
    t.TableId = TFmrTableId(spec.Cluster, spec.Path);
    t.PartId = spec.PartId;

    {
        TString body;
        ui64 id = idBase;
        for (ui64 k = spec.LowStart; k <= spec.LowEnd; ++k) {
            for (ui64 i = 0; i < spec.RowsPerLowKey; ++i) {
                body += MakeRowTextYson(k, id++);
            }
        }
        for (ui64 i = 0; i < spec.BoundaryTailCount; ++i) {
            body += MakeRowTextYson(spec.BoundaryKey, id++);
        }
        t.ChunkFiles.push_back(WriteTempFile(body));
        t.ChunkStats.push_back(MakeSortedChunk(spec.ChunkWeight, spec.LowStart, spec.BoundaryKey));
    }

    {
        TString body;
        ui64 id = idBase + 1'000'000;
        for (ui64 i = 0; i < spec.BoundaryHeadCount1; ++i) {
            body += MakeRowTextYson(spec.BoundaryKey, id++);
        }
        for (ui64 i = 0; i < spec.BoundaryHeadCount2; ++i) {
            body += MakeRowTextYson(spec.BoundaryKey, id++);
        }
        for (ui64 k = spec.MidStart; k <= spec.MidEnd; ++k) {
            for (ui64 i = 0; i < spec.RowsPerMidKey; ++i) {
                body += MakeRowTextYson(k, id++);
            }
        }
        t.ChunkFiles.push_back(WriteTempFile(body));
        t.ChunkStats.push_back(MakeSortedChunk(spec.ChunkWeight, spec.BoundaryKey, spec.MidEnd));
    }

    {
        TString body;
        ui64 id = idBase + 2'000'000;
        for (ui64 k = spec.HighStart; k <= spec.HighEnd; ++k) {
            for (ui64 i = 0; i < spec.RowsPerHighKey; ++i) {
                body += MakeRowTextYson(k, id++);
            }
        }
        t.ChunkFiles.push_back(WriteTempFile(body));
        t.ChunkStats.push_back(MakeSortedChunk(spec.ChunkWeight, spec.HighStart, spec.HighEnd));
    }

    return t;
}

TGeneratedSortedTable GenerateBoundaryKeySpanning3ChunksTable(const TBoundaryKeyTableSpec& spec, ui64 idBase) {
    return GenerateBoundaryKeySpanningMultipleChunksTable(spec, idBase);
}

void BuildPartitionerInputs(
    const TVector<TGeneratedSortedTable>& tables,
    std::unordered_map<TFmrTableId, std::vector<TString>>& partIdsForTables,
    std::unordered_map<TString, std::vector<TChunkStats>>& partIdStats
) {
    partIdsForTables.clear();
    partIdStats.clear();

    for (const auto& t : tables) {
        partIdsForTables[t.TableId] = {t.PartId};
        partIdStats[t.PartId] = std::vector<TChunkStats>(t.ChunkStats.begin(), t.ChunkStats.end());
    }
}

TFingerprintCounts CountSourceFingerprintsFromFiles(
    const TVector<TGeneratedSortedTable>& tables,
    const TVector<TString>& keyColumns
) {
    auto jobService = MakeFileYtJobService();
    TFingerprintCounts src;

    for (const auto& t : tables) {
        for (const auto& f : t.ChunkFiles) {
            NYT::TRichYPath richPath;
            TYtTableRef ytTable(richPath, f.Name());
            auto reader = jobService->MakeReader(ytTable);
            const TString binary = reader->ReadAll();
            AddFingerprintsFromBinaryYson(binary, keyColumns, src);
        }
    }

    return src;
}

TFingerprintCounts CountTaskFingerprintsFromFiles(
    const std::vector<TTaskTableInputRef>& tasks,
    const TVector<TGeneratedSortedTable>& tables,
    const TVector<TString>& keyColumns
) {
    auto jobService = MakeFileYtJobService();
    TFingerprintCounts dst;

    for (const auto& task : tasks) {
        TVector<IBlockIterator::TPtr> iters;
        iters.reserve(task.Inputs.size());

        for (const auto& input : task.Inputs) {
            if (!std::holds_alternative<TFmrTableInputRef>(input)) {
                continue;
            }
            const auto& fmr = std::get<TFmrTableInputRef>(input);
            if (fmr.TableRanges.empty()) {
                continue;
            }

            const auto tableIdx = FindTableIndexById(tables, fmr.TableId);
            if (!tableIdx.Defined()) {
                continue;
            }
            const auto& table = tables[*tableIdx];

            TVector<NYT::TRawTableReaderPtr> readers;
            for (const auto& tr : fmr.TableRanges) {
                for (ui64 chunk = tr.MinChunk; chunk < tr.MaxChunk; ++chunk) {
                    if (chunk >= table.ChunkFiles.size()) {
                        continue;
                    }
                    NYT::TRichYPath richPath;
                    TYtTableRef ytTable(richPath, table.ChunkFiles[chunk].Name());
                    readers.push_back(jobService->MakeReader(ytTable));
                }
            }

            if (!readers.empty()) {
                iters.push_back(MakeIntrusive<TYtBlockIterator>(
                    std::move(readers),
                    keyColumns,
                    TYtBlockIteratorSettings{},
                    TVector<ESortOrder>(keyColumns.size(), ESortOrder::Ascending),
                    fmr.IsFirstRowInclusive,
                    fmr.FirstRowKeys,
                    fmr.LastRowKeys
                ));
            }
        }

        if (iters.empty()) {
            continue;
        }

        auto merge = MakeIntrusive<TSortedMergeReader>(
            std::move(iters),
            TVector<ESortOrder>(keyColumns.size(), ESortOrder::Ascending)
        );
        const TString mergedBinary = merge->ReadAll();
        AddFingerprintsFromBinaryYson(mergedBinary, keyColumns, dst);
    }

    return dst;
}

std::pair<TFingerprintCounts, TFingerprintCounts> DiffFingerprintMultisets(
    const TFingerprintCounts& src,
    const TFingerprintCounts& dst
) {
    TFingerprintCounts onlyInSrc;
    TFingerprintCounts onlyInDst;

    for (const auto& [fp, sc] : src) {
        const ui64 dc = dst.Value(fp, 0);
        if (sc > dc) {
            const ui64 diff = sc - dc;
            onlyInSrc[fp] = diff;
        }
    }

    for (const auto& [fp, dc] : dst) {
        const ui64 sc = src.Value(fp, 0);
        if (dc > sc) {
            const ui64 diff = dc - sc;
            onlyInDst[fp] = diff;
        }
    }

    return {std::move(onlyInSrc), std::move(onlyInDst)};
}

} // namespace NYql::NFmr::NTestTools

