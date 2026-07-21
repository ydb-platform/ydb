#include <ydb/core/ydb_convert/table_description.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/base/table_index.h>

namespace {

bool HasEnoughIndexImplDescriptions(const NKikimrSchemeOp::TTableDescription& in) {
    for (const auto& tableIndex : in.GetTableIndexes()) {
        const ui32 implSize = tableIndex.IndexImplTableDescriptionsSize();

        switch (tableIndex.GetType()) {
            case NKikimrSchemeOp::EIndexType::EIndexTypeGlobal:
            case NKikimrSchemeOp::EIndexType::EIndexTypeGlobalAsync:
            case NKikimrSchemeOp::EIndexType::EIndexTypeGlobalUnique:
            case NKikimrSchemeOp::EIndexType::EIndexTypeGlobalFulltextPlain:
            case NKikimrSchemeOp::EIndexType::EIndexTypeGlobalJson:
                if (implSize < 1) {
                    return false;
                }
                break;
            case NKikimrSchemeOp::EIndexType::EIndexTypeGlobalVectorKmeansTree:
                if (implSize <= NKikimr::NTableIndex::NKMeans::PostingTablePosition) {
                    return false;
                }
                if (tableIndex.GetKeyColumnNames().size() > 1
                    && implSize <= NKikimr::NTableIndex::NKMeans::PrefixTablePosition)
                {
                    return false;
                }
                break;
            case NKikimrSchemeOp::EIndexType::EIndexTypeGlobalFulltextRelevance:
                if (implSize <= NKikimr::NTableIndex::NFulltext::PostingTablePosition) {
                    return false;
                }
                break;
            default:
                return false;
        }
    }
    return true;
}

}

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    Ydb::Table::DescribeTableResult out;
    NKikimrSchemeOp::TTableDescription in;

    (void)in.ParseFromArray(data, size);

    if (in.ColumnsSize() == 0) {
        auto* column = in.AddColumns();
        column->SetName("key");
        column->SetType("Uint64");
        in.AddKeyColumnNames("key");
    }

    if (!HasEnoughIndexImplDescriptions(in)) {
        return 0;
    }

    try {
        NKikimr::FillIndexDescription(out, in);
    } catch (...) {
    }

    return 0;
}
