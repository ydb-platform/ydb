#include "block_field_serializer.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/protos/dirty_map.pb.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

void SaveBlockField(const TBlockRangeField& field, TBlockFieldProto* proto)
{
    proto->Clear();
    if (field.Empty()) {
        return;
    }

    if (field.IsBitmapBased()) {
        proto->SetBitMask(field.Serialize());
    } else {
        proto->SetRunLengthEncoding(field.Serialize());
    }
}

void LoadBlockField(const TBlockFieldProto& proto, TBlockRangeField* field)
{
    field->Clear();
    switch (proto.GetEncodingCase()) {
        case TBlockFieldProto::kRunLengthEncoding: {
            field->DeserializeFromRLE(proto.GetRunLengthEncoding());
            break;
        }
        case TBlockFieldProto::kBitMask: {
            field->DeserializeFromBitmap(proto.GetBitMask());
            break;
        }
        case TBlockFieldProto::ENCODING_NOT_SET:
            break;
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
