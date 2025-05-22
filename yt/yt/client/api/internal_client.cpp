#include "internal_client.h"

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

void TSerializableHunkDescriptor::Register(TRegistrar registrar)
{
    registrar.BaseClassParameter("chunk_id", &TThis::ChunkId);
    registrar.BaseClassParameter("erasure_codec", &TThis::ErasureCodec)
        .Default(NErasure::ECodec::None);
    registrar.BaseClassParameter("block_index", &TThis::BlockIndex);
    registrar.BaseClassParameter("block_offset", &TThis::BlockOffset);
    registrar.BaseClassParameter("block_size", &TThis::BlockSize)
        .Default(std::nullopt);
    registrar.BaseClassParameter("length", &TThis::Length);
}

TSerializableHunkDescriptorPtr CreateSerializableHunkDescriptor(const THunkDescriptor& descriptor)
{
    auto serializableDescriptor = New<TSerializableHunkDescriptor>();
    serializableDescriptor->ChunkId = descriptor.ChunkId;
    serializableDescriptor->ErasureCodec = descriptor.ErasureCodec;
    serializableDescriptor->BlockIndex = descriptor.BlockIndex;
    serializableDescriptor->BlockOffset = descriptor.BlockOffset;
    serializableDescriptor->BlockSize = descriptor.BlockSize;
    serializableDescriptor->Length = descriptor.Length;
    serializableDescriptor->Postprocess();

    return serializableDescriptor;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
