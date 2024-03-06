#include "internal_client.h"

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

void TSerializableHunkDescriptor::Register(TRegistrar registrar)
{
    registrar.BaseClassParameter("chunk_id", &TThis::ChunkId);
    registrar.BaseClassParameter("erasure_codec", &TThis::ErasureCodec)
        .Optional();
    registrar.BaseClassParameter("block_index", &TThis::BlockIndex);
    registrar.BaseClassParameter("block_offset", &TThis::BlockOffset);
    registrar.BaseClassParameter("block_size", &TThis::BlockSize)
        .Optional();
    registrar.BaseClassParameter("length", &TThis::Length);
}

TSerializableHunkDescriptor::TSerializableHunkDescriptor(const THunkDescriptor& descriptor)
    : THunkDescriptor(descriptor)
{
    ::NYT::NYTree::TYsonStructRegistry::Get()->InitializeStruct(this);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
