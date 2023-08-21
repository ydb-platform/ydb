#pragma once

#include "public.h"

#include <yt/yt/client/table_client/public.h>

namespace NYT::NApi::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

IRowStreamEncoderPtr CreateWireRowStreamEncoder(NTableClient::TNameTablePtr nameTable);
IRowStreamDecoderPtr CreateWireRowStreamDecoder(NTableClient::TNameTablePtr nameTable);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy
