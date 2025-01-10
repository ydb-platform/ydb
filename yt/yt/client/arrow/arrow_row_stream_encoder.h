#pragma once

#include "public.h"

#include <yt/yt/client/api/rpc_proxy/public.h>

#include <yt/yt/client/table_client/public.h>

#include <yt/yt/client/formats/public.h>

namespace NYT::NArrow {

////////////////////////////////////////////////////////////////////////////////

NApi::NRpcProxy::IRowStreamEncoderPtr CreateArrowRowStreamEncoder(
    NTableClient::TTableSchemaPtr schema,
    std::optional<std::vector<std::string>> columns,
    NTableClient::TNameTablePtr nameTable,
    NApi::NRpcProxy::IRowStreamEncoderPtr fallbackEncoder,
    NFormats::TControlAttributesConfigPtr controlAttributesConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NArrow
