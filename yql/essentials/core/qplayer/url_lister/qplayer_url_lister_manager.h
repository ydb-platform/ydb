#pragma once

#include <yql/essentials/core/url_lister/interface/url_lister_manager.h>

#include <yql/essentials/core/qplayer/storage/interface/yql_qstorage.h>

namespace NYql::NCommon {
IUrlListerManagerPtr WrapUrlListerManagerWithQContext(IUrlListerManagerPtr underlying, const TQContext& qContext);
} // namespace NYql::NCommon
