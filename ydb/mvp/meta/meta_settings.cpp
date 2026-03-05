#include "meta_settings.h"

#include <ydb/mvp/meta/link_source.h>

namespace NMVP {

TSupportLinkSources::TSupportLinkSources() = default;
TSupportLinkSources::~TSupportLinkSources() = default;
TSupportLinkSources::TSupportLinkSources(TSupportLinkSources&&) noexcept = default;
TSupportLinkSources& TSupportLinkSources::operator=(TSupportLinkSources&&) noexcept = default;

} // namespace NMVP
