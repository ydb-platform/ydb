#pragma once

#include <memory>

namespace NKikimr::NPQ::NNameResolver {

struct TTopicNames;

using TTopicNamesPtr = std::shared_ptr<const TTopicNames>;

} // namespace NKikimr::NPQ::NNameResolver
