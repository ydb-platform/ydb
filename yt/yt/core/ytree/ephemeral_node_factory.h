#pragma once

#include "node.h"

namespace NYT::NYTree {

////////////////////////////////////////////////////////////////////////////////

//! Builds a transactional factory for creating ephemeral
//! (non-persistent, in memory) YTree nodes.
std::unique_ptr<ITransactionalNodeFactory> CreateEphemeralNodeFactory(bool shouldHideAttributes = false);

//! Returns a cached instance of non-transactional factory.
INodeFactory* GetEphemeralNodeFactory(bool shouldHideAttributes = false);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree

