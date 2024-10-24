#pragma once

#include "public.h"

#include <yt/yt/core/yson/consumer.h>

namespace NYT::NYTree {

////////////////////////////////////////////////////////////////////////////////

//! Reconstructs a YTree from IYsonConsumer calls.
struct ITreeBuilder
    : public virtual NYson::IYsonConsumer
{
    //! Resets the instance.
    virtual void BeginTree() = 0;

    //! Returns the root node of the constructed tree.
    /*!
     *  \note
     *  Must be called after the tree is fully constructed.
     */
    virtual INodePtr EndTree() = 0;


    //! Enables inserting a pre-existing subtree into
    //! the currently constructed one.
    /*!
     *  The given subtree is injected as-is, no cloning is done.
     */
    virtual void OnNode(INodePtr node) = 0;
};

// COMPAT(omgronny): There are two different functions for creating a builder
// due to UDFs that depend on this code.
//! Creates a builder that makes explicit calls to the factory with limit of tree size.
/*!
 *  \param factory A factory used for materializing the nodes.
 *
 *  \param treeSizeLimit The maximum size of the tree.
 */
std::unique_ptr<ITreeBuilder> CreateBuilderFromFactory(
    INodeFactory* factory,
    int treeSizeLimit);

//! Creates a builder that makes explicit calls to the factory.
/*!
 *  \param factory A factory used for materializing the nodes.
 */
std::unique_ptr<ITreeBuilder> CreateBuilderFromFactory(INodeFactory* factory);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree
