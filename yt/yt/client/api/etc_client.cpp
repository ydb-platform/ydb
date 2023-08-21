#include "etc_client.h"

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

bool TCheckClusterLivenessOptions::IsCheckTrivial() const
{
    return
        !CheckCypressRoot &&
        !CheckSecondaryMasterCells &&
        !CheckTabletCellBundle;
}

bool TCheckClusterLivenessOptions::operator==(const TCheckClusterLivenessOptions& other) const
{
    return
        CheckCypressRoot == other.CheckCypressRoot &&
        CheckSecondaryMasterCells == other.CheckSecondaryMasterCells &&
        CheckTabletCellBundle == other.CheckTabletCellBundle;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi

