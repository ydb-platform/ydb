#include "common.h"

namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering {

NArrow::TSimpleRow TPortionBorderView::GetIndexKeyVerified(const TPortionStore& portions) const {
    return GetIndexKey(*portions.GetPortionVerified(PortionId));
}

}   // namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering
