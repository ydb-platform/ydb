#include "abstract.h"

namespace NKikimr::NOlap::NReader {

const TReadStats& TScanIteratorBase::GetStats() const {
    return Default<TReadStats>();
}

}