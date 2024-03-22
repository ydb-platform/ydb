#include "read_context.h"

namespace NKikimr::NOlap::NReader {

IDataReader::IDataReader(const std::shared_ptr<TReadContext>& context)
    : Context(context) {
}

}
