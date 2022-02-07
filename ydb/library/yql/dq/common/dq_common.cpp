#include "dq_common.h"

IOutputStream& operator<<(IOutputStream& stream, const NYql::NDq::TTxId& txId) {
    std::visit([&stream](auto arg) {
        stream << arg;
    }, txId);
    return stream;
}
