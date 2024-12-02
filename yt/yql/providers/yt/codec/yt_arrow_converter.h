#include <yql/essentials/public/udf/arrow/block_builder.h>
#include <yql/essentials/minikql/mkql_node.h>

#include <arrow/io/interfaces.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/reader.h>
#include <arrow/array.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <arrow/result.h>
#include <arrow/buffer.h>

namespace NYql {

class IYtColumnConverter {
public:
    virtual arrow::Datum Convert(std::shared_ptr<arrow::ArrayData> block) = 0;
    virtual ~IYtColumnConverter() = default;
};

std::unique_ptr<IYtColumnConverter> MakeYtColumnConverter(NKikimr::NMiniKQL::TType* type, const NUdf::IPgBuilder* pgBuilder, arrow::MemoryPool& pool, bool isNative);
}
