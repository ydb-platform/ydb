#pragma once

#include <yql/essentials/minikql/mkql_node.h>

#include <arrow/memory_pool.h>
#include <arrow/type.h>

namespace NYql {

class IYtOutputColumnConverter {
public:
    using TPtr = std::unique_ptr<IYtOutputColumnConverter>;

    virtual ~IYtOutputColumnConverter() = default;

    virtual std::shared_ptr<arrow20::Field> BuildSchemaField(std::string name) = 0;
    virtual std::shared_ptr<arrow20::ArrayData> Convert(std::shared_ptr<arrow20::ArrayData> block) = 0;
};

IYtOutputColumnConverter::TPtr MakeYtOutputColumnConverter(NKikimr::NMiniKQL::TType* type, arrow20::MemoryPool* pool);

}
