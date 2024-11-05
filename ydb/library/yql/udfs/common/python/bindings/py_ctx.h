#pragma once

#include "py_ptr.h"

#include <ydb/library/yql/public/udf/udf_types.h>
#include <ydb/library/yql/public/udf/udf_type_builder.h>
#include <ydb/library/yql/public/udf/udf_type_inspection.h>
#include <ydb/library/yql/public/udf/udf_value_builder.h>
#include <ydb/library/yql/public/udf/udf_string.h>

#include <util/generic/ptr.h>
#include <util/generic/intrlist.h>

#include <unordered_map>

namespace NPython {

enum class EBytesDecodeMode {
    Never,
    Strict,
};

class IMemoryLock {
public:
    virtual ~IMemoryLock() = default;
    virtual void Acquire() = 0;
    virtual void Release() = 0;
};

struct TPyCleanupListItemBase: public TIntrusiveListItem<TPyCleanupListItemBase> {
    virtual ~TPyCleanupListItemBase() = default;
    virtual void Cleanup() = 0;
};

template <typename TValueType>
class TPyCleanupListItem: public TPyCleanupListItemBase {
public:
    TPyCleanupListItem() = default;
    virtual ~TPyCleanupListItem() {
        Unlink();
    }

    void Cleanup() override {
        Value = {};
    }

    template <typename TCtx>
    void Set(const TIntrusivePtr<TCtx>& ctx, TValueType val) {
        Value = std::move(val);
        ctx->CleanupList.PushBack(this);
    }

    bool IsSet() const {
        return !!Value;
    }

    const TValueType& Get() const {
        if (!Value) {
            throw yexception() << "Trying to use python wrap object with destroyed yql value";
        }
        return Value;
    }

private:
    TValueType Value;
};

struct TPyContext: public TSimpleRefCount<TPyContext> {
    const NKikimr::NUdf::ITypeInfoHelper::TPtr TypeInfoHelper;
    const NKikimr::NUdf::TStringRef ResourceTag;
    const NKikimr::NUdf::TSourcePosition Pos;
    TIntrusiveList<TPyCleanupListItemBase> CleanupList;

    TPyContext(NKikimr::NUdf::ITypeInfoHelper::TPtr helper, const NKikimr::NUdf::TStringRef& tag, const NKikimr::NUdf::TSourcePosition& pos)
        : TypeInfoHelper(std::move(helper))
        , ResourceTag(tag)
        , Pos(pos)
    {
    }

    void Cleanup() {
        for (auto& o: CleanupList) {
            o.Cleanup();
        }
        CleanupList.Clear();
    }

    ~TPyContext() = default;

    using TPtr = TIntrusivePtr<TPyContext>;
};

struct TPyCastContext: public TSimpleRefCount<TPyCastContext> {
    const NKikimr::NUdf::IValueBuilder *const ValueBuilder;
    const TPyContext::TPtr PyCtx;
    std::unordered_map<const NKikimr::NUdf::TType*, TPyObjectPtr> StructTypes;
    bool LazyInputObjects = true;
    TPyObjectPtr YsonConverterIn;
    TPyObjectPtr YsonConverterOut;
    EBytesDecodeMode BytesDecodeMode = EBytesDecodeMode::Never;
    TPyObjectPtr Decimal;
    std::unordered_map<ui32, TPyObjectPtr> TimezoneNames;
    THolder<IMemoryLock> MemoryLock;

    TPyCastContext(
            const NKikimr::NUdf::IValueBuilder* builder,
            TPyContext::TPtr pyCtx,
            THolder<IMemoryLock> memoryLock = {});

    ~TPyCastContext();

    const TPyObjectPtr& GetTimezoneName(ui32 id);
    const TPyObjectPtr& GetDecimal();

    using TPtr = TIntrusivePtr<TPyCastContext>;
};

using TPyCastContextPtr = TPyCastContext::TPtr;

} // namspace NPython
