#pragma once

#include <ydb/core/scheme/scheme_type_id.h>
#include <ydb/core/scheme/scheme_tablecell.h>
#include <util/generic/ptr.h>
#include <util/generic/hash.h>

namespace NKikimr {

class IBlockBuilder {
public:
    virtual ~IBlockBuilder() = default;

    virtual bool Start(const std::vector<std::pair<TString, NScheme::TTypeInfo>>& columns, ui64 maxRowsInBlock, ui64 maxBytesInBlock, TString& err) = 0;
    virtual void AddRow(const TDbTupleRef& key, const TDbTupleRef& value) = 0;
    virtual TString Finish() = 0;
    virtual size_t Bytes() const = 0;

private:
    friend class TFormatFactory;

    virtual std::unique_ptr<IBlockBuilder> Clone() const = 0;
};


class TFormatFactory : public TThrRefBase {
public:
    void RegisterBlockBuilder(std::unique_ptr<IBlockBuilder>&& builder, const TString& format) {
        Formats[format] = std::move(builder);
    }

    std::unique_ptr<IBlockBuilder> CreateBlockBuilder(const TString& format) const {
        auto it = Formats.FindPtr(format);
        if (!it)
            return nullptr;
        return (*it)->Clone();
    }
private:
    THashMap<TString, std::unique_ptr<IBlockBuilder>> Formats;
};

}
