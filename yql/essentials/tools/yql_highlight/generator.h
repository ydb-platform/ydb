#pragma once

#include <yql/essentials/sql/v1/highlight/sql_highlight.h>

#include <util/generic/ptr.h>
#include <util/folder/path.h>

namespace NSQLHighlight {

    class IGenerator: public TThrRefBase {
    public:
        using TPtr = TIntrusivePtr<IGenerator>;

        virtual void Write(IOutputStream& out, const THighlighting& highlighting) = 0;
        virtual void Write(const TFsPath& path, const THighlighting& highlighting) = 0;
    };

    IGenerator::TPtr MakeOnlyFileGenerator(std::function<void(IOutputStream&, const THighlighting&)> function);

} // namespace NSQLHighlight
