#pragma once

#include "types.h"

#include <util/generic/strbuf.h>
#include <util/generic/list.h>
#include <util/generic/stack.h>

namespace NClickHouse {
    struct TTypeAst {
        enum EMeta {
            Array,
            Null,
            Nullable,
            Number,
            Terminal,
            Tuple,
            Enum
        };

        /// Type's category.
        EMeta Meta;
        /// Type's name.
        TStringBuf Name;
        /// Value associated with the node, used for fixed-width types and enum values.
        i64 Value = 0;
        /// Subelements of the type. Used to store enum's names and values as well.
        TList<TTypeAst> Elements;
    };

    class TTypeParser {
        struct TToken {
            enum EType {
                Invalid = 0,
                Name,
                Number,
                LPar,
                RPar,
                Comma,
                QuotedString, // string with quotation marks included
                EOS
            };

            EType Type;
            TStringBuf Value;
        };

    public:
        explicit TTypeParser(const TStringBuf& name);
        ~TTypeParser();

        bool Parse(TTypeAst* type);

    private:
        TToken NextToken();

    private:
        const char* Cur_;
        const char* End_;

        TTypeAst* Type_;
        TStack<TTypeAst*> OpenElements_;
    };

    /// Create type instance from string representation.
    TTypeRef ParseTypeFromString(const TStringBuf& type_name);

}
