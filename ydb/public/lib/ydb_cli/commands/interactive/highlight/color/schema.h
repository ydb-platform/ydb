#pragma once

#include <contrib/restricted/patched/replxx/include/replxx.hxx>

#include <util/generic/fwd.h>

namespace NYdb::NConsoleClient {

    using TColor = replxx::Replxx::Color;

    struct TColorSchema {
        TColor keyword;
        TColor operation;
        struct {
            TColor function;
            TColor type;
            TColor variable;
            TColor quoted;
        } identifier;
        TColor string;
        TColor number;
        TColor comment;
        TColor unknown;
    };


    TColorSchema GetColorSchema(const std::string& name = "");

} // namespace NYdb::NConsoleClient
