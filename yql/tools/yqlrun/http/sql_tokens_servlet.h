#pragma once

#include "servlet.h"

namespace NYql {
namespace NHttp {

///////////////////////////////////////////////////////////////////////////////
// TSqlTokensServlet
///////////////////////////////////////////////////////////////////////////////
class TSqlTokensServlet: public IServlet {
public:
    TSqlTokensServlet();
    void DoGet(const TRequest& req, TResponse& resp) const override final;

private:
    TString Script_;
};

} // namespace NHttp
} // namespace NYql
