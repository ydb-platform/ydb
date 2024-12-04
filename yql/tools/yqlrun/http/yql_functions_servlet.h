#pragma once

#include "servlet.h"


namespace NYql {
namespace NHttp {

///////////////////////////////////////////////////////////////////////////////
// TYqlFunctoinsServlet
///////////////////////////////////////////////////////////////////////////////
class TYqlFunctoinsServlet: public IServlet
{
public:
    void DoGet(const TRequest& req, TResponse& resp) const override final;
};

} // namspace NNttp
} // namspace NYql
