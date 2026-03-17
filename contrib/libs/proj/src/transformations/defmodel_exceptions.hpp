/******************************************************************************
 * Project:  PROJ
 * Purpose:  Functionality related to deformation model
 * Author:   Even Rouault, <even.rouault at spatialys.com>
 *
 ******************************************************************************
 * Copyright (c) 2020, Even Rouault, <even.rouault at spatialys.com>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included
 * in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
 * OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
 * THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 *****************************************************************************/

#ifndef DEFORMATON_MODEL_NAMESPACE
#error "Should be included only by defmodel.hpp"
#endif

#include <exception>

namespace DEFORMATON_MODEL_NAMESPACE {

// ---------------------------------------------------------------------------

/** Parsing exception. */
class ParsingException : public std::exception {
  public:
    explicit ParsingException(const std::string &msg) : msg_(msg) {}
    const char *what() const noexcept override;

  private:
    std::string msg_;
};

const char *ParsingException::what() const noexcept { return msg_.c_str(); }

// ---------------------------------------------------------------------------

class UnimplementedException : public std::exception {
  public:
    explicit UnimplementedException(const std::string &msg) : msg_(msg) {}
    const char *what() const noexcept override;

  private:
    std::string msg_;
};

const char *UnimplementedException::what() const noexcept {
    return msg_.c_str();
}

// ---------------------------------------------------------------------------

/** Evaluator exception. */
class EvaluatorException : public std::exception {
  public:
    explicit EvaluatorException(const std::string &msg) : msg_(msg) {}
    const char *what() const noexcept override;

  private:
    std::string msg_;
};

const char *EvaluatorException::what() const noexcept { return msg_.c_str(); }

// ---------------------------------------------------------------------------

} // namespace DEFORMATON_MODEL_NAMESPACE
