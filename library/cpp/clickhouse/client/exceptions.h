#pragma once

#include "query.h"

#include <util/generic/yexception.h>

namespace NClickHouse {
    class TServerException: public yexception {
    public:
        TServerException(std::unique_ptr<TException> e)
            : Exception_(std::move(e))
        {
        }

        const TException& GetException() const {
            return *Exception_;
        }

        const char* what() const noexcept override {
            return Exception_->DisplayText.c_str();
        }

    private:
        std::unique_ptr<TException> Exception_;
    };

}
