#pragma once

#include <ydb/library/yql/public/purecalc/purecalc.h>

namespace NYql {
    namespace NPureCalc {
        template <typename T>
        class TEmptyStreamImpl: public IStream<T> {
        public:
            T Fetch() override {
                return nullptr;
            }
        };

        template <typename T>
        THolder<IStream<T>> EmptyStream() {
            return MakeHolder<TEmptyStreamImpl<T>>();
        }
    }
}
