#pragma once

#include <ydb/library/yql/public/purecalc/common/interface.h>

namespace NYql {
    namespace NPureCalc {
        namespace NPrivate {
            template <typename T>
            class TVectorStream final: public IStream<T*> {
            private:
                size_t I_;
                TVector<T> Data_;

            public:
                explicit TVectorStream(TVector<T> data)
                    : I_(0)
                    , Data_(std::move(data))
                {
                }

            public:
                T* Fetch() override {
                    if (I_ >= Data_.size()) {
                        return nullptr;
                    } else {
                        return &Data_[I_++];
                    }
                }
            };
        }

        /**
         * Convert vector into a purecalc stream.
         */
        template <typename T>
        THolder<IStream<T*>> StreamFromVector(TVector<T> data) {
            return MakeHolder<NPrivate::TVectorStream<T>>(std::move(data));
        }
    }
}
