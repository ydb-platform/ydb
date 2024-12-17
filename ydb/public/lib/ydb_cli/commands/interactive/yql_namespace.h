#pragma once

#include <library/cpp/threading/future/core/future.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/generic/ptr.h>

namespace NYdb {
    namespace NConsoleClient {

        using NThreading::TFuture;

        using TYQLName = std::string;
        using TYQLNamesList = TVector<TYQLName>;

        class TYQLNamespace {
        public:
            virtual TFuture<TYQLNamesList> GetNames() = 0;
            virtual TFuture<TYQLNamesList> GetNamesStartingWith(TStringBuf prefix);
            virtual ~TYQLNamespace() = default;
        };

        class TYQLKeywords final: public TYQLNamespace {
        public:
            explicit TYQLKeywords(TYQLNamesList keywords);

            TFuture<TYQLNamesList> GetNames() override;

        private:
            TYQLNamesList Keywords;
        };

    } // namespace NConsoleClient
} // namespace NYdb
