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

        // TODO: Find a better name
        class TYQLNameSource {
        public:
            virtual TFuture<TYQLNamesList> GetNames() = 0;
            virtual TFuture<TYQLNamesList> GetNamesStartingWith(TStringBuf prefix);
            virtual ~TYQLNameSource() = default;
        };

        class TYQLKeywordSource final: public TYQLNameSource {
        public:
            explicit TYQLKeywordSource(TYQLNamesList keywords);

            TFuture<TYQLNamesList> GetNames() override;

        private:
            TYQLNamesList Keywords;
        };

    } // namespace NConsoleClient
} // namespace NYdb
