#pragma once

#include <library/cpp/threading/future/core/future.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/generic/ptr.h>

namespace NSQLComplete {

    using NThreading::TFuture;

    using TSqlName = std::string;
    using TSqlNamesList = TVector<TSqlName>;

    class TSqlNamespace {
    public:
        virtual TFuture<TSqlNamesList> GetNames() = 0;
        virtual TFuture<TSqlNamesList> GetNamesStartingWith(TStringBuf prefix);
        virtual ~TSqlNamespace() = default;
    };

    class TSqlKeywords final: public TSqlNamespace {
    public:
        explicit TSqlKeywords(TSqlNamesList keywords);

        TFuture<TSqlNamesList> GetNames() override;

    private:
        TSqlNamesList Keywords;
    };

} // namespace NSQLComplete
