#pragma once

#include <ydb/library/yql/public/purecalc/purecalc.h>

namespace NYql {
    namespace NPureCalc {
        class TFakeInputSpec: public TInputSpecBase {
        public:
            TVector<NYT::TNode> Schemas = {NYT::TNode::CreateList()};

        public:
            const TVector<NYT::TNode>& GetSchemas() const override {
                return Schemas;
            }
        };

        class TFakeOutputSpec: public TOutputSpecBase {
        public:
            NYT::TNode Schema = NYT::TNode::CreateList();

        public:
            const NYT::TNode& GetSchema() const override {
                return Schema;
            }
        };

        template <>
        struct TInputSpecTraits<TFakeInputSpec> {
            static const constexpr bool IsPartial = false;

            static const constexpr bool SupportPullStreamMode = false;
            static const constexpr bool SupportPullListMode = false;
            static const constexpr bool SupportPushStreamMode = false;

            using TConsumerType = void;
        };

        template <>
        struct TOutputSpecTraits<TFakeOutputSpec> {
            static const constexpr bool IsPartial = false;

            static const constexpr bool SupportPullStreamMode = false;
            static const constexpr bool SupportPullListMode = false;
            static const constexpr bool SupportPushStreamMode = false;

            using TPullStreamReturnType = void;
            using TPullListReturnType = void;
        };

        NYT::TNode MakeFakeSchema(bool pg = false);
        TFakeInputSpec FakeIS(ui32 inputsNumber = 1, bool pg = false);
        TFakeOutputSpec FakeOS(bool pg = false);
    }
}
