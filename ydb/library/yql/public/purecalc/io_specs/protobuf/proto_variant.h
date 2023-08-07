#pragma once

#include <ydb/library/yql/public/purecalc/common/interface.h>

#include <array>

namespace NYql::NPureCalc::NPrivate {
    using TProtoRawMultiOutput = std::pair<ui32, google::protobuf::Message*>;

    template <typename... T>
    using TProtoMultiOutput = std::variant<T*...>;

    template <size_t I, typename... T>
    using TProtoOutput = std::add_pointer_t<typename TTypeList<T...>::template TGet<I>>;

    template <size_t I, typename... T>
    TProtoMultiOutput<T...> InitProtobufsVariant(google::protobuf::Message* ptr) {
        static_assert(std::conjunction_v<std::is_base_of<google::protobuf::Message, T>...>);
        return TProtoMultiOutput<T...>(std::in_place_index<I>, static_cast<TProtoOutput<I, T...>>(ptr));
    }

    template <typename... T>
    class TProtobufsMappingBase {
    public:
        TProtobufsMappingBase()
            : InitFuncs_(BuildInitFuncs(std::make_index_sequence<sizeof...(T)>()))
        {
        }

    private:
        typedef TProtoMultiOutput<T...> (*initfunc)(google::protobuf::Message*);

        template <size_t... I>
        inline std::array<initfunc, sizeof...(T)> BuildInitFuncs(std::index_sequence<I...>) {
            return {&InitProtobufsVariant<I, T...>...};
        }

    protected:
        const std::array<initfunc, sizeof...(T)> InitFuncs_;
    };

    template <typename... T>
    class TProtobufsMappingStream: public IStream<TProtoMultiOutput<T...>>, public TProtobufsMappingBase<T...> {
    public:
        TProtobufsMappingStream(THolder<IStream<TProtoRawMultiOutput>> oldStream)
            : OldStream_(std::move(oldStream))
        {
        }

    public:
        TProtoMultiOutput<T...> Fetch() override {
            auto&& oldItem = OldStream_->Fetch();
            return this->InitFuncs_[oldItem.first](oldItem.second);
        }

    private:
        THolder<IStream<TProtoRawMultiOutput>> OldStream_;
    };

    template <typename... T>
    class TProtobufsMappingConsumer: public IConsumer<TProtoRawMultiOutput>, public TProtobufsMappingBase<T...> {
    public:
        TProtobufsMappingConsumer(THolder<IConsumer<TProtoMultiOutput<T...>>> oldConsumer)
            : OldConsumer_(std::move(oldConsumer))
        {
        }

    public:
        void OnObject(TProtoRawMultiOutput oldItem) override {
            OldConsumer_->OnObject(this->InitFuncs_[oldItem.first](oldItem.second));
        }

        void OnFinish() override {
            OldConsumer_->OnFinish();
        }

    private:
        THolder<IConsumer<TProtoMultiOutput<T...>>> OldConsumer_;
    };
}
