from dataclasses import dataclass

from pure_protobuf.dataclasses_ import field, message, one_of, part
from pure_protobuf.oneof import OneOf_

from ... import pricing
from .common import ProtobufSerializer, deserialize_one_of_field


@message
@dataclass
class StaticPricingStrategy(ProtobufSerializer[pricing.StaticPricingStrategy]):
    @staticmethod
    def serialize(obj: pricing.StaticPricingStrategy) -> 'StaticPricingStrategy':
        return StaticPricingStrategy()

    def deserialize(self) -> pricing.StaticPricingStrategy:
        return pricing.StaticPricingStrategy()


@message
@dataclass
class DynamicPricingStrategy(ProtobufSerializer[pricing.DynamicPricingStrategy]):
    max_ratio: float = field(1)

    @staticmethod
    def serialize(obj: pricing.DynamicPricingStrategy) -> 'DynamicPricingStrategy':
        return DynamicPricingStrategy(obj.max_ratio)

    def deserialize(self) -> pricing.DynamicPricingStrategy:
        return pricing.DynamicPricingStrategy(self.max_ratio)


@message
@dataclass
class PricingStrategy(ProtobufSerializer[pricing.PricingStrategy]):
    strategy: OneOf_ = one_of(
        static=part(StaticPricingStrategy, 1),
        dynamic=part(DynamicPricingStrategy, 2),
    )

    @staticmethod
    def serialize(obj: pricing.PricingStrategy) -> 'PricingStrategy':
        strategy = PricingStrategy()
        if isinstance(obj, pricing.StaticPricingStrategy):
            strategy.strategy.static = StaticPricingStrategy.serialize(obj)
        elif isinstance(obj, pricing.DynamicPricingStrategy):
            strategy.strategy.dynamic = DynamicPricingStrategy.serialize(obj)
        else:
            raise ValueError(f'unexpected pricing strategy type: {type(obj)}')
        return strategy

    def deserialize(self) -> pricing.PricingStrategy:
        return deserialize_one_of_field(self.strategy)
