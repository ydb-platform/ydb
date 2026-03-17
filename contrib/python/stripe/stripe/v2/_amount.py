# -*- coding: utf-8 -*-
# NOT codegenned
from typing_extensions import TypedDict
from stripe._stripe_object import StripeObject


class Amount(StripeObject):
    value: int
    currency: str


class AmountParam(TypedDict):
    value: int
    currency: str
