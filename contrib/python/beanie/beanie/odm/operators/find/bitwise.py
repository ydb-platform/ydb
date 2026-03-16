from typing import Union

from beanie.odm.fields import ExpressionField
from beanie.odm.operators.find import BaseFindOperator


class BaseFindBitwiseOperator(BaseFindOperator):
    operator = ""

    def __init__(self, field: Union[str, ExpressionField], bitmask):
        self.field = field
        self.bitmask = bitmask

    @property
    def query(self):
        return {self.field: {self.operator: self.bitmask}}


class BitsAllClear(BaseFindBitwiseOperator):
    """
    `$bitsAllClear` query operator

    MongoDB doc:
    <https://docs.mongodb.com/manual/reference/operator/query/bitsAllClear/>
    """

    operator = "$bitsAllClear"


class BitsAllSet(BaseFindBitwiseOperator):
    """
    `$bitsAllSet` query operator

    MongoDB doc:
    https://docs.mongodb.com/manual/reference/operator/query/bitsAllSet/
    """

    operator = "$bitsAllSet"


class BitsAnyClear(BaseFindBitwiseOperator):
    """
    `$bitsAnyClear` query operator

    MongoDB doc:
    https://docs.mongodb.com/manual/reference/operator/query/bitsAnyClear/
    """

    operator = "$bitsAnyClear"


class BitsAnySet(BaseFindBitwiseOperator):
    """
    `$bitsAnySet` query operator

    MongoDB doc:
    https://docs.mongodb.com/manual/reference/operator/query/bitsAnySet/
    """

    operator = "$bitsAnySet"
