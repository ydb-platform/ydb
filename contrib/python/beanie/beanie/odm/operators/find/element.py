from abc import ABC
from typing import List, Union

from beanie.odm.operators.find import BaseFindOperator


class BaseFindElementOperator(BaseFindOperator, ABC): ...


class Exists(BaseFindElementOperator):
    """
    `$exists` query operator

    Example:

    ```python
    class Product(Document):
        price: float

    Exists(Product.price, True)
    ```

    Will return query object like

    ```python
    {"price": {"$exists": True}}
    ```

    MongoDB doc:
    <https://docs.mongodb.com/manual/reference/operator/query/exists/>
    """

    def __init__(
        self,
        field,
        value: bool = True,
    ):
        self.field = field
        self.value = value

    @property
    def query(self):
        return {self.field: {"$exists": self.value}}


class Type(BaseFindElementOperator):
    """
    `$type` query operator

    Example:

    ```python
    class Product(Document):
        price: float

    Type(Product.price, "decimal")
    ```

    Will return query object like

    ```python
    {"price": {"$type": "decimal"}}
    ```

    MongoDB doc:
    <https://docs.mongodb.com/manual/reference/operator/query/type/>
    """

    def __init__(self, field, types: Union[List[str], str]):
        self.field = field
        self.types = types

    @property
    def query(self):
        return {self.field: {"$type": self.types}}
