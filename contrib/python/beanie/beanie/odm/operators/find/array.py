from abc import ABC
from typing import Any, Optional

from beanie.odm.operators.find import BaseFindOperator


class BaseFindArrayOperator(BaseFindOperator, ABC): ...


class All(BaseFindArrayOperator):
    """
    `$all` array query operator

    Example:

    ```python
    class Sample(Document):
        results: List[int]

    All(Sample.results, [80, 85])
    ```

    Will return query object like

    ```python
    {"results": {"$all": [80, 85]}}
    ```

    MongoDB doc:
    <https://docs.mongodb.com/manual/reference/operator/query/all>
    """

    def __init__(
        self,
        field,
        values: list,
    ):
        self.field = field
        self.values_list = values

    @property
    def query(self):
        return {self.field: {"$all": self.values_list}}


class ElemMatch(BaseFindArrayOperator):
    """
    `$elemMatch` array query operator

    Example:

    ```python
    class Sample(Document):
        results: List[int]

    ElemMatch(Sample.results, {"$in": [80, 85]})
    ```

    Will return query object like

    ```python
    {"results": {"$elemMatch": {"$in": [80, 85]}}}
    ```

    MongoDB doc:
    <https://docs.mongodb.com/manual/reference/operator/query/elemMatch/>
    """

    def __init__(
        self,
        field,
        expression: Optional[dict] = None,
        **kwargs: Any,
    ):
        self.field = field

        if expression is None:
            self.expression = kwargs
        else:
            self.expression = expression

    @property
    def query(self):
        return {self.field: {"$elemMatch": self.expression}}


class Size(BaseFindArrayOperator):
    """
    `$size` array query operator

    Example:

    ```python
    class Sample(Document):
        results: List[int]

    Size(Sample.results, 2)
    ```

    Will return query object like

    ```python
    {"results": {"$size": 2}}
    ```

    MongoDB doc:
    <https://docs.mongodb.com/manual/reference/operator/query/size/>
    """

    def __init__(
        self,
        field,
        num: int,
    ):
        self.field = field
        self.num = num

    @property
    def query(self):
        return {self.field: {"$size": self.num}}
