from abc import ABC
from typing import Optional

from beanie.odm.operators.find import BaseFindOperator


class BaseFindEvaluationOperator(BaseFindOperator, ABC): ...


class Expr(BaseFindEvaluationOperator):
    """
    `$type` query operator

    Example:

    ```python
    class Sample(Document):
        one: int
        two: int

    Expr({"$gt": [ "$one" , "$two" ]})
    ```

    Will return query object like

    ```python
    {"$expr": {"$gt": [ "$one" , "$two" ]}}
    ```

    MongoDB doc:
    <https://docs.mongodb.com/manual/reference/operator/query/expr/>
    """

    def __init__(self, expression: dict):
        self.expression = expression

    @property
    def query(self):
        return {"$expr": self.expression}


class JsonSchema(BaseFindEvaluationOperator):
    """
    `$jsonSchema` query operator

    MongoDB doc:
    <https://docs.mongodb.com/manual/reference/operator/query/jsonSchema/>
    """

    def __init__(self, expression: dict):
        self.expression = expression

    @property
    def query(self):
        return {"$jsonSchema": self.expression}


class Mod(BaseFindEvaluationOperator):
    """
    `$mod` query operator

    Example:

    ```python
    class Sample(Document):
        one: int

    Mod(Sample.one, 4, 0)
    ```

    Will return query object like

    ```python
    { "one": { "$mod": [ 4, 0 ] } }
    ```

    MongoDB doc:
    <https://docs.mongodb.com/manual/reference/operator/query/mod/>
    """

    def __init__(self, field, divisor: int, remainder: int):
        self.field = field
        self.divisor = divisor
        self.remainder = remainder

    @property
    def query(self):
        return {self.field: {"$mod": [self.divisor, self.remainder]}}


class RegEx(BaseFindEvaluationOperator):
    """
    `$regex` query operator

    MongoDB doc:
    <https://docs.mongodb.com/manual/reference/operator/query/regex/>
    """

    def __init__(
        self,
        field,
        pattern: str,
        options: Optional[str] = None,
    ):
        self.field = field
        self.pattern = pattern
        self.options = options

    @property
    def query(self):
        expression = {"$regex": self.pattern}
        if self.options:
            expression["$options"] = self.options
        return {self.field: expression}


class Text(BaseFindEvaluationOperator):
    """
    `$text` query operator

    Example:

    ```python
    class Sample(Document):
        description: Indexed(str, pymongo.TEXT)

    Text("coffee")
    ```

    Will return query object like

    ```python
    {
        "$text": {
            "$search": "coffee" ,
            "$caseSensitive": False,
            "$diacriticSensitive": False
        }
    }
    ```

    MongoDB doc:
    <https://docs.mongodb.com/manual/reference/operator/query/text/>
    """

    def __init__(
        self,
        search: str,
        language: Optional[str] = None,
        case_sensitive: bool = False,
        diacritic_sensitive: bool = False,
    ):
        """

        :param search: str
        :param language: Optional[str] = None
        :param case_sensitive: bool = False
        :param diacritic_sensitive: bool = False
        """
        self.search = search
        self.language = language
        self.case_sensitive = case_sensitive
        self.diacritic_sensitive = diacritic_sensitive

    @property
    def query(self):
        expression = {
            "$text": {
                "$search": self.search,
                "$caseSensitive": self.case_sensitive,
                "$diacriticSensitive": self.diacritic_sensitive,
            }
        }
        if self.language:
            expression["$text"]["$language"] = self.language
        return expression


class Where(BaseFindEvaluationOperator):
    """
    `$where` query operator

    MongoDB doc:
    <https://docs.mongodb.com/manual/reference/operator/query/where/>
    """

    def __init__(self, expression: str):
        self.expression = expression

    @property
    def query(self):
        return {"$where": self.expression}
