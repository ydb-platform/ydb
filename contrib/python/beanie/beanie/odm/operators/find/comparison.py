from beanie.odm.operators.find import BaseFindOperator


class BaseFindComparisonOperator(BaseFindOperator):
    operator = ""

    def __init__(
        self,
        field,
        other,
    ) -> None:
        self.field = field
        self.other = other

    @property
    def query(self):
        return {self.field: {self.operator: self.other}}


class Eq(BaseFindComparisonOperator):
    """
    `equal` query operator

    Example:

    ```python
    class Product(Document):
        price: float

    Eq(Product.price, 2)
    ```

    Will return query object like

    ```python
    {"price": 2}
    ```

    MongoDB doc:
    <https://docs.mongodb.com/manual/reference/operator/query/eq/>
    """

    @property
    def query(self):
        return {self.field: self.other}


class GT(BaseFindComparisonOperator):
    """
    `$gt` query operator

    Example:

    ```python
    class Product(Document):
        price: float

    GT(Product.price, 2)
    ```

    Will return query object like

    ```python
    {"price": {"$gt": 2}}
    ```

    MongoDB doc:
    <https://docs.mongodb.com/manual/reference/operator/query/gt/>
    """

    operator = "$gt"


class GTE(BaseFindComparisonOperator):
    """
    `$gte` query operator

    Example:

    ```python
    class Product(Document):
        price: float

    GTE(Product.price, 2)
    ```

    Will return query object like

    ```python
    {"price": {"$gte": 2}}
    ```

    MongoDB doc:
    <https://docs.mongodb.com/manual/reference/operator/query/gte/>
    """

    operator = "$gte"


class In(BaseFindComparisonOperator):
    """
    `$in` query operator

    Example:

    ```python
    class Product(Document):
        price: float

    In(Product.price, [2, 3, 4])
    ```

    Will return query object like

    ```python
    {"price": {"$in": [2, 3, 4]}}
    ```

    MongoDB doc:
    <https://docs.mongodb.com/manual/reference/operator/query/in/>
    """

    operator = "$in"


class NotIn(BaseFindComparisonOperator):
    """
    `$nin` query operator

    Example:

    ```python
    class Product(Document):
        price: float

    NotIn(Product.price, [2, 3, 4])
    ```

    Will return query object like

    ```python
    {"price": {"$nin": [2, 3, 4]}}
    ```

    MongoDB doc:
    <https://docs.mongodb.com/manual/reference/operator/query/nin/>
    """

    operator = "$nin"


class LT(BaseFindComparisonOperator):
    """
    `$lt` query operator

    Example:

    ```python
    class Product(Document):
        price: float

    LT(Product.price, 2)
    ```

    Will return query object like

    ```python
    {"price": {"$lt": 2}}
    ```

    MongoDB doc:
    <https://docs.mongodb.com/manual/reference/operator/query/lt/>
    """

    operator = "$lt"


class LTE(BaseFindComparisonOperator):
    """
    `$lte` query operator

    Example:

    ```python
    class Product(Document):
        price: float

    LTE(Product.price, 2)
    ```

    Will return query object like

    ```python
    {"price": {"$lte": 2}}
    ```

    MongoDB doc:
    <https://docs.mongodb.com/manual/reference/operator/query/lte/>
    """

    operator = "$lte"


class NE(BaseFindComparisonOperator):
    """
    `$ne` query operator

    Example:

    ```python
    class Product(Document):
        price: float

    NE(Product.price, 2)
    ```

    Will return query object like

    ```python
    {"price": {"$ne": 2}}
    ```

    MongoDB doc:
    <https://docs.mongodb.com/manual/reference/operator/query/ne/>
    """

    operator = "$ne"
