from beanie.odm.operators.update import BaseUpdateOperator


class BaseUpdateArrayOperator(BaseUpdateOperator):
    operator = ""

    def __init__(self, expression):
        self.expression = expression

    @property
    def query(self):
        return {self.operator: self.expression}


class AddToSet(BaseUpdateArrayOperator):
    """
    `$addToSet` update array query operator

    Example:

    ```python
    class Sample(Document):
        results: List[int]

    AddToSet({Sample.results: 2})
    ```

    Will return query object like

    ```python
    {"$addToSet": {"results": 2}}
    ```

    MongoDB docs:
    <https://docs.mongodb.com/manual/reference/operator/update/addToSet/>
    """

    operator = "$addToSet"


class Pop(BaseUpdateArrayOperator):
    """
    `$pop` update array query operator

    Example:

    ```python
    class Sample(Document):
        results: List[int]

    Pop({Sample.results: 2})
    ```

    Will return query object like

    ```python
    {"$pop": {"results": -1}}
    ```

    MongoDB docs:
    <https://docs.mongodb.com/manual/reference/operator/update/pop/>
    """

    operator = "$pop"


class Pull(BaseUpdateArrayOperator):
    """
    `$pull` update array query operator

    Example:

    ```python
    class Sample(Document):
        results: List[int]

    Pull(In(Sample.result: [1,2,3,4,5])
    ```

    Will return query object like

    ```python
    {"$pull": { "results": { $in: [1,2,3,4,5] }}}
    ```

    MongoDB docs:
    <https://docs.mongodb.com/manual/reference/operator/update/pull/>
    """

    operator = "$pull"


class Push(BaseUpdateArrayOperator):
    """
    `$push` update array query operator

    Example:

    ```python
    class Sample(Document):
        results: List[int]

    Push({Sample.results: 1})
    ```

    Will return query object like

    ```python
    {"$push": { "results": 1}}
    ```

    MongoDB docs:
    <https://docs.mongodb.com/manual/reference/operator/update/push/>
    """

    operator = "$push"


class PullAll(BaseUpdateArrayOperator):
    """
    `$pullAll` update array query operator

    Example:

    ```python
    class Sample(Document):
        results: List[int]

    PullAll({ Sample.results: [ 0, 5 ] })
    ```

    Will return query object like

    ```python
    {"$pullAll": { "results": [ 0, 5 ] }}
    ```

    MongoDB docs:
    <https://docs.mongodb.com/manual/reference/operator/update/pullAll/>
    """

    operator = "$pullAll"
