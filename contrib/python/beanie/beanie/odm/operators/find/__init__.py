from abc import ABC

from beanie.odm.operators import BaseOperator


class BaseFindOperator(BaseOperator, ABC): ...
