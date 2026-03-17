from inspect import signature
from typing import Any, List, Type

from beanie.migrations.controllers.base import BaseMigrationController
from beanie.odm.documents import Document


def free_fall_migration(document_models: List[Type[Document]]):
    class FreeFallMigrationController(BaseMigrationController):
        def __init__(self, function):
            self.function = function
            self.function_signature = signature(function)
            self.document_models = document_models

        def __call__(self, *args: Any, **kwargs: Any):
            pass

        @property
        def models(self) -> List[Type[Document]]:
            return self.document_models

        async def run(self, session):
            function_kwargs = {"session": session}
            if "self" in self.function_signature.parameters:
                function_kwargs["self"] = None
            await self.function(**function_kwargs)

    return FreeFallMigrationController
