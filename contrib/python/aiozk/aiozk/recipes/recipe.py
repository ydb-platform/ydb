from typing import ClassVar

from aiozk import exc


class Recipe:
    sub_recipes: ClassVar = {}

    def __init__(self, base_path='/'):
        self.client = None
        self.base_path = base_path

        for attribute_name, recipe_class in self.sub_recipes.items():
            recipe_args = ['base_path']
            if isinstance(recipe_class, tuple):
                recipe_class, recipe_args = recipe_class

            recipe_args = [getattr(self, arg) for arg in recipe_args]

            recipe = recipe_class(*recipe_args)

            setattr(self, attribute_name, recipe)

    def set_client(self, client):
        self.client = client
        for sub_recipe in self.sub_recipes:
            getattr(self, sub_recipe).client = client

    @classmethod
    def validate_dependencies(cls):
        return True

    async def ensure_path(self):
        await self.client.ensure_path(self.base_path)

    async def create_znode(self, path):
        try:
            await self.client.create(path)
        except exc.NodeExists:
            pass
        except exc.NoNode:
            try:
                await self.ensure_path()
            except exc.NodeExists:
                pass
