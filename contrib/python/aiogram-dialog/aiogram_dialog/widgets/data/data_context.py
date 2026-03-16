from aiogram_dialog.api.internal.widgets import DataGetter
from aiogram_dialog.api.protocols import DialogManager


class CompositeGetter:
    def __init__(self, *getters: DataGetter):
        self.getters: list[DataGetter] = list(getters)

    async def __call__(self, **kwargs):
        data = {}
        for g in self.getters:
            data.update(await g(**kwargs))
        return data


class StaticGetter:
    def __init__(self, data: dict):
        self.data = data

    async def __call__(self, **kwargs):
        return self.data


class PreviewAwareGetter:
    def __init__(self, normal_getter: DataGetter, preview_getter: DataGetter):
        self.normal_getter = normal_getter
        self.preview_getter = preview_getter

    async def __call__(self, dialog_manager: DialogManager, **kwargs):
        if dialog_manager.is_preview():
            return await self.preview_getter(
                dialog_manager=dialog_manager, **kwargs,
            )
        else:
            return await self.normal_getter(
                dialog_manager=dialog_manager, **kwargs,
            )
