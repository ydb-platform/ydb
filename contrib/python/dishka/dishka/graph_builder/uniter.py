from dishka.dependency_source import Factory, FactoryUnionMode
from dishka.entities.factory_type import FactoryType
from dishka.entities.key import DependencyKey
from dishka.entities.marker import BoolMarker, or_markers
from dishka.entities.validation_settings import ValidationSettings
from dishka.exceptions import (
    ImplicitOverrideDetectedError,
    NothingOverriddenError,
)
from .moved_objects_tracker import MovedObjectsTracker


class SelectorGroupProcessor:
    def __init__(
        self,
        *,
        skip_validation: bool = False,
        validation_settings: ValidationSettings,
        moved_objects_tracker: MovedObjectsTracker,
    ) -> None:
        self.skip_validation = skip_validation
        self.validation_settings = validation_settings
        self.moved_objects_tracker = moved_objects_tracker

    def _ensure_override_flags(
            self,
            factory: Factory,
            prev_factory: Factory | None,
    ) -> None:
        if self.skip_validation:
            return
        if (
            not prev_factory and
            self.validation_settings.nothing_overridden and
            factory.when_override == BoolMarker(True)
        ):
            raise NothingOverriddenError(factory)

        if (
            prev_factory and
            self.validation_settings.implicit_override and
            factory.when_override is None
        ):
            raise ImplicitOverrideDetectedError(
                prev_factory,
                factory,
            )

    def unite(
        self,
        union_mode: FactoryUnionMode,
        provides: DependencyKey,
        group: list[Factory],
    ) -> list[Factory]:
        if not group:
            return []
        res_factories: list[Factory] = []
        prev_factory = None

        for factory in group:
            self._ensure_override_flags(factory, prev_factory)
            # implicit and explicit override
            if factory.when_override in (None, BoolMarker(True)):
                res_factories = []

            new_provides = self.moved_objects_tracker.move(provides)
            prev_factory = factory
            new_factory = factory.replace(provides=new_provides)
            res_factories.append(new_factory)
        if (
                len(res_factories) == 1 and
                prev_factory and  # at least one factory found
                prev_factory.when_override in (None, BoolMarker(True))
        ):
            return [prev_factory]

        factory = Factory(
            cache=union_mode.cache,
            scope=union_mode.scope,
            provides=provides,
            is_to_bind=False,
            dependencies=(),
            type_=FactoryType.SELECTOR,
            kw_dependencies={},
            source=None,
            when_override=None,
            when_active=or_markers(*(
                factory.when_active
                for factory in res_factories
            )),
            when_component=provides.component,
            # reverse list, so last wins
            when_dependencies=res_factories[::-1],
        )
        res_factories.append(factory)
        return res_factories


class CollectionGroupProcessor:
    def __init__(
        self,
        *,
        skip_validation: bool = False,
        validation_settings: ValidationSettings,
        moved_objects_tracker: MovedObjectsTracker,
    ) -> None:
        self.skip_validation = skip_validation
        self.validation_settings = validation_settings
        self.moved_objects_tracker = moved_objects_tracker

    def unite(
        self,
        union_mode: FactoryUnionMode,
        provides: DependencyKey,
        group: list[Factory],
        collection_factory: Factory,
    ) -> list[Factory]:
        """
        Generate new factories from united and patch `collection_factory`
        """
        res_factories: list[Factory] = []
        moved_factories: list[Factory] = []
        for factory in group:
            # explicit override only
            if factory.when_override == BoolMarker(True):
                if (
                    not moved_factories and
                    not self.skip_validation and
                    self.validation_settings.nothing_overridden
                ):
                    raise NothingOverriddenError(factory)

                res_factories = []
                moved_factories = []

            new_provides = self.moved_objects_tracker.move(provides)
            new_factory = factory.replace(
                provides=new_provides,
            )
            moved_factories.append(new_factory)
            res_factories.append(new_factory)

        collection_factory.when_dependencies = moved_factories[:]
        return res_factories
