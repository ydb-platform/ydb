from collections.abc import Sequence

from dishka.entities.factory_type import FactoryData
from dishka.entities.key import DependencyKey
from .name import get_key_name, get_name, get_source_name


def render_suggestions_for_missing(
    requested_for: FactoryData | None,
    requested_key: DependencyKey,
    suggest_other_scopes: Sequence[FactoryData],
    suggest_other_components: Sequence[FactoryData],
    suggest_abstract_factories: Sequence[FactoryData],
    suggest_concrete_factories: Sequence[FactoryData],
) -> str:
    suggestion = ""
    if suggest_other_scopes:
        scopes = " or ".join(
            str(factory.scope)
            for factory in suggest_other_scopes
        )
        if requested_for:
            srcname = get_name(requested_for.source, include_module=False)
            suggestion += f"\n * Try changing scope of `{srcname}` to {scopes}"
        else:
            suggestion += f"\n * Check if you forgot enter {scopes}"

    if suggest_other_components:
        dep_name = get_name(requested_key.type_hint, include_module=True)
        components_str = " or ".join(
            f"Annotated[{dep_name}, FromComponent({f.provides.component!r})]"
            for f in suggest_other_components
        )
        suggestion += f"\n * Try using {components_str}"

    if suggest_abstract_factories:
        abstract_names = ""
        for factory in suggest_abstract_factories:
            source_name = get_source_name(factory)
            provides_name = get_key_name(factory.provides)
            abstract_names += "(" + provides_name
            if source_name:
                abstract_names += ", " + source_name
            abstract_names += ");"

        suggestion += "\n * Try use `AnyOf` "
        suggestion += "or changing the requested dependency to a more abstract"
        suggestion += ". Found factories for more abstract dependencies: "
        suggestion += abstract_names

    if suggest_concrete_factories:
        concreate_names = ""
        for factory in suggest_concrete_factories:
            source_name = get_source_name(factory)
            provides_name = get_key_name(factory.provides)
            concreate_names += "(" + provides_name
            if source_name:
                concreate_names += ", " + source_name
            concreate_names += ");"

        deb_name = get_name(requested_key.type_hint, include_module=True)

        suggestion += "\n * Try use `WithParents` "
        suggestion += "or changing `provides` to "
        suggestion += f"`{deb_name}`"

        suggestion += ". Found factories for more concrete dependencies: "
        suggestion += concreate_names

    return suggestion
