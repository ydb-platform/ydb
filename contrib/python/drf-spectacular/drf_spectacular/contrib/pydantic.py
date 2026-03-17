from drf_spectacular.drainage import set_override, warn
from drf_spectacular.extensions import OpenApiSerializerExtension
from drf_spectacular.plumbing import ResolvedComponent, build_basic_type
from drf_spectacular.types import OpenApiTypes


class PydanticExtension(OpenApiSerializerExtension):
    """
    Allows using pydantic models on @extend_schema(request=..., response=...) to
    describe your API.

    We only have partial support for pydantic's version of dataclass, due to the way they
    are designed. The outermost class (the @extend_schema argument) has to be a subclass
    of pydantic.BaseModel. Inside this outermost BaseModel, any combination of dataclass
    and BaseModel can be used.
    """

    target_class = "pydantic.BaseModel"
    match_subclasses = True

    def get_name(self, auto_schema, direction):
        # due to the fact that it is complicated to pull out every field member BaseModel class
        # of the entry model, we simply use the class name as string for object. This hack may
        # create false positive warnings, so turn it off. However, this may suppress correct
        # warnings involving the entry class.
        # TODO suppression may be migrated to new ComponentIdentity system
        set_override(self.target, 'suppress_collision_warning', True)
        return self.target.__name__

    def map_serializer(self, auto_schema, direction):
        # let pydantic generate a JSON schema
        try:
            from pydantic.json_schema import model_json_schema
        except ImportError:
            warn("Only pydantic >= 2 is supported. defaulting to generic object.")
            return build_basic_type(OpenApiTypes.OBJECT)

        schema = model_json_schema(self.target, ref_template="#/components/schemas/{model}", mode="serialization")

        # pull out potential sub-schemas and put them into component section
        for sub_name, sub_schema in schema.pop("$defs", {}).items():
            component = ResolvedComponent(
                name=sub_name,
                type=ResolvedComponent.SCHEMA,
                object=sub_name,
                schema=sub_schema,
            )
            auto_schema.registry.register_on_missing(component)

        return schema
