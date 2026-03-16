from typing import Any, Dict, Iterator, Optional, Tuple, TypeVar

from typing_extensions import Self

from office365.runtime.odata.json_format import ODataJsonFormat
from office365.runtime.odata.v3.json_light_format import JsonLightFormat

P_T = TypeVar("P_T", int, float, str, bool, "ClientValue")


class ClientValue(object):
    """Represent complex type.
    Complex types consist of a list of properties with no key, and can therefore only exist as properties of a
    containing entity or as a temporary value
    """

    def set_property(self, k, v, persist_changes=True):
        # type: (str|int, Any, bool) -> Self
        prop_type = getattr(self, k, None)
        if isinstance(prop_type, ClientValue) and v is not None:
            if isinstance(v, list):
                [
                    prop_type.set_property(i, p_v, persist_changes)
                    for i, p_v in enumerate(v)
                ]
            else:
                [
                    prop_type.set_property(k, p_v, persist_changes)
                    for k, p_v in v.items()
                ]
            setattr(self, k, prop_type)
        else:
            setattr(self, k, v)
        return self

    def get_property(self, k):
        # type: (str) -> P_T
        return getattr(self, k)

    def __iter__(self):
        # type: () -> Iterator[Tuple[str, P_T]]
        for n, v in vars(self).items():
            yield n, v

    def to_json(self, json_format=None):
        # type: (Optional[ODataJsonFormat]) -> Dict
        """Serializes a client value"""

        def _is_valid_value(val):
            from office365.runtime.client_value_collection import ClientValueCollection

            if val is None:
                return False
            elif isinstance(val, ClientValueCollection) and len(val) == 0:
                return False
            return True

        json = {k: v for k, v in self if _is_valid_value(v)}
        for n, v in json.items():
            if isinstance(v, ClientValue):
                json[n] = v.to_json(json_format)
        if (
            isinstance(json_format, JsonLightFormat)
            and json_format.include_control_information
            and self.entity_type_name is not None
        ):
            json[json_format.metadata_type] = {"type": self.entity_type_name}
        return json

    @property
    def entity_type_name(self):
        # type: () -> str
        """Returns server type name of value"""
        return type(self).__name__
