# Purpose: Grouping entities by DXF attributes or a key function.
# Copyright (c) 2017-2022, Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import Iterable, Hashable, TYPE_CHECKING, Optional

from ezdxf.lldxf.const import DXFValueError, DXFAttributeError

if TYPE_CHECKING:
    from ezdxf.entities import DXFEntity
    from ezdxf.eztypes import KeyFunc


def groupby(
    entities: Iterable[DXFEntity],
    dxfattrib: str = "",
    key: Optional[KeyFunc] = None,
) -> dict[Hashable, list[DXFEntity]]:
    """
    Groups a sequence of DXF entities by a DXF attribute like ``'layer'``,
    returns a dict with `dxfattrib` values as key and a list of entities
    matching this `dxfattrib`.
    A `key` function can be used to combine some DXF attributes (e.g. layer and
    color) and should return a hashable data type like a tuple of strings,
    integers or floats, `key` function example::

        def group_key(entity: DXFEntity):
            return entity.dxf.layer, entity.dxf.color

    For not suitable DXF entities return ``None`` to exclude this entity, in
    this case it's not required, because :func:`groupby` catches
    :class:`DXFAttributeError` exceptions to exclude entities, which do not
    provide layer and/or color attributes, automatically.

    Result dict for `dxfattrib` = ``'layer'`` may look like this::

        {
            '0': [ ... list of entities ],
            'ExampleLayer1': [ ... ],
            'ExampleLayer2': [ ... ],
            ...
        }

    Result dict for `key` = `group_key`, which returns a ``(layer, color)``
    tuple, may look like this::

        {
            ('0', 1): [ ... list of entities ],
            ('0', 3): [ ... ],
            ('0', 7): [ ... ],
            ('ExampleLayer1', 1): [ ... ],
            ('ExampleLayer1', 2): [ ... ],
            ('ExampleLayer1', 5): [ ... ],
            ('ExampleLayer2', 7): [ ... ],
            ...
        }

    All entity containers (modelspace, paperspace layouts and blocks) and the
    :class:`~ezdxf.query.EntityQuery` object have a dedicated :meth:`groupby`
    method.

    Args:
        entities: sequence of DXF entities to group by a DXF attribute or a
            `key` function
        dxfattrib: grouping DXF attribute like ``'layer'``
        key: key function, which accepts a :class:`DXFEntity` as argument and
            returns a hashable grouping key or ``None`` to ignore this entity

    """
    if all((dxfattrib, key)):
        raise DXFValueError(
            "Specify a dxfattrib or a key function, but not both."
        )
    if dxfattrib != "":
        key = lambda entity: entity.dxf.get_default(dxfattrib)
    if key is None:
        raise DXFValueError(
            "no valid argument found, specify a dxfattrib or a key function, "
            "but not both."
        )

    result: dict[Hashable, list[DXFEntity]] = dict()
    for dxf_entity in entities:
        if not dxf_entity.is_alive:
            continue
        try:
            group_key = key(dxf_entity)
        except DXFAttributeError:
            # ignore DXF entities, which do not support all query attributes
            continue
        if group_key is not None:
            group = result.setdefault(group_key, [])
            group.append(dxf_entity)
    return result
