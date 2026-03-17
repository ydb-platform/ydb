from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, Type

from beanie.odm.fields import LinkInfo, LinkTypes

if TYPE_CHECKING:
    from beanie import Document


# TODO: check if this is the most efficient way for
#  appending subqueries to the queries var


def construct_lookup_queries(
    cls: Type["Document"],
    nesting_depth: Optional[int] = None,
    nesting_depths_per_field: Optional[Dict[str, int]] = None,
) -> List[Dict[str, Any]]:
    queries: List = []
    link_fields = cls.get_link_fields()
    if link_fields is not None:
        for link_info in link_fields.values():
            final_nesting_depth = (
                nesting_depths_per_field.get(link_info.field_name, None)
                if nesting_depths_per_field is not None
                else None
            )
            if final_nesting_depth is None:
                final_nesting_depth = nesting_depth
            construct_query(
                link_info=link_info,
                queries=queries,
                database_major_version=cls._database_major_version,
                current_depth=final_nesting_depth,
            )
    return queries


def construct_query(
    link_info: LinkInfo,
    queries: List,
    database_major_version: int,
    current_depth: Optional[int] = None,
):
    if link_info.is_fetchable is False or (
        current_depth is not None and current_depth <= 0
    ):
        return
    if link_info.link_type in [
        LinkTypes.DIRECT,
        LinkTypes.OPTIONAL_DIRECT,
    ]:
        if database_major_version >= 5 or link_info.nested_links is None:
            lookup_steps = [
                {
                    "$lookup": {
                        "from": link_info.document_class.get_motor_collection().name,  # type: ignore
                        "localField": f"{link_info.lookup_field_name}.$id",
                        "foreignField": "_id",
                        "as": f"_link_{link_info.field_name}",
                    }
                },
                {
                    "$unwind": {
                        "path": f"$_link_{link_info.field_name}",
                        "preserveNullAndEmptyArrays": True,
                    }
                },
                {
                    "$addFields": {
                        link_info.field_name: {
                            "$cond": {
                                "if": {
                                    "$ifNull": [
                                        f"$_link_{link_info.field_name}",
                                        False,
                                    ]
                                },
                                "then": f"$_link_{link_info.field_name}",
                                "else": f"${link_info.field_name}",
                            }
                        }
                    }
                },
                {"$project": {f"_link_{link_info.field_name}": 0}},
            ]  # type: ignore
            new_depth = (
                current_depth - 1 if current_depth is not None else None
            )
            if link_info.nested_links is not None:
                lookup_steps[0]["$lookup"]["pipeline"] = []  # type: ignore
                for nested_link in link_info.nested_links:
                    construct_query(
                        link_info=link_info.nested_links[nested_link],
                        queries=lookup_steps[0]["$lookup"]["pipeline"],  # type: ignore
                        database_major_version=database_major_version,
                        current_depth=new_depth,
                    )
            queries += lookup_steps

        else:
            lookup_steps = [
                {
                    "$lookup": {
                        "from": link_info.document_class.get_motor_collection().name,  # type: ignore
                        "let": {
                            "link_id": f"${link_info.lookup_field_name}.$id"
                        },
                        "as": f"_link_{link_info.field_name}",
                        "pipeline": [
                            {
                                "$match": {
                                    "$expr": {"$eq": ["$_id", "$$link_id"]}
                                }
                            },
                        ],
                    }
                },
                {
                    "$unwind": {
                        "path": f"$_link_{link_info.field_name}",
                        "preserveNullAndEmptyArrays": True,
                    }
                },
                {
                    "$addFields": {
                        link_info.field_name: {
                            "$cond": {
                                "if": {
                                    "$ifNull": [
                                        f"$_link_{link_info.field_name}",
                                        False,
                                    ]
                                },
                                "then": f"$_link_{link_info.field_name}",
                                "else": f"${link_info.field_name}",
                            }
                        }
                    }
                },
                {"$project": {f"_link_{link_info.field_name}": 0}},
            ]
            new_depth = (
                current_depth - 1 if current_depth is not None else None
            )
            for nested_link in link_info.nested_links:
                construct_query(
                    link_info=link_info.nested_links[nested_link],
                    queries=lookup_steps[0]["$lookup"]["pipeline"],  # type: ignore
                    database_major_version=database_major_version,
                    current_depth=new_depth,
                )
            queries += lookup_steps

    elif link_info.link_type in [
        LinkTypes.BACK_DIRECT,
        LinkTypes.OPTIONAL_BACK_DIRECT,
    ]:
        if database_major_version >= 5 or link_info.nested_links is None:
            lookup_steps = [
                {
                    "$lookup": {
                        "from": link_info.document_class.get_motor_collection().name,  # type: ignore
                        "localField": "_id",
                        "foreignField": f"{link_info.lookup_field_name}.$id",
                        "as": f"_link_{link_info.field_name}",
                    }
                },
                {
                    "$unwind": {
                        "path": f"$_link_{link_info.field_name}",
                        "preserveNullAndEmptyArrays": True,
                    }
                },
                {
                    "$addFields": {
                        link_info.field_name: {
                            "$cond": {
                                "if": {
                                    "$ifNull": [
                                        f"$_link_{link_info.field_name}",
                                        False,
                                    ]
                                },
                                "then": f"$_link_{link_info.field_name}",
                                "else": f"${link_info.field_name}",
                            }
                        }
                    }
                },
                {"$project": {f"_link_{link_info.field_name}": 0}},
            ]  # type: ignore
            new_depth = (
                current_depth - 1 if current_depth is not None else None
            )
            if link_info.nested_links is not None:
                lookup_steps[0]["$lookup"]["pipeline"] = []  # type: ignore
                for nested_link in link_info.nested_links:
                    construct_query(
                        link_info=link_info.nested_links[nested_link],
                        queries=lookup_steps[0]["$lookup"]["pipeline"],  # type: ignore
                        database_major_version=database_major_version,
                        current_depth=new_depth,
                    )
            queries += lookup_steps

        else:
            lookup_steps = [
                {
                    "$lookup": {
                        "from": link_info.document_class.get_motor_collection().name,  # type: ignore
                        "let": {"link_id": "$_id"},
                        "as": f"_link_{link_info.field_name}",
                        "pipeline": [
                            {
                                "$match": {
                                    "$expr": {
                                        "$eq": [
                                            f"${link_info.lookup_field_name}.$id",
                                            "$$link_id",
                                        ]
                                    }
                                }
                            },
                        ],
                    }
                },
                {
                    "$unwind": {
                        "path": f"$_link_{link_info.field_name}",
                        "preserveNullAndEmptyArrays": True,
                    }
                },
                {
                    "$addFields": {
                        link_info.field_name: {
                            "$cond": {
                                "if": {
                                    "$ifNull": [
                                        f"$_link_{link_info.field_name}",
                                        False,
                                    ]
                                },
                                "then": f"$_link_{link_info.field_name}",
                                "else": f"${link_info.field_name}",
                            }
                        }
                    }
                },
                {"$project": {f"_link_{link_info.field_name}": 0}},
            ]
            new_depth = (
                current_depth - 1 if current_depth is not None else None
            )
            for nested_link in link_info.nested_links:
                construct_query(
                    link_info=link_info.nested_links[nested_link],
                    queries=lookup_steps[0]["$lookup"]["pipeline"],  # type: ignore
                    database_major_version=database_major_version,
                    current_depth=new_depth,
                )
            queries += lookup_steps

    elif link_info.link_type in [
        LinkTypes.LIST,
        LinkTypes.OPTIONAL_LIST,
    ]:
        if database_major_version >= 5 or link_info.nested_links is None:
            queries.append(
                {
                    "$lookup": {
                        "from": link_info.document_class.get_motor_collection().name,  # type: ignore
                        "localField": f"{link_info.lookup_field_name}.$id",
                        "foreignField": "_id",
                        "as": link_info.field_name,
                    }
                }
            )
            new_depth = (
                current_depth - 1 if current_depth is not None else None
            )
            if link_info.nested_links is not None:
                queries[-1]["$lookup"]["pipeline"] = []
                for nested_link in link_info.nested_links:
                    construct_query(
                        link_info=link_info.nested_links[nested_link],
                        queries=queries[-1]["$lookup"]["pipeline"],
                        database_major_version=database_major_version,
                        current_depth=new_depth,
                    )
        else:
            lookup_step = {
                "$lookup": {
                    "from": link_info.document_class.get_motor_collection().name,  # type: ignore
                    "let": {"link_id": f"${link_info.lookup_field_name}.$id"},
                    "as": link_info.field_name,
                    "pipeline": [
                        {"$match": {"$expr": {"$in": ["$_id", "$$link_id"]}}},
                    ],
                }
            }
            new_depth = (
                current_depth - 1 if current_depth is not None else None
            )
            for nested_link in link_info.nested_links:
                construct_query(
                    link_info=link_info.nested_links[nested_link],
                    queries=lookup_step["$lookup"]["pipeline"],
                    database_major_version=database_major_version,
                    current_depth=new_depth,
                )
            queries.append(lookup_step)

    elif link_info.link_type in [
        LinkTypes.BACK_LIST,
        LinkTypes.OPTIONAL_BACK_LIST,
    ]:
        if database_major_version >= 5 or link_info.nested_links is None:
            queries.append(
                {
                    "$lookup": {
                        "from": link_info.document_class.get_motor_collection().name,  # type: ignore
                        "localField": "_id",
                        "foreignField": f"{link_info.lookup_field_name}.$id",
                        "as": link_info.field_name,
                    }
                }
            )
            new_depth = (
                current_depth - 1 if current_depth is not None else None
            )
            if link_info.nested_links is not None:
                queries[-1]["$lookup"]["pipeline"] = []
                for nested_link in link_info.nested_links:
                    construct_query(
                        link_info=link_info.nested_links[nested_link],
                        queries=queries[-1]["$lookup"]["pipeline"],
                        database_major_version=database_major_version,
                        current_depth=new_depth,
                    )
        else:
            lookup_step = {
                "$lookup": {
                    "from": link_info.document_class.get_motor_collection().name,  # type: ignore
                    "let": {"link_id": "$_id"},
                    "as": link_info.field_name,
                    "pipeline": [
                        {
                            "$match": {
                                "$expr": {
                                    "$in": [
                                        "$$link_id",
                                        f"${link_info.lookup_field_name}.$id",
                                    ]
                                }
                            }
                        }
                    ],
                }
            }
            new_depth = (
                current_depth - 1 if current_depth is not None else None
            )
            for nested_link in link_info.nested_links:
                construct_query(
                    link_info=link_info.nested_links[nested_link],
                    queries=lookup_step["$lookup"]["pipeline"],
                    database_major_version=database_major_version,
                    current_depth=new_depth,
                )
            queries.append(lookup_step)

    return queries


def split_text_query(
    query: Dict[str, Any],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Divide query into text and non-text matches

    :param query: Dict[str, Any] - query dict
    :return: Tuple[Dict[str, Any], Dict[str, Any]] - text and non-text queries,
        respectively
    """

    root_text_query_args: Dict[str, Any] = query.get("$text", None)
    root_non_text_queries: Dict[str, Any] = {
        k: v for k, v in query.items() if k not in {"$text", "$and"}
    }

    text_queries: List[Dict[str, Any]] = (
        [{"$text": root_text_query_args}] if root_text_query_args else []
    )
    non_text_queries: List[Dict[str, Any]] = (
        [root_non_text_queries] if root_non_text_queries else []
    )

    for match_case in query.get("$and", []):
        if "$text" in match_case:
            text_queries.append(match_case)
        else:
            non_text_queries.append(match_case)

    return text_queries, non_text_queries
