import pickle

import pytest
from lxml.etree import QName

from zeep import xsd
from zeep.xsd import valueobjects


def test_simple_args():
    xsd_type = xsd.ComplexType(
        xsd.Sequence(
            [xsd.Element("item_1", xsd.String()), xsd.Element("item_2", xsd.String())]
        )
    )
    args = tuple(["value-1", "value-2"])
    kwargs = {}
    result = valueobjects._process_signature(xsd_type, args, kwargs)
    assert result == {"item_1": "value-1", "item_2": "value-2"}


def test_simple_args_attributes():
    xsd_type = xsd.ComplexType(
        xsd.Sequence(
            [xsd.Element("item_1", xsd.String()), xsd.Element("item_2", xsd.String())]
        ),
        [xsd.Attribute("attr_1", xsd.String())],
    )
    args = tuple(["value-1", "value-2", "bla"])
    kwargs = {}
    result = valueobjects._process_signature(xsd_type, args, kwargs)
    assert result == {"item_1": "value-1", "item_2": "value-2", "attr_1": "bla"}


def test_simple_args_attributes_as_kwargs():
    xsd_type = xsd.ComplexType(
        xsd.Sequence(
            [xsd.Element("item_1", xsd.String()), xsd.Element("item_2", xsd.String())]
        ),
        [xsd.Attribute("attr_1", xsd.String())],
    )
    args = tuple(["value-1", "value-2"])
    kwargs = {"attr_1": "bla"}
    result = valueobjects._process_signature(xsd_type, args, kwargs)
    assert result == {"item_1": "value-1", "item_2": "value-2", "attr_1": "bla"}


def test_simple_args_too_many():
    xsd_type = xsd.ComplexType(
        xsd.Sequence(
            [xsd.Element("item_1", xsd.String()), xsd.Element("item_2", xsd.String())]
        )
    )
    args = tuple(["value-1", "value-2", "value-3"])
    kwargs = {}

    try:
        valueobjects._process_signature(xsd_type, args, kwargs)
    except TypeError as exc:
        assert str(exc) == ("__init__() takes at most 2 positional arguments (3 given)")
    else:
        assert False, "TypeError not raised"


def test_simple_args_too_few():
    xsd_type = xsd.ComplexType(
        xsd.Sequence(
            [xsd.Element("item_1", xsd.String()), xsd.Element("item_2", xsd.String())]
        )
    )
    args = tuple(["value-1"])
    kwargs = {}
    valueobjects._process_signature(xsd_type, args, kwargs)


def test_simple_kwargs():
    xsd_type = xsd.ComplexType(
        xsd.Sequence(
            [xsd.Element("item_1", xsd.String()), xsd.Element("item_2", xsd.String())]
        )
    )
    args = tuple([])
    kwargs = {"item_1": "value-1", "item_2": "value-2"}
    result = valueobjects._process_signature(xsd_type, args, kwargs)
    assert result == {"item_1": "value-1", "item_2": "value-2"}


def test_simple_mixed():
    xsd_type = xsd.ComplexType(
        xsd.Sequence(
            [xsd.Element("item_1", xsd.String()), xsd.Element("item_2", xsd.String())]
        )
    )
    args = tuple(["value-1"])
    kwargs = {"item_2": "value-2"}
    result = valueobjects._process_signature(xsd_type, args, kwargs)
    assert result == {"item_1": "value-1", "item_2": "value-2"}


def test_choice():
    xsd_type = xsd.ComplexType(
        xsd.Sequence(
            [
                xsd.Choice(
                    [
                        xsd.Element("item_1", xsd.String()),
                        xsd.Element("item_2", xsd.String()),
                    ]
                )
            ]
        )
    )
    args = tuple([])
    kwargs = {"item_2": "value-2"}
    result = valueobjects._process_signature(xsd_type, args, kwargs)
    assert result == {"item_1": None, "item_2": "value-2"}


def test_choice_max_occurs_simple_interface():
    fields = xsd.ComplexType(
        xsd.Sequence(
            [
                xsd.Choice(
                    [
                        xsd.Element("item_1", xsd.String()),
                        xsd.Element("item_2", xsd.String()),
                    ],
                    max_occurs=2,
                )
            ]
        )
    )
    args = tuple([])
    kwargs = {"_value_1": [{"item_1": "foo"}, {"item_2": "bar"}]}
    result = valueobjects._process_signature(fields, args, kwargs)
    assert result == {"_value_1": [{"item_1": "foo"}, {"item_2": "bar"}]}


def test_choice_max_occurs():
    fields = xsd.ComplexType(
        xsd.Sequence(
            [
                xsd.Choice(
                    [
                        xsd.Element("item_1", xsd.String()),
                        xsd.Element("item_2", xsd.String()),
                    ],
                    max_occurs=3,
                )
            ]
        )
    )
    args = tuple([])
    kwargs = {"_value_1": [{"item_1": "foo"}, {"item_2": "bar"}, {"item_1": "bla"}]}
    result = valueobjects._process_signature(fields, args, kwargs)
    assert result == {
        "_value_1": [{"item_1": "foo"}, {"item_2": "bar"}, {"item_1": "bla"}]
    }


def test_choice_max_occurs_on_choice():
    xsd_type = xsd.ComplexType(
        xsd.Sequence(
            [
                xsd.Choice(
                    [
                        xsd.Element("item_1", xsd.String(), max_occurs=2),
                        xsd.Element("item_2", xsd.String()),
                    ],
                    max_occurs=2,
                )
            ]
        )
    )
    args = tuple([])
    kwargs = {"_value_1": [{"item_1": ["foo", "bar"]}, {"item_2": "bla"}]}
    result = valueobjects._process_signature(xsd_type, args, kwargs)
    assert result == {"_value_1": [{"item_1": ["foo", "bar"]}, {"item_2": "bla"}]}


def test_choice_mixed():
    xsd_type = xsd.ComplexType(
        xsd.Sequence(
            [
                xsd.Choice(
                    [
                        xsd.Element("item_1", xsd.String()),
                        xsd.Element("item_2", xsd.String()),
                    ]
                ),
                xsd.Element("item_2", xsd.String()),
            ]
        ),
        qname=QName("http://tests.python-zeep.org", "container"),
    )
    expected = "{http://tests.python-zeep.org}container(({item_1: xsd:string} | {item_2: xsd:string}), item_2__1: xsd:string)"
    assert xsd_type.signature() == expected

    args = tuple([])
    kwargs = {"item_1": "value-1", "item_2__1": "value-2"}
    result = valueobjects._process_signature(xsd_type, args, kwargs)
    assert result == {"item_1": "value-1", "item_2": None, "item_2__1": "value-2"}


def test_choice_sequences_simple():
    xsd_type = xsd.ComplexType(
        xsd.Sequence(
            [
                xsd.Choice(
                    [
                        xsd.Sequence(
                            [
                                xsd.Element("item_1", xsd.String()),
                                xsd.Element("item_2", xsd.String()),
                            ]
                        ),
                        xsd.Sequence(
                            [
                                xsd.Element("item_3", xsd.String()),
                                xsd.Element("item_4", xsd.String()),
                            ]
                        ),
                    ]
                )
            ]
        )
    )
    args = tuple([])
    kwargs = {"item_1": "value-1", "item_2": "value-2"}
    result = valueobjects._process_signature(xsd_type, args, kwargs)
    assert result == {
        "item_1": "value-1",
        "item_2": "value-2",
        "item_3": None,
        "item_4": None,
    }


def test_choice_sequences_no_match():
    xsd_type = xsd.ComplexType(
        xsd.Sequence(
            [
                xsd.Choice(
                    [
                        xsd.Sequence(
                            [
                                xsd.Element("item_1", xsd.String()),
                                xsd.Element("item_2", xsd.String()),
                            ]
                        ),
                        xsd.Sequence(
                            [
                                xsd.Element("item_3", xsd.String()),
                                xsd.Element("item_4", xsd.String()),
                            ]
                        ),
                    ]
                )
            ]
        )
    )
    args = tuple([])
    with pytest.raises(TypeError):
        kwargs = {"item_1": "value-1", "item_3": "value-3"}
        valueobjects._process_signature(xsd_type, args, kwargs)


def test_choice_sequences_no_match_last():
    xsd_type = xsd.ComplexType(
        xsd.Sequence(
            [
                xsd.Choice(
                    [
                        xsd.Sequence(
                            [
                                xsd.Element("item_1", xsd.String()),
                                xsd.Element("item_2", xsd.String()),
                            ]
                        ),
                        xsd.Sequence(
                            [
                                xsd.Element("item_3", xsd.String()),
                                xsd.Element("item_4", xsd.String()),
                            ]
                        ),
                    ]
                )
            ]
        )
    )
    args = tuple([])
    with pytest.raises(TypeError):
        kwargs = {"item_2": "value-2", "item_4": "value-4"}
        valueobjects._process_signature(xsd_type, args, kwargs)


def test_choice_sequences_no_match_nested():
    xsd_type = xsd.ComplexType(
        xsd.Sequence(
            [
                xsd.Choice(
                    [
                        xsd.Sequence(
                            [
                                xsd.Element("item_1", xsd.String()),
                                xsd.Element("item_2", xsd.String()),
                            ]
                        )
                    ]
                )
            ]
        )
    )
    args = tuple([])
    kwargs = {"item_1": "value-1"}
    value = valueobjects._process_signature(xsd_type, args, kwargs)
    assert value == {"item_1": "value-1", "item_2": None}


def test_choice_sequences_optional_elms():
    xsd_type = xsd.ComplexType(
        xsd.Sequence(
            [
                xsd.Choice(
                    [
                        xsd.Sequence(
                            [
                                xsd.Element("item_1", xsd.String()),
                                xsd.Element("item_2", xsd.String(), min_occurs=0),
                            ]
                        ),
                        xsd.Sequence(
                            [
                                xsd.Element("item_1", xsd.String()),
                                xsd.Element("item_2", xsd.String()),
                                xsd.Element("item_3", xsd.String()),
                            ]
                        ),
                    ]
                )
            ]
        )
    )
    args = tuple([])
    kwargs = {"item_1": "value-1"}
    result = valueobjects._process_signature(xsd_type, args, kwargs)
    assert result == {"item_1": "value-1", "item_2": None, "item_3": None}


def test_choice_sequences_max_occur():
    xsd_type = xsd.ComplexType(
        xsd.Sequence(
            [
                xsd.Choice(
                    [
                        xsd.Sequence(
                            [
                                xsd.Element("item_1", xsd.String()),
                                xsd.Element("item_2", xsd.String()),
                            ]
                        ),
                        xsd.Sequence(
                            [
                                xsd.Element("item_2", xsd.String()),
                                xsd.Element("item_3", xsd.String()),
                            ]
                        ),
                    ],
                    max_occurs=2,
                )
            ]
        )
    )
    args = tuple([])
    kwargs = {
        "_value_1": [
            {"item_1": "value-1", "item_2": "value-2"},
            {"item_2": "value-2", "item_3": "value-3"},
        ]
    }

    result = valueobjects._process_signature(xsd_type, args, kwargs)
    assert result == {
        "_value_1": [
            {"item_1": "value-1", "item_2": "value-2"},
            {"item_2": "value-2", "item_3": "value-3"},
        ]
    }


def test_choice_sequences_init_dict():
    xsd_type = xsd.ComplexType(
        xsd.Sequence(
            [
                xsd.Choice(
                    [
                        xsd.Sequence(
                            [
                                xsd.Element("item_1", xsd.String()),
                                xsd.Element("item_2", xsd.String()),
                            ]
                        ),
                        xsd.Sequence(
                            [
                                xsd.Element("item_2", xsd.String()),
                                xsd.Element("item_3", xsd.String()),
                            ]
                        ),
                    ],
                    max_occurs=2,
                )
            ]
        )
    )
    args = tuple([])
    kwargs = {"_value_1": {"item_1": "value-1", "item_2": "value-2"}}

    result = valueobjects._process_signature(xsd_type, args, kwargs)
    assert result == {"_value_1": [{"item_1": "value-1", "item_2": "value-2"}]}


def test_pickle():
    xsd_type = xsd.ComplexType(
        xsd.Sequence(
            [xsd.Element("item_1", xsd.String()), xsd.Element("item_2", xsd.String())]
        )
    )

    obj = xsd_type(item_1="x", item_2="y")

    data = pickle.dumps(obj)
    obj_rt = pickle.loads(data)

    assert obj.item_1 == "x"
    assert obj.item_2 == "y"
    assert obj_rt.item_1 == "x"
    assert obj_rt.item_2 == "y"


def test_json():
    xsd_type = xsd.ComplexType(
        xsd.Sequence(
            [xsd.Element("item_1", xsd.String()), xsd.Element("item_2", xsd.String())]
        )
    )

    obj = xsd_type(item_1="x", item_2="y")
    assert obj.__json__() == {"item_1": "x", "item_2": "y"}
