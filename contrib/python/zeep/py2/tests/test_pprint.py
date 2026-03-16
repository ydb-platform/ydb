from zeep.xsd import printer


def test_dict():
    pprint = printer.PrettyPrinter()
    data = {
        "foo": "bar",
        "foo_2": "bar",
        "foo_3": "bar",
        "foo_4": {"foo": "1", "bar": {"bala": "qwe"}, "x": [1, 2, 3, 4], "y": []},
    }
    pprint.pformat(data)


def test_list():
    pprint = printer.PrettyPrinter()
    data = [{"foo": "bar", "foo_2": "bar"}, {"foo": "bar", "foo_2": "bar"}]
    pprint.pformat(data)
