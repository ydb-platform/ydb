from preshed.maps import PreshMap


def test_pop1():
    table = PreshMap()
    table[10] = 20
    table[30] = 25
    assert table[10] == 20
    assert table[30] == 25
    table.pop(30)
    assert table[10] == 20
