from ...lib.std import operator_itemgetter


# ---

nth_item = operator_itemgetter
first_item = nth_item(0)
second_item = nth_item(1)
third_item = nth_item(2)
