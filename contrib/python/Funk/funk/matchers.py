import precisely


def to_matcher(value):
    if precisely.is_matcher(value):
        return value
    else:
        return precisely.equal_to(value)
