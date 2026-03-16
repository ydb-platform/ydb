from pydantic import BaseModel


def is_pydantic_base_model(obj):
    """
    Return true is obj is a pydantic.BaseModel subclass.
    """
    return robuste_issubclass(obj, BaseModel)


def robuste_issubclass(cls1, cls2):
    """
    function likes issubclass but returns False instead of raise type error
    if first parameter is not a class.
    """
    try:
        return issubclass(cls1, cls2)
    except TypeError:
        return False
