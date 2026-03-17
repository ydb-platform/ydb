from .helpers import camel_to_snake_case


class Element:
    """This class holds attributes common to both the BlockElement and
    InlineElement classes.
    This class should not be subclassed by any other classes beside these.
    """

    override: bool

    @classmethod
    def get_type(cls, snake_case: bool = False) -> str:
        """
        Return the Markdown element type that the object represents.

        :param snake_case: Return the element type name in snake case if True
        """

        # Prevent override of BlockElement and InlineElement
        if (
            cls.override
            and cls.__base__
            and cls.__base__ not in Element.__subclasses__()
        ):
            name = cls.__base__.__name__
        else:
            name = cls.__name__
        return camel_to_snake_case(name) if snake_case else name

    def __repr__(self) -> str:
        try:
            from objprint import objstr
        except ImportError:
            from pprint import pformat

            if hasattr(self, "children"):
                children = f" children={pformat(self.children)}"
            else:
                children = ""

            return f"<{self.__class__.__name__}{children}>"
        else:
            return objstr(self, honor_existing=False, include=["children"])
