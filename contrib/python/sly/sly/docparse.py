# docparse.py
#
# Support doc-string parsing classes

__all__ = [ 'DocParseMeta' ]

class DocParseMeta(type):
    '''
    Metaclass that processes the class docstring through a parser and
    incorporates the result into the resulting class definition. This
    allows Python classes to be defined with alternative syntax. 
    To use this class, you first need to define a lexer and parser:

        from sly import Lexer, Parser
        class MyLexer(Lexer):
           ...

        class MyParser(Parser):
           ...

    You then need to define a metaclass that inherits from DocParseMeta.
    This class must specify the associated lexer and parser classes.
    For example:

        class MyDocParseMeta(DocParseMeta):
            lexer = MyLexer
            parser = MyParser

    This metaclass is then used as a base for processing user-defined
    classes:

        class Base(metaclass=MyDocParseMeta):
            pass

        class Spam(Base):
            """
            doc string is parsed
            ...
            """

    It is expected that the MyParser() class would return a dictionary. 
    This dictionary is used to create the final class Spam in this example.
    '''

    @staticmethod
    def __new__(meta, clsname, bases, clsdict):
        if '__doc__' in clsdict:
            lexer = meta.lexer()
            parser = meta.parser()
            lexer.cls_name = parser.cls_name = clsname
            lexer.cls_qualname = parser.cls_qualname = clsdict['__qualname__']
            lexer.cls_module = parser.cls_module = clsdict['__module__']
            parsedict = parser.parse(lexer.tokenize(clsdict['__doc__']))
            assert isinstance(parsedict, dict), 'Parser must return a dictionary'
            clsdict.update(parsedict)
        return super().__new__(meta, clsname, bases, clsdict)

    @classmethod
    def __init_subclass__(cls):
        assert hasattr(cls, 'parser') and hasattr(cls, 'lexer')
