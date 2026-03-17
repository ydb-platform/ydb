from collections import namedtuple
import sys

try:
    basestring
    scalar_types = (str, unicode, int, long, float)
    integer_types = (int, long)
except NameError:
    basestring = str
    scalar_types = (str, int, float)
    integer_types = (int,)

_Term = namedtuple("Term", "tag data args span")
class Term(_Term):
    def __new__(cls, tag, data, args, span):
        #XXX AstroTag tracks (name, tag_code) and source span
        if data and not isinstance(data, scalar_types):
            raise ValueError("Term data can't be of type %r" % (type(data),))
        if data and args:
            raise ValueError("Term %s can't have both data and children" % (tag,))
        if args is None:
            args = ()
        return _Term.__new__(cls, tag, data, tuple(args), span)

    def __iter__(self):
        #and now I feel a bit silly subclassing namedtuple
        raise NotImplementedError()

    def __eq__(self, other):
        try:
            if self.tag.name == ".bag." and other.tag.name == ".bag.":
                return (self.data, set(self.args)
                  ) == (other.data, set(other.args))
            return (     self.tag, self.data, self.args
                   ) == (other.tag, other.data, other.args)
        except AttributeError:
            return False


    def __hash__(self):
        return hash((Term, self.tag, self.data, self.args))


    def __repr__(self):
        return "term('%s')" % (self._unparse(4).replace("'", "\\'"))


    def _unparse(self, indentLevel=0):
        newlineAndIndent = '\n' + (' ' * indentLevel)
        if self.data is not None:
            if self.tag.name == '.String.':
                return '"%s"' % repr(self.data)[1:-1].replace("\\'", "'").replace('"', '\\\\"')
            elif self.tag.name == '.char.':
                return "'%s'" % repr(self.data)[1:-1].replace("'", "\\'").replace('\\"', '"')
            else:
                return str(self.data)
        args = ', '.join([a._unparse() for a in self.args])
        if self.tag.name == '.tuple.':
            return "[%s]" % (args,)
        elif self.tag.name == '.attr.':
            return "%s: %s" % (self.args[0]._unparse(indentLevel),
                               self.args[1]._unparse(indentLevel))
        elif self.tag.name == '.bag.':
            return "{%s}" % (args,)
        elif len(self.args) == 1 and self.args[0].tag.name == '.bag.':
            return "%s%s" % (self.tag._unparse(indentLevel), args)
        else:
            if len(self.args) == 0:
                return self.tag._unparse(indentLevel)
            return "%s(%s)" % (self.tag._unparse(indentLevel), args)

    def withSpan(self, span):
        return Term(self.tag, self.data, self.args, span)


    def build(self, builder):
        if self.data is None:
            f = builder.leafTag(self.tag, self.span)
        else:
            f = builder.leafData(self.data, self.span)

        return builder.term(f, [arg.build(builder) for arg in self.args])


    def __cmp__(self, other):
        tagc = cmp(self.tag, other.tag)
        if tagc:
            return tagc
        datac = cmp(self.data, other.data)
        if datac:
            return datac
        if self.tag.name == ".bag." and other.tag.name == ".bag.":
            return cmp(set(self.args), set(other.args))
        return cmp(self.args, other.args)

    def __int__(self):
        return int(self.data)

    def __float__(self):
        return float(self.data)

    def withoutArgs(self):
        return Term(self.tag, self.data, (), self.span)

    def asFunctor(self):
        if self.args:
            raise ValueError("Terms with args can't be used as functors")
        else:
            return self.tag


class Tag(object):
    def __init__(self, name):
        if name[0] == '':
            raise ValueError("Tags must have names")
        self.name = name

    def __eq__(self, other):
        return other.__class__ == self.__class__ and self.name == other.name

    def __ne__(self, other):
        return not self == other

    def __repr__(self):
        return "Tag(%r)" % (self.name,)

    def __hash__(self):
        return hash((Tag, self.name))

    def _unparse(self, indentLevel=0):
        return self.name

def coerceToTerm(val):
    from ometa.runtime import character, unicodeCharacter
    if isinstance(val, Term):
        return val
    if val is None:
        return Term(Tag("null"), None, None, None)
    if val is True:
        return Term(Tag("true"), None, None, None)
    if val is False:
        return Term(Tag("false"), None, None, None)
    if isinstance(val, integer_types):
        return Term(Tag(".int."), val, None, None)
    if isinstance(val, float):
        return Term(Tag(".float64."), val, None, None)
    if isinstance(val, (character, unicodeCharacter)):
        return Term(Tag(".char."), val, None, None)
    if isinstance(val, basestring):
        return Term(Tag(".String."), val, None, None)
    if isinstance(val, (list, tuple)):
        return Term(Tag(".tuple."), None, tuple(coerceToTerm(item) for item in val), None)
    if isinstance(val, set):
        return Term(Tag('.bag.'), None, tuple(coerceToTerm(item) for item in val), None)
    if isinstance(val, dict):
        return Term(Tag('.bag.'), None, tuple(Term(Tag('.attr.'), None,
                                                   (coerceToTerm(k), coerceToTerm(v)), None)
                                         for (k, v) in val.items()),
                    None)
    raise ValueError("Could not coerce %r to Term" % (val,))

class TermMaker(object):
    def __getattr__(self, name):
        def mkterm(*args, **kwargs):
            return Term(Tag(name), None,
                        tuple([coerceToTerm(a) for a in args]),
                        kwargs.get('span', None))
        return mkterm

termMaker = TermMaker()

