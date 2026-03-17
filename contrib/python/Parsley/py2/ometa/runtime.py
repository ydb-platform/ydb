# -*- test-case-name: ometa.test.test_runtime -*-
"""
Code needed to run a grammar after it has been compiled.
"""
import time
from textwrap import dedent
from terml.nodes import coerceToTerm, Term, termMaker as t
from ometa.builder import moduleFromGrammar, writePython

try:
    basestring
except NameError:
    basestring = str
    unicode = str

TIMING = False

class ParseError(Exception):
    """
    ?Redo from start
    """

    def __init__(self, input, position, message, trail=None):
        Exception.__init__(self, position, message)
        self.position = position
        self.error = message or []
        self.input = input
        self.trail = trail or []


    def __eq__(self, other):
        if other.__class__ == self.__class__:
            return (self.position, self.error) == (other.position, other.error)


    def mergeWith(self, other):
        """
        Merges in another error's error and trail.
        """
        self.error = list(set(self.error + other.error))
        self.args = (self.position, self.error)
        self.trail = other.trail or self.trail or []


    def formatReason(self):
        if not self.error:
            return "Syntax error"
        if len(self.error) == 1:
            if self.error[0][0] == 'message':
                return self.error[0][1]
            if self.error[0][2] == None:
                return 'expected a %s' % (self.error[0][1])
            else:
                typ = self.error[0][1]
                if typ is None:
                    if isinstance(self.input, basestring):
                        typ = 'character'
                    else:
                        typ = 'object'
                return 'expected the %s %r' % (typ, self.error[0][2])
        else:
            bits = []
            for s in self.error:
                if s[0] == 'message':
                    desc = s[1]
                elif s[2] is None:
                    desc = "a " + s[1]
                else:
                    desc = repr(s[2])
                    if s[1] is not None:
                        desc = "%s %s" % (s[1], desc)
                bits.append(desc)
            bits.sort()
            return "expected one of %s, or %s" % (', '.join(bits[:-1]),
                                                  bits[-1])


    def formatError(self):
        """
        Return a pretty string containing error info about string
        parsing failure.
        """
        #de-twineifying
        lines = str(self.input).split('\n')
        counter = 0
        lineNo = 1
        columnNo = 0
        for line in lines:
            newCounter = counter + len(line)
            if newCounter > self.position:
                columnNo = self.position - counter
                break
            else:
                counter += len(line) + 1
                lineNo += 1
        reason = self.formatReason()
        return ('\n' + line + '\n' + (' ' * columnNo + '^') +
                "\nParse error at line %s, column %s: %s. trail: [%s]\n"
                % (lineNo, columnNo, reason, ' '.join(self.trail)))


    def __str__(self):
        return self.formatError()


    def withMessage(self, msg):
        return ParseError(self.input, self.position, msg, self.trail)


class EOFError(ParseError):
    """
    Raised when the end of input is encountered.
    """
    def __init__(self, input, position):
        ParseError.__init__(self, input, position, eof())


def expected(typ, val=None):
    """
    Return an indication of expected input and the position where it was
    expected and not encountered.
    """

    return [("expected", typ, val)]


def eof():
    """
    Return an indication that the end of the input was reached.
    """
    return [("message", "end of input")]


def joinErrors(errors):
    """
    Return the error from the branch that matched the most of the input.
    """
    if len(errors) == 1:
        return errors[0]

    highestPos = -1
    results = set()
    trail = None

    for err in errors:
        pos = err.position
        if pos < highestPos:
            continue
        elif pos > highestPos:
            highestPos = pos
            trail = err.trail or None
            results = set(err.error)
        else:
            trail = err.trail or trail
            results.update(err.error)

    return ParseError(errors[0].input, highestPos, list(results), trail)


class character(str):
    """
    Type to allow distinguishing characters from strings.
    """

    def __iter__(self):
        """
        Prevent string patterns and list patterns from matching single
        characters.
        """
        raise TypeError("Characters are not iterable")



class unicodeCharacter(unicode):
    """
    Type to distinguish characters from Unicode strings.
    """
    def __iter__(self):
        """
        Prevent string patterns and list patterns from matching single
        characters.
        """
        raise TypeError("Characters are not iterable")



class InputStream(object):
    """
    The basic input mechanism used by OMeta grammars.
    """

    def fromIterable(cls, iterable):
        """
        @param iterable: Any iterable Python object.
        """
        if isinstance(iterable, (character, unicodeCharacter)):
            raise TypeError("Characters are not iterable")
        if isinstance(iterable, bytes):
            return WrappedValueInputStream(iterable, 0, wrapper=character)
        elif isinstance(iterable, unicode):
            return WrappedValueInputStream(iterable, 0,
                                           wrapper=unicodeCharacter)
        else:
            return cls(list(iterable), 0)
    fromIterable = classmethod(fromIterable)


    def fromFile(cls, f, encoding='utf-8'):
        if getattr(f, 'seek', None) and getattr(f, 'tell', None):
            position = f.tell()
        else:
            position = 0
        txt = f.read()
        return cls(txt, position)
    fromFile = classmethod(fromFile)


    def fromText(cls, t, name="<string>"):
        return cls(t, 0)

    fromText = classmethod(fromText)


    def __init__(self, data, position):
        self.data = data
        self.position = position
        self.memo = {}
        self.tl = None
        self.error = ParseError(self.data, self.position, None)

    def head(self):
        if self.position >= len(self.data):
            if getattr(self.data, 'join', None):
                data = self.data.__class__('').join(self.data)
            else:
                data = self.data
            raise EOFError(data, self.position + 1)
        return self.data[self.position], self.error

    def nullError(self, msg=None):
        if msg:
            return self.error.withMessage(msg)
        else:
            return self.error

    def tail(self):
        if self.tl is None:
            self.tl = InputStream(self.data, self.position+1)
        return self.tl

    def advanceBy(self, n):
        return InputStream(self.data, self.position + n)

    def slice(self, n):
        data = self.data[self.position:self.position + n]
        tail = self.advanceBy(n)
        return data, self.nullError(), tail

    def prev(self):
        return InputStream(self.data, self.position-1)

    def getMemo(self, name):
        """
        Returns the memo record for the named rule.
        @param name: A rule name.
        """
        return self.memo.get(name, None)


    def setMemo(self, name, rec):
        """
        Store a memo record for the given value and position for the given
        rule.
        @param name: A rule name.
        @param rec: A memo record.
        """
        self.memo[name] = rec
        return rec

    def __cmp__(self, other):
        return cmp((self.data, self.position), (other.data, other.position))


class WrappedValueInputStream(InputStream):

    def __init__(self, data, position, wrapper=None):
        InputStream.__init__(self, data, position)
        self.wrapper = wrapper

    def head(self):
        v, e = InputStream.head(self)
        return self.wrapper(v), e

    def tail(self):
        if self.tl is None:
            self.tl = WrappedValueInputStream(self.data, self.position+1,
                                              self.wrapper)
        return self.tl

    def advanceBy(self, n):
        return InputStream(self.data, self.position + n, self.wrapper)

    def slice(self, n):
        data = self.data[self.position:self.position + n]
        tail = self.advanceBy(n)
        return [self.wrapper(x) for x in data], self.nullError(), tail

class ArgInput(object):
    def __init__(self, arg, parent):
        self.arg = arg
        self.parent = parent
        self.memo = {}
        self.err = parent.nullError()

    @property
    def position(self):
        return self.parent.position + 1j

    @property
    def data(self):
        return self.parent.data

    def head(self):
        return self.arg, self.err

    def tail(self):
        return self.parent

    def advanceBy(self, n):
        return self.parent.advanceBy(n - 1)

    def slice(self, n):
        prevVal, _, input =  self.parent.slice(n - 1)
        return [self.arg] + list(prevVal), self.err, input

    def nullError(self):
        return self.parent.nullError()


    def getMemo(self, name):
        """
        Returns the memo record for the named rule.
        @param name: A rule name.
        """
        return self.memo.get(name, None)


    def setMemo(self, name, rec):
        """
        Store a memo record for the given value and position for the given
        rule.
        @param name: A rule name.
        @param rec: A memo record.
        """
        self.memo[name] = rec
        return rec


class LeftRecursion(object):
    """
    Marker for left recursion in a grammar rule.
    """
    detected = False


class OMetaBase(object):
    """
    Base class providing implementations of the fundamental OMeta
    operations. Built-in rules are defined here.
    """
    globals = None
    tree = False
    def __init__(self, input, globals=None, name='<string>', tree=False,
            stream=False):
        """
        @param input: The string or input object (if stream=True) to be parsed.

        @param globals: A dictionary of names to objects, for use in evaluating
        embedded Python expressions.

        @param tree: Whether the input should be treated as part of a
        tree of nested iterables, rather than being a standalone
        string.

        @param stream: Whether the input should be treated as an existing
        InputStream object.
        """
        if stream:
            self.input = input
        elif self.tree or tree:
            self.input = InputStream.fromIterable(input)
        else:
            self.input = InputStream.fromText(input)
        self.locals = {}
        if self.globals is None:
            if globals is None:
                self.globals = {}
            else:
                self.globals = globals
        if basestring is str:
            self.globals['basestring'] = str
            self.globals['unichr'] = chr
        self.currentError = self.input.nullError()

    def considerError(self, error, typ=None):
        if error:
            newPos = error.position
            curPos = self.currentError.position
            if newPos > curPos:
                self.currentError = error
            elif newPos == curPos:
                self.currentError.mergeWith(error)


    def _trace(self, src, span, inputPos):
        pass

    def superApply(self, ruleName, *args):
        """
        Apply the named rule as defined on this object's superclass.

        @param ruleName: A rule name.
        """
        r = getattr(super(self.__class__, self), "rule_"+ruleName, None)
        if r is not None:
            self.input.setMemo(ruleName, None)
            return self._apply(r, ruleName, args)
        else:
            raise NameError("No rule named '%s'" %(ruleName,))

    def foreignApply(self, grammarName, ruleName, globals_, locals_, *args):
        """
        Apply the named rule of a foreign grammar.

        @param grammarName: name to look up in locals/globals for grammar
        @param ruleName: rule name
        """
        grammar = locals_.get(grammarName, None)
        if grammar is None:
            grammar = globals_[grammarName]

        grammar = getattr(grammar, "_grammarClass", grammar)
        instance = grammar(self.input, stream=True)
        rule = getattr(instance, "rule_" + ruleName, None)
        if rule is not None:
            self.input.setMemo(ruleName, None)
            result = instance._apply(rule, ruleName, args)
            self.input = instance.input
            return result
        else:
            raise NameError("No rule named '%s' on grammar '%s'" %
                    (ruleName, grammarName))

    def apply(self, ruleName, *args):
        """
        Apply the named rule, optionally with some arguments.

        @param ruleName: A rule name.
        """
        r = getattr(self, "rule_"+ruleName, None)
        if r is not None:
            val, err = self._apply(r, ruleName, args)
            return val, err

        else:
            raise NameError("No rule named '%s'" %(ruleName,))


    def _apply(self, rule, ruleName, args):
        """
        Apply a rule method to some args.
        @param rule: A method of this object.
        @param ruleName: The name of the rule invoked.
        @param args: A sequence of arguments to it.
        """
        if args:
            if basestring is str:
                attrname = '__code__'
            else:
                attrname = 'func_code'
            if ((not getattr(rule, attrname, None))
                 or getattr(rule, attrname).co_argcount - 1 != len(args)):
                for arg in args[::-1]:
                    self.input = ArgInput(arg, self.input)
                return rule()
            else:
                return rule(*args)
        memoRec = self.input.getMemo(ruleName)
        if memoRec is None:
            oldPosition = self.input
            lr = LeftRecursion()
            memoRec = self.input.setMemo(ruleName, lr)
            try:
                memoRec = self.input.setMemo(ruleName,
                                         [rule(), self.input])
            except ParseError as e:
                e.trail.append(ruleName)
                raise
            if lr.detected:
                sentinel = self.input
                while True:
                    try:
                        self.input = oldPosition
                        ans = rule()
                        if (self.input == sentinel):
                            break

                        memoRec = oldPosition.setMemo(ruleName,
                                                     [ans, self.input])
                    except ParseError:
                        break
            self.input = oldPosition

        elif isinstance(memoRec, LeftRecursion):
            memoRec.detected = True
            raise self.input.nullError()
        self.input = memoRec[1]
        return memoRec[0]


    def exactly(self, wanted):
        """
        Match a single item from the input equal to the given
        specimen, or a sequence of characters if the input is string.
        @param wanted: What to match.
        """
        i = self.input
        if not self.tree and len(wanted) > 1:
            val, p, self.input = self.input.slice(len(wanted))
        else:
            val, p = self.input.head()
            self.input = self.input.tail()
        if wanted == val:
            return val, p
        else:
            self.input = i
            raise p.withMessage(expected(None, wanted))


    def many(self, fn, *initial):
        """
        Call C{fn} until it fails to match the input. Collect the resulting
        values into a list.

        @param fn: A callable of no arguments.
        @param initial: Initial values to populate the returned list with.
        """
        ans = []
        for x, e in initial:
            ans.append(x)
        while True:
            try:
                m = self.input
                v, _ = fn()
                ans.append(v)
            except ParseError as err:
                e = err
                self.input = m
                break
        return ans, e


    def repeat(self, min, max, fn):
        """
        Call C{fn} C{max} times or until it fails to match the
        input. Fail if less than C{min} matches were made.
        Collect the results into a list.
        """
        if min == max == 0:
            return '', None
        ans = []
        for i in range(min):
            v, e = fn()
            ans.append(v)

        for i in range(min, max):
            try:
                m = self.input
                v, e = fn()
                ans.append(v)
            except ParseError as err:
                e = err
                self.input = m
                break
        return ans, e

    def _or(self, fns):
        """
        Call each of a list of functions in sequence until one succeeds,
        rewinding the input between each.

        @param fns: A list of no-argument callables.
        """
        errors = []
        for f in fns:
            try:
                m = self.input
                ret, err = f()
                errors.append(err)
                return ret, joinErrors(errors)
            except ParseError as e:
                errors.append(e)
                self.input = m
        raise joinErrors(errors)


    def _not(self, fn):
        """
        Call the given function. Raise ParseError iff it does not.

        @param fn: A callable of no arguments.
        """
        m = self.input
        try:
            fn()
        except ParseError:
            self.input = m
            return True, self.input.nullError()
        else:
            raise self.input.nullError()


    def eatWhitespace(self):
        """
        Consume input until a non-whitespace character is reached.
        """
        while True:
            try:
                c, e = self.input.head()
            except EOFError as err:
                e = err
                break
            tl = self.input.tail()
            if c.isspace():
                self.input = tl
            else:
                break
        return True, e


    def pred(self, expr):
        """
        Call the given function, raising ParseError if it returns false.

        @param expr: A callable of no arguments.
        """
        val, e = expr()
        if not val:
            raise e
        else:
            return True, e


    def listpattern(self, expr):
        """
        Call the given function, treating the next object on the stack as an
        iterable to be used for input.

        @param expr: A callable of no arguments.
        """
        v, e = self.rule_anything()
        oldInput = self.input
        try:
            self.input = InputStream.fromIterable(v)
        except TypeError:
            raise e.withMessage(expected("an iterable"))

        expr()
        self.end()
        self.input = oldInput
        return v, e


    def consumedby(self, expr):
        oldInput = self.input
        _, e = expr()
        slice = oldInput.data[oldInput.position:self.input.position]
        return slice, e


    def stringtemplate(self, template, vals):
        output = []
        checkIndent = False
        currentIndent = ""
        for chunk in template.args:
            if chunk.tag.name == ".String.":
                output.append(chunk.data)
                if checkIndent and chunk.data.isspace():
                    currentIndent = chunk.data
                    checkIndent = False
                if chunk.data.endswith('\n'):
                    checkIndent = True
            elif chunk.tag.name == "QuasiExprHole":
                v = vals[chunk.args[0].data]
                if not isinstance(v, basestring):
                    try:
                        vs = list(v)
                    except TypeError:
                        raise TypeError("Only know how to templatize strings and lists of strings")
                    lines = []
                    for x in vs:
                        lines.extend(x.split('\n'))
                    compacted_lines = []
                    for line in lines:
                        if line:
                            compacted_lines.append(line)
                        elif compacted_lines:
                            compacted_lines[-1] = compacted_lines[-1] + '\n'
                    v = ("\n" + currentIndent).join(compacted_lines)
                output.append(v)
            else:
                raise TypeError("didn't expect %r in string template" % chunk)
        return ''.join(output).rstrip('\n'), None

    def end(self):
        """
        Match the end of the stream.
        """
        return self._not(self.rule_anything)


    def lookahead(self, f):
        """
        Execute the given callable, rewinding the stream no matter whether it
        returns successfully or not.

        @param f: A callable of no arguments.
        """
        try:
            m = self.input
            x = f()
            return x
        finally:
            self.input = m


    def token(self, tok):
        """
        Match and return the given string, consuming any preceding whitespace.
        """
        m = self.input
        try:
            self.eatWhitespace()
            for c in tok:
                v, e = self.exactly(c)
            return tok, e
        except ParseError as e:
            self.input = m
            raise e.withMessage(expected("token", tok))

    def label(self, foo, label):
        """
        Wrap a function and add label to expected message.
        """
        try:
            val, err = foo()
            err2 = err.withMessage([("Custom Exception:", label, None)])
            if self.currentError == err:
                self.currentError = err2
            return val, err2
        except ParseError as e:
            raise e.withMessage([("Custom Exception:", label, None)])


    def letter(self):
        """
        Match a single letter.
        """
        x, e = self.rule_anything()
        if x.isalpha():
            return x, e
        else:
            raise e.withMessage(expected("letter"))


    def letterOrDigit(self):
        """
        Match a single alphanumeric character.
        """
        x, e = self.rule_anything()
        if x.isalnum():
            return x, e
        else:
            raise e.withMessage(expected("letter or digit"))

    def digit(self):
        """
        Match a single digit.
        """
        x, e = self.rule_anything()
        if x.isdigit():
            return x, e
        else:
            raise e.withMessage(expected("digit"))

    rule_digit = digit

    rule_letterOrDigit = letterOrDigit
    rule_letter = letter
    rule_end = end
    rule_ws = eatWhitespace
    rule_exactly = exactly

    #Deprecated.
    rule_spaces = eatWhitespace
    rule_token = token


    def rule_anything(self):
        """
        Match a single item from the input of any kind.
        """
        h, p = self.input.head()
        self.input = self.input.tail()
        return h, p



class OMetaGrammarBase(OMetaBase):
    """
    Common methods for the OMeta grammar parser itself, and its variants.
    """
    tree_target = False

    @classmethod
    def makeGrammar(cls, grammar, name='Grammar'):
        """
        Define a new parser class with the rules in the given grammar.

        @param grammar: A string containing a PyMeta grammar.
        @param name: The name of the class to be generated.
        @param superclass: The class the generated class is a child of.
        """
        g = cls(grammar)
        if TIMING:
            start = time.time()
        tree = g.parseGrammar(name)
        if TIMING:
            print("Grammar %r parsed in %g secs" % (name, time.time() - start))
            def cnt(n):
                count = sum(cnt(a) for a in n.args) + 1
                return count
            print("%d nodes." % (cnt(tree)))
            start = time.time()
        modname = "pymeta_grammar__" + name
        filename = "/pymeta_generated_code/" + modname + ".py"
        source = writePython(tree, grammar)
        if TIMING:
            print("Grammar %r generated in %g secs" % (name, time.time() - start))
        return moduleFromGrammar(source, name, modname, filename)


    def __init__(self, grammar, *a, **kw):
        OMetaBase.__init__(self, dedent(grammar), *a, **kw)
        self._spanStart = 0


    def parseGrammar(self, name):
        """
        Entry point for converting a grammar to code (of some variety).

        @param name: The name for this grammar.

        @param builder: A class that implements the grammar-building interface
        (interface to be explicitly defined later)
        """
        self.name = name
        res, err = self.apply("grammar")
        try:
            self.input.head()
        except EOFError:
            pass
        else:
            raise err
        return res


    def startSpan(self):
        self._spanStart = self.input.position

    def getSpan(self):
        return (self._spanStart, self.input.position)

    def applicationArgs(self, finalChar):
        """
        Collect rule arguments, a list of Python expressions separated by
        spaces.
        """
        args = []
        while True:
            try:
                (arg, endchar), err = self.pythonExpr(" " + finalChar)
                if not arg:
                    break
                args.append(t.Action(arg))
                if endchar == finalChar:
                    break
                if endchar == ' ':
                    self.rule_anything()
            except ParseError:
                break
        if args:
            return args
        else:
            raise self.input.nullError()

    def ruleValueExpr(self, singleLine, span=None):
        """
        Find and generate code for a Python expression terminated by a close
        paren/brace or end of line.
        """
        (expr, endchar), err = self.pythonExpr(endChars="\r\n)]")
        # if str(endchar) in ")]" or (singleLine and endchar):
        #     self.input = self.input.prev()
        return t.Action(expr, span=span)

    def semanticActionExpr(self, span=None):
        """
        Find and generate code for a Python expression terminated by a
        close-paren, whose return value is ignored.
        """
        val = t.Action(self.pythonExpr(')')[0][0], span=span)
        self.exactly(')')
        return val

    def semanticPredicateExpr(self, span=None):
        """
        Find and generate code for a Python expression terminated by a
        close-paren, whose return value determines the success of the pattern
        it's in.
        """
        expr = t.Action(self.pythonExpr(')')[0][0], span=span)
        self.exactly(')')
        return t.Predicate(expr, span=span)


    def eatWhitespace(self):
        """
        Consume input until a non-whitespace character is reached.
        """
        consumingComment = False
        while True:
            try:
                c, e = self.input.head()
            except EOFError as e:
                break
            t = self.input.tail()
            if c.isspace() or consumingComment:
                self.input = t
                if c == '\n':
                    consumingComment = False
            elif c == '#':
                consumingComment = True
            else:
                break
        return True, e
    rule_spaces = eatWhitespace


    def pythonExpr(self, endChars="\r\n"):
        """
        Extract a Python expression from the input and return it.

        @arg endChars: A set of characters delimiting the end of the expression.
        """
        delimiters = { "(": ")", "[": "]", "{": "}"}
        stack = []
        expr = []
        endchar = None
        while True:
            try:
                c, e = self.rule_anything()
                endchar = c
            except ParseError as err:
                e = err
                endchar = None
                break
            if c in endChars and len(stack) == 0:
                self.input = self.input.prev()
                break
            else:
                expr.append(c)
                if c in delimiters:
                    stack.append(delimiters[c])
                elif len(stack) > 0 and c == stack[-1]:
                    stack.pop()
                elif c in delimiters.values():
                    raise ParseError(self.input.data, self.input.position,
                                     expected("Python expression"))
                elif c in "\"'":
                    while True:
                        strc, stre = self.rule_anything()
                        expr.append(strc)
                        slashcount = 0
                        while strc == '\\':
                            strc, stre = self.rule_anything()
                            expr.append(strc)
                            slashcount += 1
                        if strc == c and slashcount % 2 == 0:
                            break

        if len(stack) > 0:
            raise ParseError(self.input.data, self.input.position,
                             expected("Python expression"))
        return (''.join(expr).strip(), endchar), e


    def isTree(self):
        self.tree_target = True


class TreeTransformerBase(OMetaBase):

    @classmethod
    def transform(cls, term):
        g = cls([term])
        return g.apply("transform")

    def _transform_data(self, tt):
        if tt.data is not None:
            return tt.data
        name = tt.tag.name
        if name == 'null':
            return None
        if name == 'true':
            return True
        if name == 'false':
            return False
        raise ValueError()

    def rule_transform(self):
        tt, e = self.rule_anything()
        if isinstance(tt, Term):
            try:
                return self._transform_data(tt), e
            except ValueError:
                name = tt.tag.name
                if name == '.tuple.':
                    return self._transform_iterable(tt.args)
                else:
                    if getattr(self, 'rule_' + name, None):
                        return self.apply(name, tt)
                    else:
                        return self.apply("unknown_term", tt)
        else:
            return self._transform_iterable(tt)

    def _transform_iterable(self, contents):
        oldInput = self.input
        self.input = InputStream.fromIterable(contents)
        v = self.many(self.rule_transform)
        self.end()
        self.input = oldInput
        return v

    def rule_unknown_term(self):
        tt, _ = self.rule_anything()
        oldInput = self.input
        self.input = InputStream.fromIterable(tt.args)
        newargs, e = self.many(self.rule_transform)
        self.end()
        self.input = oldInput
        return Term(tt.tag, None, tuple(coerceToTerm(a) for a in newargs),
                    tt.span), e

    def rule_null(self):
        tt, e = self.rule_anything()
        if not tt.tag.name == "null":
            raise self.input.nullError()
        return None, self.input.nullError()

    def termpattern(self, name, expr):
        v, e = self.rule_anything()
        if name == ".tuple." and getattr(v, 'tag', None) is None:
            newInput = v
        elif name != v.tag.name:
            raise e.withMessage(expected("a Term named " + name))
        else:
            newInput = v.args
        oldInput = self.input
        try:
            self.input = InputStream.fromIterable(newInput)
        except TypeError:
            raise e.withMessage(expected("a Term"))

        expr()
        self.end()
        self.input = oldInput
        return v, e

    def exactly(self, wanted):
        """
        Match a single item from the input equal to the given
        specimen, or a sequence of characters if the input is string.
        @param wanted: What to match.
        """
        i = self.input
        if not self.tree and len(wanted) > 1:
            val, p, self.input = self.input.slice(len(wanted))
        else:
            val, p = self.input.head()
            self.input = self.input.tail()
        if getattr(val, 'data', None) is not None:
            val = val.data
        if wanted == val:
            return val, p
        else:
            self.input = i
            raise p.withMessage(expected(None, wanted))

    rule_exactly = exactly
