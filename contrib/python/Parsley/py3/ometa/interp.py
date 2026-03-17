import string
from ometa.runtime import (InputStream, ParseError, EOFError, ArgInput,
                           joinErrors, expected, LeftRecursion)

def decomposeGrammar(grammar):
    rules = {}
    #XXX remove all these asserts once we have quasiterms
    assert grammar.tag.name == 'Grammar'
    for rule in grammar.args[2].args:
        assert rule.tag.name == 'Rule'
        rules[rule.args[0].data] = rule.args[1]
    return rules

# after 12, i'm worse than a gremlin
_feed_me = object()


class TrampolinedGrammarInterpreter(object):
    """
    An interpreter for OMeta grammars that processes input
    incrementally.
    """
    def __init__(self, grammar, rule, callback=None, globals=None):
        self.grammar = grammar
        self.position = 0
        self.callback = callback
        self.globals = globals or {}
        self.rules = decomposeGrammar(grammar)
        self.next = self.setNext(rule)
        self._localsStack = []
        self.currentResult = None
        self.input = InputStream([], 0)
        self.ended = False
        self._spanStart = 0


    def receive(self, buf):
        """
        Feed data to the parser.
        """
        if not buf:
            # No data. Nothing to do.
            return
        if self.ended:
            raise ValueError("Can't feed a parser that's been ended.")
        self.input.data.extend(buf)
        x = None
        for x in self.next:
            if x is _feed_me:
                return x
        if self.callback:
            self.callback(*x)
        self.ended = True


    def end(self):
        """
        Close the input stream, indicating to the grammar that no more
        input will arrive.
        """
        if self.ended:
            return
        self.ended = True
        x = None
        for x in self.next:
            pass
        if self.callback:
            self.callback(*x)


    def setNext(self, rule):
        if not isinstance(rule, tuple):
            rule = (rule, )
        return self.apply(rule[0], None, rule[1:])


    ## Implementation note: each method, instead of being a function
    ## returning a value, is a generator that will yield '_feed_me' an
    ## arbitrary number of times, then finally yield the value of the
    ## expression being evaluated.


    def _apply(self, rule, ruleName, args):
        """
        Apply a rule method to some args.
        @param rule: A method of this object.
        @param ruleName: The name of the rule invoked.
        @param args: A sequence of arguments to it.
        """
        if args:
            if ((not getattr(rule, 'func_code', None))
                 or rule.func_code.co_argcount - 1 != len(args)):
                for arg in args[::-1]:
                    self.input = ArgInput(arg, self.input)
                g = rule()
            else:
                g = rule(*args)
            for x in g:
                if x is _feed_me: yield x
            yield x
            return
        memoRec = self.input.getMemo(ruleName)
        if memoRec is None:
            oldPosition = self.input
            lr = LeftRecursion()
            memoRec = self.input.setMemo(ruleName, lr)

            try:
                inp = self.input
                for x in rule():
                    if x is _feed_me: yield x
                memoRec = inp.setMemo(ruleName, [x, self.input])
            except ParseError:
                raise
            if lr.detected:
                sentinel = self.input
                while True:
                    try:
                        self.input = oldPosition
                        for x in rule():
                            if x is _feed_me: yield x
                        ans = x
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
        yield memoRec[0]


    def _eval(self, expr):
        """
        Dispatch to a parse_<term name> method for the given grammar expression.
        """
        return getattr(self, "parse_" + expr.tag.name)(*expr.args)


    def parse_Apply(self, ruleName, codeName, args):
        for x in self.apply(ruleName.data, codeName.data, args.args):
            if x is _feed_me: yield x
        yield x


    def apply(self, ruleName, codeName, args):
        """
        Invoke a rule, optionally with arguments.
        """
        argvals = []
        # we tell whether a rule is a manually set one by the codeName
        # if it's None, then we think it's set by setNext
        if codeName is None:
            argvals = args
        else:
            for a in args:
                for x in self._eval(a):
                    if x is _feed_me: yield x
                argvals.append(x[0])
        _locals = {}
        self._localsStack.append(_locals)
        try:
            #XXX super
            rul = self.rules.get(ruleName)
            if rul:
                def f():
                    return self._eval(rul)
            else:
                #ruleName may be a Twine, so gotta call str()
                f = getattr(self, str('rule_' + ruleName))
            for x in self._apply(f, ruleName, argvals):
                if x is _feed_me: yield x
            yield x
        finally:
            self._localsStack.pop()


    def parse_Exactly(self, spec):
        """
        Accept a one or more characters that equal the given spec.
        """
        wanted = spec.data
        result = []
        for c in wanted:
            try:
                val, p = self.input.head()
            except EOFError:
                yield _feed_me
                val, p = self.input.head()
            result.append(val)
            if val == c:
                self.input = self.input.tail()
            else:
                raise self.err(p.withMessage(expected(None, wanted)))
        yield ''.join(result), p


    def parse_Token(self, spec):
        """
        Consume leading whitespace then the given string.
        """
        val = ' '
        while val.isspace():
            try:
                val, p = self.input.head()
            except EOFError:
                yield _feed_me
                val, p = self.input.head()
            if val.isspace():
                self.input = self.input.tail()
        wanted = spec.data
        result = []
        for c in wanted:
            try:
                val, p = self.input.head()
            except EOFError:
                yield _feed_me
                val, p = self.input.head()
            result.append(val)
            if val == c:
                self.input = self.input.tail()
            else:
                raise self.err(p.withMessage(expected("token", wanted)))
        yield ''.join(result), p


    def parse_And(self, expr):
        """
        Execute multiple subexpressions in order, returning the result
        of the last one.
        """
        seq = expr.args
        x = None, self.input.nullError()
        for subexpr in seq:
            for x in self._eval(subexpr):
                if x is _feed_me: yield x
            self.currentError = x[1]
        yield x


    def parse_Or(self, expr):
        """
        Execute multiple subexpressions, returning the result of the
        first one that succeeds.
        """
        errors = []
        i = self.input
        for subexpr in expr.args:
            try:
                for x in self._eval(subexpr):
                    if x is _feed_me: yield x
                val, p = x
                errors.append(p)
                self.currentError = joinErrors(errors)
                yield x
                return
            except ParseError as err:
                errors.append(err)
                self.input = i
        raise self.err(joinErrors(errors))


    def parse_Many(self, expr, ans=None):
        """
        Execute an expression repeatedly until it fails to match,
        collecting the results into a list. Implementation of '*'.
        """
        ans = ans or []
        err = None
        while True:
            try:
                m = self.input
                for x in self._eval(expr):
                    if x is _feed_me: yield x
                ans.append(x[0])
                self.currentError = x[1]
            except ParseError as error:
                err = error
                self.input = m
                break
        yield ans, err


    def parse_Many1(self, expr):
        """
        Execute an expression one or more times, collecting the
        results into a list. Implementation of '+'.
        """
        for x in self._eval(expr):
            if x is _feed_me: yield x
        for x in self.parse_Many(expr, ans=[x[0]]):
            if x is _feed_me: yield x
        yield x


    def parse_Repeat(self, min, max, expr):
        """
        Execute an expression between C{min} and C{max} times,
        collecting the results into a list. Implementation of '{}'.
        """
        if min.tag.name == '.int.':
            min = min.data
        else:
            min = self._localsStack[-1][min.data]
        if max.tag.name == '.int.':
            max = max.data
        elif max.tag.name == 'null':
            max = None
        else:
            max = self._localsStack[-1][max.data]

        e = None

        if min == max == 0:
            yield '', None
            return
        ans = []
        for i in range(min):
            for x in self._eval(expr):
                if x is _feed_me: yield x
            v, e = x
            ans.append(v)

        if max is not None:
            repeats = range(min, max)
            for i in repeats:
                try:
                    m = self.input
                    for x in self._eval(expr):
                        if x is _feed_me: yield x
                    v, e = x
                    ans.append(v)
                except ParseError as err:
                    e = err
                    self.input = m
                    break
        yield ans, e



    def parse_Optional(self, expr):
        """
        Execute an expression, returning None if it
        fails. Implementation of '?'.
        """
        i = self.input
        try:
            for x in self._eval(expr):
                if x is _feed_me: yield x
            yield x
        except ParseError:
            self.input = i
            yield (None, self.input.nullError())


    def parse_Not(self, expr):
        """
        Execute an expression, returning True if it fails and failing
        otherwise. Implementation of '~'.
        """
        m = self.input
        try:
            for x in self._eval(expr):
                if x is _feed_me: yield x
        except ParseError:
            self.input = m
            yield True, self.input.nullError()
        else:
            raise self.err(self.input.nullError())


    def parse_Label(self, expr, label_term):
        """
        Execute an expression , if it fails apply the label to the exception.
        """
        label = label_term.data
        try:
            for x in self._eval(expr):
                if x is _feed_me:
                    yield x
            print ("^^", label)
            self.currentError = x[1].withMessage([("Custom Exception:", label, None)])
            yield x[0], self.currentError
        except ParseError as e:
            err=e
            raise self.err(e.withMessage([("Custom Exception:", label, None)]))


    def parse_Lookahead(self, expr):
        """
        Execute an expression, then reset the input stream to the
        position before execution. Implementation of '~~'.
        """
        try:
            i = self.input
            for x in self._eval(expr):
                if x is _feed_me: yield x
        finally:
            self.input = i


    def parse_Bind(self, name, expr):
        """
        Execute an expression and bind its result to the given name.
        """
        for x in self._eval(expr):
            if x is _feed_me: yield x
        v, err = x
        if name.data:
            self._localsStack[-1][name.data] = v
        else:
            for n, val in zip(name.args, v):
                self._localsStack[-1][n.data] = val
        yield v, err


    def parse_Predicate(self, expr):
        """
        Run a Python expression and fail if it returns False.
        """
        for x in self._eval(expr):
            if x is _feed_me: yield x
        val, err = x
        if not val:
            raise self.err(err)
        else:
            yield True, err


    def parse_Action(self, expr):
        """
        Run a Python expression, return its result.
        """
        val = eval(expr.data, self.globals, self._localsStack[-1])
        yield val, self.input.nullError()


    def parse_ConsumedBy(self, expr):
        """
        Run an expression. Return the literal contents of the input
        stream it consumed.
        """
        oldInput = self.input
        for x in self._eval(expr):
            if x is _feed_me: yield x
        slice = oldInput.data[oldInput.position:self.input.position]
        yield "".join(slice), x[1]

    def rule_anything(self):
        """
        Match a single character.
        """
        try:
            val, p = self.input.head()
        except EOFError:
            yield _feed_me
            val, p = self.input.head()
        self.input = self.input.tail()
        yield val, p

    def rule_letter(self):
        """
        Match a single letter.
        """
        try:
            val, p = self.input.head()
        except EOFError:
            yield _feed_me
            val, p = self.input.head()
        if val in string.ascii_letters:
            self.input = self.input.tail()
            yield val, p
        else:
            raise self.err(p.withMessage(expected("letter")))

    def rule_digit(self):
        """
        Match a digit.
        """
        try:
            val, p = self.input.head()
        except EOFError:
            yield _feed_me
            val, p = self.input.head()
        if val in string.digits:
            self.input = self.input.tail()
            yield val, p
        else:
            raise self.err(p.withMessage(expected("digit")))

    def err(self, e):
        e.input = ''.join(str(i) for i in e.input)
        raise e

class GrammarInterpreter(object):

    def __init__(self, grammar, base, globals=None):
        """
        grammar: A term tree representing a grammar.
        """
        self.grammar = grammar
        self.base = base
        self.rules = {}
        self._localsStack = []
        self._globals = globals or {}
        self.rules = decomposeGrammar(grammar)
        self.run = None
        self._spanStart = 0

    def apply(self, input, rulename, tree=False):
        self.run = self.base(input, self._globals, tree=tree)
        #XXX hax, fix grammar parser to distinguish tree from nontree grammars
        if not isinstance(self.run.input.data, str):
            tree = True
            self.run.tree = True
        v, err = self._apply(self.run, rulename, ())
        return self.run.input, v, err


    def _apply(self, run, ruleName, args):
        argvals = [self._eval(run, a)[0] for a in args]
        _locals = {'self': run}
        self._localsStack.append(_locals)
        try:
            if ruleName == 'super':
                return run.superApply(ruleName, argvals)
            else:
                rul = self.rules.get(ruleName)
                if rul:
                    return run._apply(
                        (lambda: self._eval(run, rul)),
                        ruleName, argvals)
                else:
                    #ruleName may be a Twine, so calling str()
                    x = run._apply(getattr(run, str('rule_' + ruleName)),
                                   ruleName, argvals)
                    return x
        finally:
            self._localsStack.pop()


    def _eval(self, run, expr):
        name = expr.tag.name
        args = expr.args
        if name == "Apply":
            ruleName = args[0].data
            return self._apply(run, ruleName, args[2].args)

        elif name == "Exactly":
            return run.exactly(args[0].data)

        elif name == "Label":
            label = args[1].data
            try:
                val, err = self._eval(run, args[0])
                return val, err.withMessage([("Custom Exception:", label, None)])
            except ParseError as err:
                e=err
                raise e.withMessage([("Custom Exception:", label, None)])

        elif name == "Token":
            if run.tree:
                return run._apply(run.rule_exactly, "exactly", [args[0].data])
            else:
                return run._apply(run.rule_token, "token", [args[0].data])

        elif name in ("Many", "Many1"):
            ans = [self._eval(run, args[0])[0]] if name == "Many1" else []
            err = None
            while True:
                try:
                    m = run.input
                    v, _ = self._eval(run, args[0])
                    ans.append(v)
                except ParseError as e:
                    err = e
                    run.input = m
                    break
            return ans, err

        elif name == "Repeat":
            if args[0].tag.name == '.int.':
                min = args[0].data
            else:
                min = self._localsStack[-1][args[0].data]
            if args[1].tag.name == '.int.':
                max = args[1].data
            elif args[1].tag.name == 'null':
                max = None
            else:
                max = self._localsStack[-1][args[1].data]
            if min == max == 0:
                return "", None
            ans = []
            e = None
            for i in range(min):
                v, e = self._eval(run, args[2])
                ans.append(v)

            for i in range(min, max):
                try:
                    m = run.input
                    v, e = self._eval(run, args[2])
                    ans.append(v)
                except ParseError as err:
                    e = err
                    run.input = m
                    break
            return ans, e

        elif name == "Optional":
            i = run.input
            try:
                return self._eval(run, args[0])
            except ParseError:
                run.input = i
                return (None, run.input.nullError())

        elif name == "Or":
            errors = []
            for e in args[0].args:
                try:
                    m = run.input
                    x = self._eval(run, e)
                    ret, err = x
                    errors.append(err)
                    return ret, joinErrors(errors)
                except ParseError as err:
                    errors.append(err)
                    run.input = m
            raise joinErrors(errors)


        elif name == "Not":
            m = run.input
            try:
                self._eval(run, args[0])
            except ParseError as err:
                run.input = m
                return True, run.input.nullError()
            else:
                raise run.input.nullError()


        elif name == "Lookahead":
            try:
                m = run.input
                return self._eval(run, args[0])
            finally:
                run.input = m

        elif name == "And":
            v = None, run.input.nullError()
            for e in args[0].args:
                v = self._eval(run, e)
            return v

        elif name == "Bind":
            v, err =  self._eval(run, args[1])
            if args[0].data:
                self._localsStack[-1][args[0].data] = v
            else:
                for n, val in zip(args[0].args, v):
                    self._localsStack[-1][n.data] = val
            return v, err

        elif name == "Predicate":
            val, err = self._eval(run, args[0])
            if not val:
                raise err
            else:
                return True, err

        elif name == "List":
            v, e = run.rule_anything()
            oldInput = run.input
            try:
                run.input = InputStream.fromIterable(v)
            except TypeError:
                raise e.withMessage(expected("an iterable"))
            self._eval(run, args[0])
            run.end()
            run.input = oldInput
            return v, e

        elif name in ("Action", "Python"):
            lo = self._localsStack[-1]
            val = eval(args[0].data, self._globals, lo)
            return (val, run.input.nullError())

        elif name == "ConsumedBy":
            oldInput = run.input
            _, err = self._eval(run, args[0])
            slice = oldInput.data[oldInput.position:run.input.position]
            return slice, err

        else:
            raise ValueError("Unrecognized term: %r" % (name,))
