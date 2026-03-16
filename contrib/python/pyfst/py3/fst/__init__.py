from fst._fst import EPSILON, EPSILON_ID, SymbolTable,\
        read, read_log, read_std, read_symbols,\
        LogWeight, LogArc, LogState, LogVectorFst,\
        TropicalWeight, StdArc, StdState, StdVectorFst


def _make_transducer_class(clsname, parent):
    def __init__(self, isyms=None, osyms=None):
        """Transducer(isyms=None, osyms=None) -> transducer with input/output symbol tables"""
        parent.__init__(self, isyms, osyms)
        self.start = self.add_state()
        self.isyms = (isyms if isyms is not None else SymbolTable())
        self.osyms = (osyms if osyms is not None else SymbolTable())

    def add_arc(self, src, tgt, ilabel, olabel, weight=None):
        """fst.add_arc(int source, int dest, ilabel, olabel, weight=None):
        add an arc source->dest labeled with labels ilabel:olabel and weighted with weight"""
        while src > len(self) - 1:
            self.add_state()
        parent.add_arc(self, src, tgt, self.isyms[ilabel], self.osyms[olabel],
                weight=weight)

    def __getitem__(self, stateid):
        while stateid > len(self) - 1:
            self.add_state()
        return parent.__getitem__(self, stateid)

    return type(clsname, (parent,), {'__init__': __init__,
                                     'add_arc': add_arc,
                                     '__getitem__': __getitem__})


StdTransducer = _make_transducer_class('StdTransducer', StdVectorFst)
LogTransducer = _make_transducer_class('LogTransducer', LogVectorFst)


class Transducer(object):
    """Transducer(isyms=None, osyms=None, semiring='tropical') -> transducer
    from the desired semiring"""
    def __new__(cls, isyms=None, osyms=None, semiring='tropical'):
        return (LogTransducer if semiring == 'log' else StdTransducer)(isyms, osyms)


def _make_acceptor_class(clsname, parent):
    def __init__(self, syms=None):
        """Acceptor(syms=None) -> acceptor transducer with an input/output symbol table"""
        syms = (syms if syms is not None else SymbolTable())
        parent.__init__(self, syms, syms)

    def add_arc(self, src, tgt, label, weight=None):
        """fst.add_arc(int source, int dest, label, weight=None):
        add an arc source->dest labeled with label and weighted with weight"""
        parent.add_arc(self, src, tgt, label, label, weight)

    return type(clsname, (parent,), {'__init__': __init__,
                                     'add_arc': add_arc})


StdAcceptor = _make_acceptor_class('StdAcceptor', StdTransducer)
LogAcceptor = _make_acceptor_class('LogAcceptor', LogTransducer)


class Acceptor(object):
    """Acceptor(syms=None, semiring='tropical') -> acceptor from the desired semiring"""
    def __new__(cls, syms=None, semiring='tropical'):
        return (LogAcceptor if semiring == 'log' else StdAcceptor)(syms)
    

def linear_chain(text, syms=None, semiring='tropical'):
    """linear_chain(text, syms=None) -> linear chain acceptor for the given input text"""
    chain = Acceptor(syms, semiring=semiring)
    for i, c in enumerate(text):
        chain.add_arc(i, i+1, c)
    chain[i+1].final = True
    return chain
