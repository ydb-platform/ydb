from libcpp.string cimport string
from util cimport istream

cdef extern from "<fst/symbol-table.h>" namespace "fst":    
    cdef cppclass SymbolTable:
        SymbolTable(SymbolTable &table)
        SymbolTable(string &name)
        long AddSymbol(string &symbol, long key)
        long AddSymbol(string &symbol)
        string& Name()
        bint Write(string &filename)
        #WriteText (ostream &strm)
        string Find(long key)
        long Find(const char* symbol)
        unsigned NumSymbols()
        string LabeledCheckSum()

    cdef cppclass SymbolTableIterator:
        SymbolTableIterator(SymbolTable& table)
        bint Done()
        long Value()
        string Symbol()
        void Next()
        void Reset()

    cdef SymbolTable* SymbolTableRead "fst::SymbolTable::Read" (istream &strm,
            string& source)
