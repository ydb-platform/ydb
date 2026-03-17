
Some text

    #!python
    def __init__ (self, pattern) :
        self.pattern = pattern
        self.compiled_re = re.compile("^(.*)%s(.*)$" % pattern, re.DOTALL)

    def getCompiledRegExp (self) :
        return self.compiled_re

More text