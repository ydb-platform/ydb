def createParserClass(GrammarBase, ruleGlobals):
    if ruleGlobals is None:
        ruleGlobals = {}
    class vm_emit(GrammarBase):
        def rule_Grammar(self):
            _locals = {'self': self}
            self.locals['Grammar'] = _locals
            def _G_termpattern_1():
                self._trace('#TreeTransformer\nGrammar(str', (0, 28), self.input.position)
                _G_apply_2, lastError = self._apply(self.rule_str, "str", [])
                self.considerError(lastError, None)
                _locals['name'] = _G_apply_2
                self._trace('#TreeTransformer\nGrammar(str:name str', (0, 37), self.input.position)
                _G_apply_3, lastError = self._apply(self.rule_str, "str", [])
                self.considerError(lastError, None)
                _locals['tree'] = _G_apply_3
                _G_apply_4, lastError = self._apply(self.rule_transform, "transform", [])
                self.considerError(lastError, None)
                _locals['rules'] = _G_apply_4
                return (_locals['rules'], self.currentError)
            _G_termpattern_5, lastError = self.termpattern('Grammar', _G_termpattern_1)
            self.considerError(lastError, 'Grammar')
            from terml.parser import parseTerm as term
            _G_stringtemplate_6, lastError = self.stringtemplate(term('["class ", QuasiExprHole("name"), ":\n", "    tree = ", QuasiExprHole("tree"), "\n", "    ", QuasiExprHole("rules"), "\n"]'), _locals)
            self.considerError(lastError, None)
            return (_G_stringtemplate_6, self.currentError)


        def rule_Rule(self):
            _locals = {'self': self}
            self.locals['Rule'] = _locals
            def _G_termpattern_7():
                self._trace('#TreeTransformer\nGrammar(str:name str:tree @rules) {{{\nclass $name:\n    tree = $tree\n    $rules\n}}}\nRule(str', (0, 108), self.input.position)
                _G_apply_8, lastError = self._apply(self.rule_str, "str", [])
                self.considerError(lastError, None)
                _locals['name'] = _G_apply_8
                _G_apply_9, lastError = self._apply(self.rule_transform, "transform", [])
                self.considerError(lastError, None)
                _locals['rules'] = _G_apply_9
                return (_locals['rules'], self.currentError)
            _G_termpattern_10, lastError = self.termpattern('Rule', _G_termpattern_7)
            self.considerError(lastError, 'Rule')
            from terml.parser import parseTerm as term
            _G_stringtemplate_11, lastError = self.stringtemplate(term('[QuasiExprHole("name"), " = [\n", "    ", QuasiExprHole("rules"), "\n", "]\n"]'), _locals)
            self.considerError(lastError, None)
            return (_G_stringtemplate_11, self.currentError)


        def rule_Ascend(self):
            _locals = {'self': self}
            self.locals['Ascend'] = _locals
            def _G_termpattern_12():
                return (None, self.currentError)
            _G_termpattern_13, lastError = self.termpattern('Ascend', _G_termpattern_12)
            self.considerError(lastError, 'Ascend')
            from terml.parser import parseTerm as term
            _G_stringtemplate_14, lastError = self.stringtemplate(term('["t.Ascend(),"]'), _locals)
            self.considerError(lastError, None)
            return (_G_stringtemplate_14, self.currentError)


        def rule_Bind(self):
            _locals = {'self': self}
            self.locals['Bind'] = _locals
            def _G_termpattern_15():
                self._trace('#TreeTransformer\nGrammar(str:name str:tree @rules) {{{\nclass $name:\n    tree = $tree\n    $rules\n}}}\nRule(str:name @rules) {{{\n$name = [\n    $rules\n]\n}}}\n\nAscend() --> t.Ascend(),\nBind(str', (0, 187), self.input.position)
                _G_apply_16, lastError = self._apply(self.rule_str, "str", [])
                self.considerError(lastError, None)
                _locals['x'] = _G_apply_16
                return (_locals['x'], self.currentError)
            _G_termpattern_17, lastError = self.termpattern('Bind', _G_termpattern_15)
            self.considerError(lastError, 'Bind')
            from terml.parser import parseTerm as term
            _G_stringtemplate_18, lastError = self.stringtemplate(term('["t.Bind(", QuasiExprHole("x"), "),"]'), _locals)
            self.considerError(lastError, None)
            return (_G_stringtemplate_18, self.currentError)


        def rule_Call(self):
            _locals = {'self': self}
            self.locals['Call'] = _locals
            def _G_termpattern_19():
                self._trace('#TreeTransformer\nGrammar(str:name str:tree @rules) {{{\nclass $name:\n    tree = $tree\n    $rules\n}}}\nRule(str:name @rules) {{{\n$name = [\n    $rules\n]\n}}}\n\nAscend() --> t.Ascend(),\nBind(str:x) --> t.Bind($x),\nCall(str', (0, 215), self.input.position)
                _G_apply_20, lastError = self._apply(self.rule_str, "str", [])
                self.considerError(lastError, None)
                _locals['x'] = _G_apply_20
                return (_locals['x'], self.currentError)
            _G_termpattern_21, lastError = self.termpattern('Call', _G_termpattern_19)
            self.considerError(lastError, 'Call')
            from terml.parser import parseTerm as term
            _G_stringtemplate_22, lastError = self.stringtemplate(term('["t.Call(", QuasiExprHole("x"), "),"]'), _locals)
            self.considerError(lastError, None)
            return (_G_stringtemplate_22, self.currentError)


        def rule_Choice(self):
            _locals = {'self': self}
            self.locals['Choice'] = _locals
            def _G_termpattern_23():
                self._trace('#TreeTransformer\nGrammar(str:name str:tree @rules) {{{\nclass $name:\n    tree = $tree\n    $rules\n}}}\nRule(str:name @rules) {{{\n$name = [\n    $rules\n]\n}}}\n\nAscend() --> t.Ascend(),\nBind(str:x) --> t.Bind($x),\nCall(str:x) --> t.Call($x),\nChoice(str', (0, 245), self.input.position)
                _G_apply_24, lastError = self._apply(self.rule_str, "str", [])
                self.considerError(lastError, None)
                _locals['x'] = _G_apply_24
                return (_locals['x'], self.currentError)
            _G_termpattern_25, lastError = self.termpattern('Choice', _G_termpattern_23)
            self.considerError(lastError, 'Choice')
            from terml.parser import parseTerm as term
            _G_stringtemplate_26, lastError = self.stringtemplate(term('["t.Choice(", QuasiExprHole("x"), "),"]'), _locals)
            self.considerError(lastError, None)
            return (_G_stringtemplate_26, self.currentError)


        def rule_Commit(self):
            _locals = {'self': self}
            self.locals['Commit'] = _locals
            def _G_termpattern_27():
                self._trace('#TreeTransformer\nGrammar(str:name str:tree @rules) {{{\nclass $name:\n    tree = $tree\n    $rules\n}}}\nRule(str:name @rules) {{{\n$name = [\n    $rules\n]\n}}}\n\nAscend() --> t.Ascend(),\nBind(str:x) --> t.Bind($x),\nCall(str:x) --> t.Call($x),\nChoice(str:x) --> t.Choice($x),\nCommit(str', (0, 277), self.input.position)
                _G_apply_28, lastError = self._apply(self.rule_str, "str", [])
                self.considerError(lastError, None)
                _locals['x'] = _G_apply_28
                return (_locals['x'], self.currentError)
            _G_termpattern_29, lastError = self.termpattern('Commit', _G_termpattern_27)
            self.considerError(lastError, 'Commit')
            from terml.parser import parseTerm as term
            _G_stringtemplate_30, lastError = self.stringtemplate(term('["t.Commit(", QuasiExprHole("x"), "),"]'), _locals)
            self.considerError(lastError, None)
            return (_G_stringtemplate_30, self.currentError)


        def rule_Descend(self):
            _locals = {'self': self}
            self.locals['Descend'] = _locals
            def _G_termpattern_31():
                return (None, self.currentError)
            _G_termpattern_32, lastError = self.termpattern('Descend', _G_termpattern_31)
            self.considerError(lastError, 'Descend')
            from terml.parser import parseTerm as term
            _G_stringtemplate_33, lastError = self.stringtemplate(term('["t.Descend(),"]'), _locals)
            self.considerError(lastError, None)
            return (_G_stringtemplate_33, self.currentError)


        def rule_EndSlice(self):
            _locals = {'self': self}
            self.locals['EndSlice'] = _locals
            def _G_termpattern_34():
                return (None, self.currentError)
            _G_termpattern_35, lastError = self.termpattern('EndSlice', _G_termpattern_34)
            self.considerError(lastError, 'EndSlice')
            from terml.parser import parseTerm as term
            _G_stringtemplate_36, lastError = self.stringtemplate(term('["t.EndSlice(),"]'), _locals)
            self.considerError(lastError, None)
            return (_G_stringtemplate_36, self.currentError)


        def rule_Fail(self):
            _locals = {'self': self}
            self.locals['Fail'] = _locals
            def _G_termpattern_37():
                return (None, self.currentError)
            _G_termpattern_38, lastError = self.termpattern('Fail', _G_termpattern_37)
            self.considerError(lastError, 'Fail')
            from terml.parser import parseTerm as term
            _G_stringtemplate_39, lastError = self.stringtemplate(term('["t.Fail(),"]'), _locals)
            self.considerError(lastError, None)
            return (_G_stringtemplate_39, self.currentError)


        def rule_ForeignCall(self):
            _locals = {'self': self}
            self.locals['ForeignCall'] = _locals
            def _G_termpattern_40():
                self._trace('#TreeTransformer\nGrammar(str:name str:tree @rules) {{{\nclass $name:\n    tree = $tree\n    $rules\n}}}\nRule(str:name @rules) {{{\n$name = [\n    $rules\n]\n}}}\n\nAscend() --> t.Ascend(),\nBind(str:x) --> t.Bind($x),\nCall(str:x) --> t.Call($x),\nChoice(str:x) --> t.Choice($x),\nCommit(str:x) --> t.Commit($x),\nDescend() --> t.Descend(),\nEndSlice() --> t.EndSlice(),\nFail() --> t.Fail(),\nForeignCall(str', (0, 391), self.input.position)
                _G_apply_41, lastError = self._apply(self.rule_str, "str", [])
                self.considerError(lastError, None)
                _locals['x'] = _G_apply_41
                self._trace('#TreeTransformer\nGrammar(str:name str:tree @rules) {{{\nclass $name:\n    tree = $tree\n    $rules\n}}}\nRule(str:name @rules) {{{\n$name = [\n    $rules\n]\n}}}\n\nAscend() --> t.Ascend(),\nBind(str:x) --> t.Bind($x),\nCall(str:x) --> t.Call($x),\nChoice(str:x) --> t.Choice($x),\nCommit(str:x) --> t.Commit($x),\nDescend() --> t.Descend(),\nEndSlice() --> t.EndSlice(),\nFail() --> t.Fail(),\nForeignCall(str:x str', (0, 397), self.input.position)
                _G_apply_42, lastError = self._apply(self.rule_str, "str", [])
                self.considerError(lastError, None)
                _locals['y'] = _G_apply_42
                return (_locals['y'], self.currentError)
            _G_termpattern_43, lastError = self.termpattern('ForeignCall', _G_termpattern_40)
            self.considerError(lastError, 'ForeignCall')
            from terml.parser import parseTerm as term
            _G_stringtemplate_44, lastError = self.stringtemplate(term('["t.ForeignCall(", QuasiExprHole("x"), ", ", QuasiExprHole("y"), "),"]'), _locals)
            self.considerError(lastError, None)
            return (_G_stringtemplate_44, self.currentError)


        def rule_Match(self):
            _locals = {'self': self}
            self.locals['Match'] = _locals
            def _G_termpattern_45():
                self._trace('#TreeTransformer\nGrammar(str:name str:tree @rules) {{{\nclass $name:\n    tree = $tree\n    $rules\n}}}\nRule(str:name @rules) {{{\n$name = [\n    $rules\n]\n}}}\n\nAscend() --> t.Ascend(),\nBind(str:x) --> t.Bind($x),\nCall(str:x) --> t.Call($x),\nChoice(str:x) --> t.Choice($x),\nCommit(str:x) --> t.Commit($x),\nDescend() --> t.Descend(),\nEndSlice() --> t.EndSlice(),\nFail() --> t.Fail(),\nForeignCall(str:x str:y) --> t.ForeignCall($x, $y),\nMatch(str', (0, 437), self.input.position)
                _G_apply_46, lastError = self._apply(self.rule_str, "str", [])
                self.considerError(lastError, None)
                _locals['x'] = _G_apply_46
                return (_locals['x'], self.currentError)
            _G_termpattern_47, lastError = self.termpattern('Match', _G_termpattern_45)
            self.considerError(lastError, 'Match')
            from terml.parser import parseTerm as term
            _G_stringtemplate_48, lastError = self.stringtemplate(term('["t.Match(", QuasiExprHole("x"), "),"]'), _locals)
            self.considerError(lastError, None)
            return (_G_stringtemplate_48, self.currentError)


        def rule_Predicate(self):
            _locals = {'self': self}
            self.locals['Predicate'] = _locals
            def _G_termpattern_49():
                return (None, self.currentError)
            _G_termpattern_50, lastError = self.termpattern('Predicate', _G_termpattern_49)
            self.considerError(lastError, 'Predicate')
            from terml.parser import parseTerm as term
            _G_stringtemplate_51, lastError = self.stringtemplate(term('["t.Predicate(),"]'), _locals)
            self.considerError(lastError, None)
            return (_G_stringtemplate_51, self.currentError)


        def rule_Push(self):
            _locals = {'self': self}
            self.locals['Push'] = _locals
            def _G_termpattern_52():
                return (None, self.currentError)
            _G_termpattern_53, lastError = self.termpattern('Push', _G_termpattern_52)
            self.considerError(lastError, 'Push')
            from terml.parser import parseTerm as term
            _G_stringtemplate_54, lastError = self.stringtemplate(term('["t.Push(),"]'), _locals)
            self.considerError(lastError, None)
            return (_G_stringtemplate_54, self.currentError)


        def rule_Python(self):
            _locals = {'self': self}
            self.locals['Python'] = _locals
            def _G_termpattern_55():
                self._trace('#TreeTransformer\nGrammar(str:name str:tree @rules) {{{\nclass $name:\n    tree = $tree\n    $rules\n}}}\nRule(str:name @rules) {{{\n$name = [\n    $rules\n]\n}}}\n\nAscend() --> t.Ascend(),\nBind(str:x) --> t.Bind($x),\nCall(str:x) --> t.Call($x),\nChoice(str:x) --> t.Choice($x),\nCommit(str:x) --> t.Commit($x),\nDescend() --> t.Descend(),\nEndSlice() --> t.EndSlice(),\nFail() --> t.Fail(),\nForeignCall(str:x str:y) --> t.ForeignCall($x, $y),\nMatch(str:x) --> t.Match($x),\nPredicate() --> t.Predicate(),\nPush() --> t.Push(),\nPython(str', (0, 520), self.input.position)
                _G_apply_56, lastError = self._apply(self.rule_str, "str", [])
                self.considerError(lastError, None)
                _locals['x'] = _G_apply_56
                return (_locals['x'], self.currentError)
            _G_termpattern_57, lastError = self.termpattern('Python', _G_termpattern_55)
            self.considerError(lastError, 'Python')
            from terml.parser import parseTerm as term
            _G_stringtemplate_58, lastError = self.stringtemplate(term('["t.Python(", QuasiExprHole("x"), "),"]'), _locals)
            self.considerError(lastError, None)
            return (_G_stringtemplate_58, self.currentError)


        def rule_StartSlice(self):
            _locals = {'self': self}
            self.locals['StartSlice'] = _locals
            def _G_termpattern_59():
                return (None, self.currentError)
            _G_termpattern_60, lastError = self.termpattern('StartSlice', _G_termpattern_59)
            self.considerError(lastError, 'StartSlice')
            from terml.parser import parseTerm as term
            _G_stringtemplate_61, lastError = self.stringtemplate(term('["t.StartSlice(),"]'), _locals)
            self.considerError(lastError, None)
            return (_G_stringtemplate_61, self.currentError)


        def rule_SuperCall(self):
            _locals = {'self': self}
            self.locals['SuperCall'] = _locals
            def _G_termpattern_62():
                self._trace('#TreeTransformer\nGrammar(str:name str:tree @rules) {{{\nclass $name:\n    tree = $tree\n    $rules\n}}}\nRule(str:name @rules) {{{\n$name = [\n    $rules\n]\n}}}\n\nAscend() --> t.Ascend(),\nBind(str:x) --> t.Bind($x),\nCall(str:x) --> t.Call($x),\nChoice(str:x) --> t.Choice($x),\nCommit(str:x) --> t.Commit($x),\nDescend() --> t.Descend(),\nEndSlice() --> t.EndSlice(),\nFail() --> t.Fail(),\nForeignCall(str:x str:y) --> t.ForeignCall($x, $y),\nMatch(str:x) --> t.Match($x),\nPredicate() --> t.Predicate(),\nPush() --> t.Push(),\nPython(str:x) --> t.Python($x),\nStartSlice() --> t.StartSlice(),\nSuperCall(str', (0, 588), self.input.position)
                _G_apply_63, lastError = self._apply(self.rule_str, "str", [])
                self.considerError(lastError, None)
                _locals['x'] = _G_apply_63
                return (_locals['x'], self.currentError)
            _G_termpattern_64, lastError = self.termpattern('SuperCall', _G_termpattern_62)
            self.considerError(lastError, 'SuperCall')
            from terml.parser import parseTerm as term
            _G_stringtemplate_65, lastError = self.stringtemplate(term('["t.SuperCall(", QuasiExprHole("x"), "),"]'), _locals)
            self.considerError(lastError, None)
            return (_G_stringtemplate_65, self.currentError)


        def rule_str(self):
            _locals = {'self': self}
            self.locals['str'] = _locals
            self._trace('#TreeTransformer\nGrammar(str:name str:tree @rules) {{{\nclass $name:\n    tree = $tree\n    $rules\n}}}\nRule(str:name @rules) {{{\n$name = [\n    $rules\n]\n}}}\n\nAscend() --> t.Ascend(),\nBind(str:x) --> t.Bind($x),\nCall(str:x) --> t.Call($x),\nChoice(str:x) --> t.Choice($x),\nCommit(str:x) --> t.Commit($x),\nDescend() --> t.Descend(),\nEndSlice() --> t.EndSlice(),\nFail() --> t.Fail(),\nForeignCall(str:x str:y) --> t.ForeignCall($x, $y),\nMatch(str:x) --> t.Match($x),\nPredicate() --> t.Predicate(),\nPush() --> t.Push(),\nPython(str:x) --> t.Python($x),\nStartSlice() --> t.StartSlice(),\nSuperCall(str:x) --> t.SuperCall($x),\n\nstr = anything', (0, 628), self.input.position)
            _G_apply_66, lastError = self._apply(self.rule_anything, "anything", [])
            self.considerError(lastError, 'str')
            _locals['s'] = _G_apply_66
            _G_python_67, lastError = eval('str(s.data)', self.globals, _locals), None
            self.considerError(lastError, 'str')
            return (_G_python_67, self.currentError)


        tree = True
    if vm_emit.globals is not None:
        vm_emit.globals = vm_emit.globals.copy()
        vm_emit.globals.update(ruleGlobals)
    else:
        vm_emit.globals = ruleGlobals
    return vm_emit