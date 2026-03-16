def createParserClass(GrammarBase, ruleGlobals):
    if ruleGlobals is None:
        ruleGlobals = {}
    class vm(GrammarBase):
        def rule_Exactly(self):
            _locals = {'self': self}
            self.locals['Exactly'] = _locals
            def _G_termpattern_1():
                _G_apply_2, lastError = self._apply(self.rule_anything, "anything", [])
                self.considerError(lastError, None)
                _locals['x'] = _G_apply_2
                return (_locals['x'], self.currentError)
            _G_termpattern_3, lastError = self.termpattern('Exactly', _G_termpattern_1)
            self.considerError(lastError, 'Exactly')
            _G_python_4, lastError = eval('[t.Match(x)]', self.globals, _locals), None
            self.considerError(lastError, 'Exactly')
            return (_G_python_4, self.currentError)


        def rule_Token(self):
            _locals = {'self': self}
            self.locals['Token'] = _locals
            def _G_termpattern_5():
                _G_apply_6, lastError = self._apply(self.rule_anything, "anything", [])
                self.considerError(lastError, None)
                _locals['x'] = _G_apply_6
                return (_locals['x'], self.currentError)
            _G_termpattern_7, lastError = self.termpattern('Token', _G_termpattern_5)
            self.considerError(lastError, 'Token')
            _G_python_8, lastError = eval('[t.Match(x)]', self.globals, _locals), None
            self.considerError(lastError, 'Token')
            return (_G_python_8, self.currentError)


        def rule_Many(self):
            _locals = {'self': self}
            self.locals['Many'] = _locals
            def _G_termpattern_9():
                _G_apply_10, lastError = self._apply(self.rule_transform, "transform", [])
                self.considerError(lastError, None)
                _locals['x'] = _G_apply_10
                return (_locals['x'], self.currentError)
            _G_termpattern_11, lastError = self.termpattern('Many', _G_termpattern_9)
            self.considerError(lastError, 'Many')
            _G_python_12, lastError = eval('[t.Choice(len(x) + 2)] + x + [t.Commit(-len(x) - 1)]', self.globals, _locals), None
            self.considerError(lastError, 'Many')
            return (_G_python_12, self.currentError)


        def rule_Many1(self):
            _locals = {'self': self}
            self.locals['Many1'] = _locals
            def _G_termpattern_13():
                _G_apply_14, lastError = self._apply(self.rule_transform, "transform", [])
                self.considerError(lastError, None)
                _locals['x'] = _G_apply_14
                return (_locals['x'], self.currentError)
            _G_termpattern_15, lastError = self.termpattern('Many1', _G_termpattern_13)
            self.considerError(lastError, 'Many1')
            _G_python_16, lastError = eval('x + [t.Choice(len(x) + 2)] + x + [t.Commit(-len(x) - 1)]', self.globals, _locals), None
            self.considerError(lastError, 'Many1')
            return (_G_python_16, self.currentError)


        def rule_Repeat(self):
            _locals = {'self': self}
            self.locals['Repeat'] = _locals
            def _G_termpattern_17():
                _G_apply_18, lastError = self._apply(self.rule_anything, "anything", [])
                self.considerError(lastError, None)
                _locals['min'] = _G_apply_18
                _G_apply_19, lastError = self._apply(self.rule_anything, "anything", [])
                self.considerError(lastError, None)
                _locals['max'] = _G_apply_19
                _G_apply_20, lastError = self._apply(self.rule_transform, "transform", [])
                self.considerError(lastError, None)
                _locals['x'] = _G_apply_20
                return (_locals['x'], self.currentError)
            _G_termpattern_21, lastError = self.termpattern('Repeat', _G_termpattern_17)
            self.considerError(lastError, 'Repeat')
            _G_python_22, lastError = eval('[t.Python(repr(int(min))), t.Push(), t.Python(repr(int(max))),\n        t.Push(), t.RepeatChoice(len(x) + 2)] + x + [t.Commit(-len(x) - 1)]', self.globals, _locals), None
            self.considerError(lastError, 'Repeat')
            return (_G_python_22, self.currentError)


        def rule_Optional(self):
            _locals = {'self': self}
            self.locals['Optional'] = _locals
            def _G_termpattern_23():
                _G_apply_24, lastError = self._apply(self.rule_transform, "transform", [])
                self.considerError(lastError, None)
                _locals['x'] = _G_apply_24
                return (_locals['x'], self.currentError)
            _G_termpattern_25, lastError = self.termpattern('Optional', _G_termpattern_23)
            self.considerError(lastError, 'Optional')
            _G_python_26, lastError = eval('[t.Choice(len(x) + 2)] + x + [t.Commit(2), t.Python("None")]', self.globals, _locals), None
            self.considerError(lastError, 'Optional')
            return (_G_python_26, self.currentError)


        def rule_Or(self):
            _locals = {'self': self}
            self.locals['Or'] = _locals
            def _G_or_27():
                def _G_termpattern_28():
                    _G_apply_29, lastError = self._apply(self.rule_transform, "transform", [])
                    self.considerError(lastError, None)
                    _locals['xs'] = _G_apply_29
                    return (_locals['xs'], self.currentError)
                _G_termpattern_30, lastError = self.termpattern('Or', _G_termpattern_28)
                self.considerError(lastError, None)
                def _G_or_31():
                    def _G_pred_32():
                        _G_python_33, lastError = eval('len(xs) == 1', self.globals, _locals), None
                        self.considerError(lastError, None)
                        return (_G_python_33, self.currentError)
                    _G_pred_34, lastError = self.pred(_G_pred_32)
                    self.considerError(lastError, None)
                    self._trace('#TreeTransformer\nExactly(:x) -> [t.Match(x)]\nToken(:x) -> [t.Match(x)]\nMany(@x) -> [t.Choice(len(x) + 2)] + x + [t.Commit(-len(x) - 1)]\nMany1(@x) -> x + [t.Choice(len(x) + 2)] + x + [t.Commit(-len(x) - 1)]\nRepeat(:min :max @x)\n    -> [t.Python(repr(int(min))), t.Push(), t.Python(repr(int(max))),\n        t.Push(), t.RepeatChoice(len(x) + 2)] + x + [t.Commit(-len(x) - 1)]\nOptional(@x) -> [t.Choice(len(x) + 2)] + x + [t.Commit(2), t.Python("None")]\n# Right-associate Or() as needed. Note that Or() can have a list of a single\n# element.\nOr(@xs) = ?(len(xs) == 1) transform(xs[0])', (0, 580), self.input.position)
                    _G_python_35, lastError = eval('xs[0]', self.globals, _locals), None
                    self.considerError(lastError, None)
                    _G_apply_36, lastError = self._apply(self.rule_transform, "transform", [_G_python_35])
                    self.considerError(lastError, None)
                    return (_G_apply_36, self.currentError)
                def _G_or_37():
                    def _G_pred_38():
                        _G_python_39, lastError = eval('len(xs) == 2', self.globals, _locals), None
                        self.considerError(lastError, None)
                        return (_G_python_39, self.currentError)
                    _G_pred_40, lastError = self.pred(_G_pred_38)
                    self.considerError(lastError, None)
                    self._trace('#TreeTransformer\nExactly(:x) -> [t.Match(x)]\nToken(:x) -> [t.Match(x)]\nMany(@x) -> [t.Choice(len(x) + 2)] + x + [t.Commit(-len(x) - 1)]\nMany1(@x) -> x + [t.Choice(len(x) + 2)] + x + [t.Commit(-len(x) - 1)]\nRepeat(:min :max @x)\n    -> [t.Python(repr(int(min))), t.Push(), t.Python(repr(int(max))),\n        t.Push(), t.RepeatChoice(len(x) + 2)] + x + [t.Commit(-len(x) - 1)]\nOptional(@x) -> [t.Choice(len(x) + 2)] + x + [t.Commit(2), t.Python("None")]\n# Right-associate Or() as needed. Note that Or() can have a list of a single\n# element.\nOr(@xs) = ?(len(xs) == 1) transform(xs[0])\n        | ?(len(xs) == 2) transform(t.Or(xs[0], xs[1]))', (0, 636), self.input.position)
                    _G_python_41, lastError = eval('t.Or(xs[0], xs[1])', self.globals, _locals), None
                    self.considerError(lastError, None)
                    _G_apply_42, lastError = self._apply(self.rule_transform, "transform", [_G_python_41])
                    self.considerError(lastError, None)
                    return (_G_apply_42, self.currentError)
                def _G_or_43():
                    self._trace('#TreeTransformer\nExactly(:x) -> [t.Match(x)]\nToken(:x) -> [t.Match(x)]\nMany(@x) -> [t.Choice(len(x) + 2)] + x + [t.Commit(-len(x) - 1)]\nMany1(@x) -> x + [t.Choice(len(x) + 2)] + x + [t.Commit(-len(x) - 1)]\nRepeat(:min :max @x)\n    -> [t.Python(repr(int(min))), t.Push(), t.Python(repr(int(max))),\n        t.Push(), t.RepeatChoice(len(x) + 2)] + x + [t.Commit(-len(x) - 1)]\nOptional(@x) -> [t.Choice(len(x) + 2)] + x + [t.Commit(2), t.Python("None")]\n# Right-associate Or() as needed. Note that Or() can have a list of a single\n# element.\nOr(@xs) = ?(len(xs) == 1) transform(xs[0])\n        | ?(len(xs) == 2) transform(t.Or(xs[0], xs[1]))\n        |                 transform(t.Or(xs[0], t.Or(xs[1:])))', (0, 699), self.input.position)
                    _G_python_44, lastError = eval('t.Or(xs[0], t.Or(xs[1:]))', self.globals, _locals), None
                    self.considerError(lastError, None)
                    _G_apply_45, lastError = self._apply(self.rule_transform, "transform", [_G_python_44])
                    self.considerError(lastError, None)
                    return (_G_apply_45, self.currentError)
                _G_or_46, lastError = self._or([_G_or_31, _G_or_37, _G_or_43])
                self.considerError(lastError, None)
                return (_G_or_46, self.currentError)
            def _G_or_47():
                def _G_termpattern_48():
                    _G_apply_49, lastError = self._apply(self.rule_transform, "transform", [])
                    self.considerError(lastError, None)
                    _locals['left'] = _G_apply_49
                    _G_apply_50, lastError = self._apply(self.rule_transform, "transform", [])
                    self.considerError(lastError, None)
                    _locals['right'] = _G_apply_50
                    return (_locals['right'], self.currentError)
                _G_termpattern_51, lastError = self.termpattern('Or', _G_termpattern_48)
                self.considerError(lastError, None)
                _G_python_52, lastError = eval('[t.Choice(len(left) + 2)] + left + [t.Commit(len(right) + 1)] + right', self.globals, _locals), None
                self.considerError(lastError, None)
                return (_G_python_52, self.currentError)
            _G_or_53, lastError = self._or([_G_or_27, _G_or_47])
            self.considerError(lastError, 'Or')
            return (_G_or_53, self.currentError)


        def rule_Not(self):
            _locals = {'self': self}
            self.locals['Not'] = _locals
            def _G_termpattern_54():
                _G_apply_55, lastError = self._apply(self.rule_transform, "transform", [])
                self.considerError(lastError, None)
                _locals['x'] = _G_apply_55
                return (_locals['x'], self.currentError)
            _G_termpattern_56, lastError = self.termpattern('Not', _G_termpattern_54)
            self.considerError(lastError, 'Not')
            _G_python_57, lastError = eval('[t.Choice(len(x) + 3)] + x + [t.Commit(1), t.Fail()]', self.globals, _locals), None
            self.considerError(lastError, 'Not')
            return (_G_python_57, self.currentError)


        def rule_Lookahead(self):
            _locals = {'self': self}
            self.locals['Lookahead'] = _locals
            def _G_termpattern_58():
                _G_apply_59, lastError = self._apply(self.rule_anything, "anything", [])
                self.considerError(lastError, None)
                _locals['x'] = _G_apply_59
                return (_locals['x'], self.currentError)
            _G_termpattern_60, lastError = self.termpattern('Lookahead', _G_termpattern_58)
            self.considerError(lastError, 'Lookahead')
            self._trace('#TreeTransformer\nExactly(:x) -> [t.Match(x)]\nToken(:x) -> [t.Match(x)]\nMany(@x) -> [t.Choice(len(x) + 2)] + x + [t.Commit(-len(x) - 1)]\nMany1(@x) -> x + [t.Choice(len(x) + 2)] + x + [t.Commit(-len(x) - 1)]\nRepeat(:min :max @x)\n    -> [t.Python(repr(int(min))), t.Push(), t.Python(repr(int(max))),\n        t.Push(), t.RepeatChoice(len(x) + 2)] + x + [t.Commit(-len(x) - 1)]\nOptional(@x) -> [t.Choice(len(x) + 2)] + x + [t.Commit(2), t.Python("None")]\n# Right-associate Or() as needed. Note that Or() can have a list of a single\n# element.\nOr(@xs) = ?(len(xs) == 1) transform(xs[0])\n        | ?(len(xs) == 2) transform(t.Or(xs[0], xs[1]))\n        |                 transform(t.Or(xs[0], t.Or(xs[1:])))\nOr(@left @right)\n    -> [t.Choice(len(left) + 2)] + left + [t.Commit(len(right) + 1)] + right\nNot(@x) -> [t.Choice(len(x) + 3)] + x + [t.Commit(1), t.Fail()]\nLookahead(:x) = transform(t.Not(t.Not(x)))', (0, 900), self.input.position)
            _G_python_61, lastError = eval('t.Not(t.Not(x))', self.globals, _locals), None
            self.considerError(lastError, 'Lookahead')
            _G_apply_62, lastError = self._apply(self.rule_transform, "transform", [_G_python_61])
            self.considerError(lastError, 'Lookahead')
            return (_G_apply_62, self.currentError)


        def rule_And(self):
            _locals = {'self': self}
            self.locals['And'] = _locals
            def _G_termpattern_63():
                _G_apply_64, lastError = self._apply(self.rule_transform, "transform", [])
                self.considerError(lastError, None)
                _locals['xs'] = _G_apply_64
                return (_locals['xs'], self.currentError)
            _G_termpattern_65, lastError = self.termpattern('And', _G_termpattern_63)
            self.considerError(lastError, 'And')
            _G_python_66, lastError = eval('sum(xs, [])', self.globals, _locals), None
            self.considerError(lastError, 'And')
            return (_G_python_66, self.currentError)


        def rule_Bind(self):
            _locals = {'self': self}
            self.locals['Bind'] = _locals
            def _G_termpattern_67():
                _G_apply_68, lastError = self._apply(self.rule_anything, "anything", [])
                self.considerError(lastError, None)
                _locals['name'] = _G_apply_68
                _G_apply_69, lastError = self._apply(self.rule_transform, "transform", [])
                self.considerError(lastError, None)
                _locals['x'] = _G_apply_69
                return (_locals['x'], self.currentError)
            _G_termpattern_70, lastError = self.termpattern('Bind', _G_termpattern_67)
            self.considerError(lastError, 'Bind')
            _G_python_71, lastError = eval('x + [t.Bind(name)]', self.globals, _locals), None
            self.considerError(lastError, 'Bind')
            return (_G_python_71, self.currentError)


        def rule_Predicate(self):
            _locals = {'self': self}
            self.locals['Predicate'] = _locals
            def _G_termpattern_72():
                _G_apply_73, lastError = self._apply(self.rule_transform, "transform", [])
                self.considerError(lastError, None)
                _locals['x'] = _G_apply_73
                return (_locals['x'], self.currentError)
            _G_termpattern_74, lastError = self.termpattern('Predicate', _G_termpattern_72)
            self.considerError(lastError, 'Predicate')
            _G_python_75, lastError = eval('x + [t.Predicate()]', self.globals, _locals), None
            self.considerError(lastError, 'Predicate')
            return (_G_python_75, self.currentError)


        def rule_Action(self):
            _locals = {'self': self}
            self.locals['Action'] = _locals
            def _G_termpattern_76():
                _G_apply_77, lastError = self._apply(self.rule_anything, "anything", [])
                self.considerError(lastError, None)
                _locals['x'] = _G_apply_77
                return (_locals['x'], self.currentError)
            _G_termpattern_78, lastError = self.termpattern('Action', _G_termpattern_76)
            self.considerError(lastError, 'Action')
            _G_python_79, lastError = eval('[t.Python(x.data)]', self.globals, _locals), None
            self.considerError(lastError, 'Action')
            return (_G_python_79, self.currentError)


        def rule_Python(self):
            _locals = {'self': self}
            self.locals['Python'] = _locals
            def _G_termpattern_80():
                _G_apply_81, lastError = self._apply(self.rule_anything, "anything", [])
                self.considerError(lastError, None)
                _locals['x'] = _G_apply_81
                return (_locals['x'], self.currentError)
            _G_termpattern_82, lastError = self.termpattern('Python', _G_termpattern_80)
            self.considerError(lastError, 'Python')
            _G_python_83, lastError = eval('[t.Python(x.data)]', self.globals, _locals), None
            self.considerError(lastError, 'Python')
            return (_G_python_83, self.currentError)


        def rule_List(self):
            _locals = {'self': self}
            self.locals['List'] = _locals
            def _G_termpattern_84():
                _G_apply_85, lastError = self._apply(self.rule_transform, "transform", [])
                self.considerError(lastError, None)
                _locals['x'] = _G_apply_85
                return (_locals['x'], self.currentError)
            _G_termpattern_86, lastError = self.termpattern('List', _G_termpattern_84)
            self.considerError(lastError, 'List')
            _G_python_87, lastError = eval('[t.Descend()] + x + [t.Ascend()]', self.globals, _locals), None
            self.considerError(lastError, 'List')
            return (_G_python_87, self.currentError)


        def rule_ConsumedBy(self):
            _locals = {'self': self}
            self.locals['ConsumedBy'] = _locals
            def _G_termpattern_88():
                _G_apply_89, lastError = self._apply(self.rule_transform, "transform", [])
                self.considerError(lastError, None)
                _locals['x'] = _G_apply_89
                return (_locals['x'], self.currentError)
            _G_termpattern_90, lastError = self.termpattern('ConsumedBy', _G_termpattern_88)
            self.considerError(lastError, 'ConsumedBy')
            _G_python_91, lastError = eval('[t.StartSlice()] + x + [t.EndSlice()]', self.globals, _locals), None
            self.considerError(lastError, 'ConsumedBy')
            return (_G_python_91, self.currentError)


        def rule_pushes(self):
            _locals = {'self': self}
            self.locals['pushes'] = _locals
            _G_apply_92, lastError = self._apply(self.rule_anything, "anything", [])
            self.considerError(lastError, 'pushes')
            _locals['xs'] = _G_apply_92
            _G_python_93, lastError = eval('[inner for x in xs for inner in [x[0], t.Push()]]', self.globals, _locals), None
            self.considerError(lastError, 'pushes')
            return (_G_python_93, self.currentError)


        def rule_Apply(self):
            _locals = {'self': self}
            self.locals['Apply'] = _locals
            def _G_or_94():
                def _G_termpattern_95():
                    self._trace('#TreeTransformer\nExactly(:x) -> [t.Match(x)]\nToken(:x) -> [t.Match(x)]\nMany(@x) -> [t.Choice(len(x) + 2)] + x + [t.Commit(-len(x) - 1)]\nMany1(@x) -> x + [t.Choice(len(x) + 2)] + x + [t.Commit(-len(x) - 1)]\nRepeat(:min :max @x)\n    -> [t.Python(repr(int(min))), t.Push(), t.Python(repr(int(max))),\n        t.Push(), t.RepeatChoice(len(x) + 2)] + x + [t.Commit(-len(x) - 1)]\nOptional(@x) -> [t.Choice(len(x) + 2)] + x + [t.Commit(2), t.Python("None")]\n# Right-associate Or() as needed. Note that Or() can have a list of a single\n# element.\nOr(@xs) = ?(len(xs) == 1) transform(xs[0])\n        | ?(len(xs) == 2) transform(t.Or(xs[0], xs[1]))\n        |                 transform(t.Or(xs[0], t.Or(xs[1:])))\nOr(@left @right)\n    -> [t.Choice(len(left) + 2)] + left + [t.Commit(len(right) + 1)] + right\nNot(@x) -> [t.Choice(len(x) + 3)] + x + [t.Commit(1), t.Fail()]\nLookahead(:x) = transform(t.Not(t.Not(x)))\nAnd(@xs) -> sum(xs, [])\nBind(:name @x) -> x + [t.Bind(name)]\nPredicate(@x) -> x + [t.Predicate()]\nAction(:x) -> [t.Python(x.data)]\nPython(:x) -> [t.Python(x.data)]\nList(@x) -> [t.Descend()] + x + [t.Ascend()]\nConsumedBy(@x) -> [t.StartSlice()] + x + [t.EndSlice()]\n\npushes :xs -> [inner for x in xs for inner in [x[0], t.Push()]]\nApply("super"', (0, 1244), self.input.position)
                    _G_exactly_96, lastError = self.exactly('super')
                    self.considerError(lastError, None)
                    _G_apply_97, lastError = self._apply(self.rule_anything, "anything", [])
                    self.considerError(lastError, None)
                    _locals['code'] = _G_apply_97
                    _G_apply_98, lastError = self._apply(self.rule_transform, "transform", [])
                    self.considerError(lastError, None)
                    _locals['args'] = _G_apply_98
                    return (_locals['args'], self.currentError)
                _G_termpattern_99, lastError = self.termpattern('Apply', _G_termpattern_95)
                self.considerError(lastError, None)
                self._trace('#TreeTransformer\nExactly(:x) -> [t.Match(x)]\nToken(:x) -> [t.Match(x)]\nMany(@x) -> [t.Choice(len(x) + 2)] + x + [t.Commit(-len(x) - 1)]\nMany1(@x) -> x + [t.Choice(len(x) + 2)] + x + [t.Commit(-len(x) - 1)]\nRepeat(:min :max @x)\n    -> [t.Python(repr(int(min))), t.Push(), t.Python(repr(int(max))),\n        t.Push(), t.RepeatChoice(len(x) + 2)] + x + [t.Commit(-len(x) - 1)]\nOptional(@x) -> [t.Choice(len(x) + 2)] + x + [t.Commit(2), t.Python("None")]\n# Right-associate Or() as needed. Note that Or() can have a list of a single\n# element.\nOr(@xs) = ?(len(xs) == 1) transform(xs[0])\n        | ?(len(xs) == 2) transform(t.Or(xs[0], xs[1]))\n        |                 transform(t.Or(xs[0], t.Or(xs[1:])))\nOr(@left @right)\n    -> [t.Choice(len(left) + 2)] + left + [t.Commit(len(right) + 1)] + right\nNot(@x) -> [t.Choice(len(x) + 3)] + x + [t.Commit(1), t.Fail()]\nLookahead(:x) = transform(t.Not(t.Not(x)))\nAnd(@xs) -> sum(xs, [])\nBind(:name @x) -> x + [t.Bind(name)]\nPredicate(@x) -> x + [t.Predicate()]\nAction(:x) -> [t.Python(x.data)]\nPython(:x) -> [t.Python(x.data)]\nList(@x) -> [t.Descend()] + x + [t.Ascend()]\nConsumedBy(@x) -> [t.StartSlice()] + x + [t.EndSlice()]\n\npushes :xs -> [inner for x in xs for inner in [x[0], t.Push()]]\nApply("super" :code @args) pushes(args)', (0, 1270), self.input.position)
                _G_python_100, lastError = eval('args', self.globals, _locals), None
                self.considerError(lastError, None)
                _G_apply_101, lastError = self._apply(self.rule_pushes, "pushes", [_G_python_100])
                self.considerError(lastError, None)
                _locals['xs'] = _G_apply_101
                _G_python_102, lastError = eval('xs + [t.SuperCall(code)]', self.globals, _locals), None
                self.considerError(lastError, None)
                return (_G_python_102, self.currentError)
            def _G_or_103():
                def _G_termpattern_104():
                    _G_apply_105, lastError = self._apply(self.rule_anything, "anything", [])
                    self.considerError(lastError, None)
                    _locals['rule'] = _G_apply_105
                    _G_apply_106, lastError = self._apply(self.rule_anything, "anything", [])
                    self.considerError(lastError, None)
                    _locals['code'] = _G_apply_106
                    _G_apply_107, lastError = self._apply(self.rule_transform, "transform", [])
                    self.considerError(lastError, None)
                    _locals['args'] = _G_apply_107
                    return (_locals['args'], self.currentError)
                _G_termpattern_108, lastError = self.termpattern('Apply', _G_termpattern_104)
                self.considerError(lastError, None)
                self._trace('#TreeTransformer\nExactly(:x) -> [t.Match(x)]\nToken(:x) -> [t.Match(x)]\nMany(@x) -> [t.Choice(len(x) + 2)] + x + [t.Commit(-len(x) - 1)]\nMany1(@x) -> x + [t.Choice(len(x) + 2)] + x + [t.Commit(-len(x) - 1)]\nRepeat(:min :max @x)\n    -> [t.Python(repr(int(min))), t.Push(), t.Python(repr(int(max))),\n        t.Push(), t.RepeatChoice(len(x) + 2)] + x + [t.Commit(-len(x) - 1)]\nOptional(@x) -> [t.Choice(len(x) + 2)] + x + [t.Commit(2), t.Python("None")]\n# Right-associate Or() as needed. Note that Or() can have a list of a single\n# element.\nOr(@xs) = ?(len(xs) == 1) transform(xs[0])\n        | ?(len(xs) == 2) transform(t.Or(xs[0], xs[1]))\n        |                 transform(t.Or(xs[0], t.Or(xs[1:])))\nOr(@left @right)\n    -> [t.Choice(len(left) + 2)] + left + [t.Commit(len(right) + 1)] + right\nNot(@x) -> [t.Choice(len(x) + 3)] + x + [t.Commit(1), t.Fail()]\nLookahead(:x) = transform(t.Not(t.Not(x)))\nAnd(@xs) -> sum(xs, [])\nBind(:name @x) -> x + [t.Bind(name)]\nPredicate(@x) -> x + [t.Predicate()]\nAction(:x) -> [t.Python(x.data)]\nPython(:x) -> [t.Python(x.data)]\nList(@x) -> [t.Descend()] + x + [t.Ascend()]\nConsumedBy(@x) -> [t.StartSlice()] + x + [t.EndSlice()]\n\npushes :xs -> [inner for x in xs for inner in [x[0], t.Push()]]\nApply("super" :code @args) pushes(args):xs -> xs + [t.SuperCall(code)]\nApply(:rule :code @args) pushes(args)', (0, 1339), self.input.position)
                _G_python_109, lastError = eval('args', self.globals, _locals), None
                self.considerError(lastError, None)
                _G_apply_110, lastError = self._apply(self.rule_pushes, "pushes", [_G_python_109])
                self.considerError(lastError, None)
                _locals['xs'] = _G_apply_110
                _G_python_111, lastError = eval('xs + [t.Call(rule)]', self.globals, _locals), None
                self.considerError(lastError, None)
                return (_G_python_111, self.currentError)
            _G_or_112, lastError = self._or([_G_or_94, _G_or_103])
            self.considerError(lastError, 'Apply')
            return (_G_or_112, self.currentError)


        def rule_ForeignApply(self):
            _locals = {'self': self}
            self.locals['ForeignApply'] = _locals
            def _G_termpattern_113():
                _G_apply_114, lastError = self._apply(self.rule_anything, "anything", [])
                self.considerError(lastError, None)
                _locals['grammar'] = _G_apply_114
                _G_apply_115, lastError = self._apply(self.rule_anything, "anything", [])
                self.considerError(lastError, None)
                _locals['rule'] = _G_apply_115
                _G_apply_116, lastError = self._apply(self.rule_anything, "anything", [])
                self.considerError(lastError, None)
                _locals['code'] = _G_apply_116
                _G_apply_117, lastError = self._apply(self.rule_transform, "transform", [])
                self.considerError(lastError, None)
                _locals['args'] = _G_apply_117
                return (_locals['args'], self.currentError)
            _G_termpattern_118, lastError = self.termpattern('ForeignApply', _G_termpattern_113)
            self.considerError(lastError, 'ForeignApply')
            self._trace('#TreeTransformer\nExactly(:x) -> [t.Match(x)]\nToken(:x) -> [t.Match(x)]\nMany(@x) -> [t.Choice(len(x) + 2)] + x + [t.Commit(-len(x) - 1)]\nMany1(@x) -> x + [t.Choice(len(x) + 2)] + x + [t.Commit(-len(x) - 1)]\nRepeat(:min :max @x)\n    -> [t.Python(repr(int(min))), t.Push(), t.Python(repr(int(max))),\n        t.Push(), t.RepeatChoice(len(x) + 2)] + x + [t.Commit(-len(x) - 1)]\nOptional(@x) -> [t.Choice(len(x) + 2)] + x + [t.Commit(2), t.Python("None")]\n# Right-associate Or() as needed. Note that Or() can have a list of a single\n# element.\nOr(@xs) = ?(len(xs) == 1) transform(xs[0])\n        | ?(len(xs) == 2) transform(t.Or(xs[0], xs[1]))\n        |                 transform(t.Or(xs[0], t.Or(xs[1:])))\nOr(@left @right)\n    -> [t.Choice(len(left) + 2)] + left + [t.Commit(len(right) + 1)] + right\nNot(@x) -> [t.Choice(len(x) + 3)] + x + [t.Commit(1), t.Fail()]\nLookahead(:x) = transform(t.Not(t.Not(x)))\nAnd(@xs) -> sum(xs, [])\nBind(:name @x) -> x + [t.Bind(name)]\nPredicate(@x) -> x + [t.Predicate()]\nAction(:x) -> [t.Python(x.data)]\nPython(:x) -> [t.Python(x.data)]\nList(@x) -> [t.Descend()] + x + [t.Ascend()]\nConsumedBy(@x) -> [t.StartSlice()] + x + [t.EndSlice()]\n\npushes :xs -> [inner for x in xs for inner in [x[0], t.Push()]]\nApply("super" :code @args) pushes(args):xs -> xs + [t.SuperCall(code)]\nApply(:rule :code @args) pushes(args):xs -> xs + [t.Call(rule)]\nForeignApply(:grammar :rule :code @args) pushes(args)', (0, 1419), self.input.position)
            _G_python_119, lastError = eval('args', self.globals, _locals), None
            self.considerError(lastError, 'ForeignApply')
            _G_apply_120, lastError = self._apply(self.rule_pushes, "pushes", [_G_python_119])
            self.considerError(lastError, 'ForeignApply')
            _locals['xs'] = _G_apply_120
            _G_python_121, lastError = eval('(xs +\n    [t.ForeignCall(grammar, rule)])', self.globals, _locals), None
            self.considerError(lastError, 'ForeignApply')
            return (_G_python_121, self.currentError)


        def rule_Rule(self):
            _locals = {'self': self}
            self.locals['Rule'] = _locals
            def _G_termpattern_122():
                _G_apply_123, lastError = self._apply(self.rule_anything, "anything", [])
                self.considerError(lastError, None)
                _locals['name'] = _G_apply_123
                _G_apply_124, lastError = self._apply(self.rule_transform, "transform", [])
                self.considerError(lastError, None)
                _locals['xs'] = _G_apply_124
                return (_locals['xs'], self.currentError)
            _G_termpattern_125, lastError = self.termpattern('Rule', _G_termpattern_122)
            self.considerError(lastError, 'Rule')
            _G_python_126, lastError = eval('t.Rule(name, xs)', self.globals, _locals), None
            self.considerError(lastError, 'Rule')
            return (_G_python_126, self.currentError)


        def rule_Grammar(self):
            _locals = {'self': self}
            self.locals['Grammar'] = _locals
            def _G_termpattern_127():
                _G_apply_128, lastError = self._apply(self.rule_anything, "anything", [])
                self.considerError(lastError, None)
                _locals['name'] = _G_apply_128
                _G_apply_129, lastError = self._apply(self.rule_anything, "anything", [])
                self.considerError(lastError, None)
                _locals['tree'] = _G_apply_129
                _G_apply_130, lastError = self._apply(self.rule_transform, "transform", [])
                self.considerError(lastError, None)
                _locals['rules'] = _G_apply_130
                return (_locals['rules'], self.currentError)
            _G_termpattern_131, lastError = self.termpattern('Grammar', _G_termpattern_127)
            self.considerError(lastError, 'Grammar')
            _G_python_132, lastError = eval('t.Grammar(name, tree, rules)', self.globals, _locals), None
            self.considerError(lastError, 'Grammar')
            return (_G_python_132, self.currentError)


        tree = True
    if vm.globals is not None:
        vm.globals = vm.globals.copy()
        vm.globals.update(ruleGlobals)
    else:
        vm.globals = ruleGlobals
    return vm