def createParserClass(GrammarBase, ruleGlobals):
    if ruleGlobals is None:
        ruleGlobals = {}
    class quasiterm(GrammarBase):
        def rule_schema(self):
            _locals = {'self': self}
            self.locals['schema'] = _locals
            def _G_many1_1():
                self._trace(' production', (8, 19), self.input.position)
                _G_apply_2, lastError = self._apply(self.rule_production, "production", [])
                self.considerError(lastError, None)
                return (_G_apply_2, self.currentError)
            _G_many1_3, lastError = self.many(_G_many1_1, _G_many1_1())
            self.considerError(lastError, 'schema')
            _locals['ps'] = _G_many1_3
            _G_python_4, lastError = eval('schema(ps)', self.globals, _locals), None
            self.considerError(lastError, 'schema')
            return (_G_python_4, self.currentError)


        def rule_production(self):
            _locals = {'self': self}
            self.locals['production'] = _locals
            self._trace(' tag', (50, 54), self.input.position)
            _G_apply_5, lastError = self._apply(self.rule_tag, "tag", [])
            self.considerError(lastError, 'production')
            _locals['t'] = _G_apply_5
            self._trace(' ws', (56, 59), self.input.position)
            _G_apply_6, lastError = self._apply(self.rule_ws, "ws", [])
            self.considerError(lastError, 'production')
            self._trace(" '::='", (59, 65), self.input.position)
            _G_exactly_7, lastError = self.exactly('::=')
            self.considerError(lastError, 'production')
            self._trace(' argList', (65, 73), self.input.position)
            _G_apply_8, lastError = self._apply(self.rule_argList, "argList", [])
            self.considerError(lastError, 'production')
            _locals['a'] = _G_apply_8
            self._trace(' ws', (75, 78), self.input.position)
            _G_apply_9, lastError = self._apply(self.rule_ws, "ws", [])
            self.considerError(lastError, 'production')
            self._trace(" ';'", (78, 82), self.input.position)
            _G_exactly_10, lastError = self.exactly(';')
            self.considerError(lastError, 'production')
            _G_python_11, lastError = eval('production(t, a)', self.globals, _locals), None
            self.considerError(lastError, 'production')
            return (_G_python_11, self.currentError)


        def rule_functor(self):
            _locals = {'self': self}
            self.locals['functor'] = _locals
            def _G_or_12():
                self._trace('spaces', (115, 121), self.input.position)
                _G_apply_13, lastError = self._apply(self.rule_spaces, "spaces", [])
                self.considerError(lastError, None)
                def _G_or_14():
                    self._trace('functorHole', (125, 136), self.input.position)
                    _G_apply_15, lastError = self._apply(self.rule_functorHole, "functorHole", [])
                    self.considerError(lastError, None)
                    self._trace(' functorHole', (136, 148), self.input.position)
                    _G_apply_16, lastError = self._apply(self.rule_functorHole, "functorHole", [])
                    self.considerError(lastError, None)
                    _G_python_17, lastError = eval('reserved("hole-tagged-hole")', self.globals, _locals), None
                    self.considerError(lastError, None)
                    return (_G_python_17, self.currentError)
                def _G_or_18():
                    def _G_optional_19():
                        self._trace("'.'", (203, 206), self.input.position)
                        _G_exactly_20, lastError = self.exactly('.')
                        self.considerError(lastError, None)
                        return (_G_exactly_20, self.currentError)
                    def _G_optional_21():
                        return (None, self.input.nullError())
                    _G_or_22, lastError = self._or([_G_optional_19, _G_optional_21])
                    self.considerError(lastError, None)
                    self._trace(' functorHole', (207, 219), self.input.position)
                    _G_apply_23, lastError = self._apply(self.rule_functorHole, "functorHole", [])
                    self.considerError(lastError, None)
                    return (_G_apply_23, self.currentError)
                def _G_or_24():
                    self._trace('tag', (242, 245), self.input.position)
                    _G_apply_25, lastError = self._apply(self.rule_tag, "tag", [])
                    self.considerError(lastError, None)
                    _locals['t'] = _G_apply_25
                    self._trace(' functorHole', (247, 259), self.input.position)
                    _G_apply_26, lastError = self._apply(self.rule_functorHole, "functorHole", [])
                    self.considerError(lastError, None)
                    _locals['h'] = _G_apply_26
                    _G_python_27, lastError = eval('taggedHole(t, h)', self.globals, _locals), None
                    self.considerError(lastError, None)
                    return (_G_python_27, self.currentError)
                _G_or_28, lastError = self._or([_G_or_14, _G_or_18, _G_or_24])
                self.considerError(lastError, None)
                return (_G_or_28, self.currentError)
            def _G_or_29():
                self._trace(' super', (296, 302), self.input.position)
                _G_apply_30, lastError = self.superApply("functor", )
                self.considerError(lastError, None)
                return (_G_apply_30, self.currentError)
            _G_or_31, lastError = self._or([_G_or_12, _G_or_29])
            self.considerError(lastError, 'functor')
            return (_G_or_31, self.currentError)


        def rule_arg(self):
            _locals = {'self': self}
            self.locals['arg'] = _locals
            self._trace(' interleave', (309, 320), self.input.position)
            _G_apply_32, lastError = self._apply(self.rule_interleave, "interleave", [])
            self.considerError(lastError, 'arg')
            _locals['l'] = _G_apply_32
            def _G_many_33():
                self._trace('ws', (324, 326), self.input.position)
                _G_apply_34, lastError = self._apply(self.rule_ws, "ws", [])
                self.considerError(lastError, None)
                self._trace(" '|'", (326, 330), self.input.position)
                _G_exactly_35, lastError = self.exactly('|')
                self.considerError(lastError, None)
                self._trace(' interleave', (330, 341), self.input.position)
                _G_apply_36, lastError = self._apply(self.rule_interleave, "interleave", [])
                self.considerError(lastError, None)
                return (_G_apply_36, self.currentError)
            _G_many_37, lastError = self.many(_G_many_33)
            self.considerError(lastError, 'arg')
            _locals['r'] = _G_many_37
            _G_python_38, lastError = eval('_or(l, *r)', self.globals, _locals), None
            self.considerError(lastError, 'arg')
            return (_G_python_38, self.currentError)


        def rule_interleave(self):
            _locals = {'self': self}
            self.locals['interleave'] = _locals
            self._trace(' action', (372, 379), self.input.position)
            _G_apply_39, lastError = self._apply(self.rule_action, "action", [])
            self.considerError(lastError, 'interleave')
            _locals['l'] = _G_apply_39
            def _G_many_40():
                self._trace('ws', (383, 385), self.input.position)
                _G_apply_41, lastError = self._apply(self.rule_ws, "ws", [])
                self.considerError(lastError, None)
                self._trace(" '&'", (385, 389), self.input.position)
                _G_exactly_42, lastError = self.exactly('&')
                self.considerError(lastError, None)
                self._trace(' action', (389, 396), self.input.position)
                _G_apply_43, lastError = self._apply(self.rule_action, "action", [])
                self.considerError(lastError, None)
                return (_G_apply_43, self.currentError)
            _G_many_44, lastError = self.many(_G_many_40)
            self.considerError(lastError, 'interleave')
            _locals['r'] = _G_many_44
            _G_python_45, lastError = eval('interleave(l, *r)', self.globals, _locals), None
            self.considerError(lastError, 'interleave')
            return (_G_python_45, self.currentError)


        def rule_action(self):
            _locals = {'self': self}
            self.locals['action'] = _locals
            self._trace(' pred', (430, 435), self.input.position)
            _G_apply_46, lastError = self._apply(self.rule_pred, "pred", [])
            self.considerError(lastError, 'action')
            _locals['l'] = _G_apply_46
            def _G_or_47():
                self._trace('ws', (439, 441), self.input.position)
                _G_apply_48, lastError = self._apply(self.rule_ws, "ws", [])
                self.considerError(lastError, None)
                self._trace(" '->'", (441, 446), self.input.position)
                _G_exactly_49, lastError = self.exactly('->')
                self.considerError(lastError, None)
                self._trace(' pred', (446, 451), self.input.position)
                _G_apply_50, lastError = self._apply(self.rule_pred, "pred", [])
                self.considerError(lastError, None)
                _locals['r'] = _G_apply_50
                _G_python_51, lastError = eval('action(l, *r)', self.globals, _locals), None
                self.considerError(lastError, None)
                return (_G_python_51, self.currentError)
            def _G_or_52():
                _G_python_53, lastError = eval('l', self.globals, _locals), None
                self.considerError(lastError, None)
                return (_G_python_53, self.currentError)
            _G_or_54, lastError = self._or([_G_or_47, _G_or_52])
            self.considerError(lastError, 'action')
            return (_G_or_54, self.currentError)


        def rule_pred(self):
            _locals = {'self': self}
            self.locals['pred'] = _locals
            def _G_or_55():
                self._trace(' some', (519, 524), self.input.position)
                _G_apply_56, lastError = self._apply(self.rule_some, "some", [])
                self.considerError(lastError, None)
                return (_G_apply_56, self.currentError)
            def _G_or_57():
                self._trace('ws', (528, 530), self.input.position)
                _G_apply_58, lastError = self._apply(self.rule_ws, "ws", [])
                self.considerError(lastError, None)
                self._trace(" '!'", (530, 534), self.input.position)
                _G_exactly_59, lastError = self.exactly('!')
                self.considerError(lastError, None)
                self._trace(' some', (534, 539), self.input.position)
                _G_apply_60, lastError = self._apply(self.rule_some, "some", [])
                self.considerError(lastError, None)
                _locals['x'] = _G_apply_60
                _G_python_61, lastError = eval('not(x)', self.globals, _locals), None
                self.considerError(lastError, None)
                return (_G_python_61, self.currentError)
            _G_or_62, lastError = self._or([_G_or_55, _G_or_57])
            self.considerError(lastError, 'pred')
            return (_G_or_62, self.currentError)


        def rule_some(self):
            _locals = {'self': self}
            self.locals['some'] = _locals
            def _G_or_63():
                self._trace('quant', (561, 566), self.input.position)
                _G_apply_64, lastError = self._apply(self.rule_quant, "quant", [])
                self.considerError(lastError, None)
                _locals['q'] = _G_apply_64
                _G_python_65, lastError = eval('some(None, q)', self.globals, _locals), None
                self.considerError(lastError, None)
                return (_G_python_65, self.currentError)
            def _G_or_66():
                self._trace(' prim', (596, 601), self.input.position)
                _G_apply_67, lastError = self._apply(self.rule_prim, "prim", [])
                self.considerError(lastError, None)
                _locals['l'] = _G_apply_67
                def _G_optional_68():
                    def _G_or_69():
                        self._trace('ws', (607, 609), self.input.position)
                        _G_apply_70, lastError = self._apply(self.rule_ws, "ws", [])
                        self.considerError(lastError, None)
                        self._trace(" '**'", (609, 614), self.input.position)
                        _G_exactly_71, lastError = self.exactly('**')
                        self.considerError(lastError, None)
                        self._trace(' prim', (614, 619), self.input.position)
                        _G_apply_72, lastError = self._apply(self.rule_prim, "prim", [])
                        self.considerError(lastError, None)
                        _locals['r'] = _G_apply_72
                        _G_python_73, lastError = eval('matchSeparatedSequence(l, r)', self.globals, _locals), None
                        self.considerError(lastError, None)
                        return (_G_python_73, self.currentError)
                    def _G_or_74():
                        self._trace('ws', (676, 678), self.input.position)
                        _G_apply_75, lastError = self._apply(self.rule_ws, "ws", [])
                        self.considerError(lastError, None)
                        self._trace(" '++'", (678, 683), self.input.position)
                        _G_exactly_76, lastError = self.exactly('++')
                        self.considerError(lastError, None)
                        self._trace(' prim', (683, 688), self.input.position)
                        _G_apply_77, lastError = self._apply(self.rule_prim, "prim", [])
                        self.considerError(lastError, None)
                        _locals['r'] = _G_apply_77
                        _G_python_78, lastError = eval('matchSeparatedSequence1(l, r)', self.globals, _locals), None
                        self.considerError(lastError, None)
                        return (_G_python_78, self.currentError)
                    _G_or_79, lastError = self._or([_G_or_69, _G_or_74])
                    self.considerError(lastError, None)
                    return (_G_or_79, self.currentError)
                def _G_optional_80():
                    return (None, self.input.nullError())
                _G_or_81, lastError = self._or([_G_optional_68, _G_optional_80])
                self.considerError(lastError, None)
                _locals['seq'] = _G_or_81
                def _G_optional_82():
                    self._trace('\n           quant', (749, 766), self.input.position)
                    _G_apply_83, lastError = self._apply(self.rule_quant, "quant", [])
                    self.considerError(lastError, None)
                    return (_G_apply_83, self.currentError)
                def _G_optional_84():
                    return (None, self.input.nullError())
                _G_or_85, lastError = self._or([_G_optional_82, _G_optional_84])
                self.considerError(lastError, None)
                _locals['q'] = _G_or_85
                _G_python_86, lastError = eval('some(seq or l, q)', self.globals, _locals), None
                self.considerError(lastError, None)
                return (_G_python_86, self.currentError)
            _G_or_87, lastError = self._or([_G_or_63, _G_or_66])
            self.considerError(lastError, 'some')
            return (_G_or_87, self.currentError)


        def rule_quant(self):
            _locals = {'self': self}
            self.locals['quant'] = _locals
            self._trace(' ws', (800, 803), self.input.position)
            _G_apply_88, lastError = self._apply(self.rule_ws, "ws", [])
            self.considerError(lastError, 'quant')
            def _G_or_89():
                self._trace("'?'", (805, 808), self.input.position)
                _G_exactly_90, lastError = self.exactly('?')
                self.considerError(lastError, None)
                return (_G_exactly_90, self.currentError)
            def _G_or_91():
                self._trace("'+'", (810, 813), self.input.position)
                _G_exactly_92, lastError = self.exactly('+')
                self.considerError(lastError, None)
                return (_G_exactly_92, self.currentError)
            def _G_or_93():
                self._trace(" '*'", (815, 819), self.input.position)
                _G_exactly_94, lastError = self.exactly('*')
                self.considerError(lastError, None)
                return (_G_exactly_94, self.currentError)
            _G_or_95, lastError = self._or([_G_or_89, _G_or_91, _G_or_93])
            self.considerError(lastError, 'quant')
            return (_G_or_95, self.currentError)


        def rule_prim(self):
            _locals = {'self': self}
            self.locals['prim'] = _locals
            def _G_or_96():
                self._trace(' term', (827, 832), self.input.position)
                _G_apply_97, lastError = self._apply(self.rule_term, "term", [])
                self.considerError(lastError, None)
                return (_G_apply_97, self.currentError)
            def _G_or_98():
                self._trace("'.'", (841, 844), self.input.position)
                _G_exactly_99, lastError = self.exactly('.')
                self.considerError(lastError, None)
                _G_python_100, lastError = eval('any()', self.globals, _locals), None
                self.considerError(lastError, None)
                return (_G_python_100, self.currentError)
            def _G_or_101():
                self._trace('literal', (863, 870), self.input.position)
                _G_apply_102, lastError = self._apply(self.rule_literal, "literal", [])
                self.considerError(lastError, None)
                _locals['l'] = _G_apply_102
                self._trace(' ws', (872, 875), self.input.position)
                _G_apply_103, lastError = self._apply(self.rule_ws, "ws", [])
                self.considerError(lastError, None)
                self._trace(" '..'", (875, 880), self.input.position)
                _G_exactly_104, lastError = self.exactly('..')
                self.considerError(lastError, None)
                self._trace(' literal', (880, 888), self.input.position)
                _G_apply_105, lastError = self._apply(self.rule_literal, "literal", [])
                self.considerError(lastError, None)
                _locals['r'] = _G_apply_105
                _G_python_106, lastError = eval('range(l, r)', self.globals, _locals), None
                self.considerError(lastError, None)
                return (_G_python_106, self.currentError)
            def _G_or_107():
                self._trace(' ws', (913, 916), self.input.position)
                _G_apply_108, lastError = self._apply(self.rule_ws, "ws", [])
                self.considerError(lastError, None)
                self._trace(" '^'", (916, 920), self.input.position)
                _G_exactly_109, lastError = self.exactly('^')
                self.considerError(lastError, None)
                self._trace(' string', (920, 927), self.input.position)
                _G_apply_110, lastError = self._apply(self.rule_string, "string", [])
                self.considerError(lastError, None)
                _locals['s'] = _G_apply_110
                _G_python_111, lastError = eval('anyOf(s)', self.globals, _locals), None
                self.considerError(lastError, None)
                return (_G_python_111, self.currentError)
            def _G_or_112():
                self._trace(' ws', (948, 951), self.input.position)
                _G_apply_113, lastError = self._apply(self.rule_ws, "ws", [])
                self.considerError(lastError, None)
                self._trace(" '('", (951, 955), self.input.position)
                _G_exactly_114, lastError = self.exactly('(')
                self.considerError(lastError, None)
                self._trace(' argList', (955, 963), self.input.position)
                _G_apply_115, lastError = self._apply(self.rule_argList, "argList", [])
                self.considerError(lastError, None)
                _locals['l'] = _G_apply_115
                self._trace(' ws', (965, 968), self.input.position)
                _G_apply_116, lastError = self._apply(self.rule_ws, "ws", [])
                self.considerError(lastError, None)
                self._trace(" ')'", (968, 972), self.input.position)
                _G_exactly_117, lastError = self.exactly(')')
                self.considerError(lastError, None)
                _G_python_118, lastError = eval('l', self.globals, _locals), None
                self.considerError(lastError, None)
                return (_G_python_118, self.currentError)
            _G_or_119, lastError = self._or([_G_or_96, _G_or_98, _G_or_101, _G_or_107, _G_or_112])
            self.considerError(lastError, 'prim')
            return (_G_or_119, self.currentError)


        def rule_simpleint(self):
            _locals = {'self': self}
            self.locals['simpleint'] = _locals
            self._trace(' decdigits', (990, 1000), self.input.position)
            _G_apply_120, lastError = self._apply(self.rule_decdigits, "decdigits", [])
            self.considerError(lastError, 'simpleint')
            _locals['ds'] = _G_apply_120
            _G_python_121, lastError = eval('int(ds)', self.globals, _locals), None
            self.considerError(lastError, 'simpleint')
            return (_G_python_121, self.currentError)


        def rule_functorHole(self):
            _locals = {'self': self}
            self.locals['functorHole'] = _locals
            def _G_or_122():
                self._trace(" '$'", (1028, 1032), self.input.position)
                _G_exactly_123, lastError = self.exactly('$')
                self.considerError(lastError, None)
                def _G_or_124():
                    self._trace('simpleint', (1041, 1050), self.input.position)
                    _G_apply_125, lastError = self._apply(self.rule_simpleint, "simpleint", [])
                    self.considerError(lastError, None)
                    _locals['i'] = _G_apply_125
                    return (_locals['i'], self.currentError)
                def _G_or_126():
                    self._trace(" '{'", (1054, 1058), self.input.position)
                    _G_exactly_127, lastError = self.exactly('{')
                    self.considerError(lastError, None)
                    self._trace(' simpleint', (1058, 1068), self.input.position)
                    _G_apply_128, lastError = self._apply(self.rule_simpleint, "simpleint", [])
                    self.considerError(lastError, None)
                    _locals['i'] = _G_apply_128
                    self._trace(" '}'", (1070, 1074), self.input.position)
                    _G_exactly_129, lastError = self.exactly('}')
                    self.considerError(lastError, None)
                    return (_G_exactly_129, self.currentError)
                def _G_or_130():
                    self._trace('tag', (1078, 1081), self.input.position)
                    _G_apply_131, lastError = self._apply(self.rule_tag, "tag", [])
                    self.considerError(lastError, None)
                    _locals['t'] = _G_apply_131
                    _G_python_132, lastError = eval('t.name', self.globals, _locals), None
                    self.considerError(lastError, None)
                    _locals['i'] = _G_python_132
                    return (_locals['i'], self.currentError)
                _G_or_133, lastError = self._or([_G_or_124, _G_or_126, _G_or_130])
                self.considerError(lastError, None)
                _G_python_134, lastError = eval('dollarHole(i)', self.globals, _locals), None
                self.considerError(lastError, None)
                return (_G_python_134, self.currentError)
            def _G_or_135():
                def _G_or_136():
                    self._trace("'@'", (1129, 1132), self.input.position)
                    _G_exactly_137, lastError = self.exactly('@')
                    self.considerError(lastError, None)
                    return (_G_exactly_137, self.currentError)
                def _G_or_138():
                    self._trace(" '='", (1134, 1138), self.input.position)
                    _G_exactly_139, lastError = self.exactly('=')
                    self.considerError(lastError, None)
                    return (_G_exactly_139, self.currentError)
                _G_or_140, lastError = self._or([_G_or_136, _G_or_138])
                self.considerError(lastError, None)
                def _G_or_141():
                    self._trace('simpleint', (1141, 1150), self.input.position)
                    _G_apply_142, lastError = self._apply(self.rule_simpleint, "simpleint", [])
                    self.considerError(lastError, None)
                    _locals['i'] = _G_apply_142
                    return (_locals['i'], self.currentError)
                def _G_or_143():
                    self._trace(" '{'", (1154, 1158), self.input.position)
                    _G_exactly_144, lastError = self.exactly('{')
                    self.considerError(lastError, None)
                    self._trace(' simpleint', (1158, 1168), self.input.position)
                    _G_apply_145, lastError = self._apply(self.rule_simpleint, "simpleint", [])
                    self.considerError(lastError, None)
                    _locals['i'] = _G_apply_145
                    self._trace(" '}'", (1170, 1174), self.input.position)
                    _G_exactly_146, lastError = self.exactly('}')
                    self.considerError(lastError, None)
                    return (_G_exactly_146, self.currentError)
                def _G_or_147():
                    self._trace('tag', (1178, 1181), self.input.position)
                    _G_apply_148, lastError = self._apply(self.rule_tag, "tag", [])
                    self.considerError(lastError, None)
                    _locals['t'] = _G_apply_148
                    _G_python_149, lastError = eval('t.name', self.globals, _locals), None
                    self.considerError(lastError, None)
                    _locals['i'] = _G_python_149
                    return (_locals['i'], self.currentError)
                _G_or_150, lastError = self._or([_G_or_141, _G_or_143, _G_or_147])
                self.considerError(lastError, None)
                _G_python_151, lastError = eval('patternHole(i)', self.globals, _locals), None
                self.considerError(lastError, None)
                return (_G_python_151, self.currentError)
            _G_or_152, lastError = self._or([_G_or_122, _G_or_135])
            self.considerError(lastError, 'functorHole')
            return (_G_or_152, self.currentError)


    if quasiterm.globals is not None:
        quasiterm.globals = quasiterm.globals.copy()
        quasiterm.globals.update(ruleGlobals)
    else:
        quasiterm.globals = ruleGlobals
    return quasiterm