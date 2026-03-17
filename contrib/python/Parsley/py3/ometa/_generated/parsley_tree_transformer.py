def createParserClass(GrammarBase, ruleGlobals):
    if ruleGlobals is None:
        ruleGlobals = {}
    class parsley_tree_transformer(GrammarBase):
        def rule_termPattern(self):
            _locals = {'self': self}
            self.locals['termPattern'] = _locals
            def _G_optional_1():
                self._trace(' indentation', (13, 25), self.input.position)
                _G_apply_2, lastError = self._apply(self.rule_indentation, "indentation", [])
                self.considerError(lastError, None)
                return (_G_apply_2, self.currentError)
            def _G_optional_3():
                return (None, self.input.nullError())
            _G_or_4, lastError = self._or([_G_optional_1, _G_optional_3])
            self.considerError(lastError, 'termPattern')
            self._trace(' name', (26, 31), self.input.position)
            _G_apply_5, lastError = self._apply(self.rule_name, "name", [])
            self.considerError(lastError, 'termPattern')
            _locals['name'] = _G_apply_5
            def _G_pred_6():
                _G_python_7, lastError = eval('name[0].isupper()', self.globals, _locals), None
                self.considerError(lastError, None)
                return (_G_python_7, self.currentError)
            _G_pred_8, lastError = self.pred(_G_pred_6)
            self.considerError(lastError, 'termPattern')
            self._trace("\n              '('", (57, 75), self.input.position)
            _G_exactly_9, lastError = self.exactly('(')
            self.considerError(lastError, 'termPattern')
            def _G_optional_10():
                self._trace(' expr', (75, 80), self.input.position)
                _G_apply_11, lastError = self._apply(self.rule_expr, "expr", [])
                self.considerError(lastError, None)
                return (_G_apply_11, self.currentError)
            def _G_optional_12():
                return (None, self.input.nullError())
            _G_or_13, lastError = self._or([_G_optional_10, _G_optional_12])
            self.considerError(lastError, 'termPattern')
            _locals['patts'] = _G_or_13
            self._trace(" ')'", (87, 91), self.input.position)
            _G_exactly_14, lastError = self.exactly(')')
            self.considerError(lastError, 'termPattern')
            _G_python_15, lastError = eval('t.TermPattern(name, patts)', self.globals, _locals), None
            self.considerError(lastError, 'termPattern')
            return (_G_python_15, self.currentError)


        def rule_subtransform(self):
            _locals = {'self': self}
            self.locals['subtransform'] = _locals
            self._trace(' ws', (137, 140), self.input.position)
            _G_apply_16, lastError = self._apply(self.rule_ws, "ws", [])
            self.considerError(lastError, 'subtransform')
            self._trace(" '@'", (140, 144), self.input.position)
            _G_exactly_17, lastError = self.exactly('@')
            self.considerError(lastError, 'subtransform')
            self._trace(' name', (144, 149), self.input.position)
            _G_apply_18, lastError = self._apply(self.rule_name, "name", [])
            self.considerError(lastError, 'subtransform')
            _locals['n'] = _G_apply_18
            _G_python_19, lastError = eval("t.Bind(n, t.Apply('transform', self.rulename, []))", self.globals, _locals), None
            self.considerError(lastError, 'subtransform')
            return (_G_python_19, self.currentError)


        def rule_wide_templatedValue(self):
            _locals = {'self': self}
            self.locals['wide_templatedValue'] = _locals
            self._trace(' ws', (228, 231), self.input.position)
            _G_apply_20, lastError = self._apply(self.rule_ws, "ws", [])
            self.considerError(lastError, 'wide_templatedValue')
            self._trace(" '-->'", (231, 237), self.input.position)
            _G_exactly_21, lastError = self.exactly('-->')
            self.considerError(lastError, 'wide_templatedValue')
            def _G_many_22():
                self._trace(" ' '", (237, 241), self.input.position)
                _G_exactly_23, lastError = self.exactly(' ')
                self.considerError(lastError, None)
                return (_G_exactly_23, self.currentError)
            _G_many_24, lastError = self.many(_G_many_22)
            self.considerError(lastError, 'wide_templatedValue')
            self._trace(' wideTemplateBits', (242, 259), self.input.position)
            _G_apply_25, lastError = self._apply(self.rule_wideTemplateBits, "wideTemplateBits", [])
            self.considerError(lastError, 'wide_templatedValue')
            _locals['contents'] = _G_apply_25
            _G_python_26, lastError = eval('t.StringTemplate(contents)', self.globals, _locals), None
            self.considerError(lastError, 'wide_templatedValue')
            return (_G_python_26, self.currentError)


        def rule_tall_templatedValue(self):
            _locals = {'self': self}
            self.locals['tall_templatedValue'] = _locals
            def _G_optional_27():
                self._trace(' hspace', (320, 327), self.input.position)
                _G_apply_28, lastError = self._apply(self.rule_hspace, "hspace", [])
                self.considerError(lastError, None)
                return (_G_apply_28, self.currentError)
            def _G_optional_29():
                return (None, self.input.nullError())
            _G_or_30, lastError = self._or([_G_optional_27, _G_optional_29])
            self.considerError(lastError, 'tall_templatedValue')
            self._trace(" '{{{'", (328, 334), self.input.position)
            _G_exactly_31, lastError = self.exactly('{{{')
            self.considerError(lastError, 'tall_templatedValue')
            def _G_many_32():
                def _G_or_33():
                    self._trace("' '", (336, 339), self.input.position)
                    _G_exactly_34, lastError = self.exactly(' ')
                    self.considerError(lastError, None)
                    return (_G_exactly_34, self.currentError)
                def _G_or_35():
                    self._trace(" '\\t'", (341, 346), self.input.position)
                    _G_exactly_36, lastError = self.exactly('\t')
                    self.considerError(lastError, None)
                    return (_G_exactly_36, self.currentError)
                _G_or_37, lastError = self._or([_G_or_33, _G_or_35])
                self.considerError(lastError, None)
                return (_G_or_37, self.currentError)
            _G_many_38, lastError = self.many(_G_many_32)
            self.considerError(lastError, 'tall_templatedValue')
            def _G_optional_39():
                self._trace(' vspace', (348, 355), self.input.position)
                _G_apply_40, lastError = self._apply(self.rule_vspace, "vspace", [])
                self.considerError(lastError, None)
                return (_G_apply_40, self.currentError)
            def _G_optional_41():
                return (None, self.input.nullError())
            _G_or_42, lastError = self._or([_G_optional_39, _G_optional_41])
            self.considerError(lastError, 'tall_templatedValue')
            self._trace(' tallTemplateBits', (356, 373), self.input.position)
            _G_apply_43, lastError = self._apply(self.rule_tallTemplateBits, "tallTemplateBits", [])
            self.considerError(lastError, 'tall_templatedValue')
            _locals['contents'] = _G_apply_43
            self._trace(" '}}}'", (382, 388), self.input.position)
            _G_exactly_44, lastError = self.exactly('}}}')
            self.considerError(lastError, 'tall_templatedValue')
            _G_python_45, lastError = eval('t.StringTemplate(contents)', self.globals, _locals), None
            self.considerError(lastError, 'tall_templatedValue')
            return (_G_python_45, self.currentError)


        def rule_tallTemplateBits(self):
            _locals = {'self': self}
            self.locals['tallTemplateBits'] = _locals
            def _G_many_46():
                def _G_or_47():
                    self._trace('exprHole', (440, 448), self.input.position)
                    _G_apply_48, lastError = self._apply(self.rule_exprHole, "exprHole", [])
                    self.considerError(lastError, None)
                    return (_G_apply_48, self.currentError)
                def _G_or_49():
                    self._trace(' tallTemplateText', (450, 467), self.input.position)
                    _G_apply_50, lastError = self._apply(self.rule_tallTemplateText, "tallTemplateText", [])
                    self.considerError(lastError, None)
                    return (_G_apply_50, self.currentError)
                _G_or_51, lastError = self._or([_G_or_47, _G_or_49])
                self.considerError(lastError, None)
                return (_G_or_51, self.currentError)
            _G_many_52, lastError = self.many(_G_many_46)
            self.considerError(lastError, 'tallTemplateBits')
            return (_G_many_52, self.currentError)


        def rule_tallTemplateText(self):
            _locals = {'self': self}
            self.locals['tallTemplateText'] = _locals
            def _G_or_53():
                def _G_consumedby_54():
                    def _G_many1_55():
                        def _G_or_56():
                            def _G_not_57():
                                def _G_or_58():
                                    self._trace("'}}}'", (493, 498), self.input.position)
                                    _G_exactly_59, lastError = self.exactly('}}}')
                                    self.considerError(lastError, None)
                                    return (_G_exactly_59, self.currentError)
                                def _G_or_60():
                                    self._trace(" '$'", (500, 504), self.input.position)
                                    _G_exactly_61, lastError = self.exactly('$')
                                    self.considerError(lastError, None)
                                    return (_G_exactly_61, self.currentError)
                                def _G_or_62():
                                    self._trace(" '\\r'", (506, 511), self.input.position)
                                    _G_exactly_63, lastError = self.exactly('\r')
                                    self.considerError(lastError, None)
                                    return (_G_exactly_63, self.currentError)
                                def _G_or_64():
                                    self._trace(" '\\n'", (513, 518), self.input.position)
                                    _G_exactly_65, lastError = self.exactly('\n')
                                    self.considerError(lastError, None)
                                    return (_G_exactly_65, self.currentError)
                                _G_or_66, lastError = self._or([_G_or_58, _G_or_60, _G_or_62, _G_or_64])
                                self.considerError(lastError, None)
                                return (_G_or_66, self.currentError)
                            _G_not_67, lastError = self._not(_G_not_57)
                            self.considerError(lastError, None)
                            self._trace(' anything', (519, 528), self.input.position)
                            _G_apply_68, lastError = self._apply(self.rule_anything, "anything", [])
                            self.considerError(lastError, None)
                            return (_G_apply_68, self.currentError)
                        def _G_or_69():
                            self._trace(" '$'", (530, 534), self.input.position)
                            _G_exactly_70, lastError = self.exactly('$')
                            self.considerError(lastError, None)
                            self._trace(" '$'", (534, 538), self.input.position)
                            _G_exactly_71, lastError = self.exactly('$')
                            self.considerError(lastError, None)
                            return (_G_exactly_71, self.currentError)
                        _G_or_72, lastError = self._or([_G_or_56, _G_or_69])
                        self.considerError(lastError, None)
                        return (_G_or_72, self.currentError)
                    _G_many1_73, lastError = self.many(_G_many1_55, _G_many1_55())
                    self.considerError(lastError, None)
                    def _G_many_74():
                        self._trace(' vspace', (540, 547), self.input.position)
                        _G_apply_75, lastError = self._apply(self.rule_vspace, "vspace", [])
                        self.considerError(lastError, None)
                        return (_G_apply_75, self.currentError)
                    _G_many_76, lastError = self.many(_G_many_74)
                    self.considerError(lastError, None)
                    return (_G_many_76, self.currentError)
                _G_consumedby_77, lastError = self.consumedby(_G_consumedby_54)
                self.considerError(lastError, None)
                return (_G_consumedby_77, self.currentError)
            def _G_or_78():
                self._trace(' vspace', (551, 558), self.input.position)
                _G_apply_79, lastError = self._apply(self.rule_vspace, "vspace", [])
                self.considerError(lastError, None)
                return (_G_apply_79, self.currentError)
            _G_or_80, lastError = self._or([_G_or_53, _G_or_78])
            self.considerError(lastError, 'tallTemplateText')
            return (_G_or_80, self.currentError)


        def rule_wideTemplateBits(self):
            _locals = {'self': self}
            self.locals['wideTemplateBits'] = _locals
            def _G_many_81():
                def _G_or_82():
                    self._trace('exprHole', (580, 588), self.input.position)
                    _G_apply_83, lastError = self._apply(self.rule_exprHole, "exprHole", [])
                    self.considerError(lastError, None)
                    return (_G_apply_83, self.currentError)
                def _G_or_84():
                    self._trace(' wideTemplateText', (590, 607), self.input.position)
                    _G_apply_85, lastError = self._apply(self.rule_wideTemplateText, "wideTemplateText", [])
                    self.considerError(lastError, None)
                    return (_G_apply_85, self.currentError)
                _G_or_86, lastError = self._or([_G_or_82, _G_or_84])
                self.considerError(lastError, None)
                return (_G_or_86, self.currentError)
            _G_many_87, lastError = self.many(_G_many_81)
            self.considerError(lastError, 'wideTemplateBits')
            return (_G_many_87, self.currentError)


        def rule_wideTemplateText(self):
            _locals = {'self': self}
            self.locals['wideTemplateText'] = _locals
            def _G_consumedby_88():
                def _G_many1_89():
                    def _G_or_90():
                        def _G_not_91():
                            def _G_or_92():
                                self._trace('vspace', (633, 639), self.input.position)
                                _G_apply_93, lastError = self._apply(self.rule_vspace, "vspace", [])
                                self.considerError(lastError, None)
                                return (_G_apply_93, self.currentError)
                            def _G_or_94():
                                self._trace(' end', (641, 645), self.input.position)
                                _G_apply_95, lastError = self._apply(self.rule_end, "end", [])
                                self.considerError(lastError, None)
                                return (_G_apply_95, self.currentError)
                            def _G_or_96():
                                self._trace("'$'", (647, 650), self.input.position)
                                _G_exactly_97, lastError = self.exactly('$')
                                self.considerError(lastError, None)
                                return (_G_exactly_97, self.currentError)
                            _G_or_98, lastError = self._or([_G_or_92, _G_or_94, _G_or_96])
                            self.considerError(lastError, None)
                            return (_G_or_98, self.currentError)
                        _G_not_99, lastError = self._not(_G_not_91)
                        self.considerError(lastError, None)
                        self._trace(' anything', (651, 660), self.input.position)
                        _G_apply_100, lastError = self._apply(self.rule_anything, "anything", [])
                        self.considerError(lastError, None)
                        return (_G_apply_100, self.currentError)
                    def _G_or_101():
                        self._trace(" '$'", (662, 666), self.input.position)
                        _G_exactly_102, lastError = self.exactly('$')
                        self.considerError(lastError, None)
                        self._trace(" '$'", (666, 670), self.input.position)
                        _G_exactly_103, lastError = self.exactly('$')
                        self.considerError(lastError, None)
                        return (_G_exactly_103, self.currentError)
                    _G_or_104, lastError = self._or([_G_or_90, _G_or_101])
                    self.considerError(lastError, None)
                    return (_G_or_104, self.currentError)
                _G_many1_105, lastError = self.many(_G_many1_89, _G_many1_89())
                self.considerError(lastError, None)
                return (_G_many1_105, self.currentError)
            _G_consumedby_106, lastError = self.consumedby(_G_consumedby_88)
            self.considerError(lastError, 'wideTemplateText')
            return (_G_consumedby_106, self.currentError)


        def rule_exprHole(self):
            _locals = {'self': self}
            self.locals['exprHole'] = _locals
            self._trace(" '$'", (685, 689), self.input.position)
            _G_exactly_107, lastError = self.exactly('$')
            self.considerError(lastError, 'exprHole')
            self._trace(' name', (689, 694), self.input.position)
            _G_apply_108, lastError = self._apply(self.rule_name, "name", [])
            self.considerError(lastError, 'exprHole')
            _locals['n'] = _G_apply_108
            _G_python_109, lastError = eval('t.QuasiExprHole(n)', self.globals, _locals), None
            self.considerError(lastError, 'exprHole')
            return (_G_python_109, self.currentError)


        def rule_expr1(self):
            _locals = {'self': self}
            self.locals['expr1'] = _locals
            def _G_or_110():
                self._trace(' foreignApply', (727, 740), self.input.position)
                _G_apply_111, lastError = self._apply(self.rule_foreignApply, "foreignApply", [])
                self.considerError(lastError, None)
                return (_G_apply_111, self.currentError)
            def _G_or_112():
                self._trace('termPattern', (749, 760), self.input.position)
                _G_apply_113, lastError = self._apply(self.rule_termPattern, "termPattern", [])
                self.considerError(lastError, None)
                return (_G_apply_113, self.currentError)
            def _G_or_114():
                self._trace('subtransform', (769, 781), self.input.position)
                _G_apply_115, lastError = self._apply(self.rule_subtransform, "subtransform", [])
                self.considerError(lastError, None)
                return (_G_apply_115, self.currentError)
            def _G_or_116():
                self._trace('application', (790, 801), self.input.position)
                _G_apply_117, lastError = self._apply(self.rule_application, "application", [])
                self.considerError(lastError, None)
                return (_G_apply_117, self.currentError)
            def _G_or_118():
                self._trace('ruleValue', (810, 819), self.input.position)
                _G_apply_119, lastError = self._apply(self.rule_ruleValue, "ruleValue", [])
                self.considerError(lastError, None)
                return (_G_apply_119, self.currentError)
            def _G_or_120():
                self._trace('wide_templatedValue', (828, 847), self.input.position)
                _G_apply_121, lastError = self._apply(self.rule_wide_templatedValue, "wide_templatedValue", [])
                self.considerError(lastError, None)
                return (_G_apply_121, self.currentError)
            def _G_or_122():
                self._trace('tall_templatedValue', (856, 875), self.input.position)
                _G_apply_123, lastError = self._apply(self.rule_tall_templatedValue, "tall_templatedValue", [])
                self.considerError(lastError, None)
                return (_G_apply_123, self.currentError)
            def _G_or_124():
                self._trace('semanticPredicate', (884, 901), self.input.position)
                _G_apply_125, lastError = self._apply(self.rule_semanticPredicate, "semanticPredicate", [])
                self.considerError(lastError, None)
                return (_G_apply_125, self.currentError)
            def _G_or_126():
                self._trace('semanticAction', (910, 924), self.input.position)
                _G_apply_127, lastError = self._apply(self.rule_semanticAction, "semanticAction", [])
                self.considerError(lastError, None)
                return (_G_apply_127, self.currentError)
            def _G_or_128():
                self._trace('number', (933, 939), self.input.position)
                _G_apply_129, lastError = self._apply(self.rule_number, "number", [])
                self.considerError(lastError, None)
                _locals['n'] = _G_apply_129
                _G_python_130, lastError = eval('self.isTree()', self.globals, _locals), None
                self.considerError(lastError, None)
                _G_python_131, lastError = eval('n', self.globals, _locals), None
                self.considerError(lastError, None)
                return (_G_python_131, self.currentError)
            def _G_or_132():
                self._trace('character', (972, 981), self.input.position)
                _G_apply_133, lastError = self._apply(self.rule_character, "character", [])
                self.considerError(lastError, None)
                return (_G_apply_133, self.currentError)
            def _G_or_134():
                self._trace('string', (990, 996), self.input.position)
                _G_apply_135, lastError = self._apply(self.rule_string, "string", [])
                self.considerError(lastError, None)
                return (_G_apply_135, self.currentError)
            def _G_or_136():
                self._trace('ws', (1005, 1007), self.input.position)
                _G_apply_137, lastError = self._apply(self.rule_ws, "ws", [])
                self.considerError(lastError, None)
                self._trace(" '('", (1007, 1011), self.input.position)
                _G_exactly_138, lastError = self.exactly('(')
                self.considerError(lastError, None)
                def _G_optional_139():
                    self._trace(' expr', (1011, 1016), self.input.position)
                    _G_apply_140, lastError = self._apply(self.rule_expr, "expr", [])
                    self.considerError(lastError, None)
                    return (_G_apply_140, self.currentError)
                def _G_optional_141():
                    return (None, self.input.nullError())
                _G_or_142, lastError = self._or([_G_optional_139, _G_optional_141])
                self.considerError(lastError, None)
                _locals['e'] = _G_or_142
                self._trace(' ws', (1019, 1022), self.input.position)
                _G_apply_143, lastError = self._apply(self.rule_ws, "ws", [])
                self.considerError(lastError, None)
                self._trace(" ')'", (1022, 1026), self.input.position)
                _G_exactly_144, lastError = self.exactly(')')
                self.considerError(lastError, None)
                _G_python_145, lastError = eval('e', self.globals, _locals), None
                self.considerError(lastError, None)
                return (_G_python_145, self.currentError)
            def _G_or_146():
                self._trace('ws', (1040, 1042), self.input.position)
                _G_apply_147, lastError = self._apply(self.rule_ws, "ws", [])
                self.considerError(lastError, None)
                self._trace(" '['", (1042, 1046), self.input.position)
                _G_exactly_148, lastError = self.exactly('[')
                self.considerError(lastError, None)
                def _G_optional_149():
                    self._trace(' expr', (1046, 1051), self.input.position)
                    _G_apply_150, lastError = self._apply(self.rule_expr, "expr", [])
                    self.considerError(lastError, None)
                    return (_G_apply_150, self.currentError)
                def _G_optional_151():
                    return (None, self.input.nullError())
                _G_or_152, lastError = self._or([_G_optional_149, _G_optional_151])
                self.considerError(lastError, None)
                _locals['e'] = _G_or_152
                self._trace(' ws', (1054, 1057), self.input.position)
                _G_apply_153, lastError = self._apply(self.rule_ws, "ws", [])
                self.considerError(lastError, None)
                self._trace(" ']'", (1057, 1061), self.input.position)
                _G_exactly_154, lastError = self.exactly(']')
                self.considerError(lastError, None)
                _G_python_155, lastError = eval('t.TermPattern(".tuple.", e or t.And([]))', self.globals, _locals), None
                self.considerError(lastError, None)
                return (_G_python_155, self.currentError)
            _G_or_156, lastError = self._or([_G_or_110, _G_or_112, _G_or_114, _G_or_116, _G_or_118, _G_or_120, _G_or_122, _G_or_124, _G_or_126, _G_or_128, _G_or_132, _G_or_134, _G_or_136, _G_or_146])
            self.considerError(lastError, 'expr1')
            return (_G_or_156, self.currentError)


        def rule_grammar(self):
            _locals = {'self': self}
            self.locals['grammar'] = _locals
            def _G_many_157():
                self._trace(' rule', (1116, 1121), self.input.position)
                _G_apply_158, lastError = self._apply(self.rule_rule, "rule", [])
                self.considerError(lastError, None)
                return (_G_apply_158, self.currentError)
            _G_many_159, lastError = self.many(_G_many_157)
            self.considerError(lastError, 'grammar')
            _locals['rs'] = _G_many_159
            self._trace(' ws', (1125, 1128), self.input.position)
            _G_apply_160, lastError = self._apply(self.rule_ws, "ws", [])
            self.considerError(lastError, 'grammar')
            _G_python_161, lastError = eval('t.Grammar(self.name, True, rs)', self.globals, _locals), None
            self.considerError(lastError, 'grammar')
            return (_G_python_161, self.currentError)


        def rule_rule(self):
            _locals = {'self': self}
            self.locals['rule'] = _locals
            self._trace(' noindentation', (1169, 1183), self.input.position)
            _G_apply_162, lastError = self._apply(self.rule_noindentation, "noindentation", [])
            self.considerError(lastError, 'rule')
            def _G_lookahead_163():
                self._trace('name', (1187, 1191), self.input.position)
                _G_apply_164, lastError = self._apply(self.rule_name, "name", [])
                self.considerError(lastError, None)
                _locals['n'] = _G_apply_164
                return (_locals['n'], self.currentError)
            _G_lookahead_165, lastError = self.lookahead(_G_lookahead_163)
            self.considerError(lastError, 'rule')
            def _G_or_166():
                def _G_many1_167():
                    self._trace('termRulePart(n)', (1196, 1211), self.input.position)
                    _G_python_168, lastError = eval('n', self.globals, _locals), None
                    self.considerError(lastError, None)
                    _G_apply_169, lastError = self._apply(self.rule_termRulePart, "termRulePart", [_G_python_168])
                    self.considerError(lastError, None)
                    return (_G_apply_169, self.currentError)
                _G_many1_170, lastError = self.many(_G_many1_167, _G_many1_167())
                self.considerError(lastError, None)
                _locals['rs'] = _G_many1_170
                return (_locals['rs'], self.currentError)
            def _G_or_171():
                def _G_many1_172():
                    self._trace(' rulePart(n)', (1217, 1229), self.input.position)
                    _G_python_173, lastError = eval('n', self.globals, _locals), None
                    self.considerError(lastError, None)
                    _G_apply_174, lastError = self._apply(self.rule_rulePart, "rulePart", [_G_python_173])
                    self.considerError(lastError, None)
                    return (_G_apply_174, self.currentError)
                _G_many1_175, lastError = self.many(_G_many1_172, _G_many1_172())
                self.considerError(lastError, None)
                _locals['rs'] = _G_many1_175
                return (_locals['rs'], self.currentError)
            _G_or_176, lastError = self._or([_G_or_166, _G_or_171])
            self.considerError(lastError, 'rule')
            _G_python_177, lastError = eval('t.Rule(n, t.Or(rs))', self.globals, _locals), None
            self.considerError(lastError, 'rule')
            return (_G_python_177, self.currentError)


        def rule_termRulePart(self):
            _locals = {'self': self}
            self.locals['termRulePart'] = _locals
            _G_apply_178, lastError = self._apply(self.rule_anything, "anything", [])
            self.considerError(lastError, 'termRulePart')
            _locals['requiredName'] = _G_apply_178
            self._trace('  noindentation', (1288, 1303), self.input.position)
            _G_apply_179, lastError = self._apply(self.rule_noindentation, "noindentation", [])
            self.considerError(lastError, 'termRulePart')
            _G_python_180, lastError = eval('setattr(self, "rulename", requiredName)', self.globals, _locals), None
            self.considerError(lastError, 'termRulePart')
            self._trace('\n                             termPattern', (1346, 1387), self.input.position)
            _G_apply_181, lastError = self._apply(self.rule_termPattern, "termPattern", [])
            self.considerError(lastError, 'termRulePart')
            _locals['tt'] = _G_apply_181
            def _G_pred_182():
                _G_python_183, lastError = eval('tt.args[0].data == requiredName', self.globals, _locals), None
                self.considerError(lastError, None)
                return (_G_python_183, self.currentError)
            _G_pred_184, lastError = self.pred(_G_pred_182)
            self.considerError(lastError, 'termRulePart')
            def _G_optional_185():
                self._trace(' token("=")', (1425, 1436), self.input.position)
                _G_python_186, lastError = ("="), None
                self.considerError(lastError, None)
                _G_apply_187, lastError = self._apply(self.rule_token, "token", [_G_python_186])
                self.considerError(lastError, None)
                return (_G_apply_187, self.currentError)
            def _G_optional_188():
                return (None, self.input.nullError())
            _G_or_189, lastError = self._or([_G_optional_185, _G_optional_188])
            self.considerError(lastError, 'termRulePart')
            self._trace(' expr', (1437, 1442), self.input.position)
            _G_apply_190, lastError = self._apply(self.rule_expr, "expr", [])
            self.considerError(lastError, 'termRulePart')
            _locals['tail'] = _G_apply_190
            _G_python_191, lastError = eval('t.And([tt, tail])', self.globals, _locals), None
            self.considerError(lastError, 'termRulePart')
            return (_G_python_191, self.currentError)


    if parsley_tree_transformer.globals is not None:
        parsley_tree_transformer.globals = parsley_tree_transformer.globals.copy()
        parsley_tree_transformer.globals.update(ruleGlobals)
    else:
        parsley_tree_transformer.globals = ruleGlobals
    return parsley_tree_transformer