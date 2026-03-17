def createParserClass(GrammarBase, ruleGlobals):
    if ruleGlobals is None:
        ruleGlobals = {}
    class parsley(GrammarBase):
        def rule_comment(self):
            _locals = {'self': self}
            self.locals['comment'] = _locals
            self._trace(" '#'", (9, 13), self.input.position)
            _G_exactly_1, lastError = self.exactly('#')
            self.considerError(lastError, 'comment')
            def _G_many_2():
                def _G_not_3():
                    self._trace("'\\n'", (16, 20), self.input.position)
                    _G_exactly_4, lastError = self.exactly('\n')
                    self.considerError(lastError, None)
                    return (_G_exactly_4, self.currentError)
                _G_not_5, lastError = self._not(_G_not_3)
                self.considerError(lastError, None)
                self._trace(' anything', (20, 29), self.input.position)
                _G_apply_6, lastError = self._apply(self.rule_anything, "anything", [])
                self.considerError(lastError, None)
                return (_G_apply_6, self.currentError)
            _G_many_7, lastError = self.many(_G_many_2)
            self.considerError(lastError, 'comment')
            return (_G_many_7, self.currentError)


        def rule_hspace(self):
            _locals = {'self': self}
            self.locals['hspace'] = _locals
            def _G_or_8():
                self._trace(" ' '", (40, 44), self.input.position)
                _G_exactly_9, lastError = self.exactly(' ')
                self.considerError(lastError, None)
                return (_G_exactly_9, self.currentError)
            def _G_or_10():
                self._trace(" '\\t'", (46, 51), self.input.position)
                _G_exactly_11, lastError = self.exactly('\t')
                self.considerError(lastError, None)
                return (_G_exactly_11, self.currentError)
            def _G_or_12():
                self._trace(' comment', (53, 61), self.input.position)
                _G_apply_13, lastError = self._apply(self.rule_comment, "comment", [])
                self.considerError(lastError, None)
                return (_G_apply_13, self.currentError)
            _G_or_14, lastError = self._or([_G_or_8, _G_or_10, _G_or_12])
            self.considerError(lastError, 'hspace')
            return (_G_or_14, self.currentError)


        def rule_vspace(self):
            _locals = {'self': self}
            self.locals['vspace'] = _locals
            def _G_or_15():
                self._trace("  '\\r\\n'", (70, 78), self.input.position)
                _G_exactly_16, lastError = self.exactly('\r\n')
                self.considerError(lastError, None)
                return (_G_exactly_16, self.currentError)
            def _G_or_17():
                self._trace(" '\\r'", (80, 85), self.input.position)
                _G_exactly_18, lastError = self.exactly('\r')
                self.considerError(lastError, None)
                return (_G_exactly_18, self.currentError)
            def _G_or_19():
                self._trace(" '\\n'", (87, 92), self.input.position)
                _G_exactly_20, lastError = self.exactly('\n')
                self.considerError(lastError, None)
                return (_G_exactly_20, self.currentError)
            _G_or_21, lastError = self._or([_G_or_15, _G_or_17, _G_or_19])
            self.considerError(lastError, 'vspace')
            return (_G_or_21, self.currentError)


        def rule_ws(self):
            _locals = {'self': self}
            self.locals['ws'] = _locals
            def _G_many_22():
                def _G_or_23():
                    self._trace('hspace', (99, 105), self.input.position)
                    _G_apply_24, lastError = self._apply(self.rule_hspace, "hspace", [])
                    self.considerError(lastError, None)
                    return (_G_apply_24, self.currentError)
                def _G_or_25():
                    self._trace(' vspace', (107, 114), self.input.position)
                    _G_apply_26, lastError = self._apply(self.rule_vspace, "vspace", [])
                    self.considerError(lastError, None)
                    return (_G_apply_26, self.currentError)
                def _G_or_27():
                    self._trace(' comment', (116, 124), self.input.position)
                    _G_apply_28, lastError = self._apply(self.rule_comment, "comment", [])
                    self.considerError(lastError, None)
                    return (_G_apply_28, self.currentError)
                _G_or_29, lastError = self._or([_G_or_23, _G_or_25, _G_or_27])
                self.considerError(lastError, None)
                return (_G_or_29, self.currentError)
            _G_many_30, lastError = self.many(_G_many_22)
            self.considerError(lastError, 'ws')
            return (_G_many_30, self.currentError)


        def rule_emptyline(self):
            _locals = {'self': self}
            self.locals['emptyline'] = _locals
            def _G_many_31():
                self._trace(' hspace', (139, 146), self.input.position)
                _G_apply_32, lastError = self._apply(self.rule_hspace, "hspace", [])
                self.considerError(lastError, None)
                return (_G_apply_32, self.currentError)
            _G_many_33, lastError = self.many(_G_many_31)
            self.considerError(lastError, 'emptyline')
            self._trace(' vspace', (147, 154), self.input.position)
            _G_apply_34, lastError = self._apply(self.rule_vspace, "vspace", [])
            self.considerError(lastError, 'emptyline')
            return (_G_apply_34, self.currentError)


        def rule_indentation(self):
            _locals = {'self': self}
            self.locals['indentation'] = _locals
            def _G_many_35():
                self._trace(' emptyline', (168, 178), self.input.position)
                _G_apply_36, lastError = self._apply(self.rule_emptyline, "emptyline", [])
                self.considerError(lastError, None)
                return (_G_apply_36, self.currentError)
            _G_many_37, lastError = self.many(_G_many_35)
            self.considerError(lastError, 'indentation')
            def _G_many1_38():
                self._trace(' hspace', (179, 186), self.input.position)
                _G_apply_39, lastError = self._apply(self.rule_hspace, "hspace", [])
                self.considerError(lastError, None)
                return (_G_apply_39, self.currentError)
            _G_many1_40, lastError = self.many(_G_many1_38, _G_many1_38())
            self.considerError(lastError, 'indentation')
            return (_G_many1_40, self.currentError)


        def rule_noindentation(self):
            _locals = {'self': self}
            self.locals['noindentation'] = _locals
            def _G_many_41():
                self._trace(' emptyline', (203, 213), self.input.position)
                _G_apply_42, lastError = self._apply(self.rule_emptyline, "emptyline", [])
                self.considerError(lastError, None)
                return (_G_apply_42, self.currentError)
            _G_many_43, lastError = self.many(_G_many_41)
            self.considerError(lastError, 'noindentation')
            def _G_lookahead_44():
                def _G_not_45():
                    self._trace('hspace', (218, 224), self.input.position)
                    _G_apply_46, lastError = self._apply(self.rule_hspace, "hspace", [])
                    self.considerError(lastError, None)
                    return (_G_apply_46, self.currentError)
                _G_not_47, lastError = self._not(_G_not_45)
                self.considerError(lastError, None)
                return (_G_not_47, self.currentError)
            _G_lookahead_48, lastError = self.lookahead(_G_lookahead_44)
            self.considerError(lastError, 'noindentation')
            return (_G_lookahead_48, self.currentError)


        def rule_number(self):
            _locals = {'self': self}
            self.locals['number'] = _locals
            self._trace(' ws', (234, 237), self.input.position)
            _G_apply_49, lastError = self._apply(self.rule_ws, "ws", [])
            self.considerError(lastError, 'number')
            def _G_or_50():
                self._trace("'-'", (254, 257), self.input.position)
                _G_exactly_51, lastError = self.exactly('-')
                self.considerError(lastError, None)
                self._trace(' barenumber', (257, 268), self.input.position)
                _G_apply_52, lastError = self._apply(self.rule_barenumber, "barenumber", [])
                self.considerError(lastError, None)
                _locals['x'] = _G_apply_52
                _G_python_53, lastError = eval('t.Exactly(-x, span=self.getSpan())', self.globals, _locals), None
                self.considerError(lastError, None)
                return (_G_python_53, self.currentError)
            def _G_or_54():
                self._trace('barenumber', (331, 341), self.input.position)
                _G_apply_55, lastError = self._apply(self.rule_barenumber, "barenumber", [])
                self.considerError(lastError, None)
                _locals['x'] = _G_apply_55
                _G_python_56, lastError = eval('t.Exactly(x, span=self.getSpan())', self.globals, _locals), None
                self.considerError(lastError, None)
                return (_G_python_56, self.currentError)
            _G_or_57, lastError = self._or([_G_or_50, _G_or_54])
            self.considerError(lastError, 'number')
            return (_G_or_57, self.currentError)


        def rule_barenumber(self):
            _locals = {'self': self}
            self.locals['barenumber'] = _locals
            def _G_or_58():
                self._trace(" '0'", (394, 398), self.input.position)
                _G_exactly_59, lastError = self.exactly('0')
                self.considerError(lastError, None)
                def _G_or_60():
                    def _G_or_61():
                        self._trace("'x'", (401, 404), self.input.position)
                        _G_exactly_62, lastError = self.exactly('x')
                        self.considerError(lastError, None)
                        return (_G_exactly_62, self.currentError)
                    def _G_or_63():
                        self._trace("'X'", (405, 408), self.input.position)
                        _G_exactly_64, lastError = self.exactly('X')
                        self.considerError(lastError, None)
                        return (_G_exactly_64, self.currentError)
                    _G_or_65, lastError = self._or([_G_or_61, _G_or_63])
                    self.considerError(lastError, None)
                    def _G_consumedby_66():
                        def _G_many1_67():
                            self._trace('hexdigit', (411, 419), self.input.position)
                            _G_apply_68, lastError = self._apply(self.rule_hexdigit, "hexdigit", [])
                            self.considerError(lastError, None)
                            return (_G_apply_68, self.currentError)
                        _G_many1_69, lastError = self.many(_G_many1_67, _G_many1_67())
                        self.considerError(lastError, None)
                        return (_G_many1_69, self.currentError)
                    _G_consumedby_70, lastError = self.consumedby(_G_consumedby_66)
                    self.considerError(lastError, None)
                    _locals['hs'] = _G_consumedby_70
                    _G_python_71, lastError = eval('int(hs, 16)', self.globals, _locals), None
                    self.considerError(lastError, None)
                    return (_G_python_71, self.currentError)
                def _G_or_72():
                    def _G_consumedby_73():
                        def _G_many1_74():
                            self._trace('octaldigit', (462, 472), self.input.position)
                            _G_apply_75, lastError = self._apply(self.rule_octaldigit, "octaldigit", [])
                            self.considerError(lastError, None)
                            return (_G_apply_75, self.currentError)
                        _G_many1_76, lastError = self.many(_G_many1_74, _G_many1_74())
                        self.considerError(lastError, None)
                        return (_G_many1_76, self.currentError)
                    _G_consumedby_77, lastError = self.consumedby(_G_consumedby_73)
                    self.considerError(lastError, None)
                    _locals['ds'] = _G_consumedby_77
                    _G_python_78, lastError = eval('int(ds, 8)', self.globals, _locals), None
                    self.considerError(lastError, None)
                    return (_G_python_78, self.currentError)
                _G_or_79, lastError = self._or([_G_or_60, _G_or_72])
                self.considerError(lastError, None)
                return (_G_or_79, self.currentError)
            def _G_or_80():
                def _G_consumedby_81():
                    def _G_many1_82():
                        self._trace('digit', (510, 515), self.input.position)
                        _G_apply_83, lastError = self._apply(self.rule_digit, "digit", [])
                        self.considerError(lastError, None)
                        return (_G_apply_83, self.currentError)
                    _G_many1_84, lastError = self.many(_G_many1_82, _G_many1_82())
                    self.considerError(lastError, None)
                    return (_G_many1_84, self.currentError)
                _G_consumedby_85, lastError = self.consumedby(_G_consumedby_81)
                self.considerError(lastError, None)
                _locals['ds'] = _G_consumedby_85
                _G_python_86, lastError = eval('int(ds)', self.globals, _locals), None
                self.considerError(lastError, None)
                return (_G_python_86, self.currentError)
            _G_or_87, lastError = self._or([_G_or_58, _G_or_80])
            self.considerError(lastError, 'barenumber')
            return (_G_or_87, self.currentError)


        def rule_octaldigit(self):
            _locals = {'self': self}
            self.locals['octaldigit'] = _locals
            _G_apply_88, lastError = self._apply(self.rule_anything, "anything", [])
            self.considerError(lastError, 'octaldigit')
            _locals['x'] = _G_apply_88
            def _G_pred_89():
                _G_python_90, lastError = eval("x in '01234567'", self.globals, _locals), None
                self.considerError(lastError, None)
                return (_G_python_90, self.currentError)
            _G_pred_91, lastError = self.pred(_G_pred_89)
            self.considerError(lastError, 'octaldigit')
            _G_python_92, lastError = eval('x', self.globals, _locals), None
            self.considerError(lastError, 'octaldigit')
            return (_G_python_92, self.currentError)


        def rule_hexdigit(self):
            _locals = {'self': self}
            self.locals['hexdigit'] = _locals
            _G_apply_93, lastError = self._apply(self.rule_anything, "anything", [])
            self.considerError(lastError, 'hexdigit')
            _locals['x'] = _G_apply_93
            def _G_pred_94():
                _G_python_95, lastError = eval("x in '0123456789ABCDEFabcdef'", self.globals, _locals), None
                self.considerError(lastError, None)
                return (_G_python_95, self.currentError)
            _G_pred_96, lastError = self.pred(_G_pred_94)
            self.considerError(lastError, 'hexdigit')
            _G_python_97, lastError = eval('x', self.globals, _locals), None
            self.considerError(lastError, 'hexdigit')
            return (_G_python_97, self.currentError)


        def rule_escapedChar(self):
            _locals = {'self': self}
            self.locals['escapedChar'] = _locals
            self._trace(" '\\\\'", (639, 644), self.input.position)
            _G_exactly_98, lastError = self.exactly('\\')
            self.considerError(lastError, 'escapedChar')
            def _G_or_99():
                self._trace("'n'", (646, 649), self.input.position)
                _G_exactly_100, lastError = self.exactly('n')
                self.considerError(lastError, None)
                _G_python_101, lastError = ("\n"), None
                self.considerError(lastError, None)
                return (_G_python_101, self.currentError)
            def _G_or_102():
                self._trace("'r'", (680, 683), self.input.position)
                _G_exactly_103, lastError = self.exactly('r')
                self.considerError(lastError, None)
                _G_python_104, lastError = ("\r"), None
                self.considerError(lastError, None)
                return (_G_python_104, self.currentError)
            def _G_or_105():
                self._trace("'t'", (714, 717), self.input.position)
                _G_exactly_106, lastError = self.exactly('t')
                self.considerError(lastError, None)
                _G_python_107, lastError = ("\t"), None
                self.considerError(lastError, None)
                return (_G_python_107, self.currentError)
            def _G_or_108():
                self._trace("'b'", (748, 751), self.input.position)
                _G_exactly_109, lastError = self.exactly('b')
                self.considerError(lastError, None)
                _G_python_110, lastError = ("\b"), None
                self.considerError(lastError, None)
                return (_G_python_110, self.currentError)
            def _G_or_111():
                self._trace("'f'", (782, 785), self.input.position)
                _G_exactly_112, lastError = self.exactly('f')
                self.considerError(lastError, None)
                _G_python_113, lastError = ("\f"), None
                self.considerError(lastError, None)
                return (_G_python_113, self.currentError)
            def _G_or_114():
                self._trace('\'"\'', (816, 819), self.input.position)
                _G_exactly_115, lastError = self.exactly('"')
                self.considerError(lastError, None)
                _G_python_116, lastError = ('"'), None
                self.considerError(lastError, None)
                return (_G_python_116, self.currentError)
            def _G_or_117():
                self._trace("'\\''", (849, 853), self.input.position)
                _G_exactly_118, lastError = self.exactly("'")
                self.considerError(lastError, None)
                _G_python_119, lastError = ("'"), None
                self.considerError(lastError, None)
                return (_G_python_119, self.currentError)
            def _G_or_120():
                self._trace("'x'", (883, 886), self.input.position)
                _G_exactly_121, lastError = self.exactly('x')
                self.considerError(lastError, None)
                def _G_consumedby_122():
                    self._trace('hexdigit', (888, 896), self.input.position)
                    _G_apply_123, lastError = self._apply(self.rule_hexdigit, "hexdigit", [])
                    self.considerError(lastError, None)
                    self._trace(' hexdigit', (896, 905), self.input.position)
                    _G_apply_124, lastError = self._apply(self.rule_hexdigit, "hexdigit", [])
                    self.considerError(lastError, None)
                    return (_G_apply_124, self.currentError)
                _G_consumedby_125, lastError = self.consumedby(_G_consumedby_122)
                self.considerError(lastError, None)
                _locals['d'] = _G_consumedby_125
                _G_python_126, lastError = eval('chr(int(d, 16))', self.globals, _locals), None
                self.considerError(lastError, None)
                return (_G_python_126, self.currentError)
            def _G_or_127():
                self._trace("'\\\\'", (950, 954), self.input.position)
                _G_exactly_128, lastError = self.exactly('\\')
                self.considerError(lastError, None)
                _G_python_129, lastError = ("\\"), None
                self.considerError(lastError, None)
                return (_G_python_129, self.currentError)
            _G_or_130, lastError = self._or([_G_or_99, _G_or_102, _G_or_105, _G_or_108, _G_or_111, _G_or_114, _G_or_117, _G_or_120, _G_or_127])
            self.considerError(lastError, 'escapedChar')
            return (_G_or_130, self.currentError)


        def rule_character(self):
            _locals = {'self': self}
            self.locals['character'] = _locals
            self._trace(' ws', (976, 979), self.input.position)
            _G_apply_131, lastError = self._apply(self.rule_ws, "ws", [])
            self.considerError(lastError, 'character')
            self._trace(" '\\''", (979, 984), self.input.position)
            _G_exactly_132, lastError = self.exactly("'")
            self.considerError(lastError, 'character')
            def _G_many1_133():
                def _G_not_134():
                    self._trace("'\\''", (987, 991), self.input.position)
                    _G_exactly_135, lastError = self.exactly("'")
                    self.considerError(lastError, None)
                    return (_G_exactly_135, self.currentError)
                _G_not_136, lastError = self._not(_G_not_134)
                self.considerError(lastError, None)
                def _G_or_137():
                    self._trace('escapedChar', (993, 1004), self.input.position)
                    _G_apply_138, lastError = self._apply(self.rule_escapedChar, "escapedChar", [])
                    self.considerError(lastError, None)
                    return (_G_apply_138, self.currentError)
                def _G_or_139():
                    self._trace(' anything', (1006, 1015), self.input.position)
                    _G_apply_140, lastError = self._apply(self.rule_anything, "anything", [])
                    self.considerError(lastError, None)
                    return (_G_apply_140, self.currentError)
                _G_or_141, lastError = self._or([_G_or_137, _G_or_139])
                self.considerError(lastError, None)
                return (_G_or_141, self.currentError)
            _G_many1_142, lastError = self.many(_G_many1_133, _G_many1_133())
            self.considerError(lastError, 'character')
            _locals['c'] = _G_many1_142
            self._trace('\n            ws', (1020, 1035), self.input.position)
            _G_apply_143, lastError = self._apply(self.rule_ws, "ws", [])
            self.considerError(lastError, 'character')
            self._trace(" '\\''", (1035, 1040), self.input.position)
            _G_exactly_144, lastError = self.exactly("'")
            self.considerError(lastError, 'character')
            _G_python_145, lastError = eval("t.Exactly(''.join(c), span=self.getSpan())", self.globals, _locals), None
            self.considerError(lastError, 'character')
            return (_G_python_145, self.currentError)


        def rule_string(self):
            _locals = {'self': self}
            self.locals['string'] = _locals
            self._trace(' ws', (1096, 1099), self.input.position)
            _G_apply_146, lastError = self._apply(self.rule_ws, "ws", [])
            self.considerError(lastError, 'string')
            self._trace(' \'"\'', (1099, 1103), self.input.position)
            _G_exactly_147, lastError = self.exactly('"')
            self.considerError(lastError, 'string')
            def _G_many_148():
                def _G_or_149():
                    self._trace('escapedChar', (1105, 1116), self.input.position)
                    _G_apply_150, lastError = self._apply(self.rule_escapedChar, "escapedChar", [])
                    self.considerError(lastError, None)
                    return (_G_apply_150, self.currentError)
                def _G_or_151():
                    def _G_not_152():
                        self._trace('\'"\'', (1121, 1124), self.input.position)
                        _G_exactly_153, lastError = self.exactly('"')
                        self.considerError(lastError, None)
                        return (_G_exactly_153, self.currentError)
                    _G_not_154, lastError = self._not(_G_not_152)
                    self.considerError(lastError, None)
                    self._trace(' anything', (1125, 1134), self.input.position)
                    _G_apply_155, lastError = self._apply(self.rule_anything, "anything", [])
                    self.considerError(lastError, None)
                    return (_G_apply_155, self.currentError)
                _G_or_156, lastError = self._or([_G_or_149, _G_or_151])
                self.considerError(lastError, None)
                return (_G_or_156, self.currentError)
            _G_many_157, lastError = self.many(_G_many_148)
            self.considerError(lastError, 'string')
            _locals['c'] = _G_many_157
            self._trace('\n         ws', (1138, 1150), self.input.position)
            _G_apply_158, lastError = self._apply(self.rule_ws, "ws", [])
            self.considerError(lastError, 'string')
            self._trace(' \'"\'', (1150, 1154), self.input.position)
            _G_exactly_159, lastError = self.exactly('"')
            self.considerError(lastError, 'string')
            _G_python_160, lastError = eval("t.Token(''.join(c), span=self.getSpan())", self.globals, _locals), None
            self.considerError(lastError, 'string')
            return (_G_python_160, self.currentError)


        def rule_name(self):
            _locals = {'self': self}
            self.locals['name'] = _locals
            def _G_consumedby_161():
                self._trace('letter', (1208, 1214), self.input.position)
                _G_apply_162, lastError = self._apply(self.rule_letter, "letter", [])
                self.considerError(lastError, None)
                def _G_many_163():
                    def _G_or_164():
                        self._trace("'_'", (1216, 1219), self.input.position)
                        _G_exactly_165, lastError = self.exactly('_')
                        self.considerError(lastError, None)
                        return (_G_exactly_165, self.currentError)
                    def _G_or_166():
                        self._trace('letterOrDigit', (1221, 1234), self.input.position)
                        _G_apply_167, lastError = self._apply(self.rule_letterOrDigit, "letterOrDigit", [])
                        self.considerError(lastError, None)
                        return (_G_apply_167, self.currentError)
                    _G_or_168, lastError = self._or([_G_or_164, _G_or_166])
                    self.considerError(lastError, None)
                    return (_G_or_168, self.currentError)
                _G_many_169, lastError = self.many(_G_many_163)
                self.considerError(lastError, None)
                return (_G_many_169, self.currentError)
            _G_consumedby_170, lastError = self.consumedby(_G_consumedby_161)
            self.considerError(lastError, 'name')
            return (_G_consumedby_170, self.currentError)


        def rule_args(self):
            _locals = {'self': self}
            self.locals['args'] = _locals
            def _G_or_171():
                self._trace("'('", (1247, 1250), self.input.position)
                _G_exactly_172, lastError = self.exactly('(')
                self.considerError(lastError, None)
                _G_python_173, lastError = eval("self.applicationArgs(finalChar=')')", self.globals, _locals), None
                self.considerError(lastError, None)
                _locals['args'] = _G_python_173
                self._trace(" ')'", (1294, 1298), self.input.position)
                _G_exactly_174, lastError = self.exactly(')')
                self.considerError(lastError, None)
                _G_python_175, lastError = eval('args', self.globals, _locals), None
                self.considerError(lastError, None)
                return (_G_python_175, self.currentError)
            def _G_or_176():
                _G_python_177, lastError = ([]), None
                self.considerError(lastError, None)
                return (_G_python_177, self.currentError)
            _G_or_178, lastError = self._or([_G_or_171, _G_or_176])
            self.considerError(lastError, 'args')
            return (_G_or_178, self.currentError)


        def rule_application(self):
            _locals = {'self': self}
            self.locals['application'] = _locals
            def _G_optional_179():
                self._trace(' indentation', (1352, 1364), self.input.position)
                _G_apply_180, lastError = self._apply(self.rule_indentation, "indentation", [])
                self.considerError(lastError, None)
                return (_G_apply_180, self.currentError)
            def _G_optional_181():
                return (None, self.input.nullError())
            _G_or_182, lastError = self._or([_G_optional_179, _G_optional_181])
            self.considerError(lastError, 'application')
            self._trace(' name', (1365, 1370), self.input.position)
            _G_apply_183, lastError = self._apply(self.rule_name, "name", [])
            self.considerError(lastError, 'application')
            _locals['name'] = _G_apply_183
            self._trace(' args', (1375, 1380), self.input.position)
            _G_apply_184, lastError = self._apply(self.rule_args, "args", [])
            self.considerError(lastError, 'application')
            _locals['args'] = _G_apply_184
            _G_python_185, lastError = eval('t.Apply(name, self.rulename, args, span=self.getSpan())', self.globals, _locals), None
            self.considerError(lastError, 'application')
            return (_G_python_185, self.currentError)


        def rule_foreignApply(self):
            _locals = {'self': self}
            self.locals['foreignApply'] = _locals
            def _G_optional_186():
                self._trace(' indentation', (1476, 1488), self.input.position)
                _G_apply_187, lastError = self._apply(self.rule_indentation, "indentation", [])
                self.considerError(lastError, None)
                return (_G_apply_187, self.currentError)
            def _G_optional_188():
                return (None, self.input.nullError())
            _G_or_189, lastError = self._or([_G_optional_186, _G_optional_188])
            self.considerError(lastError, 'foreignApply')
            self._trace(' name', (1489, 1494), self.input.position)
            _G_apply_190, lastError = self._apply(self.rule_name, "name", [])
            self.considerError(lastError, 'foreignApply')
            _locals['grammar_name'] = _G_apply_190
            self._trace(" '.'", (1507, 1511), self.input.position)
            _G_exactly_191, lastError = self.exactly('.')
            self.considerError(lastError, 'foreignApply')
            self._trace(' name', (1511, 1516), self.input.position)
            _G_apply_192, lastError = self._apply(self.rule_name, "name", [])
            self.considerError(lastError, 'foreignApply')
            _locals['rule_name'] = _G_apply_192
            self._trace(' args', (1526, 1531), self.input.position)
            _G_apply_193, lastError = self._apply(self.rule_args, "args", [])
            self.considerError(lastError, 'foreignApply')
            _locals['args'] = _G_apply_193
            _G_python_194, lastError = eval('t.ForeignApply(grammar_name, rule_name, self.rulename, args, span=self.getSpan())', self.globals, _locals), None
            self.considerError(lastError, 'foreignApply')
            return (_G_python_194, self.currentError)


        def rule_traceable(self):
            _locals = {'self': self}
            self.locals['traceable'] = _locals
            _G_python_195, lastError = eval('self.startSpan()', self.globals, _locals), None
            self.considerError(lastError, 'traceable')
            def _G_or_196():
                self._trace('  foreignApply', (1682, 1696), self.input.position)
                _G_apply_197, lastError = self._apply(self.rule_foreignApply, "foreignApply", [])
                self.considerError(lastError, None)
                return (_G_apply_197, self.currentError)
            def _G_or_198():
                self._trace(' application', (1708, 1720), self.input.position)
                _G_apply_199, lastError = self._apply(self.rule_application, "application", [])
                self.considerError(lastError, None)
                return (_G_apply_199, self.currentError)
            def _G_or_200():
                self._trace(' ruleValue', (1732, 1742), self.input.position)
                _G_apply_201, lastError = self._apply(self.rule_ruleValue, "ruleValue", [])
                self.considerError(lastError, None)
                return (_G_apply_201, self.currentError)
            def _G_or_202():
                self._trace(' semanticPredicate', (1754, 1772), self.input.position)
                _G_apply_203, lastError = self._apply(self.rule_semanticPredicate, "semanticPredicate", [])
                self.considerError(lastError, None)
                return (_G_apply_203, self.currentError)
            def _G_or_204():
                self._trace(' semanticAction', (1784, 1799), self.input.position)
                _G_apply_205, lastError = self._apply(self.rule_semanticAction, "semanticAction", [])
                self.considerError(lastError, None)
                return (_G_apply_205, self.currentError)
            def _G_or_206():
                self._trace(' number', (1811, 1818), self.input.position)
                _G_apply_207, lastError = self._apply(self.rule_number, "number", [])
                self.considerError(lastError, None)
                _locals['n'] = _G_apply_207
                _G_python_208, lastError = eval('self.isTree()', self.globals, _locals), None
                self.considerError(lastError, None)
                _G_python_209, lastError = eval('n', self.globals, _locals), None
                self.considerError(lastError, None)
                return (_G_python_209, self.currentError)
            def _G_or_210():
                self._trace(' character', (1854, 1864), self.input.position)
                _G_apply_211, lastError = self._apply(self.rule_character, "character", [])
                self.considerError(lastError, None)
                return (_G_apply_211, self.currentError)
            def _G_or_212():
                self._trace(' string', (1876, 1883), self.input.position)
                _G_apply_213, lastError = self._apply(self.rule_string, "string", [])
                self.considerError(lastError, None)
                return (_G_apply_213, self.currentError)
            _G_or_214, lastError = self._or([_G_or_196, _G_or_198, _G_or_200, _G_or_202, _G_or_204, _G_or_206, _G_or_210, _G_or_212])
            self.considerError(lastError, 'traceable')
            return (_G_or_214, self.currentError)


        def rule_expr1(self):
            _locals = {'self': self}
            self.locals['expr1'] = _locals
            def _G_or_215():
                self._trace(' traceable', (1893, 1903), self.input.position)
                _G_apply_216, lastError = self._apply(self.rule_traceable, "traceable", [])
                self.considerError(lastError, None)
                return (_G_apply_216, self.currentError)
            def _G_or_217():
                self._trace(' ws', (1911, 1914), self.input.position)
                _G_apply_218, lastError = self._apply(self.rule_ws, "ws", [])
                self.considerError(lastError, None)
                self._trace(" '('", (1914, 1918), self.input.position)
                _G_exactly_219, lastError = self.exactly('(')
                self.considerError(lastError, None)
                self._trace(' expr', (1918, 1923), self.input.position)
                _G_apply_220, lastError = self._apply(self.rule_expr, "expr", [])
                self.considerError(lastError, None)
                _locals['e'] = _G_apply_220
                self._trace(' ws', (1925, 1928), self.input.position)
                _G_apply_221, lastError = self._apply(self.rule_ws, "ws", [])
                self.considerError(lastError, None)
                self._trace(" ')'", (1928, 1932), self.input.position)
                _G_exactly_222, lastError = self.exactly(')')
                self.considerError(lastError, None)
                _G_python_223, lastError = eval('e', self.globals, _locals), None
                self.considerError(lastError, None)
                return (_G_python_223, self.currentError)
            def _G_or_224():
                self._trace(' ws', (1945, 1948), self.input.position)
                _G_apply_225, lastError = self._apply(self.rule_ws, "ws", [])
                self.considerError(lastError, None)
                self._trace(" '<'", (1948, 1952), self.input.position)
                _G_exactly_226, lastError = self.exactly('<')
                self.considerError(lastError, None)
                self._trace(' expr', (1952, 1957), self.input.position)
                _G_apply_227, lastError = self._apply(self.rule_expr, "expr", [])
                self.considerError(lastError, None)
                _locals['e'] = _G_apply_227
                self._trace(' ws', (1959, 1962), self.input.position)
                _G_apply_228, lastError = self._apply(self.rule_ws, "ws", [])
                self.considerError(lastError, None)
                self._trace(" '>'", (1962, 1966), self.input.position)
                _G_exactly_229, lastError = self.exactly('>')
                self.considerError(lastError, None)
                _G_python_230, lastError = eval('t.ConsumedBy(e)', self.globals, _locals), None
                self.considerError(lastError, None)
                return (_G_python_230, self.currentError)
            def _G_or_231():
                self._trace(' ws', (2002, 2005), self.input.position)
                _G_apply_232, lastError = self._apply(self.rule_ws, "ws", [])
                self.considerError(lastError, None)
                self._trace(" '['", (2005, 2009), self.input.position)
                _G_exactly_233, lastError = self.exactly('[')
                self.considerError(lastError, None)
                def _G_optional_234():
                    self._trace(' expr', (2009, 2014), self.input.position)
                    _G_apply_235, lastError = self._apply(self.rule_expr, "expr", [])
                    self.considerError(lastError, None)
                    return (_G_apply_235, self.currentError)
                def _G_optional_236():
                    return (None, self.input.nullError())
                _G_or_237, lastError = self._or([_G_optional_234, _G_optional_236])
                self.considerError(lastError, None)
                _locals['e'] = _G_or_237
                self._trace(' ws', (2017, 2020), self.input.position)
                _G_apply_238, lastError = self._apply(self.rule_ws, "ws", [])
                self.considerError(lastError, None)
                self._trace(" ']'", (2020, 2024), self.input.position)
                _G_exactly_239, lastError = self.exactly(']')
                self.considerError(lastError, None)
                _G_python_240, lastError = eval('self.isTree()', self.globals, _locals), None
                self.considerError(lastError, None)
                _G_python_241, lastError = eval('t.List(e) if e else t.List()', self.globals, _locals), None
                self.considerError(lastError, None)
                return (_G_python_241, self.currentError)
            _G_or_242, lastError = self._or([_G_or_215, _G_or_217, _G_or_224, _G_or_231])
            self.considerError(lastError, 'expr1')
            return (_G_or_242, self.currentError)


        def rule_expr2(self):
            _locals = {'self': self}
            self.locals['expr2'] = _locals
            def _G_or_243():
                self._trace('ws', (2093, 2095), self.input.position)
                _G_apply_244, lastError = self._apply(self.rule_ws, "ws", [])
                self.considerError(lastError, None)
                self._trace(" '~'", (2095, 2099), self.input.position)
                _G_exactly_245, lastError = self.exactly('~')
                self.considerError(lastError, None)
                def _G_or_246():
                    self._trace("'~'", (2101, 2104), self.input.position)
                    _G_exactly_247, lastError = self.exactly('~')
                    self.considerError(lastError, None)
                    self._trace(' expr2', (2104, 2110), self.input.position)
                    _G_apply_248, lastError = self._apply(self.rule_expr2, "expr2", [])
                    self.considerError(lastError, None)
                    _locals['e'] = _G_apply_248
                    _G_python_249, lastError = eval('t.Lookahead(e)', self.globals, _locals), None
                    self.considerError(lastError, None)
                    return (_G_python_249, self.currentError)
                def _G_or_250():
                    self._trace('    expr2', (2148, 2157), self.input.position)
                    _G_apply_251, lastError = self._apply(self.rule_expr2, "expr2", [])
                    self.considerError(lastError, None)
                    _locals['e'] = _G_apply_251
                    _G_python_252, lastError = eval('t.Not(e)', self.globals, _locals), None
                    self.considerError(lastError, None)
                    return (_G_python_252, self.currentError)
                _G_or_253, lastError = self._or([_G_or_246, _G_or_250])
                self.considerError(lastError, None)
                return (_G_or_253, self.currentError)
            def _G_or_254():
                self._trace('expr1', (2199, 2204), self.input.position)
                _G_apply_255, lastError = self._apply(self.rule_expr1, "expr1", [])
                self.considerError(lastError, None)
                return (_G_apply_255, self.currentError)
            _G_or_256, lastError = self._or([_G_or_243, _G_or_254])
            self.considerError(lastError, 'expr2')
            return (_G_or_256, self.currentError)


        def rule_repeatTimes(self):
            _locals = {'self': self}
            self.locals['repeatTimes'] = _locals
            def _G_or_257():
                self._trace('barenumber', (2222, 2232), self.input.position)
                _G_apply_258, lastError = self._apply(self.rule_barenumber, "barenumber", [])
                self.considerError(lastError, None)
                _locals['x'] = _G_apply_258
                _G_python_259, lastError = eval('int(x)', self.globals, _locals), None
                self.considerError(lastError, None)
                return (_G_python_259, self.currentError)
            def _G_or_260():
                self._trace(' name', (2247, 2252), self.input.position)
                _G_apply_261, lastError = self._apply(self.rule_name, "name", [])
                self.considerError(lastError, None)
                return (_G_apply_261, self.currentError)
            _G_or_262, lastError = self._or([_G_or_257, _G_or_260])
            self.considerError(lastError, 'repeatTimes')
            return (_G_or_262, self.currentError)


        def rule_expr3(self):
            _locals = {'self': self}
            self.locals['expr3'] = _locals
            def _G_or_263():
                self._trace('expr2', (2263, 2268), self.input.position)
                _G_apply_264, lastError = self._apply(self.rule_expr2, "expr2", [])
                self.considerError(lastError, None)
                _locals['e'] = _G_apply_264
                def _G_or_265():
                    self._trace("'*'", (2294, 2297), self.input.position)
                    _G_exactly_266, lastError = self.exactly('*')
                    self.considerError(lastError, None)
                    _G_python_267, lastError = eval('t.Many(e)', self.globals, _locals), None
                    self.considerError(lastError, None)
                    return (_G_python_267, self.currentError)
                def _G_or_268():
                    self._trace("'+'", (2334, 2337), self.input.position)
                    _G_exactly_269, lastError = self.exactly('+')
                    self.considerError(lastError, None)
                    _G_python_270, lastError = eval('t.Many1(e)', self.globals, _locals), None
                    self.considerError(lastError, None)
                    return (_G_python_270, self.currentError)
                def _G_or_271():
                    self._trace("'?'", (2375, 2378), self.input.position)
                    _G_exactly_272, lastError = self.exactly('?')
                    self.considerError(lastError, None)
                    _G_python_273, lastError = eval('t.Optional(e)', self.globals, _locals), None
                    self.considerError(lastError, None)
                    return (_G_python_273, self.currentError)
                def _G_or_274():
                    self._trace('customLabel', (2419, 2430), self.input.position)
                    _G_apply_275, lastError = self._apply(self.rule_customLabel, "customLabel", [])
                    self.considerError(lastError, None)
                    _locals['l'] = _G_apply_275
                    _G_python_276, lastError = eval('t.Label(e, l)', self.globals, _locals), None
                    self.considerError(lastError, None)
                    return (_G_python_276, self.currentError)
                def _G_or_277():
                    self._trace("'{'", (2473, 2476), self.input.position)
                    _G_exactly_278, lastError = self.exactly('{')
                    self.considerError(lastError, None)
                    self._trace(' ws', (2476, 2479), self.input.position)
                    _G_apply_279, lastError = self._apply(self.rule_ws, "ws", [])
                    self.considerError(lastError, None)
                    self._trace(' repeatTimes', (2479, 2491), self.input.position)
                    _G_apply_280, lastError = self._apply(self.rule_repeatTimes, "repeatTimes", [])
                    self.considerError(lastError, None)
                    _locals['start'] = _G_apply_280
                    self._trace(' ws', (2497, 2500), self.input.position)
                    _G_apply_281, lastError = self._apply(self.rule_ws, "ws", [])
                    self.considerError(lastError, None)
                    def _G_or_282():
                        self._trace("','", (2526, 2529), self.input.position)
                        _G_exactly_283, lastError = self.exactly(',')
                        self.considerError(lastError, None)
                        self._trace(' ws', (2529, 2532), self.input.position)
                        _G_apply_284, lastError = self._apply(self.rule_ws, "ws", [])
                        self.considerError(lastError, None)
                        self._trace(' repeatTimes', (2532, 2544), self.input.position)
                        _G_apply_285, lastError = self._apply(self.rule_repeatTimes, "repeatTimes", [])
                        self.considerError(lastError, None)
                        _locals['end'] = _G_apply_285
                        self._trace(' ws', (2548, 2551), self.input.position)
                        _G_apply_286, lastError = self._apply(self.rule_ws, "ws", [])
                        self.considerError(lastError, None)
                        self._trace(" '}'", (2551, 2555), self.input.position)
                        _G_exactly_287, lastError = self.exactly('}')
                        self.considerError(lastError, None)
                        _G_python_288, lastError = eval('t.Repeat(start, end, e)', self.globals, _locals), None
                        self.considerError(lastError, None)
                        return (_G_python_288, self.currentError)
                    def _G_or_289():
                        self._trace(' ws', (2637, 2640), self.input.position)
                        _G_apply_290, lastError = self._apply(self.rule_ws, "ws", [])
                        self.considerError(lastError, None)
                        self._trace(" '}'", (2640, 2644), self.input.position)
                        _G_exactly_291, lastError = self.exactly('}')
                        self.considerError(lastError, None)
                        _G_python_292, lastError = eval('t.Repeat(start, start, e)', self.globals, _locals), None
                        self.considerError(lastError, None)
                        return (_G_python_292, self.currentError)
                    _G_or_293, lastError = self._or([_G_or_282, _G_or_289])
                    self.considerError(lastError, None)
                    return (_G_or_293, self.currentError)
                def _G_or_294():
                    _G_python_295, lastError = eval('e', self.globals, _locals), None
                    self.considerError(lastError, None)
                    return (_G_python_295, self.currentError)
                _G_or_296, lastError = self._or([_G_or_265, _G_or_268, _G_or_271, _G_or_274, _G_or_277, _G_or_294])
                self.considerError(lastError, None)
                _locals['r'] = _G_or_296
                def _G_or_297():
                    self._trace("':'", (2748, 2751), self.input.position)
                    _G_exactly_298, lastError = self.exactly(':')
                    self.considerError(lastError, None)
                    self._trace(' name', (2751, 2756), self.input.position)
                    _G_apply_299, lastError = self._apply(self.rule_name, "name", [])
                    self.considerError(lastError, None)
                    _locals['n'] = _G_apply_299
                    _G_python_300, lastError = eval('t.Bind(n, r)', self.globals, _locals), None
                    self.considerError(lastError, None)
                    return (_G_python_300, self.currentError)
                def _G_or_301():
                    self._trace(" ':('", (2787, 2792), self.input.position)
                    _G_exactly_302, lastError = self.exactly(':(')
                    self.considerError(lastError, None)
                    self._trace(' name', (2792, 2797), self.input.position)
                    _G_apply_303, lastError = self._apply(self.rule_name, "name", [])
                    self.considerError(lastError, None)
                    _locals['n'] = _G_apply_303
                    def _G_many_304():
                        self._trace("','", (2801, 2804), self.input.position)
                        _G_exactly_305, lastError = self.exactly(',')
                        self.considerError(lastError, None)
                        self._trace(' ws', (2804, 2807), self.input.position)
                        _G_apply_306, lastError = self._apply(self.rule_ws, "ws", [])
                        self.considerError(lastError, None)
                        self._trace(' name', (2807, 2812), self.input.position)
                        _G_apply_307, lastError = self._apply(self.rule_name, "name", [])
                        self.considerError(lastError, None)
                        return (_G_apply_307, self.currentError)
                    _G_many_308, lastError = self.many(_G_many_304)
                    self.considerError(lastError, None)
                    _locals['others'] = _G_many_308
                    self._trace(' ws', (2821, 2824), self.input.position)
                    _G_apply_309, lastError = self._apply(self.rule_ws, "ws", [])
                    self.considerError(lastError, None)
                    self._trace(" ')'", (2824, 2828), self.input.position)
                    _G_exactly_310, lastError = self.exactly(')')
                    self.considerError(lastError, None)
                    _G_python_311, lastError = eval('[n] + others if others else n', self.globals, _locals), None
                    self.considerError(lastError, None)
                    _locals['n'] = _G_python_311
                    _G_python_312, lastError = eval('t.Bind(n, r)', self.globals, _locals), None
                    self.considerError(lastError, None)
                    return (_G_python_312, self.currentError)
                def _G_or_313():
                    _G_python_314, lastError = eval('r', self.globals, _locals), None
                    self.considerError(lastError, None)
                    return (_G_python_314, self.currentError)
                _G_or_315, lastError = self._or([_G_or_297, _G_or_301, _G_or_313])
                self.considerError(lastError, None)
                return (_G_or_315, self.currentError)
            def _G_or_316():
                self._trace('ws', (2928, 2930), self.input.position)
                _G_apply_317, lastError = self._apply(self.rule_ws, "ws", [])
                self.considerError(lastError, None)
                self._trace(" ':'", (2930, 2934), self.input.position)
                _G_exactly_318, lastError = self.exactly(':')
                self.considerError(lastError, None)
                self._trace(' name', (2934, 2939), self.input.position)
                _G_apply_319, lastError = self._apply(self.rule_name, "name", [])
                self.considerError(lastError, None)
                _locals['n'] = _G_apply_319
                _G_python_320, lastError = eval('t.Bind(n, t.Apply("anything", self.rulename, []))', self.globals, _locals), None
                self.considerError(lastError, None)
                return (_G_python_320, self.currentError)
            _G_or_321, lastError = self._or([_G_or_263, _G_or_316])
            self.considerError(lastError, 'expr3')
            return (_G_or_321, self.currentError)


        def rule_expr4(self):
            _locals = {'self': self}
            self.locals['expr4'] = _locals
            def _G_many1_322():
                self._trace(' expr3', (3013, 3019), self.input.position)
                _G_apply_323, lastError = self._apply(self.rule_expr3, "expr3", [])
                self.considerError(lastError, None)
                return (_G_apply_323, self.currentError)
            _G_many1_324, lastError = self.many(_G_many1_322, _G_many1_322())
            self.considerError(lastError, 'expr4')
            _locals['es'] = _G_many1_324
            _G_python_325, lastError = eval('es[0] if len(es) == 1 else t.And(es)', self.globals, _locals), None
            self.considerError(lastError, 'expr4')
            return (_G_python_325, self.currentError)


        def rule_expr(self):
            _locals = {'self': self}
            self.locals['expr'] = _locals
            self._trace(' expr4', (3071, 3077), self.input.position)
            _G_apply_326, lastError = self._apply(self.rule_expr4, "expr4", [])
            self.considerError(lastError, 'expr')
            _locals['e'] = _G_apply_326
            def _G_many_327():
                self._trace('ws', (3081, 3083), self.input.position)
                _G_apply_328, lastError = self._apply(self.rule_ws, "ws", [])
                self.considerError(lastError, None)
                self._trace(" '|'", (3083, 3087), self.input.position)
                _G_exactly_329, lastError = self.exactly('|')
                self.considerError(lastError, None)
                self._trace(' expr4', (3087, 3093), self.input.position)
                _G_apply_330, lastError = self._apply(self.rule_expr4, "expr4", [])
                self.considerError(lastError, None)
                return (_G_apply_330, self.currentError)
            _G_many_331, lastError = self.many(_G_many_327)
            self.considerError(lastError, 'expr')
            _locals['es'] = _G_many_331
            _G_python_332, lastError = eval('t.Or([e] + es) if es else e', self.globals, _locals), None
            self.considerError(lastError, 'expr')
            return (_G_python_332, self.currentError)


        def rule_ruleValue(self):
            _locals = {'self': self}
            self.locals['ruleValue'] = _locals
            self._trace(' ws', (3152, 3155), self.input.position)
            _G_apply_333, lastError = self._apply(self.rule_ws, "ws", [])
            self.considerError(lastError, 'ruleValue')
            self._trace(" '->'", (3155, 3160), self.input.position)
            _G_exactly_334, lastError = self.exactly('->')
            self.considerError(lastError, 'ruleValue')
            _G_python_335, lastError = eval('self.ruleValueExpr(True)', self.globals, _locals), None
            self.considerError(lastError, 'ruleValue')
            return (_G_python_335, self.currentError)


        def rule_customLabel(self):
            _locals = {'self': self}
            self.locals['customLabel'] = _locals
            def _G_label_336():
                self._trace('ws', (3205, 3207), self.input.position)
                _G_apply_337, lastError = self._apply(self.rule_ws, "ws", [])
                self.considerError(lastError, None)
                self._trace(" '^'", (3207, 3211), self.input.position)
                _G_exactly_338, lastError = self.exactly('^')
                self.considerError(lastError, None)
                self._trace(' ws', (3211, 3214), self.input.position)
                _G_apply_339, lastError = self._apply(self.rule_ws, "ws", [])
                self.considerError(lastError, None)
                self._trace(" '('", (3214, 3218), self.input.position)
                _G_exactly_340, lastError = self.exactly('(')
                self.considerError(lastError, None)
                def _G_consumedby_341():
                    def _G_many1_342():
                        def _G_not_343():
                            self._trace("')'", (3222, 3225), self.input.position)
                            _G_exactly_344, lastError = self.exactly(')')
                            self.considerError(lastError, None)
                            return (_G_exactly_344, self.currentError)
                        _G_not_345, lastError = self._not(_G_not_343)
                        self.considerError(lastError, None)
                        self._trace(' anything', (3225, 3234), self.input.position)
                        _G_apply_346, lastError = self._apply(self.rule_anything, "anything", [])
                        self.considerError(lastError, None)
                        return (_G_apply_346, self.currentError)
                    _G_many1_347, lastError = self.many(_G_many1_342, _G_many1_342())
                    self.considerError(lastError, None)
                    return (_G_many1_347, self.currentError)
                _G_consumedby_348, lastError = self.consumedby(_G_consumedby_341)
                self.considerError(lastError, None)
                _locals['e'] = _G_consumedby_348
                self._trace(" ')'", (3239, 3243), self.input.position)
                _G_exactly_349, lastError = self.exactly(')')
                self.considerError(lastError, None)
                _G_python_350, lastError = eval('e', self.globals, _locals), None
                self.considerError(lastError, None)
                return (_G_python_350, self.currentError)
            _G_label_351, lastError = self.label(_G_label_336, "customLabelException")
            self.considerError(lastError, 'customLabel')
            return (_G_label_351, self.currentError)


        def rule_semanticPredicate(self):
            _locals = {'self': self}
            self.locals['semanticPredicate'] = _locals
            self._trace(' ws', (3295, 3298), self.input.position)
            _G_apply_352, lastError = self._apply(self.rule_ws, "ws", [])
            self.considerError(lastError, 'semanticPredicate')
            self._trace(" '?('", (3298, 3303), self.input.position)
            _G_exactly_353, lastError = self.exactly('?(')
            self.considerError(lastError, 'semanticPredicate')
            _G_python_354, lastError = eval('self.semanticPredicateExpr()', self.globals, _locals), None
            self.considerError(lastError, 'semanticPredicate')
            return (_G_python_354, self.currentError)


        def rule_semanticAction(self):
            _locals = {'self': self}
            self.locals['semanticAction'] = _locals
            self._trace(' ws', (3353, 3356), self.input.position)
            _G_apply_355, lastError = self._apply(self.rule_ws, "ws", [])
            self.considerError(lastError, 'semanticAction')
            self._trace(" '!('", (3356, 3361), self.input.position)
            _G_exactly_356, lastError = self.exactly('!(')
            self.considerError(lastError, 'semanticAction')
            _G_python_357, lastError = eval('self.semanticActionExpr()', self.globals, _locals), None
            self.considerError(lastError, 'semanticAction')
            return (_G_python_357, self.currentError)


        def rule_ruleEnd(self):
            _locals = {'self': self}
            self.locals['ruleEnd'] = _locals
            def _G_label_358():
                def _G_or_359():
                    def _G_many_360():
                        self._trace('hspace', (3404, 3410), self.input.position)
                        _G_apply_361, lastError = self._apply(self.rule_hspace, "hspace", [])
                        self.considerError(lastError, None)
                        return (_G_apply_361, self.currentError)
                    _G_many_362, lastError = self.many(_G_many_360)
                    self.considerError(lastError, None)
                    def _G_many1_363():
                        self._trace(' vspace', (3411, 3418), self.input.position)
                        _G_apply_364, lastError = self._apply(self.rule_vspace, "vspace", [])
                        self.considerError(lastError, None)
                        return (_G_apply_364, self.currentError)
                    _G_many1_365, lastError = self.many(_G_many1_363, _G_many1_363())
                    self.considerError(lastError, None)
                    return (_G_many1_365, self.currentError)
                def _G_or_366():
                    self._trace(' end', (3422, 3426), self.input.position)
                    _G_apply_367, lastError = self._apply(self.rule_end, "end", [])
                    self.considerError(lastError, None)
                    return (_G_apply_367, self.currentError)
                _G_or_368, lastError = self._or([_G_or_359, _G_or_366])
                self.considerError(lastError, None)
                return (_G_or_368, self.currentError)
            _G_label_369, lastError = self.label(_G_label_358, "rule end")
            self.considerError(lastError, 'ruleEnd')
            return (_G_label_369, self.currentError)


        def rule_rulePart(self):
            _locals = {'self': self}
            self.locals['rulePart'] = _locals
            _G_apply_370, lastError = self._apply(self.rule_anything, "anything", [])
            self.considerError(lastError, 'rulePart')
            _locals['requiredName'] = _G_apply_370
            self._trace(' noindentation', (3466, 3480), self.input.position)
            _G_apply_371, lastError = self._apply(self.rule_noindentation, "noindentation", [])
            self.considerError(lastError, 'rulePart')
            self._trace(' name', (3480, 3485), self.input.position)
            _G_apply_372, lastError = self._apply(self.rule_name, "name", [])
            self.considerError(lastError, 'rulePart')
            _locals['n'] = _G_apply_372
            def _G_pred_373():
                _G_python_374, lastError = eval('n == requiredName', self.globals, _locals), None
                self.considerError(lastError, None)
                return (_G_python_374, self.currentError)
            _G_pred_375, lastError = self.pred(_G_pred_373)
            self.considerError(lastError, 'rulePart')
            _G_python_376, lastError = eval('setattr(self, "rulename", n)', self.globals, _locals), None
            self.considerError(lastError, 'rulePart')
            def _G_optional_377():
                self._trace('\n                            expr4', (3568, 3602), self.input.position)
                _G_apply_378, lastError = self._apply(self.rule_expr4, "expr4", [])
                self.considerError(lastError, None)
                return (_G_apply_378, self.currentError)
            def _G_optional_379():
                return (None, self.input.nullError())
            _G_or_380, lastError = self._or([_G_optional_377, _G_optional_379])
            self.considerError(lastError, 'rulePart')
            _locals['args'] = _G_or_380
            def _G_or_381():
                self._trace('ws', (3638, 3640), self.input.position)
                _G_apply_382, lastError = self._apply(self.rule_ws, "ws", [])
                self.considerError(lastError, None)
                self._trace(" '='", (3640, 3644), self.input.position)
                _G_exactly_383, lastError = self.exactly('=')
                self.considerError(lastError, None)
                self._trace(' expr', (3644, 3649), self.input.position)
                _G_apply_384, lastError = self._apply(self.rule_expr, "expr", [])
                self.considerError(lastError, None)
                _locals['e'] = _G_apply_384
                self._trace(' ruleEnd', (3651, 3659), self.input.position)
                _G_apply_385, lastError = self._apply(self.rule_ruleEnd, "ruleEnd", [])
                self.considerError(lastError, None)
                _G_python_386, lastError = eval('t.And([args, e]) if args else e', self.globals, _locals), None
                self.considerError(lastError, None)
                return (_G_python_386, self.currentError)
            def _G_or_387():
                self._trace(' ruleEnd', (3755, 3763), self.input.position)
                _G_apply_388, lastError = self._apply(self.rule_ruleEnd, "ruleEnd", [])
                self.considerError(lastError, None)
                _G_python_389, lastError = eval('args', self.globals, _locals), None
                self.considerError(lastError, None)
                return (_G_python_389, self.currentError)
            _G_or_390, lastError = self._or([_G_or_381, _G_or_387])
            self.considerError(lastError, 'rulePart')
            return (_G_or_390, self.currentError)


        def rule_rule(self):
            _locals = {'self': self}
            self.locals['rule'] = _locals
            self._trace(' noindentation', (3780, 3794), self.input.position)
            _G_apply_391, lastError = self._apply(self.rule_noindentation, "noindentation", [])
            self.considerError(lastError, 'rule')
            def _G_lookahead_392():
                self._trace('name', (3798, 3802), self.input.position)
                _G_apply_393, lastError = self._apply(self.rule_name, "name", [])
                self.considerError(lastError, None)
                _locals['n'] = _G_apply_393
                return (_locals['n'], self.currentError)
            _G_lookahead_394, lastError = self.lookahead(_G_lookahead_392)
            self.considerError(lastError, 'rule')
            def _G_many1_395():
                self._trace(' rulePart(n)', (3805, 3817), self.input.position)
                _G_python_396, lastError = eval('n', self.globals, _locals), None
                self.considerError(lastError, None)
                _G_apply_397, lastError = self._apply(self.rule_rulePart, "rulePart", [_G_python_396])
                self.considerError(lastError, None)
                return (_G_apply_397, self.currentError)
            _G_many1_398, lastError = self.many(_G_many1_395, _G_many1_395())
            self.considerError(lastError, 'rule')
            _locals['rs'] = _G_many1_398
            _G_python_399, lastError = eval('t.Rule(n, t.Or(rs))', self.globals, _locals), None
            self.considerError(lastError, 'rule')
            return (_G_python_399, self.currentError)


        def rule_grammar(self):
            _locals = {'self': self}
            self.locals['grammar'] = _locals
            def _G_many_400():
                self._trace(' rule', (3856, 3861), self.input.position)
                _G_apply_401, lastError = self._apply(self.rule_rule, "rule", [])
                self.considerError(lastError, None)
                return (_G_apply_401, self.currentError)
            _G_many_402, lastError = self.many(_G_many_400)
            self.considerError(lastError, 'grammar')
            _locals['rs'] = _G_many_402
            self._trace(' ws', (3865, 3868), self.input.position)
            _G_apply_403, lastError = self._apply(self.rule_ws, "ws", [])
            self.considerError(lastError, 'grammar')
            _G_python_404, lastError = eval('t.Grammar(self.name, self.tree_target, rs)', self.globals, _locals), None
            self.considerError(lastError, 'grammar')
            return (_G_python_404, self.currentError)


    if parsley.globals is not None:
        parsley.globals = parsley.globals.copy()
        parsley.globals.update(ruleGlobals)
    else:
        parsley.globals = ruleGlobals
    return parsley