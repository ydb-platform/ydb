def createParserClass(GrammarBase, ruleGlobals):
    if ruleGlobals is None:
        ruleGlobals = {}
    class terml(GrammarBase):
        def rule_hspace(self):
            _locals = {'self': self}
            self.locals['hspace'] = _locals
            def _G_or_1():
                self._trace("' '", (10, 13), self.input.position)
                _G_exactly_2, lastError = self.exactly(' ')
                self.considerError(lastError, None)
                return (_G_exactly_2, self.currentError)
            def _G_or_3():
                self._trace("'\\t'", (14, 18), self.input.position)
                _G_exactly_4, lastError = self.exactly('\t')
                self.considerError(lastError, None)
                return (_G_exactly_4, self.currentError)
            def _G_or_5():
                self._trace("'\\f'", (19, 23), self.input.position)
                _G_exactly_6, lastError = self.exactly('\x0c')
                self.considerError(lastError, None)
                return (_G_exactly_6, self.currentError)
            def _G_or_7():
                self._trace("'#'", (25, 28), self.input.position)
                _G_exactly_8, lastError = self.exactly('#')
                self.considerError(lastError, None)
                def _G_many_9():
                    def _G_not_10():
                        self._trace('eol', (31, 34), self.input.position)
                        _G_apply_11, lastError = self._apply(self.rule_eol, "eol", [])
                        self.considerError(lastError, None)
                        return (_G_apply_11, self.currentError)
                    _G_not_12, lastError = self._not(_G_not_10)
                    self.considerError(lastError, None)
                    self._trace(' anything', (34, 43), self.input.position)
                    _G_apply_13, lastError = self._apply(self.rule_anything, "anything", [])
                    self.considerError(lastError, None)
                    return (_G_apply_13, self.currentError)
                _G_many_14, lastError = self.many(_G_many_9)
                self.considerError(lastError, None)
                return (_G_many_14, self.currentError)
            _G_or_15, lastError = self._or([_G_or_1, _G_or_3, _G_or_5, _G_or_7])
            self.considerError(lastError, 'hspace')
            return (_G_or_15, self.currentError)


        def rule_ws(self):
            _locals = {'self': self}
            self.locals['ws'] = _locals
            def _G_many_16():
                def _G_or_17():
                    self._trace("'\\r'", (54, 58), self.input.position)
                    _G_exactly_18, lastError = self.exactly('\r')
                    self.considerError(lastError, None)
                    self._trace(" '\\n'", (58, 63), self.input.position)
                    _G_exactly_19, lastError = self.exactly('\n')
                    self.considerError(lastError, None)
                    return (_G_exactly_19, self.currentError)
                def _G_or_20():
                    self._trace("'\\r'", (64, 68), self.input.position)
                    _G_exactly_21, lastError = self.exactly('\r')
                    self.considerError(lastError, None)
                    return (_G_exactly_21, self.currentError)
                def _G_or_22():
                    self._trace(" '\\n'", (70, 75), self.input.position)
                    _G_exactly_23, lastError = self.exactly('\n')
                    self.considerError(lastError, None)
                    return (_G_exactly_23, self.currentError)
                def _G_or_24():
                    self._trace(' hspace', (77, 84), self.input.position)
                    _G_apply_25, lastError = self._apply(self.rule_hspace, "hspace", [])
                    self.considerError(lastError, None)
                    return (_G_apply_25, self.currentError)
                _G_or_26, lastError = self._or([_G_or_17, _G_or_20, _G_or_22, _G_or_24])
                self.considerError(lastError, None)
                return (_G_or_26, self.currentError)
            _G_many_27, lastError = self.many(_G_many_16)
            self.considerError(lastError, 'ws')
            return (_G_many_27, self.currentError)


        def rule_number(self):
            _locals = {'self': self}
            self.locals['number'] = _locals
            self._trace(' ws', (96, 99), self.input.position)
            _G_apply_28, lastError = self._apply(self.rule_ws, "ws", [])
            self.considerError(lastError, 'number')
            self._trace(' barenumber', (99, 110), self.input.position)
            _G_apply_29, lastError = self._apply(self.rule_barenumber, "barenumber", [])
            self.considerError(lastError, 'number')
            return (_G_apply_29, self.currentError)


        def rule_barenumber(self):
            _locals = {'self': self}
            self.locals['barenumber'] = _locals
            def _G_optional_30():
                self._trace(" '-'", (123, 127), self.input.position)
                _G_exactly_31, lastError = self.exactly('-')
                self.considerError(lastError, None)
                return (_G_exactly_31, self.currentError)
            def _G_optional_32():
                return (None, self.input.nullError())
            _G_or_33, lastError = self._or([_G_optional_30, _G_optional_32])
            self.considerError(lastError, 'barenumber')
            _locals['sign'] = _G_or_33
            def _G_or_34():
                self._trace("'0'", (136, 139), self.input.position)
                _G_exactly_35, lastError = self.exactly('0')
                self.considerError(lastError, None)
                def _G_or_36():
                    def _G_or_37():
                        self._trace("'x'", (143, 146), self.input.position)
                        _G_exactly_38, lastError = self.exactly('x')
                        self.considerError(lastError, None)
                        return (_G_exactly_38, self.currentError)
                    def _G_or_39():
                        self._trace("'X'", (147, 150), self.input.position)
                        _G_exactly_40, lastError = self.exactly('X')
                        self.considerError(lastError, None)
                        return (_G_exactly_40, self.currentError)
                    _G_or_41, lastError = self._or([_G_or_37, _G_or_39])
                    self.considerError(lastError, None)
                    def _G_many_42():
                        self._trace(' hexdigit', (151, 160), self.input.position)
                        _G_apply_43, lastError = self._apply(self.rule_hexdigit, "hexdigit", [])
                        self.considerError(lastError, None)
                        return (_G_apply_43, self.currentError)
                    _G_many_44, lastError = self.many(_G_many_42)
                    self.considerError(lastError, None)
                    _locals['hs'] = _G_many_44
                    _G_python_45, lastError = eval('makeHex(sign, hs)', self.globals, _locals), None
                    self.considerError(lastError, None)
                    return (_G_python_45, self.currentError)
                def _G_or_46():
                    self._trace("floatPart(sign '0')", (208, 227), self.input.position)
                    _G_python_47, lastError = eval('sign', self.globals, _locals), None
                    self.considerError(lastError, None)
                    _G_python_48, lastError = ('0'), None
                    self.considerError(lastError, None)
                    _G_apply_49, lastError = self._apply(self.rule_floatPart, "floatPart", [_G_python_47, _G_python_48])
                    self.considerError(lastError, None)
                    return (_G_apply_49, self.currentError)
                def _G_or_50():
                    def _G_many_51():
                        self._trace('octaldigit', (249, 259), self.input.position)
                        _G_apply_52, lastError = self._apply(self.rule_octaldigit, "octaldigit", [])
                        self.considerError(lastError, None)
                        return (_G_apply_52, self.currentError)
                    _G_many_53, lastError = self.many(_G_many_51)
                    self.considerError(lastError, None)
                    _locals['ds'] = _G_many_53
                    _G_python_54, lastError = eval('makeOctal(sign, ds)', self.globals, _locals), None
                    self.considerError(lastError, None)
                    return (_G_python_54, self.currentError)
                _G_or_55, lastError = self._or([_G_or_36, _G_or_46, _G_or_50])
                self.considerError(lastError, None)
                return (_G_or_55, self.currentError)
            def _G_or_56():
                self._trace('decdigits', (305, 314), self.input.position)
                _G_apply_57, lastError = self._apply(self.rule_decdigits, "decdigits", [])
                self.considerError(lastError, None)
                _locals['ds'] = _G_apply_57
                self._trace(' floatPart(sign ds)', (317, 336), self.input.position)
                _G_python_58, lastError = eval('sign', self.globals, _locals), None
                self.considerError(lastError, None)
                _G_python_59, lastError = eval('ds', self.globals, _locals), None
                self.considerError(lastError, None)
                _G_apply_60, lastError = self._apply(self.rule_floatPart, "floatPart", [_G_python_58, _G_python_59])
                self.considerError(lastError, None)
                return (_G_apply_60, self.currentError)
            def _G_or_61():
                self._trace('decdigits', (353, 362), self.input.position)
                _G_apply_62, lastError = self._apply(self.rule_decdigits, "decdigits", [])
                self.considerError(lastError, None)
                _locals['ds'] = _G_apply_62
                _G_python_63, lastError = eval('signedInt(sign, ds)', self.globals, _locals), None
                self.considerError(lastError, None)
                return (_G_python_63, self.currentError)
            _G_or_64, lastError = self._or([_G_or_34, _G_or_56, _G_or_61])
            self.considerError(lastError, 'barenumber')
            return (_G_or_64, self.currentError)


        def rule_exponent(self):
            _locals = {'self': self}
            self.locals['exponent'] = _locals
            def _G_consumedby_65():
                def _G_or_66():
                    self._trace("'e'", (405, 408), self.input.position)
                    _G_exactly_67, lastError = self.exactly('e')
                    self.considerError(lastError, None)
                    return (_G_exactly_67, self.currentError)
                def _G_or_68():
                    self._trace(" 'E'", (410, 414), self.input.position)
                    _G_exactly_69, lastError = self.exactly('E')
                    self.considerError(lastError, None)
                    return (_G_exactly_69, self.currentError)
                _G_or_70, lastError = self._or([_G_or_66, _G_or_68])
                self.considerError(lastError, None)
                def _G_optional_71():
                    def _G_or_72():
                        self._trace("'+'", (417, 420), self.input.position)
                        _G_exactly_73, lastError = self.exactly('+')
                        self.considerError(lastError, None)
                        return (_G_exactly_73, self.currentError)
                    def _G_or_74():
                        self._trace(" '-'", (422, 426), self.input.position)
                        _G_exactly_75, lastError = self.exactly('-')
                        self.considerError(lastError, None)
                        return (_G_exactly_75, self.currentError)
                    _G_or_76, lastError = self._or([_G_or_72, _G_or_74])
                    self.considerError(lastError, None)
                    return (_G_or_76, self.currentError)
                def _G_optional_77():
                    return (None, self.input.nullError())
                _G_or_78, lastError = self._or([_G_optional_71, _G_optional_77])
                self.considerError(lastError, None)
                self._trace(' decdigits', (428, 438), self.input.position)
                _G_apply_79, lastError = self._apply(self.rule_decdigits, "decdigits", [])
                self.considerError(lastError, None)
                return (_G_apply_79, self.currentError)
            _G_consumedby_80, lastError = self.consumedby(_G_consumedby_65)
            self.considerError(lastError, 'exponent')
            return (_G_consumedby_80, self.currentError)


        def rule_floatPart(self):
            _locals = {'self': self}
            self.locals['floatPart'] = _locals
            _G_apply_81, lastError = self._apply(self.rule_anything, "anything", [])
            self.considerError(lastError, 'floatPart')
            _locals['sign'] = _G_apply_81
            _G_apply_82, lastError = self._apply(self.rule_anything, "anything", [])
            self.considerError(lastError, 'floatPart')
            _locals['ds'] = _G_apply_82
            def _G_consumedby_83():
                def _G_or_84():
                    self._trace("'.'", (466, 469), self.input.position)
                    _G_exactly_85, lastError = self.exactly('.')
                    self.considerError(lastError, None)
                    self._trace(' decdigits', (469, 479), self.input.position)
                    _G_apply_86, lastError = self._apply(self.rule_decdigits, "decdigits", [])
                    self.considerError(lastError, None)
                    def _G_optional_87():
                        self._trace(' exponent', (479, 488), self.input.position)
                        _G_apply_88, lastError = self._apply(self.rule_exponent, "exponent", [])
                        self.considerError(lastError, None)
                        return (_G_apply_88, self.currentError)
                    def _G_optional_89():
                        return (None, self.input.nullError())
                    _G_or_90, lastError = self._or([_G_optional_87, _G_optional_89])
                    self.considerError(lastError, None)
                    return (_G_or_90, self.currentError)
                def _G_or_91():
                    self._trace(' exponent', (492, 501), self.input.position)
                    _G_apply_92, lastError = self._apply(self.rule_exponent, "exponent", [])
                    self.considerError(lastError, None)
                    return (_G_apply_92, self.currentError)
                _G_or_93, lastError = self._or([_G_or_84, _G_or_91])
                self.considerError(lastError, None)
                return (_G_or_93, self.currentError)
            _G_consumedby_94, lastError = self.consumedby(_G_consumedby_83)
            self.considerError(lastError, 'floatPart')
            _locals['tail'] = _G_consumedby_94
            _G_python_95, lastError = eval('makeFloat(sign, ds, tail)', self.globals, _locals), None
            self.considerError(lastError, 'floatPart')
            return (_G_python_95, self.currentError)


        def rule_decdigits(self):
            _locals = {'self': self}
            self.locals['decdigits'] = _locals
            self._trace(' digit', (549, 555), self.input.position)
            _G_apply_96, lastError = self._apply(self.rule_digit, "digit", [])
            self.considerError(lastError, 'decdigits')
            _locals['d'] = _G_apply_96
            def _G_many_97():
                def _G_or_98():
                    _G_apply_99, lastError = self._apply(self.rule_anything, "anything", [])
                    self.considerError(lastError, None)
                    _locals['x'] = _G_apply_99
                    def _G_pred_100():
                        _G_python_101, lastError = eval('isDigit(x)', self.globals, _locals), None
                        self.considerError(lastError, None)
                        return (_G_python_101, self.currentError)
                    _G_pred_102, lastError = self.pred(_G_pred_100)
                    self.considerError(lastError, None)
                    _G_python_103, lastError = eval('x', self.globals, _locals), None
                    self.considerError(lastError, None)
                    return (_G_python_103, self.currentError)
                def _G_or_104():
                    self._trace(" '_'", (584, 588), self.input.position)
                    _G_exactly_105, lastError = self.exactly('_')
                    self.considerError(lastError, None)
                    _G_python_106, lastError = (""), None
                    self.considerError(lastError, None)
                    return (_G_python_106, self.currentError)
                _G_or_107, lastError = self._or([_G_or_98, _G_or_104])
                self.considerError(lastError, None)
                return (_G_or_107, self.currentError)
            _G_many_108, lastError = self.many(_G_many_97)
            self.considerError(lastError, 'decdigits')
            _locals['ds'] = _G_many_108
            _G_python_109, lastError = eval('concat(d, join(ds))', self.globals, _locals), None
            self.considerError(lastError, 'decdigits')
            return (_G_python_109, self.currentError)


        def rule_octaldigit(self):
            _locals = {'self': self}
            self.locals['octaldigit'] = _locals
            _G_apply_110, lastError = self._apply(self.rule_anything, "anything", [])
            self.considerError(lastError, 'octaldigit')
            _locals['x'] = _G_apply_110
            def _G_pred_111():
                _G_python_112, lastError = eval('isOctDigit(x)', self.globals, _locals), None
                self.considerError(lastError, None)
                return (_G_python_112, self.currentError)
            _G_pred_113, lastError = self.pred(_G_pred_111)
            self.considerError(lastError, 'octaldigit')
            _G_python_114, lastError = eval('x', self.globals, _locals), None
            self.considerError(lastError, 'octaldigit')
            return (_G_python_114, self.currentError)


        def rule_hexdigit(self):
            _locals = {'self': self}
            self.locals['hexdigit'] = _locals
            _G_apply_115, lastError = self._apply(self.rule_anything, "anything", [])
            self.considerError(lastError, 'hexdigit')
            _locals['x'] = _G_apply_115
            def _G_pred_116():
                _G_python_117, lastError = eval('isHexDigit(x)', self.globals, _locals), None
                self.considerError(lastError, None)
                return (_G_python_117, self.currentError)
            _G_pred_118, lastError = self.pred(_G_pred_116)
            self.considerError(lastError, 'hexdigit')
            _G_python_119, lastError = eval('x', self.globals, _locals), None
            self.considerError(lastError, 'hexdigit')
            return (_G_python_119, self.currentError)


        def rule_string(self):
            _locals = {'self': self}
            self.locals['string'] = _locals
            self._trace(' ws', (706, 709), self.input.position)
            _G_apply_120, lastError = self._apply(self.rule_ws, "ws", [])
            self.considerError(lastError, 'string')
            self._trace(' \'"\'', (709, 713), self.input.position)
            _G_exactly_121, lastError = self.exactly('"')
            self.considerError(lastError, 'string')
            def _G_many_122():
                def _G_or_123():
                    self._trace('escapedChar', (715, 726), self.input.position)
                    _G_apply_124, lastError = self._apply(self.rule_escapedChar, "escapedChar", [])
                    self.considerError(lastError, None)
                    return (_G_apply_124, self.currentError)
                def _G_or_125():
                    def _G_not_126():
                        self._trace('\'"\'', (731, 734), self.input.position)
                        _G_exactly_127, lastError = self.exactly('"')
                        self.considerError(lastError, None)
                        return (_G_exactly_127, self.currentError)
                    _G_not_128, lastError = self._not(_G_not_126)
                    self.considerError(lastError, None)
                    self._trace(' anything', (735, 744), self.input.position)
                    _G_apply_129, lastError = self._apply(self.rule_anything, "anything", [])
                    self.considerError(lastError, None)
                    return (_G_apply_129, self.currentError)
                _G_or_130, lastError = self._or([_G_or_123, _G_or_125])
                self.considerError(lastError, None)
                return (_G_or_130, self.currentError)
            _G_many_131, lastError = self.many(_G_many_122)
            self.considerError(lastError, 'string')
            _locals['c'] = _G_many_131
            self._trace(' \'"\'', (748, 752), self.input.position)
            _G_exactly_132, lastError = self.exactly('"')
            self.considerError(lastError, 'string')
            _G_python_133, lastError = eval('join(c)', self.globals, _locals), None
            self.considerError(lastError, 'string')
            return (_G_python_133, self.currentError)


        def rule_character(self):
            _locals = {'self': self}
            self.locals['character'] = _locals
            self._trace(' ws', (775, 778), self.input.position)
            _G_apply_134, lastError = self._apply(self.rule_ws, "ws", [])
            self.considerError(lastError, 'character')
            self._trace(" '\\''", (778, 783), self.input.position)
            _G_exactly_135, lastError = self.exactly("'")
            self.considerError(lastError, 'character')
            def _G_or_136():
                self._trace('escapedChar', (785, 796), self.input.position)
                _G_apply_137, lastError = self._apply(self.rule_escapedChar, "escapedChar", [])
                self.considerError(lastError, None)
                return (_G_apply_137, self.currentError)
            def _G_or_138():
                def _G_not_139():
                    def _G_or_140():
                        self._trace("'\\''", (801, 805), self.input.position)
                        _G_exactly_141, lastError = self.exactly("'")
                        self.considerError(lastError, None)
                        return (_G_exactly_141, self.currentError)
                    def _G_or_142():
                        self._trace("'\\n'", (806, 810), self.input.position)
                        _G_exactly_143, lastError = self.exactly('\n')
                        self.considerError(lastError, None)
                        return (_G_exactly_143, self.currentError)
                    def _G_or_144():
                        self._trace("'\\r'", (811, 815), self.input.position)
                        _G_exactly_145, lastError = self.exactly('\r')
                        self.considerError(lastError, None)
                        return (_G_exactly_145, self.currentError)
                    def _G_or_146():
                        self._trace("'\\\\'", (816, 820), self.input.position)
                        _G_exactly_147, lastError = self.exactly('\\')
                        self.considerError(lastError, None)
                        return (_G_exactly_147, self.currentError)
                    _G_or_148, lastError = self._or([_G_or_140, _G_or_142, _G_or_144, _G_or_146])
                    self.considerError(lastError, None)
                    return (_G_or_148, self.currentError)
                _G_not_149, lastError = self._not(_G_not_139)
                self.considerError(lastError, None)
                self._trace(' anything', (821, 830), self.input.position)
                _G_apply_150, lastError = self._apply(self.rule_anything, "anything", [])
                self.considerError(lastError, None)
                return (_G_apply_150, self.currentError)
            _G_or_151, lastError = self._or([_G_or_136, _G_or_138])
            self.considerError(lastError, 'character')
            _locals['c'] = _G_or_151
            self._trace(" '\\''", (833, 838), self.input.position)
            _G_exactly_152, lastError = self.exactly("'")
            self.considerError(lastError, 'character')
            _G_python_153, lastError = eval('Character(c)', self.globals, _locals), None
            self.considerError(lastError, 'character')
            return (_G_python_153, self.currentError)


        def rule_escapedUnicode(self):
            _locals = {'self': self}
            self.locals['escapedUnicode'] = _locals
            def _G_or_154():
                self._trace("'u'", (873, 876), self.input.position)
                _G_exactly_155, lastError = self.exactly('u')
                self.considerError(lastError, None)
                def _G_consumedby_156():
                    self._trace('hexdigit', (878, 886), self.input.position)
                    _G_apply_157, lastError = self._apply(self.rule_hexdigit, "hexdigit", [])
                    self.considerError(lastError, None)
                    self._trace(' hexdigit', (886, 895), self.input.position)
                    _G_apply_158, lastError = self._apply(self.rule_hexdigit, "hexdigit", [])
                    self.considerError(lastError, None)
                    self._trace(' hexdigit', (895, 904), self.input.position)
                    _G_apply_159, lastError = self._apply(self.rule_hexdigit, "hexdigit", [])
                    self.considerError(lastError, None)
                    self._trace(' hexdigit', (904, 913), self.input.position)
                    _G_apply_160, lastError = self._apply(self.rule_hexdigit, "hexdigit", [])
                    self.considerError(lastError, None)
                    return (_G_apply_160, self.currentError)
                _G_consumedby_161, lastError = self.consumedby(_G_consumedby_156)
                self.considerError(lastError, None)
                _locals['hs'] = _G_consumedby_161
                _G_python_162, lastError = eval('unichr(int(hs, 16))', self.globals, _locals), None
                self.considerError(lastError, None)
                return (_G_python_162, self.currentError)
            def _G_or_163():
                self._trace("'U'", (961, 964), self.input.position)
                _G_exactly_164, lastError = self.exactly('U')
                self.considerError(lastError, None)
                def _G_consumedby_165():
                    self._trace('hexdigit', (966, 974), self.input.position)
                    _G_apply_166, lastError = self._apply(self.rule_hexdigit, "hexdigit", [])
                    self.considerError(lastError, None)
                    self._trace(' hexdigit', (974, 983), self.input.position)
                    _G_apply_167, lastError = self._apply(self.rule_hexdigit, "hexdigit", [])
                    self.considerError(lastError, None)
                    self._trace(' hexdigit', (983, 992), self.input.position)
                    _G_apply_168, lastError = self._apply(self.rule_hexdigit, "hexdigit", [])
                    self.considerError(lastError, None)
                    self._trace(' hexdigit', (992, 1001), self.input.position)
                    _G_apply_169, lastError = self._apply(self.rule_hexdigit, "hexdigit", [])
                    self.considerError(lastError, None)
                    self._trace('\n                         hexdigit', (1001, 1035), self.input.position)
                    _G_apply_170, lastError = self._apply(self.rule_hexdigit, "hexdigit", [])
                    self.considerError(lastError, None)
                    self._trace(' hexdigit', (1035, 1044), self.input.position)
                    _G_apply_171, lastError = self._apply(self.rule_hexdigit, "hexdigit", [])
                    self.considerError(lastError, None)
                    self._trace(' hexdigit', (1044, 1053), self.input.position)
                    _G_apply_172, lastError = self._apply(self.rule_hexdigit, "hexdigit", [])
                    self.considerError(lastError, None)
                    self._trace(' hexdigit', (1053, 1062), self.input.position)
                    _G_apply_173, lastError = self._apply(self.rule_hexdigit, "hexdigit", [])
                    self.considerError(lastError, None)
                    return (_G_apply_173, self.currentError)
                _G_consumedby_174, lastError = self.consumedby(_G_consumedby_165)
                self.considerError(lastError, None)
                _locals['hs'] = _G_consumedby_174
                _G_python_175, lastError = eval('unichr(int(hs, 16))', self.globals, _locals), None
                self.considerError(lastError, None)
                return (_G_python_175, self.currentError)
            _G_or_176, lastError = self._or([_G_or_154, _G_or_163])
            self.considerError(lastError, 'escapedUnicode')
            return (_G_or_176, self.currentError)


        def rule_escapedOctal(self):
            _locals = {'self': self}
            self.locals['escapedOctal'] = _locals
            def _G_or_177():
                def _G_consumedby_178():
                    _G_apply_179, lastError = self._apply(self.rule_anything, "anything", [])
                    self.considerError(lastError, None)
                    _locals['a'] = _G_apply_179
                    def _G_pred_180():
                        _G_python_181, lastError = eval('contains("0123", a)', self.globals, _locals), None
                        self.considerError(lastError, None)
                        return (_G_python_181, self.currentError)
                    _G_pred_182, lastError = self.pred(_G_pred_180)
                    self.considerError(lastError, None)
                    def _G_optional_183():
                        self._trace(' octdigit', (1135, 1144), self.input.position)
                        _G_apply_184, lastError = self._apply(self.rule_octdigit, "octdigit", [])
                        self.considerError(lastError, None)
                        return (_G_apply_184, self.currentError)
                    def _G_optional_185():
                        return (None, self.input.nullError())
                    _G_or_186, lastError = self._or([_G_optional_183, _G_optional_185])
                    self.considerError(lastError, None)
                    def _G_optional_187():
                        self._trace(' octdigit', (1145, 1154), self.input.position)
                        _G_apply_188, lastError = self._apply(self.rule_octdigit, "octdigit", [])
                        self.considerError(lastError, None)
                        return (_G_apply_188, self.currentError)
                    def _G_optional_189():
                        return (None, self.input.nullError())
                    _G_or_190, lastError = self._or([_G_optional_187, _G_optional_189])
                    self.considerError(lastError, None)
                    return (_G_or_190, self.currentError)
                _G_consumedby_191, lastError = self.consumedby(_G_consumedby_178)
                self.considerError(lastError, None)
                return (_G_consumedby_191, self.currentError)
            def _G_or_192():
                def _G_consumedby_193():
                    _G_apply_194, lastError = self._apply(self.rule_anything, "anything", [])
                    self.considerError(lastError, None)
                    _locals['a'] = _G_apply_194
                    def _G_pred_195():
                        _G_python_196, lastError = eval('contains("4567", a)', self.globals, _locals), None
                        self.considerError(lastError, None)
                        return (_G_python_196, self.currentError)
                    _G_pred_197, lastError = self.pred(_G_pred_195)
                    self.considerError(lastError, None)
                    def _G_optional_198():
                        self._trace(' octdigit', (1202, 1211), self.input.position)
                        _G_apply_199, lastError = self._apply(self.rule_octdigit, "octdigit", [])
                        self.considerError(lastError, None)
                        return (_G_apply_199, self.currentError)
                    def _G_optional_200():
                        return (None, self.input.nullError())
                    _G_or_201, lastError = self._or([_G_optional_198, _G_optional_200])
                    self.considerError(lastError, None)
                    return (_G_or_201, self.currentError)
                _G_consumedby_202, lastError = self.consumedby(_G_consumedby_193)
                self.considerError(lastError, None)
                return (_G_consumedby_202, self.currentError)
            _G_or_203, lastError = self._or([_G_or_177, _G_or_192])
            self.considerError(lastError, 'escapedOctal')
            _locals['os'] = _G_or_203
            _G_python_204, lastError = eval('int(os, 8)', self.globals, _locals), None
            self.considerError(lastError, 'escapedOctal')
            return (_G_python_204, self.currentError)


        def rule_escapedChar(self):
            _locals = {'self': self}
            self.locals['escapedChar'] = _locals
            self._trace(" '\\\\'", (1246, 1251), self.input.position)
            _G_exactly_205, lastError = self.exactly('\\')
            self.considerError(lastError, 'escapedChar')
            def _G_or_206():
                self._trace("'n'", (1253, 1256), self.input.position)
                _G_exactly_207, lastError = self.exactly('n')
                self.considerError(lastError, None)
                _G_python_208, lastError = ('\n'), None
                self.considerError(lastError, None)
                return (_G_python_208, self.currentError)
            def _G_or_209():
                self._trace("'r'", (1287, 1290), self.input.position)
                _G_exactly_210, lastError = self.exactly('r')
                self.considerError(lastError, None)
                _G_python_211, lastError = ('\r'), None
                self.considerError(lastError, None)
                return (_G_python_211, self.currentError)
            def _G_or_212():
                self._trace("'t'", (1321, 1324), self.input.position)
                _G_exactly_213, lastError = self.exactly('t')
                self.considerError(lastError, None)
                _G_python_214, lastError = ('\t'), None
                self.considerError(lastError, None)
                return (_G_python_214, self.currentError)
            def _G_or_215():
                self._trace("'b'", (1355, 1358), self.input.position)
                _G_exactly_216, lastError = self.exactly('b')
                self.considerError(lastError, None)
                _G_python_217, lastError = ('\b'), None
                self.considerError(lastError, None)
                return (_G_python_217, self.currentError)
            def _G_or_218():
                self._trace("'f'", (1389, 1392), self.input.position)
                _G_exactly_219, lastError = self.exactly('f')
                self.considerError(lastError, None)
                _G_python_220, lastError = ('\f'), None
                self.considerError(lastError, None)
                return (_G_python_220, self.currentError)
            def _G_or_221():
                self._trace('\'"\'', (1423, 1426), self.input.position)
                _G_exactly_222, lastError = self.exactly('"')
                self.considerError(lastError, None)
                _G_python_223, lastError = ('"'), None
                self.considerError(lastError, None)
                return (_G_python_223, self.currentError)
            def _G_or_224():
                self._trace("'\\''", (1456, 1460), self.input.position)
                _G_exactly_225, lastError = self.exactly("'")
                self.considerError(lastError, None)
                _G_python_226, lastError = ('\''), None
                self.considerError(lastError, None)
                return (_G_python_226, self.currentError)
            def _G_or_227():
                self._trace("'?'", (1491, 1494), self.input.position)
                _G_exactly_228, lastError = self.exactly('?')
                self.considerError(lastError, None)
                _G_python_229, lastError = ('?'), None
                self.considerError(lastError, None)
                return (_G_python_229, self.currentError)
            def _G_or_230():
                self._trace("'\\\\'", (1524, 1528), self.input.position)
                _G_exactly_231, lastError = self.exactly('\\')
                self.considerError(lastError, None)
                _G_python_232, lastError = ('\\'), None
                self.considerError(lastError, None)
                return (_G_python_232, self.currentError)
            def _G_or_233():
                self._trace(' escapedUnicode', (1559, 1574), self.input.position)
                _G_apply_234, lastError = self._apply(self.rule_escapedUnicode, "escapedUnicode", [])
                self.considerError(lastError, None)
                return (_G_apply_234, self.currentError)
            def _G_or_235():
                self._trace(' escapedOctal', (1597, 1610), self.input.position)
                _G_apply_236, lastError = self._apply(self.rule_escapedOctal, "escapedOctal", [])
                self.considerError(lastError, None)
                return (_G_apply_236, self.currentError)
            def _G_or_237():
                self._trace(' eol', (1633, 1637), self.input.position)
                _G_apply_238, lastError = self._apply(self.rule_eol, "eol", [])
                self.considerError(lastError, None)
                _G_python_239, lastError = (""), None
                self.considerError(lastError, None)
                return (_G_python_239, self.currentError)
            _G_or_240, lastError = self._or([_G_or_206, _G_or_209, _G_or_212, _G_or_215, _G_or_218, _G_or_221, _G_or_224, _G_or_227, _G_or_230, _G_or_233, _G_or_235, _G_or_237])
            self.considerError(lastError, 'escapedChar')
            return (_G_or_240, self.currentError)


        def rule_eol(self):
            _locals = {'self': self}
            self.locals['eol'] = _locals
            def _G_many_241():
                self._trace(' hspace', (1651, 1658), self.input.position)
                _G_apply_242, lastError = self._apply(self.rule_hspace, "hspace", [])
                self.considerError(lastError, None)
                return (_G_apply_242, self.currentError)
            _G_many_243, lastError = self.many(_G_many_241)
            self.considerError(lastError, 'eol')
            def _G_or_244():
                self._trace("'\\r'", (1661, 1665), self.input.position)
                _G_exactly_245, lastError = self.exactly('\r')
                self.considerError(lastError, None)
                self._trace(" '\\n'", (1665, 1670), self.input.position)
                _G_exactly_246, lastError = self.exactly('\n')
                self.considerError(lastError, None)
                return (_G_exactly_246, self.currentError)
            def _G_or_247():
                self._trace("'\\r'", (1671, 1675), self.input.position)
                _G_exactly_248, lastError = self.exactly('\r')
                self.considerError(lastError, None)
                return (_G_exactly_248, self.currentError)
            def _G_or_249():
                self._trace(" '\\n'", (1677, 1682), self.input.position)
                _G_exactly_250, lastError = self.exactly('\n')
                self.considerError(lastError, None)
                return (_G_exactly_250, self.currentError)
            _G_or_251, lastError = self._or([_G_or_244, _G_or_247, _G_or_249])
            self.considerError(lastError, 'eol')
            return (_G_or_251, self.currentError)


        def rule_uriBody(self):
            _locals = {'self': self}
            self.locals['uriBody'] = _locals
            def _G_consumedby_252():
                def _G_many1_253():
                    def _G_or_254():
                        self._trace('letterOrDigit', (1697, 1710), self.input.position)
                        _G_apply_255, lastError = self._apply(self.rule_letterOrDigit, "letterOrDigit", [])
                        self.considerError(lastError, None)
                        return (_G_apply_255, self.currentError)
                    def _G_or_256():
                        self._trace("'_'", (1711, 1714), self.input.position)
                        _G_exactly_257, lastError = self.exactly('_')
                        self.considerError(lastError, None)
                        return (_G_exactly_257, self.currentError)
                    def _G_or_258():
                        self._trace("';'", (1715, 1718), self.input.position)
                        _G_exactly_259, lastError = self.exactly(';')
                        self.considerError(lastError, None)
                        return (_G_exactly_259, self.currentError)
                    def _G_or_260():
                        self._trace("'/'", (1719, 1722), self.input.position)
                        _G_exactly_261, lastError = self.exactly('/')
                        self.considerError(lastError, None)
                        return (_G_exactly_261, self.currentError)
                    def _G_or_262():
                        self._trace("'?'", (1723, 1726), self.input.position)
                        _G_exactly_263, lastError = self.exactly('?')
                        self.considerError(lastError, None)
                        return (_G_exactly_263, self.currentError)
                    def _G_or_264():
                        self._trace("':'", (1727, 1730), self.input.position)
                        _G_exactly_265, lastError = self.exactly(':')
                        self.considerError(lastError, None)
                        return (_G_exactly_265, self.currentError)
                    def _G_or_266():
                        self._trace("'@'", (1731, 1734), self.input.position)
                        _G_exactly_267, lastError = self.exactly('@')
                        self.considerError(lastError, None)
                        return (_G_exactly_267, self.currentError)
                    def _G_or_268():
                        self._trace("'&'", (1735, 1738), self.input.position)
                        _G_exactly_269, lastError = self.exactly('&')
                        self.considerError(lastError, None)
                        return (_G_exactly_269, self.currentError)
                    def _G_or_270():
                        self._trace("'='", (1739, 1742), self.input.position)
                        _G_exactly_271, lastError = self.exactly('=')
                        self.considerError(lastError, None)
                        return (_G_exactly_271, self.currentError)
                    def _G_or_272():
                        self._trace("'+'", (1743, 1746), self.input.position)
                        _G_exactly_273, lastError = self.exactly('+')
                        self.considerError(lastError, None)
                        return (_G_exactly_273, self.currentError)
                    def _G_or_274():
                        self._trace("'$'", (1747, 1750), self.input.position)
                        _G_exactly_275, lastError = self.exactly('$')
                        self.considerError(lastError, None)
                        return (_G_exactly_275, self.currentError)
                    def _G_or_276():
                        self._trace("','", (1751, 1754), self.input.position)
                        _G_exactly_277, lastError = self.exactly(',')
                        self.considerError(lastError, None)
                        return (_G_exactly_277, self.currentError)
                    def _G_or_278():
                        self._trace("'-'", (1755, 1758), self.input.position)
                        _G_exactly_279, lastError = self.exactly('-')
                        self.considerError(lastError, None)
                        return (_G_exactly_279, self.currentError)
                    def _G_or_280():
                        self._trace("'.'", (1759, 1762), self.input.position)
                        _G_exactly_281, lastError = self.exactly('.')
                        self.considerError(lastError, None)
                        return (_G_exactly_281, self.currentError)
                    def _G_or_282():
                        self._trace("'!'", (1763, 1766), self.input.position)
                        _G_exactly_283, lastError = self.exactly('!')
                        self.considerError(lastError, None)
                        return (_G_exactly_283, self.currentError)
                    def _G_or_284():
                        self._trace("'~'", (1767, 1770), self.input.position)
                        _G_exactly_285, lastError = self.exactly('~')
                        self.considerError(lastError, None)
                        return (_G_exactly_285, self.currentError)
                    def _G_or_286():
                        self._trace("'*'", (1771, 1774), self.input.position)
                        _G_exactly_287, lastError = self.exactly('*')
                        self.considerError(lastError, None)
                        return (_G_exactly_287, self.currentError)
                    def _G_or_288():
                        self._trace("'\\''", (1775, 1779), self.input.position)
                        _G_exactly_289, lastError = self.exactly("'")
                        self.considerError(lastError, None)
                        return (_G_exactly_289, self.currentError)
                    def _G_or_290():
                        self._trace("'('", (1780, 1783), self.input.position)
                        _G_exactly_291, lastError = self.exactly('(')
                        self.considerError(lastError, None)
                        return (_G_exactly_291, self.currentError)
                    def _G_or_292():
                        self._trace("')'", (1784, 1787), self.input.position)
                        _G_exactly_293, lastError = self.exactly(')')
                        self.considerError(lastError, None)
                        return (_G_exactly_293, self.currentError)
                    def _G_or_294():
                        self._trace("'%'", (1788, 1791), self.input.position)
                        _G_exactly_295, lastError = self.exactly('%')
                        self.considerError(lastError, None)
                        return (_G_exactly_295, self.currentError)
                    def _G_or_296():
                        self._trace("'\\\\'", (1792, 1796), self.input.position)
                        _G_exactly_297, lastError = self.exactly('\\')
                        self.considerError(lastError, None)
                        return (_G_exactly_297, self.currentError)
                    def _G_or_298():
                        self._trace("'|'", (1797, 1800), self.input.position)
                        _G_exactly_299, lastError = self.exactly('|')
                        self.considerError(lastError, None)
                        return (_G_exactly_299, self.currentError)
                    def _G_or_300():
                        self._trace("'#'", (1801, 1804), self.input.position)
                        _G_exactly_301, lastError = self.exactly('#')
                        self.considerError(lastError, None)
                        return (_G_exactly_301, self.currentError)
                    _G_or_302, lastError = self._or([_G_or_254, _G_or_256, _G_or_258, _G_or_260, _G_or_262, _G_or_264, _G_or_266, _G_or_268, _G_or_270, _G_or_272, _G_or_274, _G_or_276, _G_or_278, _G_or_280, _G_or_282, _G_or_284, _G_or_286, _G_or_288, _G_or_290, _G_or_292, _G_or_294, _G_or_296, _G_or_298, _G_or_300])
                    self.considerError(lastError, None)
                    return (_G_or_302, self.currentError)
                _G_many1_303, lastError = self.many(_G_many1_253, _G_many1_253())
                self.considerError(lastError, None)
                return (_G_many1_303, self.currentError)
            _G_consumedby_304, lastError = self.consumedby(_G_consumedby_252)
            self.considerError(lastError, 'uriBody')
            return (_G_consumedby_304, self.currentError)


        def rule_literal(self):
            _locals = {'self': self}
            self.locals['literal'] = _locals
            def _G_or_305():
                self._trace(' string', (1819, 1826), self.input.position)
                _G_apply_306, lastError = self._apply(self.rule_string, "string", [])
                self.considerError(lastError, None)
                _locals['x'] = _G_apply_306
                _G_python_307, lastError = eval('leafInternal(Tag(".String."), x)', self.globals, _locals), None
                self.considerError(lastError, None)
                return (_G_python_307, self.currentError)
            def _G_or_308():
                self._trace(' character', (1874, 1884), self.input.position)
                _G_apply_309, lastError = self._apply(self.rule_character, "character", [])
                self.considerError(lastError, None)
                _locals['x'] = _G_apply_309
                _G_python_310, lastError = eval('leafInternal(Tag(".char."), x)', self.globals, _locals), None
                self.considerError(lastError, None)
                return (_G_python_310, self.currentError)
            def _G_or_311():
                self._trace(' number', (1930, 1937), self.input.position)
                _G_apply_312, lastError = self._apply(self.rule_number, "number", [])
                self.considerError(lastError, None)
                _locals['x'] = _G_apply_312
                _G_python_313, lastError = eval('leafInternal(Tag(numberType(x)), x)', self.globals, _locals), None
                self.considerError(lastError, None)
                return (_G_python_313, self.currentError)
            _G_or_314, lastError = self._or([_G_or_305, _G_or_308, _G_or_311])
            self.considerError(lastError, 'literal')
            return (_G_or_314, self.currentError)


        def rule_tag(self):
            _locals = {'self': self}
            self.locals['tag'] = _locals
            def _G_or_315():
                self._trace('\n          segment', (1988, 2006), self.input.position)
                _G_apply_316, lastError = self._apply(self.rule_segment, "segment", [])
                self.considerError(lastError, None)
                _locals['seg1'] = _G_apply_316
                def _G_many_317():
                    self._trace("':'", (2013, 2016), self.input.position)
                    _G_exactly_318, lastError = self.exactly(':')
                    self.considerError(lastError, None)
                    self._trace(" ':'", (2016, 2020), self.input.position)
                    _G_exactly_319, lastError = self.exactly(':')
                    self.considerError(lastError, None)
                    self._trace(' sos', (2020, 2024), self.input.position)
                    _G_apply_320, lastError = self._apply(self.rule_sos, "sos", [])
                    self.considerError(lastError, None)
                    return (_G_apply_320, self.currentError)
                _G_many_321, lastError = self.many(_G_many_317)
                self.considerError(lastError, None)
                _locals['segs'] = _G_many_321
                _G_python_322, lastError = eval('makeTag(cons(seg1, segs))', self.globals, _locals), None
                self.considerError(lastError, None)
                return (_G_python_322, self.currentError)
            def _G_or_323():
                def _G_many1_324():
                    self._trace("':'", (2072, 2075), self.input.position)
                    _G_exactly_325, lastError = self.exactly(':')
                    self.considerError(lastError, None)
                    self._trace(" ':'", (2075, 2079), self.input.position)
                    _G_exactly_326, lastError = self.exactly(':')
                    self.considerError(lastError, None)
                    self._trace(' sos', (2079, 2083), self.input.position)
                    _G_apply_327, lastError = self._apply(self.rule_sos, "sos", [])
                    self.considerError(lastError, None)
                    return (_G_apply_327, self.currentError)
                _G_many1_328, lastError = self.many(_G_many1_324, _G_many1_324())
                self.considerError(lastError, None)
                _locals['segs'] = _G_many1_328
                _G_python_329, lastError = eval('prefixedTag(segs)', self.globals, _locals), None
                self.considerError(lastError, None)
                return (_G_python_329, self.currentError)
            _G_or_330, lastError = self._or([_G_or_315, _G_or_323])
            self.considerError(lastError, 'tag')
            return (_G_or_330, self.currentError)


        def rule_sos(self):
            _locals = {'self': self}
            self.locals['sos'] = _locals
            def _G_or_331():
                self._trace(' segment', (2119, 2127), self.input.position)
                _G_apply_332, lastError = self._apply(self.rule_segment, "segment", [])
                self.considerError(lastError, None)
                return (_G_apply_332, self.currentError)
            def _G_or_333():
                self._trace('string', (2131, 2137), self.input.position)
                _G_apply_334, lastError = self._apply(self.rule_string, "string", [])
                self.considerError(lastError, None)
                _locals['s'] = _G_apply_334
                _G_python_335, lastError = eval('tagString(s)', self.globals, _locals), None
                self.considerError(lastError, None)
                return (_G_python_335, self.currentError)
            _G_or_336, lastError = self._or([_G_or_331, _G_or_333])
            self.considerError(lastError, 'sos')
            return (_G_or_336, self.currentError)


        def rule_segment(self):
            _locals = {'self': self}
            self.locals['segment'] = _locals
            def _G_or_337():
                self._trace(' ident', (2167, 2173), self.input.position)
                _G_apply_338, lastError = self._apply(self.rule_ident, "ident", [])
                self.considerError(lastError, None)
                return (_G_apply_338, self.currentError)
            def _G_or_339():
                self._trace(' special', (2175, 2183), self.input.position)
                _G_apply_340, lastError = self._apply(self.rule_special, "special", [])
                self.considerError(lastError, None)
                return (_G_apply_340, self.currentError)
            def _G_or_341():
                self._trace(' uri', (2185, 2189), self.input.position)
                _G_apply_342, lastError = self._apply(self.rule_uri, "uri", [])
                self.considerError(lastError, None)
                return (_G_apply_342, self.currentError)
            _G_or_343, lastError = self._or([_G_or_337, _G_or_339, _G_or_341])
            self.considerError(lastError, 'segment')
            return (_G_or_343, self.currentError)


        def rule_ident(self):
            _locals = {'self': self}
            self.locals['ident'] = _locals
            self._trace(' segStart', (2198, 2207), self.input.position)
            _G_apply_344, lastError = self._apply(self.rule_segStart, "segStart", [])
            self.considerError(lastError, 'ident')
            _locals['i1'] = _G_apply_344
            def _G_many_345():
                self._trace(' segPart', (2210, 2218), self.input.position)
                _G_apply_346, lastError = self._apply(self.rule_segPart, "segPart", [])
                self.considerError(lastError, None)
                return (_G_apply_346, self.currentError)
            _G_many_347, lastError = self.many(_G_many_345)
            self.considerError(lastError, 'ident')
            _locals['ibits'] = _G_many_347
            _G_python_348, lastError = eval('join(cons(i1, ibits))', self.globals, _locals), None
            self.considerError(lastError, 'ident')
            return (_G_python_348, self.currentError)


        def rule_segStart(self):
            _locals = {'self': self}
            self.locals['segStart'] = _locals
            def _G_or_349():
                self._trace(' letter', (2262, 2269), self.input.position)
                _G_apply_350, lastError = self._apply(self.rule_letter, "letter", [])
                self.considerError(lastError, None)
                return (_G_apply_350, self.currentError)
            def _G_or_351():
                self._trace(" '_'", (2271, 2275), self.input.position)
                _G_exactly_352, lastError = self.exactly('_')
                self.considerError(lastError, None)
                return (_G_exactly_352, self.currentError)
            def _G_or_353():
                self._trace(" '$'", (2277, 2281), self.input.position)
                _G_exactly_354, lastError = self.exactly('$')
                self.considerError(lastError, None)
                return (_G_exactly_354, self.currentError)
            _G_or_355, lastError = self._or([_G_or_349, _G_or_351, _G_or_353])
            self.considerError(lastError, 'segStart')
            return (_G_or_355, self.currentError)


        def rule_segPart(self):
            _locals = {'self': self}
            self.locals['segPart'] = _locals
            def _G_or_356():
                self._trace(' letterOrDigit', (2292, 2306), self.input.position)
                _G_apply_357, lastError = self._apply(self.rule_letterOrDigit, "letterOrDigit", [])
                self.considerError(lastError, None)
                return (_G_apply_357, self.currentError)
            def _G_or_358():
                self._trace(" '_'", (2308, 2312), self.input.position)
                _G_exactly_359, lastError = self.exactly('_')
                self.considerError(lastError, None)
                return (_G_exactly_359, self.currentError)
            def _G_or_360():
                self._trace(" '.'", (2314, 2318), self.input.position)
                _G_exactly_361, lastError = self.exactly('.')
                self.considerError(lastError, None)
                return (_G_exactly_361, self.currentError)
            def _G_or_362():
                self._trace(" '-'", (2320, 2324), self.input.position)
                _G_exactly_363, lastError = self.exactly('-')
                self.considerError(lastError, None)
                return (_G_exactly_363, self.currentError)
            def _G_or_364():
                self._trace(" '$'", (2326, 2330), self.input.position)
                _G_exactly_365, lastError = self.exactly('$')
                self.considerError(lastError, None)
                return (_G_exactly_365, self.currentError)
            _G_or_366, lastError = self._or([_G_or_356, _G_or_358, _G_or_360, _G_or_362, _G_or_364])
            self.considerError(lastError, 'segPart')
            return (_G_or_366, self.currentError)


        def rule_special(self):
            _locals = {'self': self}
            self.locals['special'] = _locals
            self._trace(" '.'", (2341, 2345), self.input.position)
            _G_exactly_367, lastError = self.exactly('.')
            self.considerError(lastError, 'special')
            _locals['a'] = _G_exactly_367
            self._trace(' ident', (2347, 2353), self.input.position)
            _G_apply_368, lastError = self._apply(self.rule_ident, "ident", [])
            self.considerError(lastError, 'special')
            _locals['b'] = _G_apply_368
            _G_python_369, lastError = eval('concat(a, b)', self.globals, _locals), None
            self.considerError(lastError, 'special')
            return (_G_python_369, self.currentError)


        def rule_uri(self):
            _locals = {'self': self}
            self.locals['uri'] = _locals
            self._trace(" '<'", (2378, 2382), self.input.position)
            _G_exactly_370, lastError = self.exactly('<')
            self.considerError(lastError, 'uri')
            def _G_many_371():
                self._trace(' uriBody', (2382, 2390), self.input.position)
                _G_apply_372, lastError = self._apply(self.rule_uriBody, "uriBody", [])
                self.considerError(lastError, None)
                return (_G_apply_372, self.currentError)
            _G_many_373, lastError = self.many(_G_many_371)
            self.considerError(lastError, 'uri')
            _locals['uriChars'] = _G_many_373
            self._trace(" '>'", (2400, 2404), self.input.position)
            _G_exactly_374, lastError = self.exactly('>')
            self.considerError(lastError, 'uri')
            _G_python_375, lastError = eval('concat(b, uriChars, e)', self.globals, _locals), None
            self.considerError(lastError, 'uri')
            return (_G_python_375, self.currentError)


        def rule_functor(self):
            _locals = {'self': self}
            self.locals['functor'] = _locals
            self._trace(' ws', (2441, 2444), self.input.position)
            _G_apply_376, lastError = self._apply(self.rule_ws, "ws", [])
            self.considerError(lastError, 'functor')
            def _G_or_377():
                self._trace('literal', (2446, 2453), self.input.position)
                _G_apply_378, lastError = self._apply(self.rule_literal, "literal", [])
                self.considerError(lastError, None)
                return (_G_apply_378, self.currentError)
            def _G_or_379():
                self._trace(' tag', (2455, 2459), self.input.position)
                _G_apply_380, lastError = self._apply(self.rule_tag, "tag", [])
                self.considerError(lastError, None)
                _locals['t'] = _G_apply_380
                _G_python_381, lastError = eval('leafInternal(t, None)', self.globals, _locals), None
                self.considerError(lastError, None)
                return (_G_python_381, self.currentError)
            _G_or_382, lastError = self._or([_G_or_377, _G_or_379])
            self.considerError(lastError, 'functor')
            return (_G_or_382, self.currentError)


        def rule_baseTerm(self):
            _locals = {'self': self}
            self.locals['baseTerm'] = _locals
            self._trace(' functor', (2498, 2506), self.input.position)
            _G_apply_383, lastError = self._apply(self.rule_functor, "functor", [])
            self.considerError(lastError, 'baseTerm')
            _locals['f'] = _G_apply_383
            def _G_or_384():
                self._trace("'('", (2510, 2513), self.input.position)
                _G_exactly_385, lastError = self.exactly('(')
                self.considerError(lastError, None)
                self._trace(' argList', (2513, 2521), self.input.position)
                _G_apply_386, lastError = self._apply(self.rule_argList, "argList", [])
                self.considerError(lastError, None)
                _locals['a'] = _G_apply_386
                self._trace(' ws', (2523, 2526), self.input.position)
                _G_apply_387, lastError = self._apply(self.rule_ws, "ws", [])
                self.considerError(lastError, None)
                self._trace(" ')'", (2526, 2530), self.input.position)
                _G_exactly_388, lastError = self.exactly(')')
                self.considerError(lastError, None)
                _G_python_389, lastError = eval('makeTerm(f, a)', self.globals, _locals), None
                self.considerError(lastError, None)
                return (_G_python_389, self.currentError)
            def _G_or_390():
                _G_python_391, lastError = eval('makeTerm(f, None)', self.globals, _locals), None
                self.considerError(lastError, None)
                return (_G_python_391, self.currentError)
            _G_or_392, lastError = self._or([_G_or_384, _G_or_390])
            self.considerError(lastError, 'baseTerm')
            return (_G_or_392, self.currentError)


        def rule_arg(self):
            _locals = {'self': self}
            self.locals['arg'] = _locals
            self._trace(' term', (2600, 2605), self.input.position)
            _G_apply_393, lastError = self._apply(self.rule_term, "term", [])
            self.considerError(lastError, 'arg')
            return (_G_apply_393, self.currentError)


        def rule_argList(self):
            _locals = {'self': self}
            self.locals['argList'] = _locals
            def _G_or_394():
                self._trace('arg', (2619, 2622), self.input.position)
                _G_apply_395, lastError = self._apply(self.rule_arg, "arg", [])
                self.considerError(lastError, None)
                _locals['t'] = _G_apply_395
                def _G_many_396():
                    self._trace('ws', (2626, 2628), self.input.position)
                    _G_apply_397, lastError = self._apply(self.rule_ws, "ws", [])
                    self.considerError(lastError, None)
                    self._trace(" ','", (2628, 2632), self.input.position)
                    _G_exactly_398, lastError = self.exactly(',')
                    self.considerError(lastError, None)
                    self._trace(' arg', (2632, 2636), self.input.position)
                    _G_apply_399, lastError = self._apply(self.rule_arg, "arg", [])
                    self.considerError(lastError, None)
                    return (_G_apply_399, self.currentError)
                _G_many_400, lastError = self.many(_G_many_396)
                self.considerError(lastError, None)
                _locals['ts'] = _G_many_400
                self._trace(' ws', (2641, 2644), self.input.position)
                _G_apply_401, lastError = self._apply(self.rule_ws, "ws", [])
                self.considerError(lastError, None)
                def _G_optional_402():
                    self._trace(" ','", (2644, 2648), self.input.position)
                    _G_exactly_403, lastError = self.exactly(',')
                    self.considerError(lastError, None)
                    return (_G_exactly_403, self.currentError)
                def _G_optional_404():
                    return (None, self.input.nullError())
                _G_or_405, lastError = self._or([_G_optional_402, _G_optional_404])
                self.considerError(lastError, None)
                _G_python_406, lastError = eval('cons(t, ts)', self.globals, _locals), None
                self.considerError(lastError, None)
                return (_G_python_406, self.currentError)
            def _G_or_407():
                _G_python_408, lastError = ([]), None
                self.considerError(lastError, None)
                return (_G_python_408, self.currentError)
            _G_or_409, lastError = self._or([_G_or_394, _G_or_407])
            self.considerError(lastError, 'argList')
            return (_G_or_409, self.currentError)


        def rule_tupleTerm(self):
            _locals = {'self': self}
            self.locals['tupleTerm'] = _locals
            self._trace(' ws', (2699, 2702), self.input.position)
            _G_apply_410, lastError = self._apply(self.rule_ws, "ws", [])
            self.considerError(lastError, 'tupleTerm')
            self._trace(" '['", (2702, 2706), self.input.position)
            _G_exactly_411, lastError = self.exactly('[')
            self.considerError(lastError, 'tupleTerm')
            self._trace(' argList', (2706, 2714), self.input.position)
            _G_apply_412, lastError = self._apply(self.rule_argList, "argList", [])
            self.considerError(lastError, 'tupleTerm')
            _locals['a'] = _G_apply_412
            self._trace(' ws', (2716, 2719), self.input.position)
            _G_apply_413, lastError = self._apply(self.rule_ws, "ws", [])
            self.considerError(lastError, 'tupleTerm')
            self._trace(" ']'", (2719, 2723), self.input.position)
            _G_exactly_414, lastError = self.exactly(']')
            self.considerError(lastError, 'tupleTerm')
            _G_python_415, lastError = eval('Tuple(a)', self.globals, _locals), None
            self.considerError(lastError, 'tupleTerm')
            return (_G_python_415, self.currentError)


        def rule_bagTerm(self):
            _locals = {'self': self}
            self.locals['bagTerm'] = _locals
            self._trace(' ws', (2746, 2749), self.input.position)
            _G_apply_416, lastError = self._apply(self.rule_ws, "ws", [])
            self.considerError(lastError, 'bagTerm')
            self._trace(" '{'", (2749, 2753), self.input.position)
            _G_exactly_417, lastError = self.exactly('{')
            self.considerError(lastError, 'bagTerm')
            self._trace(' argList', (2753, 2761), self.input.position)
            _G_apply_418, lastError = self._apply(self.rule_argList, "argList", [])
            self.considerError(lastError, 'bagTerm')
            _locals['a'] = _G_apply_418
            self._trace(' ws', (2763, 2766), self.input.position)
            _G_apply_419, lastError = self._apply(self.rule_ws, "ws", [])
            self.considerError(lastError, 'bagTerm')
            self._trace(" '}'", (2766, 2770), self.input.position)
            _G_exactly_420, lastError = self.exactly('}')
            self.considerError(lastError, 'bagTerm')
            _G_python_421, lastError = eval('Bag(a)', self.globals, _locals), None
            self.considerError(lastError, 'bagTerm')
            return (_G_python_421, self.currentError)


        def rule_labelledBagTerm(self):
            _locals = {'self': self}
            self.locals['labelledBagTerm'] = _locals
            self._trace(' functor', (2799, 2807), self.input.position)
            _G_apply_422, lastError = self._apply(self.rule_functor, "functor", [])
            self.considerError(lastError, 'labelledBagTerm')
            _locals['f'] = _G_apply_422
            self._trace(' bagTerm', (2809, 2817), self.input.position)
            _G_apply_423, lastError = self._apply(self.rule_bagTerm, "bagTerm", [])
            self.considerError(lastError, 'labelledBagTerm')
            _locals['b'] = _G_apply_423
            _G_python_424, lastError = eval('LabelledBag(f, b)', self.globals, _locals), None
            self.considerError(lastError, 'labelledBagTerm')
            return (_G_python_424, self.currentError)


        def rule_extraTerm(self):
            _locals = {'self': self}
            self.locals['extraTerm'] = _locals
            def _G_or_425():
                self._trace(' tupleTerm', (2853, 2863), self.input.position)
                _G_apply_426, lastError = self._apply(self.rule_tupleTerm, "tupleTerm", [])
                self.considerError(lastError, None)
                return (_G_apply_426, self.currentError)
            def _G_or_427():
                self._trace(' labelledBagTerm', (2865, 2881), self.input.position)
                _G_apply_428, lastError = self._apply(self.rule_labelledBagTerm, "labelledBagTerm", [])
                self.considerError(lastError, None)
                return (_G_apply_428, self.currentError)
            def _G_or_429():
                self._trace(' bagTerm', (2884, 2892), self.input.position)
                _G_apply_430, lastError = self._apply(self.rule_bagTerm, "bagTerm", [])
                self.considerError(lastError, None)
                return (_G_apply_430, self.currentError)
            def _G_or_431():
                self._trace(' baseTerm', (2894, 2903), self.input.position)
                _G_apply_432, lastError = self._apply(self.rule_baseTerm, "baseTerm", [])
                self.considerError(lastError, None)
                return (_G_apply_432, self.currentError)
            _G_or_433, lastError = self._or([_G_or_425, _G_or_427, _G_or_429, _G_or_431])
            self.considerError(lastError, 'extraTerm')
            return (_G_or_433, self.currentError)


        def rule_attrTerm(self):
            _locals = {'self': self}
            self.locals['attrTerm'] = _locals
            self._trace(' extraTerm', (2915, 2925), self.input.position)
            _G_apply_434, lastError = self._apply(self.rule_extraTerm, "extraTerm", [])
            self.considerError(lastError, 'attrTerm')
            _locals['k'] = _G_apply_434
            self._trace(' ws', (2927, 2930), self.input.position)
            _G_apply_435, lastError = self._apply(self.rule_ws, "ws", [])
            self.considerError(lastError, 'attrTerm')
            self._trace(" ':'", (2930, 2934), self.input.position)
            _G_exactly_436, lastError = self.exactly(':')
            self.considerError(lastError, 'attrTerm')
            self._trace(' extraTerm', (2934, 2944), self.input.position)
            _G_apply_437, lastError = self._apply(self.rule_extraTerm, "extraTerm", [])
            self.considerError(lastError, 'attrTerm')
            _locals['v'] = _G_apply_437
            _G_python_438, lastError = eval('Attr(k, v)', self.globals, _locals), None
            self.considerError(lastError, 'attrTerm')
            return (_G_python_438, self.currentError)


        def rule_term(self):
            _locals = {'self': self}
            self.locals['term'] = _locals
            self._trace(' ws', (2968, 2971), self.input.position)
            _G_apply_439, lastError = self._apply(self.rule_ws, "ws", [])
            self.considerError(lastError, 'term')
            def _G_or_440():
                self._trace('attrTerm', (2973, 2981), self.input.position)
                _G_apply_441, lastError = self._apply(self.rule_attrTerm, "attrTerm", [])
                self.considerError(lastError, None)
                return (_G_apply_441, self.currentError)
            def _G_or_442():
                self._trace(' extraTerm', (2983, 2993), self.input.position)
                _G_apply_443, lastError = self._apply(self.rule_extraTerm, "extraTerm", [])
                self.considerError(lastError, None)
                return (_G_apply_443, self.currentError)
            _G_or_444, lastError = self._or([_G_or_440, _G_or_442])
            self.considerError(lastError, 'term')
            return (_G_or_444, self.currentError)


    if terml.globals is not None:
        terml.globals = terml.globals.copy()
        terml.globals.update(ruleGlobals)
    else:
        terml.globals = ruleGlobals
    return terml
