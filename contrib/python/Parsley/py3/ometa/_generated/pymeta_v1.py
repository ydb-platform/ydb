def createParserClass(GrammarBase, ruleGlobals):
    if ruleGlobals is None:
        ruleGlobals = {}
    class pymeta_v1(GrammarBase):
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


        def rule_number(self):
            _locals = {'self': self}
            self.locals['number'] = _locals
            self._trace(' ws', (136, 139), self.input.position)
            _G_apply_31, lastError = self._apply(self.rule_ws, "ws", [])
            self.considerError(lastError, 'number')
            def _G_or_32():
                self._trace("'-'", (141, 144), self.input.position)
                _G_exactly_33, lastError = self.exactly('-')
                self.considerError(lastError, None)
                self._trace(' barenumber', (144, 155), self.input.position)
                _G_apply_34, lastError = self._apply(self.rule_barenumber, "barenumber", [])
                self.considerError(lastError, None)
                _locals['x'] = _G_apply_34
                _G_python_35, lastError = eval('t.Exactly(-x)', self.globals, _locals), None
                self.considerError(lastError, None)
                return (_G_python_35, self.currentError)
            def _G_or_36():
                self._trace('barenumber', (192, 202), self.input.position)
                _G_apply_37, lastError = self._apply(self.rule_barenumber, "barenumber", [])
                self.considerError(lastError, None)
                _locals['x'] = _G_apply_37
                _G_python_38, lastError = eval('t.Exactly(x)', self.globals, _locals), None
                self.considerError(lastError, None)
                return (_G_python_38, self.currentError)
            _G_or_39, lastError = self._or([_G_or_32, _G_or_36])
            self.considerError(lastError, 'number')
            return (_G_or_39, self.currentError)


        def rule_barenumber(self):
            _locals = {'self': self}
            self.locals['barenumber'] = _locals
            def _G_or_40():
                self._trace(" '0'", (234, 238), self.input.position)
                _G_exactly_41, lastError = self.exactly('0')
                self.considerError(lastError, None)
                def _G_or_42():
                    def _G_or_43():
                        self._trace("'x'", (241, 244), self.input.position)
                        _G_exactly_44, lastError = self.exactly('x')
                        self.considerError(lastError, None)
                        return (_G_exactly_44, self.currentError)
                    def _G_or_45():
                        self._trace("'X'", (245, 248), self.input.position)
                        _G_exactly_46, lastError = self.exactly('X')
                        self.considerError(lastError, None)
                        return (_G_exactly_46, self.currentError)
                    _G_or_47, lastError = self._or([_G_or_43, _G_or_45])
                    self.considerError(lastError, None)
                    def _G_consumedby_48():
                        def _G_many1_49():
                            self._trace('hexdigit', (251, 259), self.input.position)
                            _G_apply_50, lastError = self._apply(self.rule_hexdigit, "hexdigit", [])
                            self.considerError(lastError, None)
                            return (_G_apply_50, self.currentError)
                        _G_many1_51, lastError = self.many(_G_many1_49, _G_many1_49())
                        self.considerError(lastError, None)
                        return (_G_many1_51, self.currentError)
                    _G_consumedby_52, lastError = self.consumedby(_G_consumedby_48)
                    self.considerError(lastError, None)
                    _locals['hs'] = _G_consumedby_52
                    _G_python_53, lastError = eval('int(hs, 16)', self.globals, _locals), None
                    self.considerError(lastError, None)
                    return (_G_python_53, self.currentError)
                def _G_or_54():
                    def _G_consumedby_55():
                        def _G_many1_56():
                            self._trace('octaldigit', (302, 312), self.input.position)
                            _G_apply_57, lastError = self._apply(self.rule_octaldigit, "octaldigit", [])
                            self.considerError(lastError, None)
                            return (_G_apply_57, self.currentError)
                        _G_many1_58, lastError = self.many(_G_many1_56, _G_many1_56())
                        self.considerError(lastError, None)
                        return (_G_many1_58, self.currentError)
                    _G_consumedby_59, lastError = self.consumedby(_G_consumedby_55)
                    self.considerError(lastError, None)
                    _locals['ds'] = _G_consumedby_59
                    _G_python_60, lastError = eval('int(ds, 8)', self.globals, _locals), None
                    self.considerError(lastError, None)
                    return (_G_python_60, self.currentError)
                _G_or_61, lastError = self._or([_G_or_42, _G_or_54])
                self.considerError(lastError, None)
                return (_G_or_61, self.currentError)
            def _G_or_62():
                def _G_consumedby_63():
                    def _G_many1_64():
                        self._trace('digit', (350, 355), self.input.position)
                        _G_apply_65, lastError = self._apply(self.rule_digit, "digit", [])
                        self.considerError(lastError, None)
                        return (_G_apply_65, self.currentError)
                    _G_many1_66, lastError = self.many(_G_many1_64, _G_many1_64())
                    self.considerError(lastError, None)
                    return (_G_many1_66, self.currentError)
                _G_consumedby_67, lastError = self.consumedby(_G_consumedby_63)
                self.considerError(lastError, None)
                _locals['ds'] = _G_consumedby_67
                _G_python_68, lastError = eval('int(ds)', self.globals, _locals), None
                self.considerError(lastError, None)
                return (_G_python_68, self.currentError)
            _G_or_69, lastError = self._or([_G_or_40, _G_or_62])
            self.considerError(lastError, 'barenumber')
            return (_G_or_69, self.currentError)


        def rule_octaldigit(self):
            _locals = {'self': self}
            self.locals['octaldigit'] = _locals
            _G_apply_70, lastError = self._apply(self.rule_anything, "anything", [])
            self.considerError(lastError, 'octaldigit')
            _locals['x'] = _G_apply_70
            def _G_pred_71():
                _G_python_72, lastError = eval("x in '01234567'", self.globals, _locals), None
                self.considerError(lastError, None)
                return (_G_python_72, self.currentError)
            _G_pred_73, lastError = self.pred(_G_pred_71)
            self.considerError(lastError, 'octaldigit')
            _G_python_74, lastError = eval('x', self.globals, _locals), None
            self.considerError(lastError, 'octaldigit')
            return (_G_python_74, self.currentError)


        def rule_hexdigit(self):
            _locals = {'self': self}
            self.locals['hexdigit'] = _locals
            _G_apply_75, lastError = self._apply(self.rule_anything, "anything", [])
            self.considerError(lastError, 'hexdigit')
            _locals['x'] = _G_apply_75
            def _G_pred_76():
                _G_python_77, lastError = eval("x in '0123456789ABCDEFabcdef'", self.globals, _locals), None
                self.considerError(lastError, None)
                return (_G_python_77, self.currentError)
            _G_pred_78, lastError = self.pred(_G_pred_76)
            self.considerError(lastError, 'hexdigit')
            _G_python_79, lastError = eval('x', self.globals, _locals), None
            self.considerError(lastError, 'hexdigit')
            return (_G_python_79, self.currentError)


        def rule_escapedChar(self):
            _locals = {'self': self}
            self.locals['escapedChar'] = _locals
            self._trace(" '\\\\'", (479, 484), self.input.position)
            _G_exactly_80, lastError = self.exactly('\\')
            self.considerError(lastError, 'escapedChar')
            def _G_or_81():
                self._trace("'n'", (486, 489), self.input.position)
                _G_exactly_82, lastError = self.exactly('n')
                self.considerError(lastError, None)
                _G_python_83, lastError = ("\n"), None
                self.considerError(lastError, None)
                return (_G_python_83, self.currentError)
            def _G_or_84():
                self._trace("'r'", (520, 523), self.input.position)
                _G_exactly_85, lastError = self.exactly('r')
                self.considerError(lastError, None)
                _G_python_86, lastError = ("\r"), None
                self.considerError(lastError, None)
                return (_G_python_86, self.currentError)
            def _G_or_87():
                self._trace("'t'", (554, 557), self.input.position)
                _G_exactly_88, lastError = self.exactly('t')
                self.considerError(lastError, None)
                _G_python_89, lastError = ("\t"), None
                self.considerError(lastError, None)
                return (_G_python_89, self.currentError)
            def _G_or_90():
                self._trace("'b'", (588, 591), self.input.position)
                _G_exactly_91, lastError = self.exactly('b')
                self.considerError(lastError, None)
                _G_python_92, lastError = ("\b"), None
                self.considerError(lastError, None)
                return (_G_python_92, self.currentError)
            def _G_or_93():
                self._trace("'f'", (622, 625), self.input.position)
                _G_exactly_94, lastError = self.exactly('f')
                self.considerError(lastError, None)
                _G_python_95, lastError = ("\f"), None
                self.considerError(lastError, None)
                return (_G_python_95, self.currentError)
            def _G_or_96():
                self._trace('\'"\'', (656, 659), self.input.position)
                _G_exactly_97, lastError = self.exactly('"')
                self.considerError(lastError, None)
                _G_python_98, lastError = ('"'), None
                self.considerError(lastError, None)
                return (_G_python_98, self.currentError)
            def _G_or_99():
                self._trace("'\\''", (689, 693), self.input.position)
                _G_exactly_100, lastError = self.exactly("'")
                self.considerError(lastError, None)
                _G_python_101, lastError = ("'"), None
                self.considerError(lastError, None)
                return (_G_python_101, self.currentError)
            def _G_or_102():
                self._trace("'\\\\'", (723, 727), self.input.position)
                _G_exactly_103, lastError = self.exactly('\\')
                self.considerError(lastError, None)
                _G_python_104, lastError = ("\\"), None
                self.considerError(lastError, None)
                return (_G_python_104, self.currentError)
            _G_or_105, lastError = self._or([_G_or_81, _G_or_84, _G_or_87, _G_or_90, _G_or_93, _G_or_96, _G_or_99, _G_or_102])
            self.considerError(lastError, 'escapedChar')
            return (_G_or_105, self.currentError)


        def rule_character(self):
            _locals = {'self': self}
            self.locals['character'] = _locals
            self._trace(' ws', (749, 752), self.input.position)
            _G_apply_106, lastError = self._apply(self.rule_ws, "ws", [])
            self.considerError(lastError, 'character')
            self._trace(" '\\''", (752, 757), self.input.position)
            _G_exactly_107, lastError = self.exactly("'")
            self.considerError(lastError, 'character')
            def _G_or_108():
                self._trace('escapedChar', (759, 770), self.input.position)
                _G_apply_109, lastError = self._apply(self.rule_escapedChar, "escapedChar", [])
                self.considerError(lastError, None)
                return (_G_apply_109, self.currentError)
            def _G_or_110():
                self._trace(' anything', (772, 781), self.input.position)
                _G_apply_111, lastError = self._apply(self.rule_anything, "anything", [])
                self.considerError(lastError, None)
                return (_G_apply_111, self.currentError)
            _G_or_112, lastError = self._or([_G_or_108, _G_or_110])
            self.considerError(lastError, 'character')
            _locals['c'] = _G_or_112
            self._trace(' ws', (784, 787), self.input.position)
            _G_apply_113, lastError = self._apply(self.rule_ws, "ws", [])
            self.considerError(lastError, 'character')
            self._trace(" '\\''", (787, 792), self.input.position)
            _G_exactly_114, lastError = self.exactly("'")
            self.considerError(lastError, 'character')
            _G_python_115, lastError = eval('t.Exactly(c)', self.globals, _locals), None
            self.considerError(lastError, 'character')
            return (_G_python_115, self.currentError)


        def rule_string(self):
            _locals = {'self': self}
            self.locals['string'] = _locals
            self._trace(' ws', (818, 821), self.input.position)
            _G_apply_116, lastError = self._apply(self.rule_ws, "ws", [])
            self.considerError(lastError, 'string')
            self._trace(' \'"\'', (821, 825), self.input.position)
            _G_exactly_117, lastError = self.exactly('"')
            self.considerError(lastError, 'string')
            def _G_many_118():
                def _G_or_119():
                    self._trace('escapedChar', (827, 838), self.input.position)
                    _G_apply_120, lastError = self._apply(self.rule_escapedChar, "escapedChar", [])
                    self.considerError(lastError, None)
                    return (_G_apply_120, self.currentError)
                def _G_or_121():
                    def _G_not_122():
                        self._trace('\'"\'', (843, 846), self.input.position)
                        _G_exactly_123, lastError = self.exactly('"')
                        self.considerError(lastError, None)
                        return (_G_exactly_123, self.currentError)
                    _G_not_124, lastError = self._not(_G_not_122)
                    self.considerError(lastError, None)
                    self._trace(' anything', (847, 856), self.input.position)
                    _G_apply_125, lastError = self._apply(self.rule_anything, "anything", [])
                    self.considerError(lastError, None)
                    return (_G_apply_125, self.currentError)
                _G_or_126, lastError = self._or([_G_or_119, _G_or_121])
                self.considerError(lastError, None)
                return (_G_or_126, self.currentError)
            _G_many_127, lastError = self.many(_G_many_118)
            self.considerError(lastError, 'string')
            _locals['c'] = _G_many_127
            self._trace(' \'"\'', (860, 864), self.input.position)
            _G_exactly_128, lastError = self.exactly('"')
            self.considerError(lastError, 'string')
            _G_python_129, lastError = eval("t.Exactly(''.join(c))", self.globals, _locals), None
            self.considerError(lastError, 'string')
            return (_G_python_129, self.currentError)


        def rule_name(self):
            _locals = {'self': self}
            self.locals['name'] = _locals
            def _G_consumedby_130():
                self._trace('letter', (899, 905), self.input.position)
                _G_apply_131, lastError = self._apply(self.rule_letter, "letter", [])
                self.considerError(lastError, None)
                def _G_many_132():
                    def _G_or_133():
                        self._trace('letterOrDigit', (907, 920), self.input.position)
                        _G_apply_134, lastError = self._apply(self.rule_letterOrDigit, "letterOrDigit", [])
                        self.considerError(lastError, None)
                        return (_G_apply_134, self.currentError)
                    def _G_or_135():
                        self._trace(" '_'", (922, 926), self.input.position)
                        _G_exactly_136, lastError = self.exactly('_')
                        self.considerError(lastError, None)
                        return (_G_exactly_136, self.currentError)
                    _G_or_137, lastError = self._or([_G_or_133, _G_or_135])
                    self.considerError(lastError, None)
                    return (_G_or_137, self.currentError)
                _G_many_138, lastError = self.many(_G_many_132)
                self.considerError(lastError, None)
                return (_G_many_138, self.currentError)
            _G_consumedby_139, lastError = self.consumedby(_G_consumedby_130)
            self.considerError(lastError, 'name')
            return (_G_consumedby_139, self.currentError)


        def rule_application(self):
            _locals = {'self': self}
            self.locals['application'] = _locals
            self._trace('ws', (945, 947), self.input.position)
            _G_apply_140, lastError = self._apply(self.rule_ws, "ws", [])
            self.considerError(lastError, 'application')
            self._trace(" '<'", (947, 951), self.input.position)
            _G_exactly_141, lastError = self.exactly('<')
            self.considerError(lastError, 'application')
            self._trace(' ws', (951, 954), self.input.position)
            _G_apply_142, lastError = self._apply(self.rule_ws, "ws", [])
            self.considerError(lastError, 'application')
            self._trace(' name', (954, 959), self.input.position)
            _G_apply_143, lastError = self._apply(self.rule_name, "name", [])
            self.considerError(lastError, 'application')
            _locals['name'] = _G_apply_143
            def _G_or_144():
                self._trace("' '", (984, 987), self.input.position)
                _G_exactly_145, lastError = self.exactly(' ')
                self.considerError(lastError, None)
                _G_python_146, lastError = eval("self.applicationArgs(finalChar='>')", self.globals, _locals), None
                self.considerError(lastError, None)
                _locals['args'] = _G_python_146
                self._trace(" '>'", (1031, 1035), self.input.position)
                _G_exactly_147, lastError = self.exactly('>')
                self.considerError(lastError, None)
                _G_python_148, lastError = eval('t.Apply(name, self.rulename, args)', self.globals, _locals), None
                self.considerError(lastError, None)
                return (_G_python_148, self.currentError)
            def _G_or_149():
                self._trace('ws', (1114, 1116), self.input.position)
                _G_apply_150, lastError = self._apply(self.rule_ws, "ws", [])
                self.considerError(lastError, None)
                self._trace(" '>'", (1116, 1120), self.input.position)
                _G_exactly_151, lastError = self.exactly('>')
                self.considerError(lastError, None)
                _G_python_152, lastError = eval('t.Apply(name, self.rulename, [])', self.globals, _locals), None
                self.considerError(lastError, None)
                return (_G_python_152, self.currentError)
            _G_or_153, lastError = self._or([_G_or_144, _G_or_149])
            self.considerError(lastError, 'application')
            return (_G_or_153, self.currentError)


        def rule_expr1(self):
            _locals = {'self': self}
            self.locals['expr1'] = _locals
            def _G_or_154():
                self._trace('application', (1190, 1201), self.input.position)
                _G_apply_155, lastError = self._apply(self.rule_application, "application", [])
                self.considerError(lastError, None)
                return (_G_apply_155, self.currentError)
            def _G_or_156():
                self._trace('ruleValue', (1213, 1222), self.input.position)
                _G_apply_157, lastError = self._apply(self.rule_ruleValue, "ruleValue", [])
                self.considerError(lastError, None)
                return (_G_apply_157, self.currentError)
            def _G_or_158():
                self._trace('semanticPredicate', (1234, 1251), self.input.position)
                _G_apply_159, lastError = self._apply(self.rule_semanticPredicate, "semanticPredicate", [])
                self.considerError(lastError, None)
                return (_G_apply_159, self.currentError)
            def _G_or_160():
                self._trace('semanticAction', (1263, 1277), self.input.position)
                _G_apply_161, lastError = self._apply(self.rule_semanticAction, "semanticAction", [])
                self.considerError(lastError, None)
                return (_G_apply_161, self.currentError)
            def _G_or_162():
                self._trace('number', (1289, 1295), self.input.position)
                _G_apply_163, lastError = self._apply(self.rule_number, "number", [])
                self.considerError(lastError, None)
                _locals['n'] = _G_apply_163
                _G_python_164, lastError = eval('self.isTree()', self.globals, _locals), None
                self.considerError(lastError, None)
                _G_python_165, lastError = eval('n', self.globals, _locals), None
                self.considerError(lastError, None)
                return (_G_python_165, self.currentError)
            def _G_or_166():
                self._trace('character', (1331, 1340), self.input.position)
                _G_apply_167, lastError = self._apply(self.rule_character, "character", [])
                self.considerError(lastError, None)
                return (_G_apply_167, self.currentError)
            def _G_or_168():
                self._trace('string', (1352, 1358), self.input.position)
                _G_apply_169, lastError = self._apply(self.rule_string, "string", [])
                self.considerError(lastError, None)
                return (_G_apply_169, self.currentError)
            def _G_or_170():
                self._trace('ws', (1370, 1372), self.input.position)
                _G_apply_171, lastError = self._apply(self.rule_ws, "ws", [])
                self.considerError(lastError, None)
                self._trace(" '('", (1372, 1376), self.input.position)
                _G_exactly_172, lastError = self.exactly('(')
                self.considerError(lastError, None)
                self._trace(' expr', (1376, 1381), self.input.position)
                _G_apply_173, lastError = self._apply(self.rule_expr, "expr", [])
                self.considerError(lastError, None)
                _locals['e'] = _G_apply_173
                self._trace(' ws', (1383, 1386), self.input.position)
                _G_apply_174, lastError = self._apply(self.rule_ws, "ws", [])
                self.considerError(lastError, None)
                self._trace(" ')'", (1386, 1390), self.input.position)
                _G_exactly_175, lastError = self.exactly(')')
                self.considerError(lastError, None)
                _G_python_176, lastError = eval('e', self.globals, _locals), None
                self.considerError(lastError, None)
                return (_G_python_176, self.currentError)
            def _G_or_177():
                self._trace('ws', (1408, 1410), self.input.position)
                _G_apply_178, lastError = self._apply(self.rule_ws, "ws", [])
                self.considerError(lastError, None)
                self._trace(" '['", (1410, 1414), self.input.position)
                _G_exactly_179, lastError = self.exactly('[')
                self.considerError(lastError, None)
                self._trace(' expr', (1414, 1419), self.input.position)
                _G_apply_180, lastError = self._apply(self.rule_expr, "expr", [])
                self.considerError(lastError, None)
                _locals['e'] = _G_apply_180
                self._trace(' ws', (1421, 1424), self.input.position)
                _G_apply_181, lastError = self._apply(self.rule_ws, "ws", [])
                self.considerError(lastError, None)
                self._trace(" ']'", (1424, 1428), self.input.position)
                _G_exactly_182, lastError = self.exactly(']')
                self.considerError(lastError, None)
                _G_python_183, lastError = eval('self.isTree()', self.globals, _locals), None
                self.considerError(lastError, None)
                _G_python_184, lastError = eval('t.List(e)', self.globals, _locals), None
                self.considerError(lastError, None)
                return (_G_python_184, self.currentError)
            _G_or_185, lastError = self._or([_G_or_154, _G_or_156, _G_or_158, _G_or_160, _G_or_162, _G_or_166, _G_or_168, _G_or_170, _G_or_177])
            self.considerError(lastError, 'expr1')
            return (_G_or_185, self.currentError)


        def rule_expr2(self):
            _locals = {'self': self}
            self.locals['expr2'] = _locals
            def _G_or_186():
                self._trace('ws', (1485, 1487), self.input.position)
                _G_apply_187, lastError = self._apply(self.rule_ws, "ws", [])
                self.considerError(lastError, None)
                self._trace(" '~'", (1487, 1491), self.input.position)
                _G_exactly_188, lastError = self.exactly('~')
                self.considerError(lastError, None)
                def _G_or_189():
                    self._trace("'~'", (1493, 1496), self.input.position)
                    _G_exactly_190, lastError = self.exactly('~')
                    self.considerError(lastError, None)
                    self._trace(' expr2', (1496, 1502), self.input.position)
                    _G_apply_191, lastError = self._apply(self.rule_expr2, "expr2", [])
                    self.considerError(lastError, None)
                    _locals['e'] = _G_apply_191
                    _G_python_192, lastError = eval('t.Lookahead(e)', self.globals, _locals), None
                    self.considerError(lastError, None)
                    return (_G_python_192, self.currentError)
                def _G_or_193():
                    self._trace('expr2', (1568, 1573), self.input.position)
                    _G_apply_194, lastError = self._apply(self.rule_expr2, "expr2", [])
                    self.considerError(lastError, None)
                    _locals['e'] = _G_apply_194
                    _G_python_195, lastError = eval('t.Not(e)', self.globals, _locals), None
                    self.considerError(lastError, None)
                    return (_G_python_195, self.currentError)
                _G_or_196, lastError = self._or([_G_or_189, _G_or_193])
                self.considerError(lastError, None)
                return (_G_or_196, self.currentError)
            def _G_or_197():
                self._trace('expr1', (1598, 1603), self.input.position)
                _G_apply_198, lastError = self._apply(self.rule_expr1, "expr1", [])
                self.considerError(lastError, None)
                return (_G_apply_198, self.currentError)
            _G_or_199, lastError = self._or([_G_or_186, _G_or_197])
            self.considerError(lastError, 'expr2')
            return (_G_or_199, self.currentError)


        def rule_expr3(self):
            _locals = {'self': self}
            self.locals['expr3'] = _locals
            def _G_or_200():
                self._trace(' expr2', (1613, 1619), self.input.position)
                _G_apply_201, lastError = self._apply(self.rule_expr2, "expr2", [])
                self.considerError(lastError, None)
                _locals['e'] = _G_apply_201
                def _G_or_202():
                    self._trace("'*'", (1637, 1640), self.input.position)
                    _G_exactly_203, lastError = self.exactly('*')
                    self.considerError(lastError, None)
                    _G_python_204, lastError = eval('t.Many(e)', self.globals, _locals), None
                    self.considerError(lastError, None)
                    return (_G_python_204, self.currentError)
                def _G_or_205():
                    self._trace("'+'", (1669, 1672), self.input.position)
                    _G_exactly_206, lastError = self.exactly('+')
                    self.considerError(lastError, None)
                    _G_python_207, lastError = eval('t.Many1(e)', self.globals, _locals), None
                    self.considerError(lastError, None)
                    return (_G_python_207, self.currentError)
                def _G_or_208():
                    self._trace("'?'", (1702, 1705), self.input.position)
                    _G_exactly_209, lastError = self.exactly('?')
                    self.considerError(lastError, None)
                    _G_python_210, lastError = eval('t.Optional(e)', self.globals, _locals), None
                    self.considerError(lastError, None)
                    return (_G_python_210, self.currentError)
                def _G_or_211():
                    _G_python_212, lastError = eval('e', self.globals, _locals), None
                    self.considerError(lastError, None)
                    return (_G_python_212, self.currentError)
                _G_or_213, lastError = self._or([_G_or_202, _G_or_205, _G_or_208, _G_or_211])
                self.considerError(lastError, None)
                _locals['r'] = _G_or_213
                def _G_or_214():
                    self._trace("':'", (1759, 1762), self.input.position)
                    _G_exactly_215, lastError = self.exactly(':')
                    self.considerError(lastError, None)
                    self._trace(' name', (1762, 1767), self.input.position)
                    _G_apply_216, lastError = self._apply(self.rule_name, "name", [])
                    self.considerError(lastError, None)
                    _locals['n'] = _G_apply_216
                    _G_python_217, lastError = eval('t.Bind(n, r)', self.globals, _locals), None
                    self.considerError(lastError, None)
                    return (_G_python_217, self.currentError)
                def _G_or_218():
                    _G_python_219, lastError = eval('r', self.globals, _locals), None
                    self.considerError(lastError, None)
                    return (_G_python_219, self.currentError)
                _G_or_220, lastError = self._or([_G_or_214, _G_or_218])
                self.considerError(lastError, None)
                return (_G_or_220, self.currentError)
            def _G_or_221():
                self._trace('ws', (1818, 1820), self.input.position)
                _G_apply_222, lastError = self._apply(self.rule_ws, "ws", [])
                self.considerError(lastError, None)
                self._trace(" ':'", (1820, 1824), self.input.position)
                _G_exactly_223, lastError = self.exactly(':')
                self.considerError(lastError, None)
                self._trace(' name', (1824, 1829), self.input.position)
                _G_apply_224, lastError = self._apply(self.rule_name, "name", [])
                self.considerError(lastError, None)
                _locals['n'] = _G_apply_224
                _G_python_225, lastError = eval('t.Bind(n, t.Apply("anything", self.rulename, []))', self.globals, _locals), None
                self.considerError(lastError, None)
                return (_G_python_225, self.currentError)
            _G_or_226, lastError = self._or([_G_or_200, _G_or_221])
            self.considerError(lastError, 'expr3')
            return (_G_or_226, self.currentError)


        def rule_expr4(self):
            _locals = {'self': self}
            self.locals['expr4'] = _locals
            def _G_many_227():
                self._trace(' expr3', (1894, 1900), self.input.position)
                _G_apply_228, lastError = self._apply(self.rule_expr3, "expr3", [])
                self.considerError(lastError, None)
                return (_G_apply_228, self.currentError)
            _G_many_229, lastError = self.many(_G_many_227)
            self.considerError(lastError, 'expr4')
            _locals['es'] = _G_many_229
            _G_python_230, lastError = eval('t.And(es)', self.globals, _locals), None
            self.considerError(lastError, 'expr4')
            return (_G_python_230, self.currentError)


        def rule_expr(self):
            _locals = {'self': self}
            self.locals['expr'] = _locals
            self._trace('  expr4', (1925, 1932), self.input.position)
            _G_apply_231, lastError = self._apply(self.rule_expr4, "expr4", [])
            self.considerError(lastError, 'expr')
            _locals['e'] = _G_apply_231
            def _G_many_232():
                self._trace('ws', (1936, 1938), self.input.position)
                _G_apply_233, lastError = self._apply(self.rule_ws, "ws", [])
                self.considerError(lastError, None)
                self._trace(" '|'", (1938, 1942), self.input.position)
                _G_exactly_234, lastError = self.exactly('|')
                self.considerError(lastError, None)
                self._trace(' expr4', (1942, 1948), self.input.position)
                _G_apply_235, lastError = self._apply(self.rule_expr4, "expr4", [])
                self.considerError(lastError, None)
                return (_G_apply_235, self.currentError)
            _G_many_236, lastError = self.many(_G_many_232)
            self.considerError(lastError, 'expr')
            _locals['es'] = _G_many_236
            _G_python_237, lastError = eval('t.Or([e] + es)', self.globals, _locals), None
            self.considerError(lastError, 'expr')
            return (_G_python_237, self.currentError)


        def rule_ruleValue(self):
            _locals = {'self': self}
            self.locals['ruleValue'] = _locals
            self._trace(' ws', (1994, 1997), self.input.position)
            _G_apply_238, lastError = self._apply(self.rule_ws, "ws", [])
            self.considerError(lastError, 'ruleValue')
            self._trace(" '=>'", (1997, 2002), self.input.position)
            _G_exactly_239, lastError = self.exactly('=>')
            self.considerError(lastError, 'ruleValue')
            _G_python_240, lastError = eval('self.ruleValueExpr(False)', self.globals, _locals), None
            self.considerError(lastError, 'ruleValue')
            return (_G_python_240, self.currentError)


        def rule_semanticPredicate(self):
            _locals = {'self': self}
            self.locals['semanticPredicate'] = _locals
            self._trace('  ws', (2052, 2056), self.input.position)
            _G_apply_241, lastError = self._apply(self.rule_ws, "ws", [])
            self.considerError(lastError, 'semanticPredicate')
            self._trace(" '?('", (2056, 2061), self.input.position)
            _G_exactly_242, lastError = self.exactly('?(')
            self.considerError(lastError, 'semanticPredicate')
            _G_python_243, lastError = eval('self.semanticPredicateExpr()', self.globals, _locals), None
            self.considerError(lastError, 'semanticPredicate')
            return (_G_python_243, self.currentError)


        def rule_semanticAction(self):
            _locals = {'self': self}
            self.locals['semanticAction'] = _locals
            self._trace(' ws', (2111, 2114), self.input.position)
            _G_apply_244, lastError = self._apply(self.rule_ws, "ws", [])
            self.considerError(lastError, 'semanticAction')
            self._trace(" '!('", (2114, 2119), self.input.position)
            _G_exactly_245, lastError = self.exactly('!(')
            self.considerError(lastError, 'semanticAction')
            _G_python_246, lastError = eval('self.semanticActionExpr()', self.globals, _locals), None
            self.considerError(lastError, 'semanticAction')
            return (_G_python_246, self.currentError)


        def rule_ruleEnd(self):
            _locals = {'self': self}
            self.locals['ruleEnd'] = _locals
            def _G_or_247():
                def _G_many_248():
                    self._trace('hspace', (2161, 2167), self.input.position)
                    _G_apply_249, lastError = self._apply(self.rule_hspace, "hspace", [])
                    self.considerError(lastError, None)
                    return (_G_apply_249, self.currentError)
                _G_many_250, lastError = self.many(_G_many_248)
                self.considerError(lastError, None)
                def _G_many1_251():
                    self._trace(' vspace', (2168, 2175), self.input.position)
                    _G_apply_252, lastError = self._apply(self.rule_vspace, "vspace", [])
                    self.considerError(lastError, None)
                    return (_G_apply_252, self.currentError)
                _G_many1_253, lastError = self.many(_G_many1_251, _G_many1_251())
                self.considerError(lastError, None)
                return (_G_many1_253, self.currentError)
            def _G_or_254():
                self._trace(' end', (2179, 2183), self.input.position)
                _G_apply_255, lastError = self._apply(self.rule_end, "end", [])
                self.considerError(lastError, None)
                return (_G_apply_255, self.currentError)
            _G_or_256, lastError = self._or([_G_or_247, _G_or_254])
            self.considerError(lastError, 'ruleEnd')
            return (_G_or_256, self.currentError)


        def rule_rulePart(self):
            _locals = {'self': self}
            self.locals['rulePart'] = _locals
            _G_apply_257, lastError = self._apply(self.rule_anything, "anything", [])
            self.considerError(lastError, 'rulePart')
            _locals['requiredName'] = _G_apply_257
            self._trace(' ws', (2208, 2211), self.input.position)
            _G_apply_258, lastError = self._apply(self.rule_ws, "ws", [])
            self.considerError(lastError, 'rulePart')
            self._trace(' name', (2211, 2216), self.input.position)
            _G_apply_259, lastError = self._apply(self.rule_name, "name", [])
            self.considerError(lastError, 'rulePart')
            _locals['n'] = _G_apply_259
            def _G_pred_260():
                _G_python_261, lastError = eval('n == requiredName', self.globals, _locals), None
                self.considerError(lastError, None)
                return (_G_python_261, self.currentError)
            _G_pred_262, lastError = self.pred(_G_pred_260)
            self.considerError(lastError, 'rulePart')
            _G_python_263, lastError = eval('setattr(self, "rulename", n)', self.globals, _locals), None
            self.considerError(lastError, 'rulePart')
            self._trace('\n                            expr4', (2299, 2333), self.input.position)
            _G_apply_264, lastError = self._apply(self.rule_expr4, "expr4", [])
            self.considerError(lastError, 'rulePart')
            _locals['args'] = _G_apply_264
            def _G_or_265():
                self._trace('ws', (2368, 2370), self.input.position)
                _G_apply_266, lastError = self._apply(self.rule_ws, "ws", [])
                self.considerError(lastError, None)
                self._trace(" '::='", (2370, 2376), self.input.position)
                _G_exactly_267, lastError = self.exactly('::=')
                self.considerError(lastError, None)
                self._trace(' expr', (2376, 2381), self.input.position)
                _G_apply_268, lastError = self._apply(self.rule_expr, "expr", [])
                self.considerError(lastError, None)
                _locals['e'] = _G_apply_268
                self._trace(' ruleEnd', (2383, 2391), self.input.position)
                _G_apply_269, lastError = self._apply(self.rule_ruleEnd, "ruleEnd", [])
                self.considerError(lastError, None)
                _G_python_270, lastError = eval('t.And([args, e])', self.globals, _locals), None
                self.considerError(lastError, None)
                return (_G_python_270, self.currentError)
            def _G_or_271():
                self._trace(' ruleEnd', (2472, 2480), self.input.position)
                _G_apply_272, lastError = self._apply(self.rule_ruleEnd, "ruleEnd", [])
                self.considerError(lastError, None)
                _G_python_273, lastError = eval('args', self.globals, _locals), None
                self.considerError(lastError, None)
                return (_G_python_273, self.currentError)
            _G_or_274, lastError = self._or([_G_or_265, _G_or_271])
            self.considerError(lastError, 'rulePart')
            return (_G_or_274, self.currentError)


        def rule_rule(self):
            _locals = {'self': self}
            self.locals['rule'] = _locals
            self._trace('ws', (2498, 2500), self.input.position)
            _G_apply_275, lastError = self._apply(self.rule_ws, "ws", [])
            self.considerError(lastError, 'rule')
            def _G_lookahead_276():
                self._trace('name', (2504, 2508), self.input.position)
                _G_apply_277, lastError = self._apply(self.rule_name, "name", [])
                self.considerError(lastError, None)
                _locals['n'] = _G_apply_277
                return (_locals['n'], self.currentError)
            _G_lookahead_278, lastError = self.lookahead(_G_lookahead_276)
            self.considerError(lastError, 'rule')
            self._trace(' rulePart(n)', (2511, 2523), self.input.position)
            _G_python_279, lastError = eval('n', self.globals, _locals), None
            self.considerError(lastError, 'rule')
            _G_apply_280, lastError = self._apply(self.rule_rulePart, "rulePart", [_G_python_279])
            self.considerError(lastError, 'rule')
            _locals['r'] = _G_apply_280
            def _G_or_281():
                def _G_many1_282():
                    self._trace('rulePart(n)', (2537, 2548), self.input.position)
                    _G_python_283, lastError = eval('n', self.globals, _locals), None
                    self.considerError(lastError, None)
                    _G_apply_284, lastError = self._apply(self.rule_rulePart, "rulePart", [_G_python_283])
                    self.considerError(lastError, None)
                    return (_G_apply_284, self.currentError)
                _G_many1_285, lastError = self.many(_G_many1_282, _G_many1_282())
                self.considerError(lastError, None)
                _locals['rs'] = _G_many1_285
                _G_python_286, lastError = eval('t.Rule(n, t.Or([r] + rs))', self.globals, _locals), None
                self.considerError(lastError, None)
                return (_G_python_286, self.currentError)
            def _G_or_287():
                _G_python_288, lastError = eval('t.Rule(n, r)', self.globals, _locals), None
                self.considerError(lastError, None)
                return (_G_python_288, self.currentError)
            _G_or_289, lastError = self._or([_G_or_281, _G_or_287])
            self.considerError(lastError, 'rule')
            return (_G_or_289, self.currentError)


        def rule_grammar(self):
            _locals = {'self': self}
            self.locals['grammar'] = _locals
            def _G_many_290():
                self._trace(' rule', (2642, 2647), self.input.position)
                _G_apply_291, lastError = self._apply(self.rule_rule, "rule", [])
                self.considerError(lastError, None)
                return (_G_apply_291, self.currentError)
            _G_many_292, lastError = self.many(_G_many_290)
            self.considerError(lastError, 'grammar')
            _locals['rs'] = _G_many_292
            self._trace(' ws', (2651, 2654), self.input.position)
            _G_apply_293, lastError = self._apply(self.rule_ws, "ws", [])
            self.considerError(lastError, 'grammar')
            _G_python_294, lastError = eval('t.Grammar(self.name, self.tree_target, rs)', self.globals, _locals), None
            self.considerError(lastError, 'grammar')
            return (_G_python_294, self.currentError)


    if pymeta_v1.globals is not None:
        pymeta_v1.globals = pymeta_v1.globals.copy()
        pymeta_v1.globals.update(ruleGlobals)
    else:
        pymeta_v1.globals = ruleGlobals
    return pymeta_v1