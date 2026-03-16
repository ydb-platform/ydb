import pytest

from inflect import (
    BadChunkingOptionError,
    NumOutOfRangeError,
    BadNumValueError,
    BadGenderError,
    UnknownClassicalModeError,
)
import inflect
from inflect.compat.pydantic import same_method


missing = object()


class Test:
    def test_enclose(self):
        # def enclose
        assert inflect.enclose("test") == "(?:test)"

    def test_joinstem(self):
        # def joinstem
        assert (
            inflect.joinstem(-2, ["ephemeris", "iris", ".*itis"])
            == "(?:ephemer|ir|.*it)"
        )

    def test_classical(self):
        # classical dicts
        assert set(inflect.def_classical.keys()) == set(inflect.all_classical.keys())
        assert set(inflect.def_classical.keys()) == set(inflect.no_classical.keys())

        # def classical
        p = inflect.engine()
        assert p.classical_dict == inflect.def_classical

        p.classical()
        assert p.classical_dict == inflect.all_classical

        with pytest.raises(TypeError):
            p.classical(0)
        with pytest.raises(TypeError):
            p.classical(1)
        with pytest.raises(TypeError):
            p.classical("names")
        with pytest.raises(TypeError):
            p.classical("names", "zero")
        with pytest.raises(TypeError):
            p.classical("all")

        p.classical(all=False)
        assert p.classical_dict == inflect.no_classical

        p.classical(names=True, zero=True)
        mydict = inflect.def_classical.copy()
        mydict.update(dict(names=1, zero=1))
        assert p.classical_dict == mydict

        p.classical(all=True)
        assert p.classical_dict == inflect.all_classical

        p.classical(all=False)
        p.classical(names=True, zero=True)
        mydict = inflect.def_classical.copy()
        mydict.update(dict(names=True, zero=True))
        assert p.classical_dict == mydict

        p.classical(all=False)
        p.classical(names=True, zero=False)
        mydict = inflect.def_classical.copy()
        mydict.update(dict(names=True, zero=False))
        assert p.classical_dict == mydict

        with pytest.raises(UnknownClassicalModeError):
            p.classical(bogus=True)

    def test_num(self):
        # def num
        p = inflect.engine()
        assert p.persistent_count is None

        p.num()
        assert p.persistent_count is None

        ret = p.num(3)
        assert p.persistent_count == 3
        assert ret == "3"

        p.num()
        ret = p.num("3")
        assert p.persistent_count == 3
        assert ret == "3"

        p.num()
        ret = p.num(count=3, show=1)
        assert p.persistent_count == 3
        assert ret == "3"

        p.num()
        ret = p.num(count=3, show=0)
        assert p.persistent_count == 3
        assert ret == ""

        with pytest.raises(BadNumValueError):
            p.num("text")

    def test_inflect(self):
        p = inflect.engine()
        for txt, ans in (
            ("num(1)", "1"),
            ("num(1,0)", ""),
            ("num(1,1)", "1"),
            ("num(1)   ", "1   "),
            ("   num(1)   ", "   1   "),
            ("num(3) num(1)", "3 1"),
        ):
            assert p.inflect(txt) == ans, f'p.inflect("{txt}") != "{ans}"'

        for txt, ans in (
            ("plural('rock')", "rocks"),
            ("plural('rock')  plural('child')", "rocks  children"),
            ("num(2) plural('rock')  plural('child')", "2 rocks  children"),
            (
                "plural('rock') plural_noun('rock') plural_verb('rocks') "
                "plural_adj('big') a('ant')",
                "rocks rocks rock big an ant",
            ),
            (
                "an('rock') no('cat') ordinal(3) number_to_words(1234) "
                "present_participle('runs')",
                "a rock no cats 3rd one thousand, two hundred and thirty-four running",
            ),
            ("a('cat',0) a('cat',1) a('cat',2) a('cat', 2)", "0 cat a cat 2 cat 2 cat"),
        ):
            assert p.inflect(txt) == ans, f'p.inflect("{txt}") != "{ans}"'

    def test_user_input_fns(self):
        p = inflect.engine()

        assert p.pl_sb_user_defined == []
        p.defnoun("VAX", "VAXen")
        assert p.plural("VAX") == "VAXEN"
        assert p.pl_sb_user_defined == ["VAX", "VAXen"]

        assert p.ud_match("word", p.pl_sb_user_defined) is None
        assert p.ud_match("VAX", p.pl_sb_user_defined) == "VAXen"
        assert p.ud_match("VVAX", p.pl_sb_user_defined) is None

        p.defnoun("cow", "cows|kine")
        assert p.plural("cow") == "cows"
        p.classical()
        assert p.plural("cow") == "kine"

        assert p.ud_match("cow", p.pl_sb_user_defined) == "cows|kine"

        p.defnoun("(.+i)o", r"$1i")
        assert p.plural("studio") == "studii"
        assert p.ud_match("studio", p.pl_sb_user_defined) == "studii"

        p.defnoun("aviatrix", "aviatrices")
        assert p.plural("aviatrix") == "aviatrices"
        assert p.ud_match("aviatrix", p.pl_sb_user_defined) == "aviatrices"
        p.defnoun("aviatrix", "aviatrixes")
        assert p.plural("aviatrix") == "aviatrixes"
        assert p.ud_match("aviatrix", p.pl_sb_user_defined) == "aviatrixes"
        p.defnoun("aviatrix", None)
        assert p.plural("aviatrix") == "aviatrices"
        assert p.ud_match("aviatrix", p.pl_sb_user_defined) is None

        p.defnoun("(cat)", r"$1s")
        assert p.plural("cat") == "cats"

        with pytest.raises(inflect.BadUserDefinedPatternError):
            p.defnoun("(??", None)

        p.defnoun(None, "any")  # check None doesn't crash it

        # defadj
        p.defadj("hir", "their")
        assert p.plural("hir") == "their"
        assert p.ud_match("hir", p.pl_adj_user_defined) == "their"

        # defa defan
        p.defa("h")
        assert p.a("h") == "a h"
        assert p.ud_match("h", p.A_a_user_defined) == "a"

        p.defan("horrendous.*")
        assert p.a("horrendously") == "an horrendously"
        assert p.ud_match("horrendously", p.A_a_user_defined) == "an"

    def test_user_input_defverb(self):
        p = inflect.engine()
        p.defverb("will", "shall", "will", "will", "will", "will")
        assert p.ud_match("will", p.pl_v_user_defined) == "will"
        assert p.plural("will") == "will"

    @pytest.mark.xfail(reason="todo")
    def test_user_input_defverb_compare(self):
        p = inflect.engine()
        p.defverb("will", "shall", "will", "will", "will", "will")
        assert p.compare("will", "shall") == "s:p"
        assert p.compare_verbs("will", "shall") == "s:p"

    def test_postprocess(self):
        p = inflect.engine()
        for orig, infl, txt in (
            ("cow", "cows", "cows"),
            ("I", "we", "we"),
            ("COW", "cows", "COWS"),
            ("Cow", "cows", "Cows"),
            ("cow", "cows|kine", "cows"),
            ("Entry", "entries", "Entries"),
            ("can of Coke", "cans of coke", "cans of Coke"),
        ):
            assert p.postprocess(orig, infl) == txt

        p.classical()
        assert p.postprocess("cow", "cows|kine") == "kine"

    def test_partition_word(self):
        p = inflect.engine()
        for txt, part in (
            (" cow ", (" ", "cow", " ")),
            ("cow", ("", "cow", "")),
            ("   cow", ("   ", "cow", "")),
            ("cow   ", ("", "cow", "   ")),
            ("  cow   ", ("  ", "cow", "   ")),
            ("", ("", "", "")),
            ("bottle of beer", ("", "bottle of beer", "")),
            # spaces give weird results
            # (' '),('', ' ', '')),
            # ('  '),(' ', ' ', '')),
            # ('   '),('  ', ' ', '')),
        ):
            assert p.partition_word(txt) == part

    def test_pl(self):
        p = inflect.engine()
        for fn, sing, plur in (
            (p.plural, "cow", "cows"),
            (p.plural, "thought", "thoughts"),
            (p.plural, "mouse", "mice"),
            (p.plural, "knife", "knives"),
            (p.plural, "knifes", "knife"),
            (p.plural, " cat  ", " cats  "),
            (p.plural, "court martial", "courts martial"),
            (p.plural, "a", "some"),
            (p.plural, "carmen", "carmina"),
            (p.plural, "quartz", "quartzes"),
            (p.plural, "care", "cares"),
            (p.plural_noun, "cow", "cows"),
            (p.plural_noun, "thought", "thoughts"),
            (p.plural_noun, "snooze", "snoozes"),
            (p.plural_verb, "runs", "run"),
            (p.plural_verb, "thought", "thought"),
            (p.plural_verb, "eyes", "eye"),
            (p.plural_adj, "a", "some"),
            (p.plural_adj, "this", "these"),
            (p.plural_adj, "that", "those"),
            (p.plural_adj, "my", "our"),
            (p.plural_adj, "cat's", "cats'"),
            (p.plural_adj, "child's", "children's"),
        ):
            assert fn(sing) == plur, '{}("{}") == "{}" != "{}"'.format(
                fn.__name__, sing, fn(sing), plur
            )

        for sing, num, plur in (
            ("cow", 1, "cow"),
            ("cow", 2, "cows"),
            ("cow", "one", "cow"),
            ("cow", "each", "cow"),
            ("cow", "two", "cows"),
            ("cow", 0, "cows"),
            ("cow", "zero", "cows"),
            ("runs", 0, "run"),
            ("runs", 1, "runs"),
            ("am", 0, "are"),
        ):
            assert p.plural(sing, num) == plur

        p.classical(zero=True)
        assert p.plural("cow", 0) == "cow"
        assert p.plural("cow", "zero") == "cow"
        assert p.plural("runs", 0) == "runs"
        assert p.plural("am", 0) == "am"
        assert p.plural_verb("runs", 1) == "runs"

        assert p.plural("die") == "dice"
        assert p.plural_noun("die") == "dice"

        with pytest.raises(Exception):
            p.plural("")
            p.plural_noun("")
            p.plural_verb("")
            p.plural_adj("")

    def test_sinoun(self):
        p = inflect.engine()
        for sing, plur in (
            ("cat", "cats"),
            ("die", "dice"),
            ("goose", "geese"),
        ):
            assert p.singular_noun(plur) == sing
            assert p.inflect("singular_noun('%s')" % plur) == sing

        assert p.singular_noun("cats", count=2) == "cats"
        assert p.singular_noun("open valves", count=2) == "open valves"

        assert p.singular_noun("zombies") == "zombie"

        assert p.singular_noun("shoes") == "shoe"
        assert p.singular_noun("dancing shoes") == "dancing shoe"

        assert p.singular_noun("Matisses") == "Matisse"
        assert p.singular_noun("bouillabaisses") == "bouillabaisse"

        assert p.singular_noun("quartzes") == "quartz"

        assert p.singular_noun("Nietzsches") == "Nietzsche"
        assert p.singular_noun("aches") == "ache"

        assert p.singular_noun("Clives") == "Clive"
        assert p.singular_noun("weaves") == "weave"

        assert p.singular_noun("status") is False
        assert p.singular_noun("hiatus") is False

    def test_gender(self):
        p = inflect.engine()
        p.gender("feminine")
        for sing, plur in (
            ("she", "they"),
            ("herself", "themselves"),
            ("hers", "theirs"),
            ("to her", "to them"),
            ("to herself", "to themselves"),
        ):
            assert (
                p.singular_noun(plur) == sing
            ), f"singular_noun({plur}) == {p.singular_noun(plur)} != {sing}"
            assert p.inflect("singular_noun('%s')" % plur) == sing

        p.gender("masculine")
        for sing, plur in (
            ("he", "they"),
            ("himself", "themselves"),
            ("his", "theirs"),
            ("to him", "to them"),
            ("to himself", "to themselves"),
        ):
            assert (
                p.singular_noun(plur) == sing
            ), f"singular_noun({plur}) == {p.singular_noun(plur)} != {sing}"
            assert p.inflect("singular_noun('%s')" % plur) == sing

        p.gender("gender-neutral")
        for sing, plur in (
            ("they", "they"),
            ("themself", "themselves"),
            ("theirs", "theirs"),
            ("to them", "to them"),
            ("to themself", "to themselves"),
        ):
            assert (
                p.singular_noun(plur) == sing
            ), f"singular_noun({plur}) == {p.singular_noun(plur)} != {sing}"
            assert p.inflect("singular_noun('%s')" % plur) == sing

        p.gender("neuter")
        for sing, plur in (
            ("it", "they"),
            ("itself", "themselves"),
            ("its", "theirs"),
            ("to it", "to them"),
            ("to itself", "to themselves"),
        ):
            assert (
                p.singular_noun(plur) == sing
            ), f"singular_noun({plur}) == {p.singular_noun(plur)} != {sing}"
            assert p.inflect("singular_noun('%s')" % plur) == sing

        with pytest.raises(BadGenderError):
            p.gender("male")

        for sing, plur, gen in (
            ("it", "they", "neuter"),
            ("she", "they", "feminine"),
            ("he", "they", "masculine"),
            ("they", "they", "gender-neutral"),
            ("she or he", "they", "feminine or masculine"),
            ("he or she", "they", "masculine or feminine"),
        ):
            assert p.singular_noun(plur, gender=gen) == sing

        with pytest.raises(BadGenderError):
            p.singular_noun("cats", gender="unknown gender")

    @pytest.mark.parametrize(
        'sing,plur,res',
        (
            ("index", "index", "eq"),
            ("index", "indexes", "s:p"),
            ("index", "indices", "s:p"),
            ("indexes", "index", "p:s"),
            ("indices", "index", "p:s"),
            ("indices", "indexes", "p:p"),
            ("indexes", "indices", "p:p"),
            ("indices", "indices", "eq"),
            ("inverted index", "inverted indices", "s:p"),
            ("inverted indices", "inverted index", "p:s"),
            ("inverted indexes", "inverted indices", "p:p"),
            ("opuses", "opera", "p:p"),
            ("opera", "opuses", "p:p"),
            ("brothers", "brethren", "p:p"),
            ("cats", "cats", "eq"),
            ("base", "basis", False),
            ("syrinx", "syringe", False),
            ("she", "he", False),
            ("opus", "operas", False),
            ("taxi", "taxes", False),
            ("time", "Times", False),
            ("time".lower(), "Times".lower(), "s:p"),
            ("courts martial", "court martial", "p:s"),
            ("my", "my", "eq"),
            ("my", "our", "s:p"),
            ("our", "our", "eq"),
            pytest.param(
                "dresses's", "dresses'", "p:p", marks=pytest.mark.xfail(reason="todo")
            ),
            pytest.param(
                "dress's", "dress'", "s:s", marks=pytest.mark.xfail(reason='todo')
            ),
            pytest.param(
                "Jess's", "Jess'", "s:s", marks=pytest.mark.xfail(reason='todo')
            ),
        ),
    )
    def test_compare_simple(self, sing, plur, res):
        assert inflect.engine().compare(sing, plur) == res

    @pytest.mark.parametrize(
        'sing,plur,res',
        (
            ("index", "index", "eq"),
            ("index", "indexes", "s:p"),
            ("index", "indices", "s:p"),
            ("indexes", "index", "p:s"),
            ("indices", "index", "p:s"),
            ("indices", "indexes", "p:p"),
            ("indexes", "indices", "p:p"),
            ("indices", "indices", "eq"),
            ("inverted index", "inverted indices", "s:p"),
            ("inverted indices", "inverted index", "p:s"),
            ("inverted indexes", "inverted indices", "p:p"),
        ),
    )
    def test_compare_nouns(self, sing, plur, res):
        assert inflect.engine().compare_nouns(sing, plur) == res

    @pytest.mark.parametrize(
        'sing,plur,res',
        (
            ("runs", "runs", "eq"),
            ("runs", "run", "s:p"),
            ("run", "run", "eq"),
        ),
    )
    def test_compare_verbs(self, sing, plur, res):
        assert inflect.engine().compare_verbs(sing, plur) == res

    @pytest.mark.parametrize(
        'sing,plur,res',
        (
            ("my", "my", "eq"),
            ("my", "our", "s:p"),
            ("our", "our", "eq"),
            pytest.param(
                "dresses's", "dresses'", "p:p", marks=pytest.mark.xfail(reason="todo")
            ),
            pytest.param(
                "dress's", "dress'", "s:s", marks=pytest.mark.xfail(reason='todo')
            ),
            pytest.param(
                "Jess's", "Jess'", "s:s", marks=pytest.mark.xfail(reason='todo')
            ),
        ),
    )
    def test_compare_adjectives(self, sing, plur, res):
        assert inflect.engine().compare_adjs(sing, plur) == res

    @pytest.mark.xfail()
    def test_compare_your_our(self):
        # multiple adjective plurals not (yet) supported
        p = inflect.engine()
        assert p.compare("your", "our") is False
        p.defadj("my", "our|your")  # what's ours is yours
        assert p.compare("your", "our") == "p:p"

    def test__pl_reg_plurals(self):
        p = inflect.engine()
        for pair, stems, end1, end2, ans in (
            ("indexes|indices", "dummy|ind", "exes", "ices", True),
            ("indexes|robots", "dummy|ind", "exes", "ices", False),
            ("beaus|beaux", ".*eau", "s", "x", True),
        ):
            assert p._pl_reg_plurals(pair, stems, end1, end2) == ans

    def test__pl_check_plurals_N(self):
        p = inflect.engine()
        assert p._pl_check_plurals_N("index", "indices") is False
        assert p._pl_check_plurals_N("indexes", "indices") is True
        assert p._pl_check_plurals_N("indices", "indexes") is True
        assert p._pl_check_plurals_N("stigmata", "stigmas") is True
        assert p._pl_check_plurals_N("phalanxes", "phalanges") is True

    def test__pl_check_plurals_adj(self):
        p = inflect.engine()
        assert p._pl_check_plurals_adj("indexes's", "indices's") is True
        assert p._pl_check_plurals_adj("indices's", "indexes's") is True
        assert p._pl_check_plurals_adj("indexes'", "indices's") is True
        assert p._pl_check_plurals_adj("indexes's", "indices'") is True
        assert p._pl_check_plurals_adj("indexes's", "indexes's") is False
        assert p._pl_check_plurals_adj("dogmas's", "dogmata's") is True
        assert p._pl_check_plurals_adj("dogmas'", "dogmata'") is True
        assert p._pl_check_plurals_adj("indexes'", "indices'") is True

    def test_count(self):
        p = inflect.engine()
        for txt, num in (
            (1, 1),
            (2, 2),
            (0, 2),
            (87, 2),
            (-7, 2),
            ("1", 1),
            ("2", 2),
            ("0", 2),
            ("no", 2),
            ("zero", 2),
            ("nil", 2),
            ("a", 1),
            ("an", 1),
            ("one", 1),
            ("each", 1),
            ("every", 1),
            ("this", 1),
            ("that", 1),
            ("dummy", 2),
        ):
            assert p.get_count(txt) == num

        assert p.get_count() == ""
        p.num(3)
        assert p.get_count() == 2

    def test__plnoun(self):
        p = inflect.engine()
        for sing, plur in (
            ("tuna", "tuna"),
            ("TUNA", "TUNA"),
            ("swordfish", "swordfish"),
            ("Governor General", "Governors General"),
            ("Governor-General", "Governors-General"),
            ("Major General", "Major Generals"),
            ("Major-General", "Major-Generals"),
            ("mother in law", "mothers in law"),
            ("mother-in-law", "mothers-in-law"),
            ("about me", "about us"),
            ("to it", "to them"),
            ("from it", "from them"),
            ("with it", "with them"),
            ("I", "we"),
            ("you", "you"),
            ("me", "us"),
            ("mine", "ours"),
            ("child", "children"),
            ("brainchild", "brainchilds"),
            ("human", "humans"),
            ("soliloquy", "soliloquies"),
            ("chairwoman", "chairwomen"),
            ("goose", "geese"),
            ("tooth", "teeth"),
            ("foot", "feet"),
            ("forceps", "forceps"),
            ("protozoon", "protozoa"),
            ("czech", "czechs"),
            ("codex", "codices"),
            ("radix", "radices"),
            ("bacterium", "bacteria"),
            ("alumnus", "alumni"),
            ("criterion", "criteria"),
            ("alumna", "alumnae"),
            ("bias", "biases"),
            ("quiz", "quizzes"),
            ("fox", "foxes"),
            ("shelf", "shelves"),
            ("leaf", "leaves"),
            ("midwife", "midwives"),
            ("scarf", "scarves"),
            ("key", "keys"),
            ("Sally", "Sallys"),
            ("sally", "sallies"),
            ("ado", "ados"),
            ("auto", "autos"),
            ("alto", "altos"),
            ("zoo", "zoos"),
            ("tomato", "tomatoes"),
        ):
            assert p._plnoun(sing) == plur, 'p._plnoun("{}") == {} != "{}"'.format(
                sing, p._plnoun(sing), plur
            )

            assert p._sinoun(plur) == sing, f'p._sinoun("{plur}") != "{sing}"'

        # words where forming singular is ambiguous or not attempted
        for sing, plur in (
            ("son of a gun", "sons of guns"),
            ("son-of-a-gun", "sons-of-guns"),
            ("basis", "bases"),
            ("Jess", "Jesses"),
        ):
            assert p._plnoun(sing) == plur, f'p._plnoun("{sing}") != "{plur}"'

        p.num(1)
        assert p._plnoun("cat") == "cat"
        p.num(3)

        p.classical(herd=True)
        assert p._plnoun("swine") == "swine"
        p.classical(herd=False)
        assert p._plnoun("swine") == "swines"
        p.classical(persons=True)
        assert p._plnoun("chairperson") == "chairpersons"
        p.classical(persons=False)
        assert p._plnoun("chairperson") == "chairpeople"
        p.classical(ancient=True)
        assert p._plnoun("formula") == "formulae"
        p.classical(ancient=False)
        assert p._plnoun("formula") == "formulas"

        p.classical()
        for sing, plur in (
            ("matrix", "matrices"),
            ("gateau", "gateaux"),
            ("millieu", "millieux"),
            ("syrinx", "syringes"),
            ("stamen", "stamina"),
            ("apex", "apices"),
            ("appendix", "appendices"),
            ("maximum", "maxima"),
            ("focus", "foci"),
            ("status", "status"),
            ("aurora", "aurorae"),
            ("soma", "somata"),
            ("iris", "irides"),
            ("solo", "soli"),
            ("oxymoron", "oxymora"),
            ("goy", "goyim"),
            ("afrit", "afriti"),
        ):
            assert p._plnoun(sing) == plur

        # p.classical(0)
        # p.classical('names')
        # classical now back to the default mode

    @pytest.mark.parametrize(
        'sing, plur',
        (
            pytest.param(
                'about ME',
                'about US',
                marks=pytest.mark.xfail(reason='does not keep case'),
            ),
            pytest.param(
                'YOU',
                'YOU',
                marks=pytest.mark.xfail(reason='does not keep case'),
            ),
        ),
    )
    def test_plnoun_retains_case(self, sing, plur):
        assert inflect.engine()._plnoun(sing) == plur

    def test_classical_pl(self):
        p = inflect.engine()
        p.classical()
        for sing, plur in (("brother", "brethren"), ("dogma", "dogmata")):
            assert p.plural(sing) == plur

    def test__pl_special_verb(self):
        p = inflect.engine()
        with pytest.raises(Exception):
            assert p._pl_special_verb("") is False
        assert p._pl_special_verb("am") == "are"
        assert p._pl_special_verb("am", 0) == "are"
        assert p._pl_special_verb("runs", 0) == "run"
        p.classical(zero=True)
        assert p._pl_special_verb("am", 0) is False
        assert p._pl_special_verb("am", 1) == "am"
        assert p._pl_special_verb("am", 2) == "are"
        assert p._pl_special_verb("runs", 0) is False
        assert p._pl_special_verb("am going to") == "are going to"
        assert p._pl_special_verb("did") == "did"
        assert p._pl_special_verb("wasn't") == "weren't"
        assert p._pl_special_verb("shouldn't") == "shouldn't"
        assert p._pl_special_verb("bias") is False
        assert p._pl_special_verb("news") is False
        assert p._pl_special_verb("Jess") is False
        assert p._pl_special_verb(" ") is False
        assert p._pl_special_verb("brushes") == "brush"
        assert p._pl_special_verb("fixes") == "fix"
        assert p._pl_special_verb("quizzes") == "quiz"
        assert p._pl_special_verb("fizzes") == "fizz"
        assert p._pl_special_verb("dresses") == "dress"
        assert p._pl_special_verb("flies") == "fly"
        assert p._pl_special_verb("canoes") == "canoe"
        assert p._pl_special_verb("horseshoes") == "horseshoe"
        assert p._pl_special_verb("does") == "do"
        # TODO: what's a real word to test this case?
        assert p._pl_special_verb("zzzoes") == "zzzo"
        assert p._pl_special_verb("runs") == "run"

    def test__pl_general_verb(self):
        p = inflect.engine()
        assert p._pl_general_verb("acts") == "act"
        assert p._pl_general_verb("act") == "act"
        assert p._pl_general_verb("saw") == "saw"
        assert p._pl_general_verb("runs", 1) == "runs"

    @pytest.mark.parametrize(
        'adj,plur',
        (
            ("a", "some"),
            ("my", "our"),
            ("John's", "Johns'"),
            ("tuna's", "tuna's"),
            ("TUNA's", "TUNA's"),
            ("bad", False),
            pytest.param(
                "JOHN's",
                "JOHNS'",
                marks=pytest.mark.xfail(reason='should this be handled?'),
            ),
            pytest.param(
                "JOHN'S",
                "JOHNS'",
                marks=pytest.mark.xfail(reason="can't handle capitals"),
            ),
            pytest.param(
                "TUNA'S",
                "TUNA'S",
                marks=pytest.mark.xfail(reason="can't handle capitals"),
            ),
        ),
    )
    def test__pl_special_adjective(self, adj, plur):
        p = inflect.engine()
        assert p._pl_special_adjective(adj) == plur

    @pytest.mark.parametrize(
        'sing, plur',
        (
            ("cat", "a cat"),
            ("euphemism", "a euphemism"),
            ("Euler number", "an Euler number"),
            ("hour", "an hour"),
            ("houri", "a houri"),
            ("nth", "an nth"),
            ("rth", "an rth"),
            ("sth", "an sth"),
            ("xth", "an xth"),
            ("ant", "an ant"),
            ("book", "a book"),
            ("RSPCA", "an RSPCA"),
            ("SONAR", "a SONAR"),
            ("FJO", "a FJO"),
            ("FJ", "an FJ"),
            ("NASA", "a NASA"),
            ("UN", "a UN"),
            ("yak", "a yak"),
            ("yttrium", "an yttrium"),
            ("a elephant", "an elephant"),
            ("a giraffe", "a giraffe"),
            ("an ewe", "a ewe"),
            ("a orangutan", "an orangutan"),
            ("R.I.P.", "an R.I.P."),
            ("C.O.D.", "a C.O.D."),
            ("e-mail", "an e-mail"),
            ("X-ray", "an X-ray"),
            ("T-square", "a T-square"),
            ("LCD", "an LCD"),
            ("XML", "an XML"),
            ("YWCA", "a YWCA"),
            ("LED", "a LED"),
            ("OPEC", "an OPEC"),
            ("FAQ", "a FAQ"),
            ("UNESCO", "a UNESCO"),
            ("a", "an a"),
            ("an", "an an"),
            ("an ant", "an ant"),
            ("a cat", "a cat"),
            ("an cat", "a cat"),
            ("a ant", "an ant"),
        ),
    )
    def test_a(self, sing, plur):
        p = inflect.engine()
        assert p.a(sing) == plur

    def test_a_alt(self):
        p = inflect.engine()
        assert p.a("cat", 1) == "a cat"
        assert p.a("cat", 2) == "2 cat"

        with pytest.raises(Exception):
            p.a("")

    def test_a_and_an_same_method(self):
        assert same_method(inflect.engine.a, inflect.engine.an)
        p = inflect.engine()
        assert same_method(p.a, p.an)

    def test_no(self):
        p = inflect.engine()
        assert p.no("cat") == "no cats"
        assert p.no("cat", count=3) == "3 cats"
        assert p.no("cat", count="three") == "three cats"
        assert p.no("cat", count=1) == "1 cat"
        assert p.no("cat", count="one") == "one cat"
        assert p.no("mouse") == "no mice"
        p.num(3)
        assert p.no("cat") == "3 cats"

    @pytest.mark.parametrize(
        'sing, plur',
        (
            ("runs", "running"),
            ("dies", "dying"),
            ("glues", "gluing"),
            ("eyes", "eying"),
            ("skis", "skiing"),
            ("names", "naming"),
            ("sees", "seeing"),
            ("hammers", "hammering"),
            ("bats", "batting"),
            ("eats", "eating"),
            ("loves", "loving"),
            ("spies", "spying"),
            ("hoes", "hoeing"),
            ("alibis", "alibiing"),
            ("is", "being"),
            ("are", "being"),
            ("had", "having"),
            ("has", "having"),
        ),
    )
    def test_prespart(self, sing, plur):
        p = inflect.engine()
        assert p.present_participle(sing) == plur

    @pytest.mark.parametrize(
        'num, ord',
        (
            ("1", "1st"),
            ("2", "2nd"),
            ("3", "3rd"),
            ("4", "4th"),
            ("10", "10th"),
            ("28", "28th"),
            ("100", "100th"),
            ("101", "101st"),
            ("1000", "1000th"),
            ("1001", "1001st"),
            ("0", "0th"),
            ("one", "first"),
            ("two", "second"),
            ("four", "fourth"),
            ("twenty", "twentieth"),
            ("one hundered", "one hunderedth"),
            ("one hundered and one", "one hundered and first"),
            ("zero", "zeroth"),
            ("n", "nth"),  # bonus!
        ),
    )
    def test_ordinal(self, num, ord):
        p = inflect.engine()
        assert p.ordinal(num) == ord

    def test_millfn(self):
        p = inflect.engine()
        millfn = p.millfn
        assert millfn(1) == " thousand"
        assert millfn(2) == " million"
        assert millfn(3) == " billion"
        assert millfn(0) == " "
        assert millfn(11) == " decillion"
        with pytest.raises(NumOutOfRangeError):
            millfn(12)

    def test_unitfn(self):
        p = inflect.engine()
        unitfn = p.unitfn
        assert unitfn(1, 2) == "one million"
        assert unitfn(1, 3) == "one billion"
        assert unitfn(5, 3) == "five billion"
        assert unitfn(5, 0) == "five "
        assert unitfn(0, 0) == " "

    def test_tenfn(self):
        p = inflect.engine()
        tenfn = p.tenfn
        assert tenfn(3, 1, 2) == "thirty-one million"
        assert tenfn(3, 0, 2) == "thirty million"
        assert tenfn(0, 1, 2) == "one million"
        assert tenfn(1, 1, 2) == "eleven million"
        assert tenfn(1, 0, 2) == "ten million"
        assert tenfn(1, 0, 0) == "ten "
        assert tenfn(0, 0, 0) == " "

    def test_hundfn(self):
        p = inflect.engine()
        hundfn = p.hundfn
        p._number_args = dict(andword="and")
        assert hundfn(4, 3, 1, 2) == "four hundred and thirty-one  million, "
        assert hundfn(4, 0, 0, 2) == "four hundred  million, "
        assert hundfn(4, 0, 5, 2) == "four hundred and five  million, "
        assert hundfn(0, 3, 1, 2) == "thirty-one  million, "
        assert hundfn(0, 0, 7, 2) == "seven  million, "

    def test_enword(self):
        p = inflect.engine()
        enword = p.enword
        assert enword("5", 1) == "five, "
        p._number_args = dict(zero="zero", one="one", andword="and")
        assert enword("0", 1) == " zero, "
        assert enword("1", 1) == " one, "
        assert enword("347", 1) == "three, four, seven, "

        assert enword("34", 2) == "thirty-four , "
        assert enword("347", 2) == "thirty-four , seven, "
        assert enword("34768", 2) == "thirty-four , seventy-six , eight, "
        assert enword("1", 2) == "one, "

        assert enword("134", 3) == " one thirty-four , "

        assert enword("0", -1) == "zero"
        assert enword("1", -1) == "one"

        assert enword("3", -1) == "three , "
        assert enword("12", -1) == "twelve , "
        assert enword("123", -1) == "one hundred and twenty-three  , "
        assert enword("1234", -1) == "one thousand, two hundred and thirty-four  , "
        assert (
            enword("12345", -1) == "twelve thousand, three hundred and forty-five  , "
        )
        assert (
            enword("123456", -1)
            == "one hundred and twenty-three  thousand, four hundred and fifty-six  , "
        )
        assert (
            enword("1234567", -1)
            == "one million, two hundred and thirty-four  thousand, "
            "five hundred and sixty-seven  , "
        )

    @pytest.mark.xfail(reason="doesn't use indicated word for 'one'")
    def test_enword_number_args_override(self):
        p = inflect.engine()
        p._number_args["one"] = "single"
        p.enword("1", 2) == "single, "

    def test_numwords(self):
        p = inflect.engine()
        numwords = p.number_to_words

        for n, word in (
            ("1", "one"),
            ("10", "ten"),
            ("100", "one hundred"),
            ("1000", "one thousand"),
            ("10000", "ten thousand"),
            ("100000", "one hundred thousand"),
            ("1000000", "one million"),
            ("10000000", "ten million"),
            ("+10", "plus ten"),
            ("-10", "minus ten"),
            ("10.", "ten point"),
            (".10", "point one zero"),
        ):
            assert numwords(n) == word

        for n, word, wrongword in (
            # TODO: should be one point two three
            ("1.23", "one point two three", "one point twenty-three"),
        ):
            assert numwords(n) == word

        for n, txt in (
            (3, "three bottles of beer on the wall"),
            (2, "two bottles of beer on the wall"),
            (1, "a solitary bottle of beer on the wall"),
            (0, "no more bottles of beer on the wall"),
        ):
            assert (
                "{}{}".format(
                    numwords(n, one="a solitary", zero="no more"),
                    p.plural(" bottle of beer on the wall", n),
                )
                == txt
            )

        assert numwords(0, one="one", zero="zero") == "zero"

        assert numwords("1234") == "one thousand, two hundred and thirty-four"
        assert numwords("1234", wantlist=True) == [
            "one thousand",
            "two hundred and thirty-four",
        ]
        assert numwords("1234567", wantlist=True) == [
            "one million",
            "two hundred and thirty-four thousand",
            "five hundred and sixty-seven",
        ]
        assert numwords("+10", wantlist=True) == ["plus", "ten"]
        assert numwords("1234", andword="") == "one thousand, two hundred thirty-four"
        assert (
            numwords("1234", andword="plus")
            == "one thousand, two hundred plus thirty-four"
        )
        assert numwords(p.ordinal("21")) == "twenty-first"
        assert numwords("9", threshold=10) == "nine"
        assert numwords("10", threshold=10) == "ten"
        assert numwords("11", threshold=10) == "11"
        assert numwords("1000", threshold=10) == "1,000"
        assert numwords("123", threshold=10) == "123"
        assert numwords("1234", threshold=10) == "1,234"
        assert numwords("1234.5678", threshold=10) == "1,234.5678"
        assert numwords("1", decimal=None) == "one"
        assert (
            numwords("1234.5678", decimal=None)
            == "twelve million, three hundred and forty-five "
            "thousand, six hundred and seventy-eight"
        )

    def test_numwords_group_chunking_error(self):
        p = inflect.engine()
        with pytest.raises(BadChunkingOptionError):
            p.number_to_words("1234", group=4)

    @pytest.mark.parametrize(
        'input,kwargs,expect',
        (
            ("12345", dict(group=2), "twelve, thirty-four, five"),
            ("123456", dict(group=3), "one twenty-three, four fifty-six"),
            ("12345", dict(group=1), "one, two, three, four, five"),
            (
                "1234th",
                dict(group=0, andword="and"),
                "one thousand, two hundred and thirty-fourth",
            ),
            (
                "1234th",
                dict(group=0),
                "one thousand, two hundred and thirty-fourth",
            ),
            ("120", dict(group=2), "twelve, zero"),
            ("120", dict(group=2, zero="oh", one="unity"), "twelve, oh"),
            (
                "555_1202",
                dict(group=1, zero="oh"),
                "five, five, five, one, two, oh, two",
            ),
            (
                "555_1202",
                dict(group=1, one="unity"),
                "five, five, five, unity, two, zero, two",
            ),
            (
                "123.456",
                dict(group=1, decimal="mark", one="one"),
                "one, two, three, mark, four, five, six",
            ),
            pytest.param(
                '12345',
                dict(group=3),
                'one hundred and twenty-three',
                marks=pytest.mark.xfail(reason="'hundred and' missing"),
            ),
            pytest.param(
                '101',
                dict(group=2, zero="oh", one="unity"),
                "ten, unity",
                marks=pytest.mark.xfail(reason="ignoring 'one' param with group=2"),
            ),
        ),
    )
    def test_numwords_group(self, input, kwargs, expect):
        p = inflect.engine()
        assert p.number_to_words(input, **kwargs) == expect

    def test_wordlist(self):
        p = inflect.engine()
        wordlist = p.join
        assert wordlist([]) == ""
        assert wordlist(("apple",)) == "apple"
        assert wordlist(("apple", "banana")) == "apple and banana"
        assert wordlist(("apple", "banana", "carrot")) == "apple, banana, and carrot"
        assert wordlist(("apple", "1,000", "carrot")) == "apple; 1,000; and carrot"
        assert (
            wordlist(("apple", "1,000", "carrot"), sep=",")
            == "apple, 1,000, and carrot"
        )
        assert (
            wordlist(("apple", "banana", "carrot"), final_sep="")
            == "apple, banana and carrot"
        )
        assert (
            wordlist(("apple", "banana", "carrot"), final_sep=";")
            == "apple, banana; and carrot"
        )
        assert (
            wordlist(("apple", "banana", "carrot"), conj="or")
            == "apple, banana, or carrot"
        )

        assert wordlist(("apple", "banana"), conj=" or ") == "apple  or  banana"
        assert wordlist(("apple", "banana"), conj="&") == "apple & banana"
        assert (
            wordlist(("apple", "banana"), conj="&", conj_spaced=False) == "apple&banana"
        )
        assert (
            wordlist(("apple", "banana"), conj="& ", conj_spaced=False)
            == "apple& banana"
        )

        assert (
            wordlist(("apple", "banana", "carrot"), conj=" or ")
            == "apple, banana,  or  carrot"
        )
        assert (
            wordlist(("apple", "banana", "carrot"), conj="+")
            == "apple, banana, + carrot"
        )
        assert (
            wordlist(("apple", "banana", "carrot"), conj="&")
            == "apple, banana, & carrot"
        )
        assert (
            wordlist(("apple", "banana", "carrot"), conj="&", conj_spaced=False)
            == "apple, banana,&carrot"
        )
        assert (
            wordlist(("apple", "banana", "carrot"), conj=" &", conj_spaced=False)
            == "apple, banana, &carrot"
        )

    def test_doc_examples(self):
        p = inflect.engine()
        assert p.plural_noun("I") == "we"
        assert p.plural_verb("saw") == "saw"
        assert p.plural_adj("my") == "our"
        assert p.plural_noun("saw") == "saws"
        assert p.plural("was") == "were"
        assert p.plural("was", 1) == "was"
        assert p.plural_verb("was", 2) == "were"
        assert p.plural_verb("was") == "were"
        assert p.plural_verb("was", 1) == "was"

        for errors, txt in (
            (0, "There were no errors"),
            (1, "There was 1 error"),
            (2, "There were 2 errors"),
        ):
            assert (
                "There {}{}".format(
                    p.plural_verb("was", errors), p.no(" error", errors)
                )
                == txt
            )

            assert (
                p.inflect(
                    "There plural_verb('was',%d) no('error',%d)" % (errors, errors)
                )
                == txt
            )

        for num1, num2, txt in ((1, 2, "I saw 2 saws"), (2, 1, "we saw 1 saw")):
            assert (
                "{}{}{} {}{}".format(
                    p.num(num1, ""),
                    p.plural("I"),
                    p.plural_verb(" saw"),
                    p.num(num2),
                    p.plural_noun(" saw"),
                )
                == txt
            )

            assert (
                p.inflect(
                    "num(%d, False)plural('I') plural_verb('saw') "
                    "num(%d) plural_noun('saw')" % (num1, num2)
                )
                == txt
            )

        assert p.a("a cat") == "a cat"

        for word, txt in (
            ("cat", "a cat"),
            ("aardvark", "an aardvark"),
            ("ewe", "a ewe"),
            ("hour", "an hour"),
        ):
            assert p.a("{} {}".format(p.number_to_words(1, one="a"), word)) == txt

        p.num(2)

    def test_unknown_method(self):
        p = inflect.engine()
        with pytest.raises(AttributeError):
            p.unknown_method
