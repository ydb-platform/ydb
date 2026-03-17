# Unicode characters and corresponding LaTeX math mode commands
# *************************************************************
#
# :Copyright: © 2011 Günter Milde
# :Date:      Last revised 2011-11-08
# :Licence:   This work may be distributed and/or modified under the
#             conditions of the `LaTeX Project Public License`_,
#             either version 1.3 of this license or (at your option)
#             any later version.
#
# .. _LaTeX Project Public License: http://www.latex-project.org/lppl.txt
#
# This is a mapping of mathematical Unicode characters to corresponding
# (La)TeX commands.
#
# While the contents of this file represent the best information
# available to the author as of the date referenced above, it
# contains omissions and maybe errors. It is likely that the
# information in this file will change from time to time.
#
# The character encoding of the file is UTF-8.
#
# Each data record consists of 8 fields. Fields are delimited by “^”.
# Spaces adjacent to the delimiter are not significant. The number and
# type of fields in this file may change in future versions.
#
# 1. code point (Unicode character number)
#
#    There may be more than one record for one code point,
#    if there are different TeX commands for the same character.
#    (changed 2015-09-21, before the code point was unique.)
#
# 2. literal character (UTF-8 encoded)
#
# 3. (La)TeX _`command`
#
#    Preferred representation of the character in (La)TeX.
#    Alternative commands are listed in the comments_ field.
#
# 4. Unicode math character class (after MathClassEx_).
#
#    .. _MathClassEx:
#       http://www.unicode.org/Public/math/revision-11/MathClassEx-11.txt
#
#    The class can be one of:
#
#    :N: Normal- includes all digits and symbols requiring only one form
#    :A: Alphabetic
#    :B: Binary
#    :C: Closing – usually paired with opening delimiter
#    :D: Diacritic
#    :F: Fence - unpaired delimiter (often used as opening or closing)
#    :G: Glyph_Part- piece of large operator
#    :L: Large -n-ary or Large operator, often takes limits
#    :O: Opening – usually paired with closing delimiter
#    :P: Punctuation
#    :R: Relation- includes arrows
#    :S: Space
#    :U: Unary – operators that are only unary
#    :V: Vary – operators that can be unary or binary depending on context
#    :X: Special –characters not covered by other classes
#
#    C, O, and F operators are stretchy. In addition some binary
#    operators, such as 002F are stretchy as noted in the descriptive
#    comments. The classes are also useful in determining extra spacing
#    around the operators as discussed in UTR#25.
#
# 5. TeX math category (after unimath-symbols_)
#
#    .. _unimath-symbols:
#       http://mirror.ctan.org/macros/latex/contrib/unicode-math/unimath-symbols.pdf
#
# 6. requirements and conflicts
#
#    Space delimited list of LaTeX packages or features [1]_ providing
#    the LaTeX command_ or conflicting with it.
#
#    Packages/features preceded by a HYPHEN-MINUS (-) use the command
#    for a different character or purpose.
#
#    To save space, packages providing/modifying (almost) all commands
#    of a feature or another package are not listed here but in the
#    ``packages.txt`` file.
#
#    .. [1] A feature can be a set of commands common to several packages,
#    	    (e.g. ``mathbb`` or ``slantedGreek``) or a constraint (e.g.
#	    ``literal`` mapping plain characters to upright face).
#
# 8. descriptive _`comments`
#
#    The descriptive comments provide more information about the
#    character, or its specific appearance or use.
#
#    Some descriptions contain references to related commands,
#    marked by a character describing the relation
#
#    :=:  equals  (alias commands),
#    :#:  approx  (compat mapping, different character with same glyph),
#    :x:  → cross reference/see also (related, false friends, and name clashes),
#    :t:  text    (text mode command),
#
#    followed by requirements in parantheses, and
#    delimited by commas.
#
#    Comments in UPPERCASE are Unicode character names
#
# no.^chr^LaTeX^unicode-math^cls^category^requirements^comments
00020^ ^^^S^^^SPACE
00021^!^!^\exclam^N^mathpunct^^EXCLAMATION MARK
00023^#^\#^\octothorpe^N^mathord^-oz^# \# (oz), NUMBER SIGN
00024^$^\$^\mathdollar^N^mathord^^= \mathdollar, DOLLAR SIGN
00025^%^\%^\percent^N^mathord^^PERCENT SIGN
00026^&^\&^\ampersand^N^mathord^^# \binampersand (stmaryrd)
00028^(^(^\lparen^O^mathopen^^LEFT PARENTHESIS
00029^)^)^\rparen^C^mathclose^^RIGHT PARENTHESIS
0002A^*^*^^N^mathord^^# \ast, (high) ASTERISK, star
0002B^+^+^\plus^V^mathbin^^PLUS SIGN
0002C^,^,^\comma^P^mathpunct^^COMMA
0002D^-^^^N^mathbin^^t -, HYPHEN-MINUS (deprecated for math)
0002E^.^.^\period^P^mathalpha^^FULL STOP, period
0002F^/^/^\mathslash^B^mathord^^# \slash, SOLIDUS
00030^0^0^^N^mathord^^DIGIT ZERO
00031^1^1^^N^mathord^^DIGIT ONE
00032^2^2^^N^mathord^^DIGIT TWO
00033^3^3^^N^mathord^^DIGIT THREE
00034^4^4^^N^mathord^^DIGIT FOUR
00035^5^5^^N^mathord^^DIGIT FIVE
00036^6^6^^N^mathord^^DIGIT SIX
00037^7^7^^N^mathord^^DIGIT SEVEN
00038^8^8^^N^mathord^^DIGIT EIGHT
00039^9^9^^N^mathord^^DIGIT NINE
0003A^:^:^\mathcolon^P^mathpunct^-literal^= \colon (literal), COLON (not ratio)
0003B^;^;^\semicolon^P^mathpunct^^SEMICOLON p:
0003C^<^<^\less^R^mathrel^^LESS-THAN SIGN r:
0003D^=^=^\equal^R^mathrel^^EQUALS SIGN r:
0003E^>^>^\greater^R^mathrel^^GREATER-THAN SIGN r:
0003F^?^?^\question^P^mathord^^QUESTION MARK
00040^@^@^\atsign^N^mathord^^at
00041^A^A^^A^mathalpha^-literal^= \mathrm{A}, LATIN CAPITAL LETTER A
00042^B^B^^A^mathalpha^-literal^= \mathrm{B}, LATIN CAPITAL LETTER B
00043^C^C^^A^mathalpha^-literal^= \mathrm{C}, LATIN CAPITAL LETTER C
00044^D^D^^A^mathalpha^-literal^= \mathrm{D}, LATIN CAPITAL LETTER D
00045^E^E^^A^mathalpha^-literal^= \mathrm{E}, LATIN CAPITAL LETTER E
00046^F^F^^A^mathalpha^-literal^= \mathrm{F}, LATIN CAPITAL LETTER F
00047^G^G^^A^mathalpha^-literal^= \mathrm{G}, LATIN CAPITAL LETTER G
00048^H^H^^A^mathalpha^-literal^= \mathrm{H}, LATIN CAPITAL LETTER H
00049^I^I^^A^mathalpha^-literal^= \mathrm{I}, LATIN CAPITAL LETTER I
0004A^J^J^^A^mathalpha^-literal^= \mathrm{J}, LATIN CAPITAL LETTER J
0004B^K^K^^A^mathalpha^-literal^= \mathrm{K}, LATIN CAPITAL LETTER K
0004C^L^L^^A^mathalpha^-literal^= \mathrm{L}, LATIN CAPITAL LETTER L
0004D^M^M^^A^mathalpha^-literal^= \mathrm{M}, LATIN CAPITAL LETTER M
0004E^N^N^^A^mathalpha^-literal^= \mathrm{N}, LATIN CAPITAL LETTER N
0004F^O^O^^A^mathalpha^-literal^= \mathrm{O}, LATIN CAPITAL LETTER O
00050^P^P^^A^mathalpha^-literal^= \mathrm{P}, LATIN CAPITAL LETTER P
00051^Q^Q^^A^mathalpha^-literal^= \mathrm{Q}, LATIN CAPITAL LETTER Q
00052^R^R^^A^mathalpha^-literal^= \mathrm{R}, LATIN CAPITAL LETTER R
00053^S^S^^A^mathalpha^-literal^= \mathrm{S}, LATIN CAPITAL LETTER S
00054^T^T^^A^mathalpha^-literal^= \mathrm{T}, LATIN CAPITAL LETTER T
00055^U^U^^A^mathalpha^-literal^= \mathrm{U}, LATIN CAPITAL LETTER U
00056^V^V^^A^mathalpha^-literal^= \mathrm{V}, LATIN CAPITAL LETTER V
00057^W^W^^A^mathalpha^-literal^= \mathrm{W}, LATIN CAPITAL LETTER W
00058^X^X^^A^mathalpha^-literal^= \mathrm{X}, LATIN CAPITAL LETTER X
00059^Y^Y^^A^mathalpha^-literal^= \mathrm{Y}, LATIN CAPITAL LETTER Y
0005A^Z^Z^^A^mathalpha^-literal^= \mathrm{Z}, LATIN CAPITAL LETTER Z
0005B^[^\lbrack^\lbrack^O^mathopen^^LEFT SQUARE BRACKET
0005C^\^\backslash^\backslash^B^mathord^^REVERSE SOLIDUS
0005D^]^\rbrack^\rbrack^C^mathclose^^RIGHT SQUARE BRACKET
0005E^^\sphat^^N^mathord^amsxtra^CIRCUMFLEX ACCENT, TeX superscript operator
0005F^_^\_^^N^mathord^^LOW LINE, TeX subscript operator
00060^`^^^D^mathord^^grave, alias for 0300
00061^a^a^^A^mathalpha^-literal^= \mathrm{a}, LATIN SMALL LETTER A
00062^b^b^^A^mathalpha^-literal^= \mathrm{b}, LATIN SMALL LETTER B
00063^c^c^^A^mathalpha^-literal^= \mathrm{c}, LATIN SMALL LETTER C
00064^d^d^^A^mathalpha^-literal^= \mathrm{d}, LATIN SMALL LETTER D
00065^e^e^^A^mathalpha^-literal^= \mathrm{e}, LATIN SMALL LETTER E
00066^f^f^^A^mathalpha^-literal^= \mathrm{f}, LATIN SMALL LETTER F
00067^g^g^^A^mathalpha^-literal^= \mathrm{g}, LATIN SMALL LETTER G
00068^h^h^^A^mathalpha^-literal^= \mathrm{h}, LATIN SMALL LETTER H
00069^i^i^^A^mathalpha^-literal^= \mathrm{i}, LATIN SMALL LETTER I
0006A^j^j^^A^mathalpha^-literal^= \mathrm{j}, LATIN SMALL LETTER J
0006B^k^k^^A^mathalpha^-literal^= \mathrm{k}, LATIN SMALL LETTER K
0006C^l^l^^A^mathalpha^-literal^= \mathrm{l}, LATIN SMALL LETTER L
0006D^m^m^^A^mathalpha^-literal^= \mathrm{m}, LATIN SMALL LETTER M
0006E^n^n^^A^mathalpha^-literal^= \mathrm{n}, LATIN SMALL LETTER N
0006F^o^o^^A^mathalpha^-literal^= \mathrm{o}, LATIN SMALL LETTER O
00070^p^p^^A^mathalpha^-literal^= \mathrm{p}, LATIN SMALL LETTER P
00071^q^q^^A^mathalpha^-literal^= \mathrm{q}, LATIN SMALL LETTER Q
00072^r^r^^A^mathalpha^-literal^= \mathrm{r}, LATIN SMALL LETTER R
00073^s^s^^A^mathalpha^-literal^= \mathrm{s}, LATIN SMALL LETTER S
00074^t^t^^A^mathalpha^-literal^= \mathrm{t}, LATIN SMALL LETTER T
00075^u^u^^A^mathalpha^-literal^= \mathrm{u}, LATIN SMALL LETTER U
00076^v^v^^A^mathalpha^-literal^= \mathrm{v}, LATIN SMALL LETTER V
00077^w^w^^A^mathalpha^-literal^= \mathrm{w}, LATIN SMALL LETTER W
00078^x^x^^A^mathalpha^-literal^= \mathrm{x}, LATIN SMALL LETTER X
00079^y^y^^A^mathalpha^-literal^= \mathrm{y}, LATIN SMALL LETTER Y
0007A^z^z^^A^mathalpha^-literal^= \mathrm{z}, LATIN SMALL LETTER Z
0007B^{^\{^\lbrace^O^mathopen^^= \lbrace, LEFT CURLY BRACKET
0007C^|^|^\vert^F^mathfence^^= \vert, vertical bar
0007D^}^\}^\rbrace^C^mathclose^^= \rbrace, RIGHT CURLY BRACKET
0007E^~^\sptilde^^N^mathord^amsxtra^# \sim, TILDE
000A0^ ^^^S^^^nbsp
000A1^¡^^^P^^^iexcl
000A2^¢^\cent^^N^mathord^wasysym^= \mathcent (txfonts), cent
000A3^£^\pounds^\sterling^N^mathord^-fourier -omlmathit^= \mathsterling (txfonts), POUND SIGN, fourier prints a dollar sign
000A4^¤^^^N^mathord^^t \currency (wasysym), curren
000A5^¥^\yen^\yen^N^mathord^amsfonts^YEN SIGN
000A6^¦^^^N^mathord^^brvbar (vertical)
000A7^§^\S^^N^mathord^^sect
000A8^¨^\spddot^^D^mathord^amsxtra^Dot /die, alias for 0308
000AC^¬^\neg^\neg^U^mathord^^= \lnot, NOT SIGN
000AE^®^\circledR^^X^mathord^amsfonts^REGISTERED SIGN
000AF^¯^^^D^mathord^^macr, alias for 0304
000B0^°^^^N^mathord^^deg
000B1^±^\pm^\pm^V^mathbin^^plus-or-minus sign
000B2^²^^^N^mathord^^sup2
000B3^³^^^N^mathord^^sup3
000B4^´^^^N^mathord^^acute, alias for 0301
000B5^µ^\Micro^^N^mathalpha^wrisym^= \tcmu (mathcomp), t \textmu (textcomp), # \mathrm{\mu} (omlmathrm), # \muup (kpfonts mathdesign), MICRO SIGN
000B6^¶^^^N^mathord^^para (paragraph sign, pilcrow)
000B7^·^^\cdotp^B^mathbin^^# \cdot, x \centerdot, b: MIDDLE DOT
000B9^¹^^^N^mathord^^sup1
000BC^¼^^^N^mathord^^frac14
000BD^½^^^N^mathord^^frac12
000BE^¾^^^N^mathord^^frac34
000BF^¿^^^P^^^iquest
000D7^×^\times^\times^B^mathbin^^MULTIPLICATION SIGN, z notation Cartesian product
000F0^ð^\eth^\matheth^^mathalpha^amssymb arevmath^eth
000F7^÷^\div^\div^B^mathbin^^divide sign
00131^ı^\imath^^A^mathalpha^-literal^imath
001B5^Ƶ^^\Zbar^^mathord^^impedance
00237^ȷ^\jmath^^A^mathalpha^-literal^jmath
002C6^ˆ^^^D^mathalpha^^circ, alias for 0302
002C7^ˇ^^^D^mathalpha^^CARON, alias for 030C
002D8^˘^^^D^mathord^^BREVE, alias for 0306
002D9^˙^^^D^mathord^^dot, alias for 0307
002DA^˚^^^D^mathord^^ring, alias for 030A
002DC^˜^^^D^mathord^^tilde, alias for 0303
00300^ ̀^\grave^\grave^D^mathaccent^^grave accent
00301^ ́^\acute^\acute^D^mathaccent^^acute accent
00302^ ̂^\hat^\hat^D^mathaccent^^# \widehat (amssymb), circumflex accent
00303^ ̃^\tilde^\tilde^D^mathaccent^^# \widetilde (yhmath, fourier), tilde
00304^ ̄^\bar^\bar^D^mathaccent^^macron
00305^ ̅^\overline^\overbar^D^mathaccent^^overbar embellishment
00306^ ̆^\breve^\breve^D^mathaccent^^breve
00307^ ̇^\dot^\dot^D^mathaccent^-oz^= \Dot (wrisym), dot above
00308^ ̈^\ddot^\ddot^D^mathaccent^^= \DDot (wrisym), dieresis
00309^ ̉^^\ovhook^^mathaccent^^COMBINING HOOK ABOVE
0030A^ ̊^\mathring^\ocirc^D^mathaccent^amssymb^= \ring (yhmath), ring
0030C^ ̌^\check^\check^D^mathaccent^^caron
00310^ ̐^^\candra^^mathaccent^^candrabindu (non-spacing)
00311^ ̑^^^D^mathaccent^^COMBINING INVERTED BREVE
00312^ ̒^^\oturnedcomma^^mathaccent^^COMBINING TURNED COMMA ABOVE
00315^ ̕^^\ocommatopright^^mathaccent^^COMBINING COMMA ABOVE RIGHT
0031A^ ̚^^\droang^^mathaccent^^left angle above (non-spacing)
00323^ ̣^^^D^mathaccent^^COMBINING DOT BELOW
0032C^ ̬^^^D^mathaccent^^COMBINING CARON BELOW
0032D^ ̭^^^D^mathaccent^^COMBINING CIRCUMFLEX ACCENT BELOW
0032E^ ̮^^^D^mathaccent^^COMBINING BREVE BELOW
0032F^ ̯^^^D^mathaccent^^COMBINING INVERTED BREVE BELOW
00330^ ̰^\utilde^\wideutilde^D^mathaccent^undertilde^under tilde accent (multiple characters and non-spacing)
00331^ ̱^\underbar^\underbar^D^mathaccent^^COMBINING MACRON BELOW
00332^ ̲^\underline^^D^mathaccent^^COMBINING LOW LINE
00333^ ̳^^^D^mathaccent^^2lowbar
00338^ ̸^\not^\not^D^mathaccent^^COMBINING LONG SOLIDUS OVERLAY
0033A^ ̺^^^D^mathaccent^^COMBINING INVERTED BRIDGE BELOW
0033F^ ̿^^^D^mathaccent^^COMBINING DOUBLE OVERLINE
00346^ ͆^^^D^mathaccent^^COMBINING BRIDGE ABOVE
00391^Α^^\upAlpha^A^mathalpha^^capital alpha, greek
00392^Β^^\upBeta^A^mathalpha^^capital beta, greek
00393^Γ^\Gamma^\upGamma^A^mathalpha^-literal^= \Gamma (-slantedGreek), = \mathrm{\Gamma}, capital gamma, greek
00394^Δ^\Delta^\upDelta^A^mathalpha^-literal^= \Delta (-slantedGreek), = \mathrm{\Delta}, capital delta, greek
00395^Ε^^\upEpsilon^A^mathalpha^^capital epsilon, greek
00396^Ζ^^\upZeta^A^mathalpha^^capital zeta, greek
00397^Η^^\upEta^A^mathalpha^^capital eta, greek
00398^Θ^\Theta^\upTheta^A^mathalpha^-literal^= \Theta (-slantedGreek), = \mathrm{\Theta}, capital theta, greek
00399^Ι^^\upIota^A^mathalpha^^capital iota, greek
0039A^Κ^^\upKappa^A^mathalpha^^capital kappa, greek
0039B^Λ^\Lambda^\upLambda^A^mathalpha^-literal^= \Lambda (-slantedGreek), = \mathrm{\Lambda}, capital lambda, greek
0039C^Μ^^\upMu^A^mathalpha^^capital mu, greek
0039D^Ν^^\upNu^A^mathalpha^^capital nu, greek
0039E^Ξ^\Xi^\upXi^A^mathalpha^-literal^= \Xi (-slantedGreek), = \mathrm{\Xi}, capital xi, greek
0039F^Ο^^\upOmicron^A^mathalpha^^capital omicron, greek
003A0^Π^\Pi^\upPi^A^mathalpha^-literal^= \Pi (-slantedGreek), = \mathrm{\Pi}, capital pi, greek
003A1^Ρ^^\upRho^A^mathalpha^^capital rho, greek
003A3^Σ^\Sigma^\upSigma^A^mathalpha^-literal^= \Sigma (-slantedGreek), = \mathrm{\Sigma}, capital sigma, greek
003A4^Τ^^\upTau^A^mathalpha^^capital tau, greek
003A5^Υ^\Upsilon^\upUpsilon^A^mathalpha^-literal^= \Upsilon (-slantedGreek), = \mathrm{\Upsilon}, capital upsilon, greek
003A6^Φ^\Phi^\upPhi^A^mathalpha^-literal^= \Phi (-slantedGreek), = \mathrm{\Phi}, capital phi, greek
003A7^Χ^^\upChi^A^mathalpha^^capital chi, greek
003A8^Ψ^\Psi^\upPsi^A^mathalpha^-literal^= \Psi (-slantedGreek), = \mathrm{\Psi}, capital psi, greek
003A9^Ω^\Omega^\upOmega^A^mathalpha^-literal^= \Omega (-slantedGreek), = \mathrm{\Omega}, capital omega, greek
003B1^α^\alpha^\upalpha^A^mathalpha^-literal^= \mathrm{\alpha} (omlmathrm), = \alphaup (kpfonts mathdesign), = \upalpha (upgreek), alpha, greek
003B2^β^\beta^\upbeta^A^mathalpha^-literal^= \mathrm{\beta} (omlmathrm), = \betaup (kpfonts mathdesign), = \upbeta (upgreek), beta, greek
003B3^γ^\gamma^\upgamma^A^mathalpha^-literal^= \mathrm{\gamma} (omlmathrm), = \gammaup (kpfonts mathdesign), = \upgamma (upgreek), gamma, greek
003B4^δ^\delta^\updelta^A^mathalpha^-literal^= \mathrm{\delta} (omlmathrm), = \deltaup (kpfonts mathdesign), = \updelta (upgreek), delta, greek
003B5^ε^\varepsilon^\upepsilon^A^mathalpha^-literal^= \mathrm{\varepsilon} (omlmathrm), = \varepsilonup (kpfonts mathdesign), = \upepsilon (upgreek), rounded epsilon, greek
003B6^ζ^\zeta^\upzeta^A^mathalpha^-literal^= \mathrm{\zeta} (omlmathrm), = \zetaup (kpfonts mathdesign), = \upzeta (upgreek), zeta, greek
003B7^η^\eta^\upeta^A^mathalpha^-literal^= \mathrm{\eta} (omlmathrm), = \etaup (kpfonts mathdesign), = \upeta (upgreek), eta, greek
003B8^θ^\theta^\uptheta^A^mathalpha^-literal^= \mathrm{\theta} (omlmathrm), = \thetaup (kpfonts mathdesign), straight theta, = \uptheta (upgreek), theta, greek
003B9^ι^\iota^\upiota^A^mathalpha^-literal^= \mathrm{\iota} (omlmathrm), = \iotaup (kpfonts mathdesign), = \upiota (upgreek), iota, greek
003BA^κ^\kappa^\upkappa^A^mathalpha^-literal^= \mathrm{\kappa} (omlmathrm), = \kappaup (kpfonts mathdesign), = \upkappa (upgreek), kappa, greek
003BB^λ^\lambda^\uplambda^A^mathalpha^-literal^= \mathrm{\lambda} (omlmathrm), = \lambdaup (kpfonts mathdesign), = \uplambda (upgreek), lambda, greek
003BC^μ^\mu^\upmu^A^mathalpha^-literal^= \mathrm{\mu} (omlmathrm), = \muup (kpfonts mathdesign), = \upmu (upgreek), mu, greek
003BD^ν^\nu^\upnu^A^mathalpha^-literal^= \mathrm{\nu} (omlmathrm), = \nuup (kpfonts mathdesign), = \upnu (upgreek), nu, greek
003BE^ξ^\xi^\upxi^A^mathalpha^-literal^= \mathrm{\xi} (omlmathrm), = \xiup (kpfonts mathdesign), = \upxi (upgreek), xi, greek
003BF^ο^^\upomicron^A^mathalpha^^small omicron, greek
003C0^π^\pi^\uppi^A^mathalpha^-literal^= \mathrm{\pi} (omlmathrm), = \piup (kpfonts mathdesign), = \uppi (upgreek), pi, greek
003C1^ρ^\rho^\uprho^A^mathalpha^-literal^= \mathrm{\rho} (omlmathrm), = \rhoup (kpfonts mathdesign), = \uprho (upgreek), rho, greek
003C2^ς^\varsigma^\upvarsigma^^mathalpha^-literal^= \mathrm{\varsigma} (omlmathrm), = \varsigmaup (kpfonts mathdesign), = \upvarsigma (upgreek), terminal sigma, greek
003C3^σ^\sigma^\upsigma^A^mathalpha^-literal^= \mathrm{\sigma} (omlmathrm), = \sigmaup (kpfonts mathdesign), = \upsigma (upgreek), sigma, greek
003C4^τ^\tau^\uptau^A^mathalpha^-literal^= \mathrm{\tau} (omlmathrm), = \tauup (kpfonts mathdesign), = \uptau (upgreek), tau, greek
003C5^υ^\upsilon^\upupsilon^A^mathalpha^-literal^= \mathrm{\upsilon} (omlmathrm), = \upsilonup (kpfonts mathdesign), = \upupsilon (upgreek), upsilon, greek
003C6^φ^\varphi^\upvarphi^A^mathalpha^-literal^= \mathrm{\varphi} (omlmathrm), = \varphiup (kpfonts mathdesign), = \upvarphi (upgreek), curly or open phi, greek
003C7^χ^\chi^\upchi^A^mathalpha^-literal^= \mathrm{\chi} (omlmathrm), = \chiup (kpfonts mathdesign), = \upchi (upgreek), chi, greek
003C8^ψ^\psi^\uppsi^A^mathalpha^-literal^= \mathrm{\psi} (omlmathrm), = \psiup (kpfonts mathdesign), = \uppsi (upgreek), psi, greek
003C9^ω^\omega^\upomega^A^mathalpha^-literal^= \mathrm{\omega} (omlmathrm), = \omegaup (kpfonts mathdesign), = \upomega (upgreek), omega, greek
003D0^ϐ^\varbeta^\upvarbeta^A^mathalpha^arevmath^rounded beta, greek
003D1^ϑ^\vartheta^\upvartheta^A^mathalpha^-literal^= \mathrm{\vartheta} (omlmathrm), = \varthetaup (kpfonts mathdesign), curly or open theta
003D2^ϒ^^\upUpsilon^A^mathalpha^^# \mathrm{\Upsilon}, GREEK UPSILON WITH HOOK SYMBOL
003D5^ϕ^\phi^\upphi^A^mathalpha^-literal^= \mathrm{\phi} (omlmathrm), = \phiup (kpfonts mathdesign), GREEK PHI SYMBOL (straight)
003D6^ϖ^\varpi^\upvarpi^A^mathalpha^-literal^= \mathrm{\varpi} (omlmathrm), = \varpiup (kpfonts mathdesign), GREEK PI SYMBOL (pomega)
003D8^Ϙ^\Qoppa^\upoldKoppa^N^mathord^arevmath^= \Koppa (wrisym), t \Qoppa (LGR), GREEK LETTER ARCHAIC KOPPA
003D9^ϙ^\qoppa^\upoldkoppa^N^mathord^arevmath^= \koppa (wrisym), t \qoppa (LGR), GREEK SMALL LETTER ARCHAIC KOPPA
003DA^Ϛ^\Stigma^\upStigma^A^mathalpha^arevmath wrisym^capital stigma
003DB^ϛ^\stigma^\upstigma^A^mathalpha^arevmath wrisym^GREEK SMALL LETTER STIGMA
003DC^Ϝ^\Digamma^\upDigamma^A^mathalpha^wrisym^capital digamma
003DD^ϝ^\digamma^\updigamma^A^mathalpha^amssymb arevmath wrisym^GREEK SMALL LETTER DIGAMMA
003DE^Ϟ^\Koppa^\upKoppa^^mathalpha^arevmath^capital koppa
003DF^ϟ^\koppa^\upkoppa^^mathalpha^arevmath^GREEK SMALL LETTER KOPPA
003E0^Ϡ^\Sampi^\upSampi^A^mathalpha^arevmath wrisym^capital sampi
003E1^ϡ^\sampi^\upsampi^A^mathalpha^arevmath^# \sampi (wrisym), GREEK SMALL LETTER SAMPI
003F0^ϰ^\varkappa^\upvarkappa^A^mathalpha^amssymb^GREEK KAPPA SYMBOL (round)
003F1^ϱ^\varrho^\upvarrho^A^mathalpha^-literal^= \mathrm{\varrho} (omlmathrm), = \varrhoup (kpfonts mathdesign), GREEK RHO SYMBOL (round)
003F4^ϴ^^\upvarTheta^A^mathalpha^^x \varTheta (amssymb), GREEK CAPITAL THETA SYMBOL
003F5^ϵ^\epsilon^\upvarepsilon^A^mathalpha^-literal^= \mathrm{\epsilon} (omlmathrm), = \epsilonup (kpfonts mathdesign), GREEK LUNATE EPSILON SYMBOL
003F6^϶^\backepsilon^\upbackepsilon^N^mathord^amssymb wrisym^GREEK REVERSED LUNATE EPSILON SYMBOL
00428^Ш^^^A^mathalpha^^t \CYRSHA (T2A), Shcy, CYRILLIC CAPITAL LETTER SHA
00606^؆^^^L^mathord^^ARABIC-INDIC CUBE ROOT
00607^؇^^^L^mathord^^ARABIC-INDIC FOURTH ROOT
00608^؈^^^A^mathord^^ARABIC RAY
02000^ ^^^S^^^enquad
02001^ ^\quad^^S^^^emquad
02002^ ^^^S^^^ensp (half an em)
02003^ ^^^S^^^emsp
02004^ ^^^S^^^THREE-PER-EM SPACE
02005^ ^^^S^^^FOUR-PER-EM SPACE, mid space
02006^ ^^^S^^^SIX-PER-EM SPACE
02007^ ^^^S^^^FIGURE SPACE
02009^ ^^^S^^^THIN SPACE
0200A^ ^^^S^^^HAIR SPACE
0200B^​^^^S^^^# \hspace{0pt}, zwsp
02010^‐^^^P^mathord^^HYPHEN (true graphic)
02012^‒^^^P^mathord^^dash
02013^–^^^P^mathord^^ndash
02014^—^^^P^mathord^^mdash
02015^―^^\horizbar^^mathord^^HORIZONTAL BAR
02016^‖^\|^\Vert^F^mathfence^^= \Vert, double vertical bar
02017^‗^^\twolowline^^mathord^^DOUBLE LOW LINE (spacing)
02020^†^\dagger^\dagger^R^mathbin^^DAGGER relation
02021^‡^\ddagger^\ddagger^R^mathbin^^DOUBLE DAGGER relation
02022^•^\bullet^\smblkcircle^B^mathbin^^BULLET (small, filled)
02025^‥^^\enleadertwodots^^mathord^^double baseline dot (en leader)
02026^…^\ldots^\unicodeellipsis^N^mathord^^ellipsis (horizontal)
02032^′^\prime^\prime^N^mathord^^PRIME or minute, not superscripted
02033^″^\second^\dprime^N^mathord^mathabx^DOUBLE PRIME or second, not superscripted
02034^‴^\third^\trprime^N^mathord^mathabx^TRIPLE PRIME (not superscripted)
02035^‵^\backprime^\backprime^N^mathord^amssymb^reverse prime, not superscripted
02036^‶^^\backdprime^N^mathord^^double reverse prime, not superscripted
02037^‷^^\backtrprime^N^mathord^^triple reverse prime, not superscripted
02038^‸^^\caretinsert^^mathord^^CARET (insertion mark)
0203B^※^^^N^^^REFERENCE MARK, Japanese kome jirushi
0203C^‼^^\Exclam^N^mathord^^# !!, DOUBLE EXCLAMATION MARK
02040^⁀^\cat^\tieconcat^B^mathbin^oz^CHARACTER TIE, z notation sequence concatenation
02043^⁃^^\hyphenbullet^^mathord^^rectangle, filled (HYPHEN BULLET)
02044^⁄^^\fracslash^B^mathbin^^# /, FRACTION SLASH
02047^⁇^^\Question^^mathord^^# ??, DOUBLE QUESTION MARK
0204E^⁎^^^B^mathbin^^# \ast, lowast, LOW ASTERISK
0204F^⁏^^^R^^^bsemi, REVERSED SEMICOLON
02050^⁐^^\closure^R^mathrel^^CLOSE UP (editing mark)
02051^⁑^^^N^^^Ast
02052^⁒^^^N^mathord^^# ./., COMMERCIAL MINUS SIGN
02057^⁗^\fourth^\qprime^N^mathord^mathabx^QUADRUPLE PRIME, not superscripted
0205F^ ^\:^^S^^^= \medspace (amsmath), MEDIUM MATHEMATICAL SPACE, four-eighteenths of an em
02061^⁡^^^B^^^FUNCTION APPLICATION
02062^⁢^^^B^^^INVISIBLE TIMES
02063^⁣^^^P^^^INVISIBLE SEPARATOR
02064^⁤^^^X^^^INVISIBLE PLUS
0207A^⁺^^^N^mathord^^SUPERSCRIPT PLUS SIGN subscript operators
0207B^⁻^^^N^mathord^^SUPERSCRIPT MINUS subscript operators
0207C^⁼^^^N^mathord^^SUPERSCRIPT EQUALS SIGN subscript operators
0207D^⁽^^^N^mathopen^^SUPERSCRIPT LEFT PARENTHESIS subscript operators
0207E^⁾^^^N^mathclose^^SUPERSCRIPT RIGHT PARENTHESIS subscript operators
0208A^₊^^^N^mathord^^SUBSCRIPT PLUS SIGN superscript operators
0208B^₋^^^N^mathord^^SUBSCRIPT MINUS superscript operators
0208C^₌^^^N^mathord^^SUBSCRIPT EQUALS SIGN superscript operators
0208D^₍^^^N^mathopen^^SUBSCRIPT LEFT PARENTHESIS superscript operators
0208E^₎^^^N^mathclose^^SUBSCRIPT RIGHT PARENTHESIS superscript operators
020AC^€^^\euro^^mathord^^EURO SIGN
020D0^x⃐^\lvec^\leftharpoonaccent^D^mathaccent^wrisym^COMBINING LEFT HARPOON ABOVE
020D1^x⃑^\vec^\rightharpoonaccent^D^mathaccent^wrisym^COMBINING RIGHT HARPOON ABOVE
020D2^x⃒^^\vertoverlay^D^mathaccent^^COMBINING LONG VERTICAL LINE OVERLAY
020D3^x⃓^^^X^mathaccent^^COMBINING SHORT VERTICAL LINE OVERLAY
020D4^x⃔^^^D^mathaccent^^COMBINING ANTICLOCKWISE ARROW ABOVE
020D5^x⃕^^^D^mathaccent^^COMBINING CLOCKWISE ARROW ABOVE
020D6^x⃖^\LVec^\overleftarrow^D^mathaccent^wrisym^# \overleftarrow, COMBINING LEFT ARROW ABOVE
020D7^x⃗^\vec^\vec^D^mathaccent^-wrisym^= \Vec (wrisym), # \overrightarrow, COMBINING RIGHT ARROW ABOVE
020D8^x⃘^^^D^mathaccent^^COMBINING RING OVERLAY
020D9^x⃙^^^D^mathaccent^^COMBINING CLOCKWISE RING OVERLAY
020DA^x⃚^^^D^mathaccent^^COMBINING ANTICLOCKWISE RING OVERLAY
020DB^x⃛^\dddot^\dddot^D^mathaccent^amsmath^= \DDDot (wrisym), COMBINING THREE DOTS ABOVE
020DC^x⃜^\ddddot^\ddddot^D^mathaccent^amsmath^COMBINING FOUR DOTS ABOVE
020DD^x⃝^^\enclosecircle^D^mathaccent^^COMBINING ENCLOSING CIRCLE
020DE^x⃞^^\enclosesquare^D^mathaccent^^COMBINING ENCLOSING SQUARE
020DF^x⃟^^\enclosediamond^D^mathaccent^^COMBINING ENCLOSING DIAMOND
020E1^x⃡^\overleftrightarrow^\overleftrightarrow^D^mathaccent^amsmath^COMBINING LEFT RIGHT ARROW ABOVE
020E4^x⃤^^\enclosetriangle^D^mathaccent^^COMBINING ENCLOSING UPWARD POINTING TRIANGLE
020E5^x⃥^^^D^mathaccent^^COMBINING REVERSE SOLIDUS OVERLAY
020E6^x⃦^^^D^mathaccent^^COMBINING DOUBLE VERTICAL STROKE OVERLAY, z notation finite function diacritic
020E7^x⃧^^\annuity^D^mathaccent^^COMBINING ANNUITY SYMBOL
020E8^x⃨^^\threeunderdot^D^mathaccent^^COMBINING TRIPLE UNDERDOT
020E9^x⃩^^\widebridgeabove^D^mathaccent^^COMBINING WIDE BRIDGE ABOVE
020EA^x⃪^^^D^mathaccent^^COMBINING LEFTWARDS ARROW OVERLAY
020EB^x⃫^^^D^mathaccent^^COMBINING LONG DOUBLE SOLIDUS OVERLAY
020EC^x⃬^^\underrightharpoondown^D^mathaccent^^COMBINING RIGHTWARDS HARPOON WITH BARB DOWNWARDS
020ED^x⃭^^\underleftharpoondown^D^mathaccent^^COMBINING LEFTWARDS HARPOON WITH BARB DOWNWARDS
020EE^x⃮^\underleftarrow^\underleftarrow^D^mathaccent^amsmath^COMBINING LEFT ARROW BELOW
020EF^x⃯^\underrightarrow^\underrightarrow^D^mathaccent^amsmath^COMBINING RIGHT ARROW BELOW
020F0^x⃰^^\asteraccent^^mathaccent^^COMBINING ASTERISK ABOVE
02102^ℂ^\mathbb{C}^\BbbC^A^mathalpha^mathbb^= \mathds{C} (dsfont), open face C
02107^ℇ^\Euler^\Eulerconst^N^mathord^wrisym^EULER CONSTANT
0210A^ℊ^\mathcal{g}^\mscrg^A^mathalpha^urwchancal^/scr g, script small letter g
0210B^ℋ^\mathcal{H}^\mscrH^A^mathalpha^^hamiltonian (script capital H)
0210C^ℌ^\mathfrak{H}^\mfrakH^A^mathalpha^eufrak^/frak H, black-letter capital H
0210D^ℍ^\mathbb{H}^\BbbH^A^mathalpha^mathbb^= \mathds{H} (dsfont), open face capital H
0210E^ℎ^^\Planckconst^N^mathord^^# h, Planck constant
0210F^ℏ^\hslash^\hslash^N^mathalpha^amssymb fourier arevmath^=\HBar (wrisym), #\hbar, Planck's h over 2pi
02110^ℐ^\mathcal{I}^\mscrI^A^mathalpha^^/scr I, script capital I
02111^ℑ^\Im^\Im^A^mathalpha^^= \mathfrak{I} (eufrak), imaginary part
02112^ℒ^\mathcal{L}^\mscrL^A^mathalpha^^lagrangian (script capital L)
02113^ℓ^\ell^\ell^A^mathalpha^^cursive small l
02115^ℕ^\mathbb{N}^\BbbN^A^mathalpha^mathbb^= \mathds{N} (dsfont), open face N
02118^℘^\wp^\wp^A^mathalpha^amssymb^weierstrass p
02119^ℙ^\mathbb{P}^\BbbP^A^mathalpha^mathbb^= \mathds{P} (dsfont), open face P
0211A^ℚ^\mathbb{Q}^\BbbQ^A^mathalpha^mathbb^= \mathds{Q} (dsfont), open face Q
0211B^ℛ^\mathcal{R}^\mscrR^A^mathalpha^^/scr R, script capital R
0211C^ℜ^\Re^\Re^A^mathalpha^^= \mathfrak{R} (eufrak), real part
0211D^ℝ^\mathbb{R}^\BbbR^A^mathalpha^mathbb^= \mathds{R} (dsfont), open face R
02124^ℤ^\mathbb{Z}^\BbbZ^A^mathalpha^mathbb^= \mathds{Z} (dsfont), open face Z
02126^Ω^\tcohm^^N^mathalpha^mathcomp^# \mathrm{\Omega}, ohm (deprecated in math, use greek letter)
02127^℧^\mho^\mho^N^mathord^amsfonts arevmath^= \Mho (wrisym), t \agemO (wasysym), conductance
02128^ℨ^\mathfrak{Z}^\mfrakZ^A^mathalpha^eufrak^/frak Z, black-letter capital Z
02129^℩^^\turnediota^N^mathalpha^^turned iota
0212B^Å^\Angstroem^\Angstrom^A^mathalpha^wrisym^# \mathring{\mathrm{A}}, Ångström capital A with ring
0212C^ℬ^\mathcal{B}^\mscrB^A^mathalpha^^bernoulli function (script capital B)
0212D^ℭ^\mathfrak{C}^\mfrakC^A^mathalpha^eufrak^black-letter capital C
0212F^ℯ^\mathcal{e}^\mscre^A^mathalpha^urwchancal^/scr e, script small letter e
02130^ℰ^\mathcal{E}^\mscrE^A^mathalpha^^/scr E, script capital E
02131^ℱ^\mathcal{F}^\mscrF^A^mathalpha^^/scr F, script capital F
02132^Ⅎ^\Finv^\Finv^N^mathord^amssymb^TURNED CAPITAL F
02133^ℳ^\mathcal{M}^\mscrM^A^mathalpha^^physics m-matrix (SCRIPT CAPITAL M)
02134^ℴ^\mathcal{o}^\mscro^A^mathalpha^urwchancal^order of (SCRIPT SMALL O)
02135^ℵ^\aleph^\aleph^A^mathalpha^^aleph, hebrew
02136^ℶ^\beth^\beth^A^mathalpha^amssymb wrisym^beth, hebrew
02137^ℷ^\gimel^\gimel^A^mathalpha^amssymb wrisym^gimel, hebrew
02138^ℸ^\daleth^\daleth^A^mathalpha^amssymb wrisym^daleth, hebrew
0213C^ℼ^\mathbb{\pi}^\Bbbpi^A^mathord^mathbbol^\DoublePi (wrisym), DOUBLE-STRUCK SMALL PI
0213D^ℽ^\mathbb{\gamma}^\Bbbgamma^A^mathalpha^mathbbol^\EulerGamma (wrisym), DOUBLE-STRUCK SMALL GAMMA
0213E^ℾ^\mathbb{\Gamma}^\BbbGamma^N^mathalpha^mathbbol^DOUBLE-STRUCK CAPITAL GAMMA
0213F^ℿ^\mathbb{\Pi}^\BbbPi^A^mathalpha^mathbbol^DOUBLE-STRUCK CAPITAL PI
02140^⅀^\mathbb{\Sigma}^\Bbbsum^L^mathop^mathbbol^DOUBLE-STRUCK N-ARY SUMMATION
02141^⅁^^\Game^N^mathord^^# \Game (amssymb), TURNED SANS-SERIF CAPITAL G (amssymb has mirrored G)
02142^⅂^^\sansLturned^N^mathord^^TURNED SANS-SERIF CAPITAL L
02143^⅃^^\sansLmirrored^N^mathord^^REVERSED SANS-SERIF CAPITAL L
02144^⅄^\Yup^\Yup^N^mathord^stmaryrd^TURNED SANS-SERIF CAPITAL Y
02145^ⅅ^\CapitalDifferentialD^\mitBbbD^N^mathord^wrisym^= \DD (wrisym), DOUBLE-STRUCK ITALIC CAPITAL D
02146^ⅆ^\DifferentialD^\mitBbbd^N^mathord^wrisym^= \dd (wrisym), DOUBLE-STRUCK ITALIC SMALL D
02147^ⅇ^\ExponetialE^\mitBbbe^N^mathord^wrisym^= \ee (wrisym), DOUBLE-STRUCK ITALIC SMALL E
02148^ⅈ^\ComplexI^\mitBbbi^N^mathord^wrisym^= \ii (wrisym), DOUBLE-STRUCK ITALIC SMALL I
02149^ⅉ^\ComplexJ^\mitBbbj^N^mathord^wrisym^= \jj (wrisym), DOUBLE-STRUCK ITALIC SMALL J
0214A^⅊^^\PropertyLine^^mathord^^PROPERTY LINE
0214B^⅋^\invamp^\upand^N^mathbin^txfonts^# \bindnasrepma (stmaryrd), TURNED AMPERSAND
02190^←^\leftarrow^\leftarrow^R^mathrel^^= \gets, a: leftward arrow
02191^↑^\uparrow^\uparrow^R^mathrel^^upward arrow
02192^→^\rightarrow^\rightarrow^R^mathrel^^= \to, = \tfun (oz), = \fun (oz), rightward arrow, z notation total function
02193^↓^\downarrow^\downarrow^R^mathrel^^downward arrow
02194^↔^\leftrightarrow^\leftrightarrow^R^mathrel^-wrisym^= \rel (oz), LEFT RIGHT ARROW, z notation relation
02195^↕^\updownarrow^\updownarrow^R^mathrel^^up and down arrow
02196^↖^\nwarrow^\nwarrow^R^mathrel^amssymb^nw pointing arrow
02197^↗^\nearrow^\nearrow^R^mathrel^^ne pointing arrow
02198^↘^\searrow^\searrow^R^mathrel^^se pointing arrow
02199^↙^\swarrow^\swarrow^R^mathrel^^sw pointing arrow
0219A^↚^\nleftarrow^\nleftarrow^R^mathrel^amssymb^not left arrow
0219B^↛^\nrightarrow^\nrightarrow^R^mathrel^amssymb^not right arrow
0219C^↜^^\leftwavearrow^R^mathrel^^left arrow-wavy
0219D^↝^^\rightwavearrow^R^mathrel^^right arrow-wavy
0219E^↞^\twoheadleftarrow^\twoheadleftarrow^R^mathrel^amssymb^left two-headed arrow
0219F^↟^^\twoheaduparrow^R^mathrel^^up two-headed arrow
021A0^↠^\twoheadrightarrow^\twoheadrightarrow^R^mathrel^amssymb^= \tsur (oz), = \surj (oz), right two-headed arrow, z notation total surjection
021A1^↡^^\twoheaddownarrow^R^mathrel^^down two-headed arrow
021A2^↢^\leftarrowtail^\leftarrowtail^R^mathrel^amssymb^left arrow-tailed
021A3^↣^\rightarrowtail^\rightarrowtail^R^mathrel^amssymb^= \tinj (oz), = \inj (oz), right arrow-tailed, z notation total injection
021A4^↤^\mapsfrom^\mapsfrom^R^mathrel^stmaryrd^= \mappedfrom (kpfonts), maps to, leftward
021A5^↥^\MapsUp^\mapsup^R^mathrel^wrisym^maps to, upward
021A6^↦^\mapsto^\mapsto^R^mathrel^^maps to, rightward, z notation maplet
021A7^↧^\MapsDown^\mapsdown^R^mathrel^wrisym^maps to, downward
021A8^↨^^\updownarrowbar^R^mathord^^UP DOWN ARROW WITH BASE (perpendicular)
021A9^↩^\hookleftarrow^\hookleftarrow^R^mathrel^^left arrow-hooked
021AA^↪^\hookrightarrow^\hookrightarrow^R^mathrel^^right arrow-hooked
021AB^↫^\looparrowleft^\looparrowleft^R^mathrel^amssymb^left arrow-looped
021AC^↬^\looparrowright^\looparrowright^R^mathrel^amssymb^right arrow-looped
021AD^↭^\leftrightsquigarrow^\leftrightsquigarrow^R^mathrel^amssymb^left and right arr-wavy
021AE^↮^\nleftrightarrow^\nleftrightarrow^R^mathrel^amssymb^not left and right arrow
021AF^↯^\lightning^\downzigzagarrow^R^mathrel^stmaryrd -wasysym^t \Lightning (marvosym), DOWNWARDS ZIGZAG ARROW
021B0^↰^\Lsh^\Lsh^R^mathrel^amssymb^a: UPWARDS ARROW WITH TIP LEFTWARDS
021B1^↱^\Rsh^\Rsh^R^mathrel^amssymb^a: UPWARDS ARROW WITH TIP RIGHTWARDS
021B2^↲^\dlsh^\Ldsh^R^mathrel^mathabx^left down angled arrow
021B3^↳^\drsh^\Rdsh^R^mathrel^mathabx^right down angled arrow
021B4^↴^^\linefeed^^mathord^^RIGHTWARDS ARROW WITH CORNER DOWNWARDS
021B5^↵^^\carriagereturn^^mathord^^downwards arrow with corner leftward = carriage return
021B6^↶^\curvearrowleft^\curvearrowleft^R^mathrel^amssymb fourier^left curved arrow
021B7^↷^\curvearrowright^\curvearrowright^R^mathrel^amssymb fourier^right curved arrow
021B8^↸^^\barovernorthwestarrow^^mathord^^NORTH WEST ARROW TO LONG BAR
021B9^↹^^\barleftarrowrightarrowba^^mathord^^LEFTWARDS ARROW TO BAR OVER RIGHTWARDS ARROW TO BAR
021BA^↺^\circlearrowleft^\acwopencirclearrow^R^mathord^amssymb^= \leftturn (wasysym), ANTICLOCKWISE OPEN CIRCLE ARROW
021BB^↻^\circlearrowright^\cwopencirclearrow^R^mathord^amssymb^= \rightturn (wasysym), CLOCKWISE OPEN CIRCLE ARROW
021BC^↼^\leftharpoonup^\leftharpoonup^R^mathrel^^left harpoon-up
021BD^↽^\leftharpoondown^\leftharpoondown^R^mathrel^^left harpoon-down
021BE^↾^\upharpoonright^\upharpoonright^R^mathrel^amssymb^= \restriction (amssymb), = \upharpoonrightup (wrisym), a: up harpoon-right
021BF^↿^\upharpoonleft^\upharpoonleft^R^mathrel^amssymb^= \upharpoonleftup (wrisym), up harpoon-left
021C0^⇀^\rightharpoonup^\rightharpoonup^R^mathrel^^right harpoon-up
021C1^⇁^\rightharpoondown^\rightharpoondown^R^mathrel^^right harpoon-down
021C2^⇂^\downharpoonright^\downharpoonright^R^mathrel^amssymb^= \upharpoonrightdown (wrisym), down harpoon-right
021C3^⇃^\downharpoonleft^\downharpoonleft^R^mathrel^amssymb^= \upharpoonleftdown (wrisym), down harpoon-left
021C4^⇄^\rightleftarrows^\rightleftarrows^R^mathrel^amssymb^= \rightleftarrow (wrisym), right arrow over left arrow
021C5^⇅^\updownarrows^\updownarrows^R^mathrel^mathabx^= \uparrowdownarrow (wrisym), up arrow, down arrow
021C6^⇆^\leftrightarrows^\leftrightarrows^R^mathrel^amssymb^= \leftrightarrow (wrisym), left arrow over right arrow
021C7^⇇^\leftleftarrows^\leftleftarrows^R^mathrel^amssymb fourier^two left arrows
021C8^⇈^\upuparrows^\upuparrows^R^mathrel^amssymb^two up arrows
021C9^⇉^\rightrightarrows^\rightrightarrows^R^mathrel^amssymb fourier^two right arrows
021CA^⇊^\downdownarrows^\downdownarrows^R^mathrel^amssymb^two down arrows
021CB^⇋^\leftrightharpoons^\leftrightharpoons^R^mathrel^amssymb^= \revequilibrium (wrisym), left harpoon over right
021CC^⇌^\rightleftharpoons^\rightleftharpoons^R^mathrel^^= \equilibrium (wrisym), right harpoon over left
021CD^⇍^\nLeftarrow^\nLeftarrow^R^mathrel^amssymb^not implied by
021CE^⇎^\nLeftrightarrow^\nLeftrightarrow^R^mathrel^amssymb^not left and right double arrows
021CF^⇏^\nRightarrow^\nRightarrow^R^mathrel^amssymb^not implies
021D0^⇐^\Leftarrow^\Leftarrow^R^mathrel^^left double arrow
021D1^⇑^\Uparrow^\Uparrow^R^mathrel^^up double arrow
021D2^⇒^\Rightarrow^\Rightarrow^R^mathrel^-marvosym^right double arrow
021D3^⇓^\Downarrow^\Downarrow^R^mathrel^^down double arrow
021D4^⇔^\Leftrightarrow^\Leftrightarrow^R^mathrel^^left and right double arrow
021D5^⇕^\Updownarrow^\Updownarrow^R^mathrel^^up and down double arrow
021D6^⇖^\Nwarrow^\Nwarrow^R^mathrel^txfonts^nw pointing double arrow
021D7^⇗^\Nearrow^\Nearrow^R^mathrel^txfonts^ne pointing double arrow
021D8^⇘^\Searrow^\Searrow^R^mathrel^txfonts^se pointing double arrow
021D9^⇙^\Swarrow^\Swarrow^R^mathrel^txfonts^sw pointing double arrow
021DA^⇚^\Lleftarrow^\Lleftarrow^R^mathrel^amssymb^left triple arrow
021DB^⇛^\Rrightarrow^\Rrightarrow^R^mathrel^amssymb^right triple arrow
021DC^⇜^\leftsquigarrow^\leftsquigarrow^R^mathrel^mathabx txfonts^LEFTWARDS SQUIGGLE ARROW
021DD^⇝^\rightsquigarrow^\rightsquigarrow^R^mathrel^amssymb^RIGHTWARDS SQUIGGLE ARROW
021DE^⇞^^\nHuparrow^R^mathord^^UPWARDS ARROW WITH DOUBLE STROKE
021DF^⇟^^\nHdownarrow^R^mathord^^DOWNWARDS ARROW WITH DOUBLE STROKE
021E0^⇠^\dashleftarrow^\leftdasharrow^R^mathord^amsfonts^LEFTWARDS DASHED ARROW
021E1^⇡^^\updasharrow^R^mathord^^UPWARDS DASHED ARROW
021E2^⇢^\dashrightarrow^\rightdasharrow^R^mathord^amsfonts^= \dasharrow (amsfonts), RIGHTWARDS DASHED ARROW
021E3^⇣^^\downdasharrow^R^mathord^^DOWNWARDS DASHED ARROW
021E4^⇤^\LeftArrowBar^\barleftarrow^R^mathrel^wrisym^LEFTWARDS ARROW TO BAR
021E5^⇥^\RightArrowBar^\rightarrowbar^R^mathrel^wrisym^RIGHTWARDS ARROW TO BAR
021E6^⇦^^\leftwhitearrow^R^mathord^^LEFTWARDS WHITE ARROW
021E7^⇧^^\upwhitearrow^R^mathord^^UPWARDS WHITE ARROW
021E8^⇨^^\rightwhitearrow^R^mathord^^RIGHTWARDS WHITE ARROW
021E9^⇩^^\downwhitearrow^R^mathord^^DOWNWARDS WHITE ARROW
021EA^⇪^^\whitearrowupfrombar^R^mathord^^UPWARDS WHITE ARROW FROM BAR
021EB^⇫^^^R^mathord^^UPWARDS WHITE ARROW ON PEDESTAL
021EC^⇬^^^R^mathord^^UPWARDS WHITE ARROW ON PEDESTAL WITH HORIZONTAL BAR
021ED^⇭^^^R^mathord^^UPWARDS WHITE ARROW ON PEDESTAL WITH VERTICAL BAR
021EE^⇮^^^R^mathord^^UPWARDS WHITE DOUBLE ARROW
021EF^⇯^^^R^mathord^^UPWARDS WHITE DOUBLE ARROW ON PEDESTAL
021F0^⇰^^^R^mathord^^RIGHTWARDS WHITE ARROW FROM WALL
021F1^⇱^^^R^mathord^^NORTH WEST ARROW TO CORNER
021F2^⇲^^^R^mathord^^SOUTH EAST ARROW TO CORNER
021F3^⇳^^^R^mathord^^UP DOWN WHITE ARROW
021F4^⇴^^\circleonrightarrow^R^mathrel^^RIGHT ARROW WITH SMALL CIRCLE
021F5^⇵^\downuparrows^\downuparrows^R^mathrel^mathabx^= \downarrowuparrow (wrisym), DOWNWARDS ARROW LEFTWARDS OF UPWARDS ARROW
021F6^⇶^^\rightthreearrows^R^mathrel^^THREE RIGHTWARDS ARROWS
021F7^⇷^^\nvleftarrow^R^mathrel^^LEFTWARDS ARROW WITH VERTICAL STROKE
021F8^⇸^\pfun^\nvrightarrow^R^mathrel^oz^RIGHTWARDS ARROW WITH VERTICAL STROKE, z notation partial function
021F9^⇹^^\nvleftrightarrow^R^mathrel^^LEFT RIGHT ARROW WITH VERTICAL STROKE, z notation partial relation
021FA^⇺^^\nVleftarrow^R^mathrel^^LEFTWARDS ARROW WITH DOUBLE VERTICAL STROKE
021FB^⇻^\ffun^\nVrightarrow^R^mathrel^oz^RIGHTWARDS ARROW WITH DOUBLE VERTICAL STROKE, z notation finite function
021FC^⇼^^\nVleftrightarrow^R^mathrel^^LEFT RIGHT ARROW WITH DOUBLE VERTICAL STROKE, z notation finite relation
021FD^⇽^\leftarrowtriangle^\leftarrowtriangle^R^mathrel^stmaryrd^LEFTWARDS OPEN-HEADED ARROW
021FE^⇾^\rightarrowtriangle^\rightarrowtriangle^R^mathrel^stmaryrd^RIGHTWARDS OPEN-HEADED ARROW
021FF^⇿^\leftrightarrowtriangle^\leftrightarrowtriangle^R^mathrel^stmaryrd^LEFT RIGHT OPEN-HEADED ARROW
02200^∀^\forall^\forall^U^mathord^^FOR ALL
02201^∁^\complement^\complement^U^mathord^amssymb fourier^COMPLEMENT sign
02202^∂^\partial^\partial^N^mathord^-literal^= \partialup (kpfonts), PARTIAL DIFFERENTIAL
02203^∃^\exists^\exists^U^mathord^^= \exi (oz), at least one exists
02204^∄^\nexists^\nexists^U^mathord^amssymb fourier^= \nexi (oz), negated exists
02205^∅^\varnothing^\varnothing^N^mathord^amssymb^circle, slash
02206^∆^^\increment^U^mathord^^# \mathrm{\Delta}, laplacian (Delta; nabla square)
02207^∇^\nabla^\nabla^U^mathord^^NABLA, del, hamilton operator
02208^∈^\in^\in^R^mathrel^^set membership, variant
02209^∉^\notin^\notin^R^mathrel^^= \nin (wrisym), negated set membership
0220A^∊^^\smallin^R^mathrel^^set membership (small set membership)
0220B^∋^\ni^\ni^R^mathrel^^= \owns, contains, variant
0220C^∌^\nni^\nni^R^mathrel^wrisym^= \notni (txfonts), = \notowner (mathabx), = \notowns (fourier), negated contains, variant
0220D^∍^^\smallni^R^mathrel^^r: contains (SMALL CONTAINS AS MEMBER)
0220E^∎^^\QED^N^mathord^^# \blacksquare (amssymb), END OF PROOF
0220F^∏^\prod^\prod^L^mathop^^product operator
02210^∐^\coprod^\coprod^L^mathop^^coproduct operator
02211^∑^\sum^\sum^L^mathop^^summation operator
02212^−^-^\minus^V^mathbin^^MINUS SIGN
02213^∓^\mp^\mp^V^mathbin^^MINUS-OR-PLUS SIGN
02214^∔^\dotplus^\dotplus^B^mathbin^amssymb^plus sign, dot above
02215^∕^\slash^\divslash^B^mathbin^^DIVISION SLASH
02216^∖^\smallsetminus^\smallsetminus^B^mathbin^amssymb fourier^small SET MINUS (cf. reverse solidus)
02217^∗^\ast^\ast^B^mathbin^^ASTERISK OPERATOR (Hodge star operator)
02218^∘^\circ^\vysmwhtcircle^B^mathbin^^composite function (small circle)
02219^∙^\bullet^\vysmblkcircle^B^mathbin^^BULLET OPERATOR
0221A^√^\sqrt^\sqrt^L^mathradical^^radical
0221B^∛^\sqrt[3]^\cuberoot^L^mathradical^^CUBE ROOT
0221C^∜^\sqrt[4]^\fourthroot^L^mathradical^^FOURTH ROOT
0221D^∝^\propto^\propto^R^mathrel^^# \varpropto (amssymb), is PROPORTIONAL TO
0221E^∞^\infty^\infty^N^mathord^^INFINITY
0221F^∟^\rightangle^\rightangle^N^mathord^wrisym^right (90 degree) angle
02220^∠^\angle^\angle^N^mathord^^ANGLE
02221^∡^\measuredangle^\measuredangle^N^mathord^amssymb wrisym^MEASURED ANGLE
02222^∢^\sphericalangle^\sphericalangle^N^mathord^amssymb wrisym^SPHERICAL ANGLE
02223^∣^\mid^\mid^R^mathrel^^r: DIVIDES
02224^∤^\nmid^\nmid^R^mathrel^amssymb^negated mid, DOES NOT DIVIDE
02225^∥^\parallel^\parallel^R^mathrel^^parallel
02226^∦^\nparallel^\nparallel^R^mathrel^amssymb fourier^not parallel
02227^∧^\wedge^\wedge^B^mathbin^amssymb^= \land, b: LOGICAL AND
02228^∨^\vee^\vee^B^mathbin^^= \lor, b: LOGICAL OR
02229^∩^\cap^\cap^B^mathbin^^INTERSECTION
0222A^∪^\cup^\cup^B^mathbin^^UNION or logical sum
0222B^∫^\int^\int^L^mathop^^INTEGRAL operator
0222C^∬^\iint^\iint^L^mathop^amsmath fourier esint wasysym^DOUBLE INTEGRAL operator
0222D^∭^\iiint^\iiint^L^mathop^amsmath fourier esint wasysym^TRIPLE INTEGRAL operator
0222E^∮^\oint^\oint^L^mathop^^CONTOUR INTEGRAL operator
0222F^∯^\oiint^\oiint^L^mathop^esint wasysym fourier^= \dbloint (wrisym), double contour integral operator
02230^∰^\oiiint^\oiiint^L^mathop^txfonts fourier^triple contour integral operator
02231^∱^^\intclockwise^L^mathop^^CLOCKWISE INTEGRAL
02232^∲^\varointclockwise^\varointclockwise^L^mathop^esint^= \clockoint (wrisym), contour integral, clockwise
02233^∳^\ointctrclockwise^\ointctrclockwise^L^mathop^esint^= \cntclockoint (wrisym), contour integral, anticlockwise
02234^∴^\therefore^\therefore^R^mathord^amssymb wrisym^= \wasytherefore (wasysym), THEREFORE
02235^∵^\because^\because^R^mathord^amssymb wrisym^BECAUSE
02236^∶^:^\mathratio^R^mathrel^^x \colon, RATIO
02237^∷^\Proportion^\Colon^R^mathrel^wrisym^# ::, two colons
02238^∸^^\dotminus^B^mathbin^^minus sign, dot above
02239^∹^\eqcolon^\dashcolon^R^mathrel^txfonts -mathabx^# -: ,EXCESS
0223A^∺^^\dotsminusdots^R^mathrel^^minus with four dots, GEOMETRIC PROPORTION
0223B^∻^^\kernelcontraction^R^mathrel^^HOMOTHETIC
0223C^∼^\sim^\sim^R^mathrel^^similar to, TILDE OPERATOR
0223D^∽^\backsim^\backsim^R^mathrel^amssymb^reverse similar
0223E^∾^^\invlazys^B^mathbin^^most positive, INVERTED LAZY S
0223F^∿^\AC^\sinewave^N^mathord^wasysym^SINE WAVE, alternating current
02240^≀^\wr^\wr^B^mathbin^amssymb^WREATH PRODUCT
02241^≁^\nsim^\nsim^R^mathrel^amssymb wrisym^not similar
02242^≂^\eqsim^\eqsim^R^mathrel^amssymb^equals, similar
02243^≃^\simeq^\simeq^R^mathrel^^similar, equals
02244^≄^\nsimeq^\nsime^R^mathrel^txfonts^not similar, equals
02245^≅^\cong^\cong^R^mathrel^^congruent with
02246^≆^^\simneqq^R^mathrel^^similar, not equals [vert only for 9573 entity]
02247^≇^\ncong^\ncong^R^mathrel^amssymb wrisym^not congruent with
02248^≈^\approx^\approx^R^mathrel^^approximate
02249^≉^\napprox^\napprox^R^mathrel^wrisym^not approximate
0224A^≊^\approxeq^\approxeq^R^mathrel^amssymb^approximate, equals
0224B^≋^^\approxident^R^mathrel^^approximately identical to
0224C^≌^^\backcong^R^mathrel^^ALL EQUAL TO
0224D^≍^\asymp^\asymp^R^mathrel^^asymptotically equal to
0224E^≎^\Bumpeq^\Bumpeq^R^mathrel^amssymb wrisym^bumpy equals
0224F^≏^\bumpeq^\bumpeq^R^mathrel^amssymb wrisym^bumpy equals, equals
02250^≐^\doteq^\doteq^R^mathrel^^= \dotequal (wrisym), equals, single dot above
02251^≑^\Doteq^\Doteq^R^mathrel^amssymb^= \doteqdot (amssymb), /doteq r: equals, even dots
02252^≒^\fallingdotseq^\fallingdotseq^R^mathrel^amssymb^equals, falling dots
02253^≓^\risingdotseq^\risingdotseq^R^mathrel^amssymb^equals, rising dots
02254^≔^\coloneq^\coloneq^R^mathrel^mathabx -txfonts^= \coloneqq (txfonts), = \SetDelayed (wrisym), # := colon, equals
02255^≕^\eqcolon^\eqcolon^R^mathrel^mathabx -txfonts^= \eqqcolon (txfonts), # =:, equals, colon
02256^≖^\eqcirc^\eqcirc^R^mathrel^amssymb^circle on equals sign
02257^≗^\circeq^\circeq^R^mathrel^amssymb^circle, equals
02258^≘^^\arceq^R^mathrel^^arc, equals; CORRESPONDS TO
02259^≙^\corresponds^\wedgeq^R^mathrel^mathabx^= \sdef (oz), t \Corresponds (marvosym), corresponds to (wedge over equals)
0225A^≚^^\veeeq^R^mathrel^^logical or, equals
0225B^≛^^\stareq^R^mathrel^^STAR EQUALS
0225C^≜^\triangleq^\triangleq^R^mathrel^amssymb^= \varsdef (oz), triangle, equals
0225D^≝^^\eqdef^R^mathrel^^equals by definition
0225E^≞^^\measeq^R^mathrel^^MEASURED BY (m over equals)
0225F^≟^^\questeq^R^mathrel^^equal with questionmark
02260^≠^\neq^\ne^R^mathrel^^= \ne, r: not equal
02261^≡^\equiv^\equiv^R^mathrel^^identical with
02262^≢^\nequiv^\nequiv^R^mathrel^wrisym^not identical with
02263^≣^^\Equiv^R^mathrel^^strict equivalence (4 lines)
02264^≤^\leq^\leq^R^mathrel^^= \le, r: less-than-or-equal
02265^≥^\geq^\geq^R^mathrel^^= \ge, r: greater-than-or-equal
02266^≦^\leqq^\leqq^R^mathrel^amssymb^less, double equals
02267^≧^\geqq^\geqq^R^mathrel^amssymb^greater, double equals
02268^≨^\lneqq^\lneqq^R^mathrel^amssymb^less, not double equals
02269^≩^\gneqq^\gneqq^R^mathrel^amssymb^greater, not double equals
0226A^≪^\ll^\ll^R^mathrel^^much less than, type 2
0226B^≫^\gg^\gg^R^mathrel^^much greater than, type 2
0226C^≬^\between^\between^R^mathrel^amssymb^BETWEEN
0226D^≭^\notasymp^\nasymp^R^mathrel^mathabx^= \nasymp (wrisym), not asymptotically equal to
0226E^≮^\nless^\nless^R^mathrel^amssymb^NOT LESS-THAN
0226F^≯^\ngtr^\ngtr^R^mathrel^amssymb^NOT GREATER-THAN
02270^≰^\nleq^\nleq^R^mathrel^amssymb wrisym^= \nleqslant (fourier), not less-than-or-equal
02271^≱^\ngeq^\ngeq^R^mathrel^amssymb wrisym^= \ngeqslant (fourier), not greater-than-or-equal
02272^≲^\lesssim^\lesssim^R^mathrel^amssymb^= \apprle (wasysym), = \LessTilde (wrisym), less, similar
02273^≳^\gtrsim^\gtrsim^R^mathrel^amssymb^= \apprge (wasysym), = \GreaterTilde (wrisym), greater, similar
02274^≴^\NotLessTilde^\nlesssim^R^mathrel^wrisym^not less, similar
02275^≵^\NotGreaterTilde^\ngtrsim^R^mathrel^wrisym^not greater, similar
02276^≶^\lessgtr^\lessgtr^R^mathrel^amssymb^less, greater
02277^≷^\gtrless^\gtrless^R^mathrel^amssymb^= \GreaterLess (wrisym), greater, less
02278^≸^^\nlessgtr^R^mathrel^wrisym^not less, greater
02279^≹^\NotGreaterLess^\ngtrless^R^mathrel^wrisym^not greater, less
0227A^≺^\prec^\prec^R^mathrel^^PRECEDES
0227B^≻^\succ^\succ^R^mathrel^^SUCCEEDS
0227C^≼^\preccurlyeq^\preccurlyeq^R^mathrel^amssymb^= \PrecedesSlantEqual (wrisym), precedes, curly equals
0227D^≽^\succcurlyeq^\succcurlyeq^R^mathrel^amssymb^= \SucceedsSlantEqual (wrisym), succeeds, curly equals
0227E^≾^\precsim^\precsim^R^mathrel^amssymb^= \PrecedesTilde (wrisym), precedes, similar
0227F^≿^\succsim^\succsim^R^mathrel^amssymb^= \SucceedsTilde (wrisym), succeeds, similar
02280^⊀^\nprec^\nprec^R^mathrel^amssymb wrisym^not precedes
02281^⊁^\nsucc^\nsucc^R^mathrel^amssymb wrisym^not succeeds
02282^⊂^\subset^\subset^R^mathrel^^subset or is implied by
02283^⊃^\supset^\supset^R^mathrel^^superset or implies
02284^⊄^\nsubset^\nsubset^R^mathrel^wrisym^not subset, variant [slash negation]
02285^⊅^\nsupset^\nsupset^R^mathrel^wrisym^not superset, variant [slash negation]
02286^⊆^\subseteq^\subseteq^R^mathrel^^subset, equals
02287^⊇^\supseteq^\supseteq^R^mathrel^^superset, equals
02288^⊈^\nsubseteq^\nsubseteq^R^mathrel^amssymb wrisym^not subset, equals
02289^⊉^\nsupseteq^\nsupseteq^R^mathrel^amssymb wrisym^not superset, equals
0228A^⊊^\subsetneq^\subsetneq^R^mathrel^amssymb^= \varsubsetneq (fourier), subset, not equals
0228B^⊋^\supsetneq^\supsetneq^R^mathrel^amssymb^superset, not equals
0228C^⊌^^\cupleftarrow^B^mathbin^^MULTISET
0228D^⊍^^\cupdot^B^mathbin^^union, with dot
0228E^⊎^\uplus^\uplus^B^mathbin^^= \buni (oz), plus sign in union
0228F^⊏^\sqsubset^\sqsubset^R^mathrel^amsfonts^square subset
02290^⊐^\sqsupset^\sqsupset^R^mathrel^amsfonts^square superset
02291^⊑^\sqsubseteq^\sqsubseteq^R^mathrel^^square subset, equals
02292^⊒^\sqsupseteq^\sqsupseteq^R^mathrel^^square superset, equals
02293^⊓^\sqcap^\sqcap^B^mathbin^^square intersection
02294^⊔^\sqcup^\sqcup^B^mathbin^^square union
02295^⊕^\oplus^\oplus^B^mathbin^^plus sign in circle
02296^⊖^\ominus^\ominus^B^mathbin^^minus sign in circle
02297^⊗^\otimes^\otimes^B^mathbin^^multiply sign in circle
02298^⊘^\oslash^\oslash^B^mathbin^^solidus in circle
02299^⊙^\odot^\odot^B^mathbin^^middle dot in circle
0229A^⊚^\circledcirc^\circledcirc^B^mathbin^amssymb^small circle in circle
0229B^⊛^\circledast^\circledast^B^mathbin^amssymb^asterisk in circle
0229C^⊜^^\circledequal^B^mathbin^^equal in circle
0229D^⊝^\circleddash^\circleddash^B^mathbin^amssymb^hyphen in circle
0229E^⊞^\boxplus^\boxplus^B^mathbin^amssymb^plus sign in box
0229F^⊟^\boxminus^\boxminus^B^mathbin^amssymb^minus sign in box
022A0^⊠^\boxtimes^\boxtimes^B^mathbin^amssymb^multiply sign in box
022A1^⊡^\boxdot^\boxdot^B^mathbin^amssymb stmaryrd^/dotsquare /boxdot b: small dot in box
022A2^⊢^\vdash^\vdash^R^mathrel^^RIGHT TACK, proves, implies, yields, (vertical, dash)
022A3^⊣^\dashv^\dashv^R^mathrel^amssymb^LEFT TACK, non-theorem, does not yield, (dash, vertical)
022A4^⊤^\top^\top^N^mathord^^DOWN TACK, top
022A5^⊥^\bot^\bot^R^mathord^^UP TACK, bottom
022A6^⊦^^\assert^R^mathrel^^# \vdash, ASSERTION (vertical, short dash)
022A7^⊧^\models^\models^R^mathrel^^MODELS (vertical, short double dash)
022A8^⊨^\vDash^\vDash^R^mathrel^amssymb fourier^TRUE (vertical, double dash)
022A9^⊩^\Vdash^\Vdash^R^mathrel^amssymb^double vertical, dash
022AA^⊪^\Vvdash^\Vvdash^R^mathrel^amssymb^triple vertical, dash
022AB^⊫^\VDash^\VDash^R^mathrel^mathabx txfonts^double vert, double dash
022AC^⊬^\nvdash^\nvdash^R^mathrel^amssymb^not vertical, dash
022AD^⊭^\nvDash^\nvDash^R^mathrel^amssymb fourier^not vertical, double dash
022AE^⊮^\nVdash^\nVdash^R^mathrel^amssymb^not double vertical, dash
022AF^⊯^\nVDash^\nVDash^R^mathrel^amssymb^not double vert, double dash
022B0^⊰^^\prurel^R^mathrel^^element PRECEDES UNDER RELATION
022B1^⊱^^\scurel^R^mathrel^^SUCCEEDS UNDER RELATION
022B2^⊲^\vartriangleleft^\vartriangleleft^R^mathrel^amssymb^left triangle, open, variant
022B3^⊳^\vartriangleright^\vartriangleright^R^mathrel^amssymb^right triangle, open, variant
022B4^⊴^\trianglelefteq^\trianglelefteq^R^mathrel^amssymb^= \unlhd (wrisym), left triangle, equals
022B5^⊵^\trianglerighteq^\trianglerighteq^R^mathrel^amssymb^= \unrhd (wrisym), right triangle, equals
022B6^⊶^\multimapdotbothA^\origof^R^mathrel^txfonts^ORIGINAL OF
022B7^⊷^\multimapdotbothB^\imageof^R^mathrel^txfonts^IMAGE OF
022B8^⊸^\multimap^\multimap^R^mathrel^amssymb^/MULTIMAP a:
022B9^⊹^^\hermitmatrix^B^mathord^^HERMITIAN CONJUGATE MATRIX
022BA^⊺^\intercal^\intercal^B^mathbin^amssymb fourier^intercal
022BB^⊻^\veebar^\veebar^B^mathbin^amssymb^logical or, bar below (large vee); exclusive disjunction
022BC^⊼^\barwedge^\barwedge^B^mathbin^amssymb^logical NAND (bar over wedge)
022BD^⊽^^\barvee^B^mathbin^^bar, vee (large vee)
022BE^⊾^^\measuredrightangle^N^mathord^^right angle-measured [with arc]
022BF^⊿^^\varlrtriangle^N^mathord^^RIGHT TRIANGLE
022C0^⋀^\bigwedge^\bigwedge^L^mathop^^logical or operator
022C1^⋁^\bigvee^\bigvee^L^mathop^^logical and operator
022C2^⋂^\bigcap^\bigcap^L^mathop^^= \dint (oz), \dinter (oz), intersection operator
022C3^⋃^\bigcup^\bigcup^L^mathop^^= \duni (oz), \dunion (oz), union operator
022C4^⋄^\diamond^\smwhtdiamond^B^mathbin^^DIAMOND OPERATOR (white diamond)
022C5^⋅^\cdot^\cdot^B^mathbin^^DOT OPERATOR (small middle dot)
022C6^⋆^\star^\star^B^mathbin^^small star, filled, low
022C7^⋇^\divideontimes^\divideontimes^B^mathbin^amssymb^division on times
022C8^⋈^\bowtie^\bowtie^R^mathrel^^= \lrtimes (txfonts), BOWTIE
022C9^⋉^\ltimes^\ltimes^B^mathbin^amssymb^times sign, left closed
022CA^⋊^\rtimes^\rtimes^B^mathbin^amssymb^times sign, right closed
022CB^⋋^\leftthreetimes^\leftthreetimes^B^mathbin^amssymb^LEFT SEMIDIRECT PRODUCT
022CC^⋌^\rightthreetimes^\rightthreetimes^B^mathbin^amssymb^RIGHT SEMIDIRECT PRODUCT
022CD^⋍^\backsimeq^\backsimeq^R^mathrel^amssymb^reverse similar, equals
022CE^⋎^\curlyvee^\curlyvee^B^mathbin^amssymb^CURLY LOGICAL OR
022CF^⋏^\curlywedge^\curlywedge^B^mathbin^amssymb^CURLY LOGICAL AND
022D0^⋐^\Subset^\Subset^R^mathrel^amssymb^DOUBLE SUBSET
022D1^⋑^\Supset^\Supset^R^mathrel^amssymb^DOUBLE SUPERSET
022D2^⋒^\Cap^\Cap^B^mathbin^amssymb^/cap /doublecap b: DOUBLE INTERSECTION
022D3^⋓^\Cup^\Cup^B^mathbin^amssymb^/cup /doublecup b: DOUBLE UNION
022D4^⋔^\pitchfork^\pitchfork^R^mathrel^amssymb^PITCHFORK
022D5^⋕^\hash^\equalparallel^R^mathrel^mathabx^parallel, equal; equal or parallel
022D6^⋖^\lessdot^\lessdot^R^mathrel^amssymb^less than, with dot
022D7^⋗^\gtrdot^\gtrdot^R^mathrel^amssymb^greater than, with dot
022D8^⋘^\lll^\lll^R^mathrel^amssymb -mathabx^triple less-than
022D9^⋙^\ggg^\ggg^R^mathrel^amssymb -mathabx^triple greater-than
022DA^⋚^\lesseqgtr^\lesseqgtr^R^mathrel^amssymb^less, equals, greater
022DB^⋛^\gtreqless^\gtreqless^R^mathrel^amssymb^greater, equals, less
022DC^⋜^^\eqless^R^mathrel^^equal-or-less
022DD^⋝^^\eqgtr^R^mathrel^^equal-or-greater
022DE^⋞^\curlyeqprec^\curlyeqprec^R^mathrel^amssymb^curly equals, precedes
022DF^⋟^\curlyeqsucc^\curlyeqsucc^R^mathrel^amssymb^curly equals, succeeds
022E0^⋠^\npreceq^\npreccurlyeq^R^mathrel^amssymb wrisym^DOES NOT PRECEDE OR EQUAL
022E1^⋡^\nsucceq^\nsucccurlyeq^R^mathrel^amssymb wrisym^not succeeds, curly equals
022E2^⋢^\nsqsubseteq^\nsqsubseteq^R^mathrel^wrisym^not, square subset, equals
022E3^⋣^\nsqsupseteq^\nsqsupseteq^R^mathrel^wrisym^not, square superset, equals
022E4^⋤^^\sqsubsetneq^R^mathrel^^square subset, not equals
022E5^⋥^^\sqsupsetneq^R^mathrel^^square superset, not equals
022E6^⋦^\lnsim^\lnsim^R^mathrel^amssymb^less, not similar
022E7^⋧^\gnsim^\gnsim^R^mathrel^amssymb^greater, not similar
022E8^⋨^\precnsim^\precnsim^R^mathrel^amssymb^precedes, not similar
022E9^⋩^\succnsim^\succnsim^R^mathrel^amssymb^succeeds, not similar
022EA^⋪^\ntriangleleft^\ntriangleleft^R^mathrel^amssymb^= \NotLeftTriangle (wrisym), not left triangle
022EB^⋫^\ntriangleright^\ntriangleright^R^mathrel^amssymb^= \NotRightTriangle (wrisym), not right triangle
022EC^⋬^\ntrianglelefteq^\ntrianglelefteq^R^mathrel^amssymb^= \nunlhd (wrisym), not left triangle, equals
022ED^⋭^\ntrianglerighteq^\ntrianglerighteq^R^mathrel^amssymb^= \nunrhd (wrisym), not right triangle, equals
022EE^⋮^\vdots^\vdots^R^mathrel^^VERTICAL ELLIPSIS
022EF^⋯^\cdots^\unicodecdots^R^mathord^^three dots, centered
022F0^⋰^\iddots^\adots^R^mathrel^mathdots^= \adots (yhmath), three dots, ascending
022F1^⋱^\ddots^\ddots^R^mathrel^^three dots, descending
022F2^⋲^^\disin^R^mathrel^^ELEMENT OF WITH LONG HORIZONTAL STROKE
022F3^⋳^^\varisins^R^mathrel^^ELEMENT OF WITH VERTICAL BAR AT END OF HORIZONTAL STROKE
022F4^⋴^^\isins^R^mathrel^^SMALL ELEMENT OF WITH VERTICAL BAR AT END OF HORIZONTAL STROKE
022F5^⋵^^\isindot^R^mathrel^^ELEMENT OF WITH DOT ABOVE
022F6^⋶^\barin^\varisinobar^R^mathrel^mathabx^ELEMENT OF WITH OVERBAR
022F7^⋷^^\isinobar^R^mathrel^^SMALL ELEMENT OF WITH OVERBAR
022F8^⋸^^\isinvb^R^mathrel^^ELEMENT OF WITH UNDERBAR
022F9^⋹^^\isinE^R^mathrel^^ELEMENT OF WITH TWO HORIZONTAL STROKES
022FA^⋺^^\nisd^R^mathrel^^CONTAINS WITH LONG HORIZONTAL STROKE
022FB^⋻^^\varnis^R^mathrel^^CONTAINS WITH VERTICAL BAR AT END OF HORIZONTAL STROKE
022FC^⋼^^\nis^R^mathrel^^SMALL CONTAINS WITH VERTICAL BAR AT END OF HORIZONTAL STROKE
022FD^⋽^^\varniobar^R^mathrel^^CONTAINS WITH OVERBAR
022FE^⋾^^\niobar^R^mathrel^^SMALL CONTAINS WITH OVERBAR
022FF^⋿^^\bagmember^R^mathrel^^# \mathsf{E}, Z NOTATION BAG MEMBERSHIP
02300^⌀^\diameter^\diameter^N^mathord^mathabx^# \varnothing (amssymb), DIAMETER SIGN
02302^⌂^^\house^N^mathord^^HOUSE
02305^⌅^^\varbarwedge^B^mathbin^^# \barwedge (amssymb), PROJECTIVE (bar over small wedge) not nand
02306^⌆^^\vardoublebarwedge^B^mathbin^^# \doublebarwedge (amssymb), PERSPECTIVE (double bar over small wedge)
02308^⌈^\lceil^\lceil^O^mathopen^^LEFT CEILING
02309^⌉^\rceil^\rceil^C^mathclose^^RIGHT CEILING
0230A^⌊^\lfloor^\lfloor^O^mathopen^^LEFT FLOOR
0230B^⌋^\rfloor^\rfloor^C^mathclose^^RIGHT FLOOR
02310^⌐^\invneg^\invnot^N^mathord^wasysym^reverse not
02311^⌑^\wasylozenge^\sqlozenge^N^mathord^wasysym^SQUARE LOZENGE
02312^⌒^^\profline^^mathord^^profile of a line
02313^⌓^^\profsurf^^mathord^^profile of a surface
02317^⌗^^\viewdata^^mathord^^VIEWDATA SQUARE
02319^⌙^^\turnednot^N^mathord^^TURNED NOT SIGN
0231C^⌜^\ulcorner^\ulcorner^O^mathopen^amsfonts^upper left corner
0231D^⌝^\urcorner^\urcorner^C^mathclose^amsfonts^upper right corner
0231E^⌞^\llcorner^\llcorner^O^mathopen^amsfonts^lower left corner
0231F^⌟^\lrcorner^\lrcorner^C^mathclose^amsfonts^lower right corner
02320^⌠^^\inttop^G^mathord^^TOP HALF INTEGRAL
02321^⌡^^\intbottom^G^mathord^^BOTTOM HALF INTEGRAL
02322^⌢^\frown^\frown^R^mathrel^^# \smallfrown, FROWN (down curve)
02323^⌣^\smile^\smile^R^mathrel^^# \smallsmile, SMILE (up curve)
0232C^⌬^^\varhexagonlrbonds^^mathord^^six carbon ring, corner down, double bonds lower right etc
02332^⌲^^\conictaper^^mathord^^CONICAL TAPER
02336^⌶^^\topbot^N^mathord^^APL FUNCTIONAL SYMBOL I-BEAM, top and bottom
02337^⌷^^^^mathord^^APL FUNCTIONAL SYMBOL SQUISH QUAD
02338^⌸^^^^mathord^^APL FUNCTIONAL SYMBOL QUAD EQUAL
02339^⌹^\APLinv^^^mathord^wasysym^APL FUNCTIONAL SYMBOL QUAD DIVIDE
0233A^⌺^^^^mathord^^APL FUNCTIONAL SYMBOL QUAD DIAMOND
0233B^⌻^^^^mathord^^APL FUNCTIONAL SYMBOL QUAD JOT
0233C^⌼^^^^mathord^^# \APLcirc{\APLbox} (wasysym), APL FUNCTIONAL SYMBOL QUAD CIRCLE
0233D^⌽^^\obar^B^mathbin^^# \APLvert{\Circle} (wasysym), x \obar (stmaryrd), APL FUNCTIONAL SYMBOL CIRCLE STILE, circle with vertical bar
0233E^⌾^^^^mathord^^# \APLcirc{\Circle} (wasysym), APL FUNCTIONAL SYMBOL CIRCLE JOT
0233F^⌿^\notslash^\APLnotslash^R^mathrel^wasysym^APL FUNCTIONAL SYMBOL SLASH BAR, solidus, bar through
02340^⍀^\notbackslash^\APLnotbackslash^^mathord^wasysym^APL FUNCTIONAL SYMBOL BACKSLASH BAR
02341^⍁^^^^mathord^^APL FUNCTIONAL SYMBOL QUAD SLASH
02342^⍂^^^^mathord^^APL FUNCTIONAL SYMBOL QUAD BACKSLASH
02343^⍃^^^^mathord^^APL FUNCTIONAL SYMBOL QUAD LESS-THAN
02344^⍄^^^^mathord^^APL FUNCTIONAL SYMBOL QUAD GREATER-THAN
02345^⍅^^^^mathord^^APL FUNCTIONAL SYMBOL LEFTWARDS VANE
02346^⍆^^^^mathord^^APL FUNCTIONAL SYMBOL RIGHTWARDS VANE
02347^⍇^\APLleftarrowbox^^^mathord^wasysym^APL FUNCTIONAL SYMBOL QUAD LEFTWARDS ARROW
02348^⍈^\APLrightarrowbox^^^mathord^wasysym^APL FUNCTIONAL SYMBOL QUAD RIGHTWARDS ARROW
02349^⍉^\invdiameter^^^mathord^wasysym^APL FUNCTIONAL SYMBOL CIRCLE BACKSLASH
0234A^⍊^^^^mathord^^APL FUNCTIONAL SYMBOL DOWN TACK UNDERBAR
0234B^⍋^^^^mathord^^# \APLvert{\APLup} (wasysym), APL FUNCTIONAL SYMBOL DELTA STILE
0234C^⍌^^^^mathord^^APL FUNCTIONAL SYMBOL QUAD DOWN CARET
0234D^⍍^^^^mathord^^APL FUNCTIONAL SYMBOL QUAD DELTA
0234E^⍎^^^^mathord^^APL FUNCTIONAL SYMBOL DOWN TACK JOT
0234F^⍏^^^^mathord^^APL FUNCTIONAL SYMBOL UPWARDS VANE
02350^⍐^\APLuparrowbox^^^mathord^wasysym^APL FUNCTIONAL SYMBOL QUAD UPWARDS ARROW
02351^⍑^^^^mathord^^APL FUNCTIONAL SYMBOL UP TACK OVERBAR
02352^⍒^^^^mathord^wasysym^# \APLvert{\APLdown} (wasysym), APL FUNCTIONAL SYMBOL DEL STILE
02353^⍓^^\APLboxupcaret^^mathord^^APL FUNCTIONAL SYMBOL QUAD UP CARET
02354^⍔^^^^mathord^^APL FUNCTIONAL SYMBOL QUAD DEL
02355^⍕^^^^mathord^^APL FUNCTIONAL SYMBOL UP TACK JOT
02356^⍖^^^^mathord^^APL FUNCTIONAL SYMBOL DOWNWARDS VANE
02357^⍗^\APLdownarrowbox^^^mathord^wasysym^APL FUNCTIONAL SYMBOL QUAD DOWNWARDS ARROW
02358^⍘^^^^mathord^^APL FUNCTIONAL SYMBOL QUOTE UNDERBAR
02359^⍙^^^^mathord^^APL FUNCTIONAL SYMBOL DELTA UNDERBAR
0235A^⍚^^^^mathord^^APL FUNCTIONAL SYMBOL DIAMOND UNDERBAR
0235B^⍛^^^^mathord^^APL FUNCTIONAL SYMBOL JOT UNDERBAR
0235C^⍜^^^^mathord^^APL FUNCTIONAL SYMBOL CIRCLE UNDERBAR
0235D^⍝^\APLcomment^^^mathord^wasysym^APL FUNCTIONAL SYMBOL UP SHOE JOT
0235E^⍞^\APLinput^^^mathord^wasysym^APL FUNCTIONAL SYMBOL QUOTE QUAD
0235F^⍟^\APLlog^^^mathord^wasysym^APL FUNCTIONAL SYMBOL CIRCLE STAR
02360^⍠^^^^mathord^^APL FUNCTIONAL SYMBOL QUAD COLON
02361^⍡^^^^mathord^^APL FUNCTIONAL SYMBOL UP TACK DIAERESIS
02362^⍢^^^^mathord^^APL FUNCTIONAL SYMBOL DEL DIAERESIS
02363^⍣^^^^mathord^^APL FUNCTIONAL SYMBOL STAR DIAERESIS
02364^⍤^^^^mathord^^APL FUNCTIONAL SYMBOL JOT DIAERESIS
02365^⍥^^^^mathord^^APL FUNCTIONAL SYMBOL CIRCLE DIAERESIS
02366^⍦^^^^mathord^^APL FUNCTIONAL SYMBOL DOWN SHOE STILE
02367^⍧^^^^mathord^^APL FUNCTIONAL SYMBOL LEFT SHOE STILE
02368^⍨^^^^mathord^^APL FUNCTIONAL SYMBOL TILDE DIAERESIS
02369^⍩^^^^mathord^^APL FUNCTIONAL SYMBOL GREATER-THAN DIAERESIS
0236A^⍪^^^^mathord^^APL FUNCTIONAL SYMBOL COMMA BAR
0236B^⍫^^^^mathord^^# \APLnot{\APLdown} (wasysym), APL FUNCTIONAL SYMBOL DEL TILDE
0236C^⍬^^^^mathord^^APL FUNCTIONAL SYMBOL ZILDE
0236D^⍭^^^^mathord^^APL FUNCTIONAL SYMBOL STILE TILDE
0236E^⍮^^^^mathord^^APL FUNCTIONAL SYMBOL SEMICOLON UNDERBAR
0236F^⍯^^^^mathord^^APL FUNCTIONAL SYMBOL QUAD NOT EQUAL
02370^⍰^^\APLboxquestion^^mathord^^APL FUNCTIONAL SYMBOL QUAD QUESTION
02371^⍱^^^^mathord^^APL FUNCTIONAL SYMBOL DOWN CARET TILDE
02372^⍲^^^^mathord^^APL FUNCTIONAL SYMBOL UP CARET TILDE
02373^⍳^^^^mathord^^APL FUNCTIONAL SYMBOL IOTA
02374^⍴^^^^mathord^^APL FUNCTIONAL SYMBOL RHO
02375^⍵^^^^mathord^^APL FUNCTIONAL SYMBOL OMEGA
02376^⍶^^^^mathord^^APL FUNCTIONAL SYMBOL ALPHA UNDERBAR
02377^⍷^^^^mathord^^APL FUNCTIONAL SYMBOL EPSILON UNDERBAR
02378^⍸^^^^mathord^^APL FUNCTIONAL SYMBOL IOTA UNDERBAR
02379^⍹^^^^mathord^^APL FUNCTIONAL SYMBOL OMEGA UNDERBAR
0237C^⍼^^\rangledownzigzagarrow^R^mathord^^RIGHT ANGLE WITH DOWNWARDS ZIGZAG ARROW
02394^⎔^^\hexagon^N^mathord^^horizontal benzene ring [hexagon flat open]
0239B^⎛^^\lparenuend^G^mathord^^LEFT PARENTHESIS UPPER HOOK
0239C^⎜^^\lparenextender^G^mathord^^LEFT PARENTHESIS EXTENSION
0239D^⎝^^\lparenlend^G^mathord^^LEFT PARENTHESIS LOWER HOOK
0239E^⎞^^\rparenuend^G^mathord^^RIGHT PARENTHESIS UPPER HOOK
0239F^⎟^^\rparenextender^G^mathord^^RIGHT PARENTHESIS EXTENSION
023A0^⎠^^\rparenlend^G^mathord^^RIGHT PARENTHESIS LOWER HOOK
023A1^⎡^^\lbrackuend^G^mathord^^LEFT SQUARE BRACKET UPPER CORNER
023A2^⎢^^\lbrackextender^G^mathord^^LEFT SQUARE BRACKET EXTENSION
023A3^⎣^^\lbracklend^G^mathord^^LEFT SQUARE BRACKET LOWER CORNER
023A4^⎤^^\rbrackuend^G^mathord^^RIGHT SQUARE BRACKET UPPER CORNER
023A5^⎥^^\rbrackextender^G^mathord^^RIGHT SQUARE BRACKET EXTENSION
023A6^⎦^^\rbracklend^G^mathord^^RIGHT SQUARE BRACKET LOWER CORNER
023A7^⎧^^\lbraceuend^G^mathord^^LEFT CURLY BRACKET UPPER HOOK
023A8^⎨^^\lbracemid^G^mathord^^LEFT CURLY BRACKET MIDDLE PIECE
023A9^⎩^^\lbracelend^G^mathord^^LEFT CURLY BRACKET LOWER HOOK
023AA^⎪^^\vbraceextender^G^mathord^^CURLY BRACKET EXTENSION
023AB^⎫^^\rbraceuend^G^mathord^^RIGHT CURLY BRACKET UPPER HOOK
023AC^⎬^^\rbracemid^G^mathord^^RIGHT CURLY BRACKET MIDDLE PIECE
023AD^⎭^^\rbracelend^G^mathord^^RIGHT CURLY BRACKET LOWER HOOK
023AE^⎮^^\intextender^G^mathord^^INTEGRAL EXTENSION
023AF^⎯^^\harrowextender^G^mathord^^HORIZONTAL LINE EXTENSION (used to extend arrows)
023B0^⎰^^\lmoustache^R^mathord^^? \lmoustache, UPPER LEFT OR LOWER RIGHT CURLY BRACKET SECTION
023B1^⎱^^\rmoustache^R^mathord^^? \rmoustache, UPPER RIGHT OR LOWER LEFT CURLY BRACKET SECTION
023B2^⎲^^\sumtop^G^mathord^^SUMMATION TOP
023B3^⎳^^\sumbottom^G^mathord^^SUMMATION BOTTOM
023B4^⎴^^\overbracket^N^mathover^^TOP SQUARE BRACKET
023B5^⎵^^\underbracket^N^mathunder^^BOTTOM SQUARE BRACKET
023B6^⎶^^\bbrktbrk^N^mathord^^BOTTOM SQUARE BRACKET OVER TOP SQUARE BRACKET
023B7^⎷^^\sqrtbottom^G^mathord^^RADICAL SYMBOL BOTTOM
023B8^⎸^^\lvboxline^^mathord^^LEFT VERTICAL BOX LINE
023B9^⎹^^\rvboxline^^mathord^^RIGHT VERTICAL BOX LINE
023CE^⏎^^\varcarriagereturn^^mathord^^RETURN SYMBOL
023D0^⏐^^^G^mathord^^VERTICAL LINE EXTENSION (VERTICAL LINE EXTENSION)
023DC^⏜^\overparen^\overparen^N^mathover^wrisym^= \wideparen (yhmath mathabx fourier), TOP PARENTHESIS (mathematical use)
023DD^⏝^\underparen^\underparen^N^mathunder^wrisym^BOTTOM PARENTHESIS (mathematical use)
023DE^⏞^\overbrace^\overbrace^N^mathover^^TOP CURLY BRACKET (mathematical use)
023DF^⏟^\underbrace^\underbrace^N^mathunder^^BOTTOM CURLY BRACKET (mathematical use)
023E0^⏠^^\obrbrak^N^mathord^^TOP TORTOISE SHELL BRACKET (mathematical use)
023E1^⏡^^\ubrbrak^N^mathord^^BOTTOM TORTOISE SHELL BRACKET (mathematical use)
023E2^⏢^^\trapezium^N^mathord^^WHITE TRAPEZIUM
023E3^⏣^^\benzenr^N^mathord^^BENZENE RING WITH CIRCLE
023E4^⏤^^\strns^N^mathord^^STRAIGHTNESS
023E5^⏥^^\fltns^N^mathord^^FLATNESS
023E6^⏦^^\accurrent^N^mathord^^# \AC (wasysym), AC CURRENT
023E7^⏧^^\elinters^N^mathord^^ELECTRICAL INTERSECTION
024C8^Ⓢ^^^N^mathord^^oS capital S in circle
02506^┆^^\bdtriplevdash^^mathord^^doubly broken vert
02580^▀^^\blockuphalf^^mathord^^UPPER HALF BLOCK
02584^▄^^\blocklowhalf^^mathord^^LOWER HALF BLOCK
02588^█^^\blockfull^^mathord^^FULL BLOCK
0258C^▌^^\blocklefthalf^^mathord^^LEFT HALF BLOCK
02590^▐^^\blockrighthalf^^mathord^^RIGHT HALF BLOCK
02591^░^^\blockqtrshaded^^mathord^^25\% shaded block
02592^▒^^\blockhalfshaded^^mathord^^50\% shaded block
02593^▓^^\blockthreeqtrshaded^^mathord^^75\% shaded block
025A0^■^^\mdlgblksquare^N^mathord^^square, filled
025A1^□^^\mdlgwhtsquare^N^mathord^^square, open
025A2^▢^^\squoval^^mathord^^WHITE SQUARE WITH ROUNDED CORNERS
025A3^▣^^\blackinwhitesquare^^mathord^^WHITE SQUARE CONTAINING BLACK SMALL SQUARE
025A4^▤^^\squarehfill^^mathord^^square, horizontal rule filled
025A5^▥^^\squarevfill^^mathord^^square, vertical rule filled
025A6^▦^^\squarehvfill^^mathord^^SQUARE WITH ORTHOGONAL CROSSHATCH FILL
025A7^▧^^\squarenwsefill^^mathord^^square, nw-to-se rule filled
025A8^▨^^\squareneswfill^^mathord^^square, ne-to-sw rule filled
025A9^▩^^\squarecrossfill^^mathord^^SQUARE WITH DIAGONAL CROSSHATCH FILL
025AA^▪^^\smblksquare^N^mathord^^sq bullet, filled
025AB^▫^^\smwhtsquare^N^mathord^^WHITE SMALL SQUARE
025AC^▬^^\hrectangleblack^^mathord^^BLACK RECTANGLE
025AD^▭^^\hrectangle^N^mathord^^horizontal rectangle, open
025AE^▮^^\vrectangleblack^N^mathord^^BLACK VERTICAL RECTANGLE
025AF^▯^^\vrectangle^N^mathord^^rectangle, white (vertical)
025B0^▰^^\parallelogramblack^N^mathord^^BLACK PARALLELOGRAM
025B1^▱^^\parallelogram^N^mathord^^parallelogram, open
025B2^▲^^\bigblacktriangleup^B^mathord^^BLACK UP-POINTING TRIANGLE
025B3^△^\bigtriangleup^\bigtriangleup^B^mathbin^-stmaryrd^= \triangle (amsfonts), # \vartriangle (amssymb), big up triangle, open
025B4^▴^\blacktriangleup^\blacktriangle^B^mathbin^mathabx^up triangle, filled
025B5^▵^\smalltriangleup^\vartriangle^B^mathbin^mathabx^# \vartriangle (amssymb), small up triangle, open
025B6^▶^\RHD^\blacktriangleright^B^mathbin^wasysym^= \blacktriangleright (fourier -mathabx), (large) right triangle, filled
025B7^▷^\rhd^\triangleright^B^mathbin^amssymb wasysym^= \rres (oz), = \RightTriangle (wrisym), (large) right triangle, open; z notation range restriction
025B8^▸^\blacktriangleright^\smallblacktriangleright^B^mathbin^mathabx -fourier^right triangle, filled
025B9^▹^\smalltriangleright^\smalltriangleright^B^mathbin^mathabx^# \triangleright, x \triangleright (mathabx), right triangle, open
025BA^►^^\blackpointerright^^mathord^^BLACK RIGHT-POINTING POINTER
025BB^▻^^\whitepointerright^^mathord^^# \triangleright (mathabx), WHITE RIGHT-POINTING POINTER
025BC^▼^^\bigblacktriangledown^B^mathord^^big down triangle, filled
025BD^▽^\bigtriangledown^\bigtriangledown^B^mathbin^-stmaryrd^big down triangle, open
025BE^▾^\blacktriangledown^\blacktriangledown^B^mathbin^mathabx^BLACK DOWN-POINTING SMALL TRIANGLE
025BF^▿^\smalltriangledown^\triangledown^B^mathbin^mathabx^# \triangledown (amssymb), WHITE DOWN-POINTING SMALL TRIANGLE
025C0^◀^\LHD^\blacktriangleleft^B^mathbin^wasysym^= \blacktriangleleft (fourier -mathabx), (large) left triangle, filled
025C1^◁^\lhd^\triangleleft^B^mathbin^amssymb wasysym^= \dres (oz), = \LeftTriangle (wrisym), (large) left triangle, open; z notation domain restriction
025C2^◂^\blacktriangleleft^\smallblacktriangleleft^B^mathbin^mathabx -fourier^left triangle, filled
025C3^◃^\smalltriangleleft^\smalltriangleleft^B^mathbin^mathabx^# \triangleleft, x \triangleleft (mathabx), left triangle, open
025C4^◄^^\blackpointerleft^B^mathord^^BLACK LEFT-POINTING POINTER
025C5^◅^^\whitepointerleft^B^mathord^^# \triangleleft (mathabx), WHITE LEFT-POINTING POINTER
025C6^◆^\Diamondblack^\mdlgblkdiamond^N^mathord^txfonts^BLACK DIAMOND
025C7^◇^\Diamond^\mdlgwhtdiamond^N^mathord^amssymb^WHITE DIAMOND; diamond, open
025C8^◈^^\blackinwhitediamond^N^mathord^^WHITE DIAMOND CONTAINING BLACK SMALL DIAMOND
025C9^◉^^\fisheye^N^mathord^^FISHEYE
025CA^◊^\lozenge^\mdlgwhtlozenge^B^mathord^amssymb^LOZENGE or total mark
025CB^○^\Circle^\mdlgwhtcircle^B^mathbin^wasysym^medium large circle
025CC^◌^^\dottedcircle^^mathord^^DOTTED CIRCLE
025CD^◍^^\circlevertfill^^mathord^^CIRCLE WITH VERTICAL FILL
025CE^◎^^\bullseye^N^mathord^^# \circledcirc (amssymb), BULLSEYE
025CF^●^\CIRCLE^\mdlgblkcircle^N^mathord^wasysym^circle, filled
025D0^◐^\LEFTcircle^\circlelefthalfblack^N^mathord^wasysym^circle, filled left half [harvey ball]
025D1^◑^\RIGHTcircle^\circlerighthalfblack^N^mathord^wasysym^circle, filled right half
025D2^◒^^\circlebottomhalfblack^N^mathord^^circle, filled bottom half
025D3^◓^^\circletophalfblack^N^mathord^^circle, filled top half
025D4^◔^^\circleurquadblack^^mathord^^CIRCLE WITH UPPER RIGHT QUADRANT BLACK
025D5^◕^^\blackcircleulquadwhite^^mathord^^CIRCLE WITH ALL BUT UPPER LEFT QUADRANT BLACK
025D6^◖^\LEFTCIRCLE^\blacklefthalfcircle^N^mathord^wasysym^LEFT HALF BLACK CIRCLE
025D7^◗^\RIGHTCIRCLE^\blackrighthalfcircle^N^mathord^wasysym^RIGHT HALF BLACK CIRCLE
025D8^◘^^\inversebullet^^mathord^^INVERSE BULLET
025D9^◙^^\inversewhitecircle^^mathord^^INVERSE WHITE CIRCLE
025DA^◚^^\invwhiteupperhalfcircle^^mathord^^UPPER HALF INVERSE WHITE CIRCLE
025DB^◛^^\invwhitelowerhalfcircle^^mathord^^LOWER HALF INVERSE WHITE CIRCLE
025DC^◜^^\ularc^^mathord^^UPPER LEFT QUADRANT CIRCULAR ARC
025DD^◝^^\urarc^^mathord^^UPPER RIGHT QUADRANT CIRCULAR ARC
025DE^◞^^\lrarc^^mathord^^LOWER RIGHT QUADRANT CIRCULAR ARC
025DF^◟^^\llarc^^mathord^^LOWER LEFT QUADRANT CIRCULAR ARC
025E0^◠^^\topsemicircle^^mathord^^UPPER HALF CIRCLE
025E1^◡^^\botsemicircle^^mathord^^LOWER HALF CIRCLE
025E2^◢^^\lrblacktriangle^N^mathord^^lower right triangle, filled
025E3^◣^^\llblacktriangle^N^mathord^^lower left triangle, filled
025E4^◤^^\ulblacktriangle^N^mathord^^upper left triangle, filled
025E5^◥^^\urblacktriangle^N^mathord^^upper right triangle, filled
025E6^◦^^\smwhtcircle^B^mathord^^WHITE BULLET
025E7^◧^^\squareleftblack^N^mathord^^square, filled left half
025E8^◨^^\squarerightblack^N^mathord^^square, filled right half
025E9^◩^^\squareulblack^N^mathord^^square, filled top left corner
025EA^◪^^\squarelrblack^N^mathord^^square, filled bottom right corner
025EB^◫^\boxbar^\boxbar^B^mathbin^stmaryrd txfonts^vertical bar in box
025EC^◬^^\trianglecdot^B^mathord^^triangle with centered dot
025ED^◭^^\triangleleftblack^^mathord^^UP-POINTING TRIANGLE WITH LEFT HALF BLACK
025EE^◮^^\trianglerightblack^^mathord^^UP-POINTING TRIANGLE WITH RIGHT HALF BLACK
025EF^◯^^\lgwhtcircle^N^mathord^^LARGE CIRCLE
025F0^◰^^\squareulquad^^mathord^^WHITE SQUARE WITH UPPER LEFT QUADRANT
025F1^◱^^\squarellquad^^mathord^^WHITE SQUARE WITH LOWER LEFT QUADRANT
025F2^◲^^\squarelrquad^^mathord^^WHITE SQUARE WITH LOWER RIGHT QUADRANT
025F3^◳^^\squareurquad^^mathord^^WHITE SQUARE WITH UPPER RIGHT QUADRANT
025F4^◴^^\circleulquad^^mathord^^WHITE CIRCLE WITH UPPER LEFT QUADRANT
025F5^◵^^\circlellquad^^mathord^^WHITE CIRCLE WITH LOWER LEFT QUADRANT
025F6^◶^^\circlelrquad^^mathord^^WHITE CIRCLE WITH LOWER RIGHT QUADRANT
025F7^◷^^\circleurquad^^mathord^^WHITE CIRCLE WITH UPPER RIGHT QUADRANT
025F8^◸^^\ultriangle^B^mathord^^UPPER LEFT TRIANGLE
025F9^◹^^\urtriangle^B^mathord^^UPPER RIGHT TRIANGLE
025FA^◺^^\lltriangle^B^mathord^^LOWER LEFT TRIANGLE
025FB^◻^\square^\mdwhtsquare^B^mathord^amssymb -fourier^WHITE MEDIUM SQUARE
025FC^◼^\blacksquare^\mdblksquare^B^mathord^amssymb -fourier^BLACK MEDIUM SQUARE
025FD^◽^^\mdsmwhtsquare^B^mathord^^WHITE MEDIUM SMALL SQUARE
025FE^◾^^\mdsmblksquare^B^mathord^^BLACK MEDIUM SMALL SQUARE
025FF^◿^^\lrtriangle^B^mathord^^LOWER RIGHT TRIANGLE
02605^★^\bigstar^\bigstar^B^mathord^amssymb^star, filled
02606^☆^^\bigwhitestar^B^mathord^^star, open
02609^☉^\Sun^\astrosun^N^mathord^mathabx^SUN
0260C^☌^^^N^mathord^wasysym^text \CONJUNCTION (wasysym), CONJUNCTION
02610^☐^\Square^^^mathord^wasysym^BALLOT BOX
02611^☑^\CheckedBox^^^mathord^wasysym^t \Checkedbox (marvosym), BALLOT BOX WITH CHECK
02612^☒^\XBox^^N^mathord^wasysym^t \Crossedbox (marvosym), BALLOT BOX WITH X
02615^☕^\steaming^^^mathord^arevmath^HOT BEVERAGE
0261E^☞^\pointright^^^mathord^arevmath^WHITE RIGHT POINTING INDEX
02620^☠^\skull^^^mathord^arevmath^SKULL AND CROSSBONES
02621^☡^^\danger^^mathord^^CAUTION SIGN, dangerous bend
02622^☢^\radiation^^^mathord^arevmath^RADIOACTIVE SIGN
02623^☣^\biohazard^^^mathord^arevmath^BIOHAZARD SIGN
0262F^☯^\yinyang^^^mathord^arevmath^YIN YANG
02639^☹^\frownie^^^mathord^wasysym^= \sadface (arevmath), WHITE FROWNING FACE
0263A^☺^\smiley^^^mathord^wasysym^= \smileface (arevmath), WHITE SMILING FACE
0263B^☻^\blacksmiley^\blacksmiley^^mathord^wasysym^= \invsmileface (arevmath), BLACK SMILING FACE
0263C^☼^\sun^\sun^^mathord^wasysym^WHITE SUN WITH RAYS
0263D^☽^\rightmoon^\rightmoon^N^mathord^wasysym mathabx^FIRST QUARTER MOON
0263E^☾^\leftmoon^\leftmoon^N^mathord^wasysym mathabx^LAST QUARTER MOON
0263F^☿^\mercury^^N^mathord^wasysym^= \Mercury (mathabx), MERCURY
02640^♀^\female^\female^N^mathord^wasysym^= \Venus (mathabx), = \girl (mathabx), venus, female
02641^♁^\earth^^N^mathord^wasysym^= \varEarth (mathabx), EARTH
02642^♂^\male^\male^N^mathord^wasysym^= \Mars (mathabx), = \boy (mathabx), mars, male
02643^♃^\jupiter^^N^mathord^wasysym^= \Jupiter (mathabx), JUPITER
02644^♄^\saturn^^N^mathord^wasysym^= \Saturn (mathabx), SATURN
02645^♅^\uranus^^^mathord^wasysym^= \Uranus (mathabx), URANUS
02646^♆^\neptune^^N^mathord^wasysym^= \Neptune (mathabx), NEPTUNE
02647^♇^\pluto^^N^mathord^wasysym^= \Pluto (mathabx), PLUTO
02648^♈^\aries^^N^mathord^wasysym^= \Aries (mathabx), ARIES
02649^♉^\taurus^^N^mathord^wasysym^= \Taurus (mathabx), TAURUS
0264A^♊^\gemini^^^mathord^wasysym^= \Gemini (mathabx), GEMINI
0264B^♋^\cancer^^^mathord^wasysym^CANCER
0264C^♌^\leo^^^mathord^wasysym^= \Leo (mathabx), LEO
0264D^♍^\virgo^^^mathord^wasysym^VIRGO
0264E^♎^\libra^^^mathord^wasysym^= \Libra (mathabx), LIBRA
0264F^♏^\scorpio^^^mathord^wasysym^= \Scorpio (mathabx), SCORPIUS
02650^♐^\sagittarius^^^mathord^wasysym^SAGITTARIUS
02651^♑^\capricornus^^^mathord^wasysym^CAPRICORN
02652^♒^\aquarius^^^mathord^wasysym^AQUARIUS
02653^♓^\pisces^^^mathord^wasysym^PISCES
02660^♠^\spadesuit^\spadesuit^N^mathord^^spades suit symbol
02661^♡^\heartsuit^\heartsuit^N^mathord^^heart suit symbol
02662^♢^\diamondsuit^\diamondsuit^N^mathord^^diamond suit symbol
02663^♣^\clubsuit^\clubsuit^N^mathord^^club suit symbol
02664^♤^\varspadesuit^\varspadesuit^N^mathord^txfonts^= \varspade (arevmath), spade, white (card suit)
02665^♥^\varheartsuit^\varheartsuit^N^mathord^txfonts^= \varheart (arevmath), filled heart (card suit)
02666^♦^\vardiamondsuit^\vardiamondsuit^N^mathord^txfonts^= \vardiamond (arevmath), filled diamond (card suit)
02667^♧^\varclubsuit^\varclubsuit^N^mathord^txfonts^= \varclub (arevmath), club, white (card suit)
02669^♩^\quarternote^\quarternote^N^mathord^arevmath wasysym^music note (sung text sign)
0266A^♪^\eighthnote^\eighthnote^^mathord^arevmath^EIGHTH NOTE
0266B^♫^\twonotes^\twonotes^^mathord^wasysym^BEAMED EIGHTH NOTES
0266C^♬^\sixteenthnote^^^mathord^arevmath^BEAMED SIXTEENTH NOTES
0266D^♭^\flat^\flat^N^mathord^^musical flat
0266E^♮^\natural^\natural^N^mathord^^music natural
0266F^♯^\sharp^\sharp^N^mathord^^= \# (oz), MUSIC SHARP SIGN, z notation infix bag count
0267B^♻^\recycle^^^mathord^arevmath^BLACK UNIVERSAL RECYCLING SYMBOL
0267E^♾^^\acidfree^^mathord^^PERMANENT PAPER SIGN
02680^⚀^^\dicei^N^mathord^^DIE FACE-1
02681^⚁^^\diceii^N^mathord^^DIE FACE-2
02682^⚂^^\diceiii^N^mathord^^DIE FACE-3
02683^⚃^^\diceiv^N^mathord^^DIE FACE-4
02684^⚄^^\dicev^N^mathord^^DIE FACE-5
02685^⚅^^\dicevi^N^mathord^^DIE FACE-6
02686^⚆^^\circledrightdot^N^mathord^^WHITE CIRCLE WITH DOT RIGHT
02687^⚇^^\circledtwodots^N^mathord^^WHITE CIRCLE WITH TWO DOTS
02688^⚈^^\blackcircledrightdot^N^mathord^^BLACK CIRCLE WITH WHITE DOT RIGHT
02689^⚉^^\blackcircledtwodots^N^mathord^^BLACK CIRCLE WITH TWO WHITE DOTS
02693^⚓^\anchor^^^mathord^arevmath^ANCHOR
02694^⚔^\swords^^^mathord^arevmath^CROSSED SWORDS
026A0^⚠^\warning^^^mathord^arevmath^WARNING SIGN
026A5^⚥^^\Hermaphrodite^^mathord^^MALE AND FEMALE SIGN
026AA^⚪^\medcirc^\mdwhtcircle^N^mathord^txfonts^MEDIUM WHITE CIRCLE
026AB^⚫^\medbullet^\mdblkcircle^N^mathord^txfonts^MEDIUM BLACK CIRCLE
026AC^⚬^^\mdsmwhtcircle^N^mathord^^MEDIUM SMALL WHITE CIRCLE
026B2^⚲^^\neuter^N^mathord^^NEUTER
0270E^✎^\pencil^^^mathord^arevmath^LOWER RIGHT PENCIL
02713^✓^\checkmark^\checkmark^N^mathord^amsfonts^= \ballotcheck (arevmath), tick, CHECK MARK
02717^✗^\ballotx^^N^mathord^arevmath^BALLOT X
02720^✠^\maltese^\maltese^N^mathord^amsfonts^MALTESE CROSS
0272A^✪^^\circledstar^N^mathord^^CIRCLED WHITE STAR
02736^✶^^\varstar^N^mathord^^SIX POINTED BLACK STAR
0273D^✽^^\dingasterisk^^mathord^^HEAVY TEARDROP-SPOKED ASTERISK
02772^❲^^\lbrbrak^O^mathopen^^LIGHT LEFT TORTOISE SHELL BRACKET ORNAMENT
02773^❳^^\rbrbrak^C^mathclose^^LIGHT RIGHT TORTOISE SHELL BRACKET ORNAMENT
0279B^➛^^\draftingarrow^^mathord^^right arrow with bold head (drafting)
027A2^➢^\arrowbullet^^^mathord^arevmath^THREE-D TOP-LIGHTED RIGHTWARDS ARROWHEAD
027C0^⟀^^\threedangle^N^mathord^^THREE DIMENSIONAL ANGLE
027C1^⟁^^\whiteinwhitetriangle^N^mathord^^WHITE TRIANGLE CONTAINING SMALL WHITE TRIANGLE
027C2^⟂^\perp^\perp^R^mathrel^^PERPENDICULAR
027C3^⟃^^\subsetcirc^R^mathord^^OPEN SUBSET
027C4^⟄^^\supsetcirc^R^mathord^^OPEN SUPERSET
027C5^⟅^\Lbag^\lbag^R^mathopen^stmaryrd txfonts^= \lbag (stmaryrd -oz), LEFT S-SHAPED BAG DELIMITER
027C6^⟆^\Rbag^\rbag^R^mathclose^stmaryrd txfonts^= \rbag (stmaryrd -oz), RIGHT S-SHAPED BAG DELIMITER
027C7^⟇^^\veedot^R^mathbin^^OR WITH DOT INSIDE
027C8^⟈^^\bsolhsub^R^mathrel^^REVERSE SOLIDUS PRECEDING SUBSET
027C9^⟉^^\suphsol^R^mathrel^^SUPERSET PRECEDING SOLIDUS
027CA^⟊^^^R^mathord^^VERTICAL BAR WITH HORIZONTAL STROKE
027CC^⟌^^\longdivision^L^mathopen^^LONG DIVISION
027D0^⟐^\Diamonddot^\diamondcdot^N^mathord^txfonts^WHITE DIAMOND WITH CENTRED DOT
027D1^⟑^^\wedgedot^B^mathbin^^AND WITH DOT
027D2^⟒^^\upin^R^mathrel^^ELEMENT OF OPENING UPWARDS
027D3^⟓^^\pullback^R^mathrel^^LOWER RIGHT CORNER WITH DOT
027D4^⟔^^\pushout^R^mathrel^^UPPER LEFT CORNER WITH DOT
027D5^⟕^^\leftouterjoin^L^mathop^^LEFT OUTER JOIN
027D6^⟖^^\rightouterjoin^L^mathop^^RIGHT OUTER JOIN
027D7^⟗^^\fullouterjoin^L^mathop^^FULL OUTER JOIN
027D8^⟘^^\bigbot^L^mathop^^LARGE UP TACK
027D9^⟙^^\bigtop^L^mathop^^LARGE DOWN TACK
027DA^⟚^^\DashVDash^R^mathrel^^LEFT AND RIGHT DOUBLE TURNSTILE
027DB^⟛^^\dashVdash^R^mathrel^^LEFT AND RIGHT TACK
027DC^⟜^\multimapinv^\multimapinv^R^mathrel^txfonts^LEFT MULTIMAP
027DD^⟝^^\vlongdash^R^mathrel^^long left tack
027DE^⟞^^\longdashv^R^mathrel^^long right tack
027DF^⟟^^\cirbot^R^mathrel^^UP TACK WITH CIRCLE ABOVE
027E0^⟠^^\lozengeminus^B^mathbin^^LOZENGE DIVIDED BY HORIZONTAL RULE
027E1^⟡^^\concavediamond^B^mathbin^^WHITE CONCAVE-SIDED DIAMOND
027E2^⟢^^\concavediamondtickleft^B^mathbin^^WHITE CONCAVE-SIDED DIAMOND WITH LEFTWARDS TICK
027E3^⟣^^\concavediamondtickright^B^mathbin^^WHITE CONCAVE-SIDED DIAMOND WITH RIGHTWARDS TICK
027E4^⟤^^\whitesquaretickleft^B^mathbin^^WHITE SQUARE WITH LEFTWARDS TICK
027E5^⟥^^\whitesquaretickright^B^mathbin^^WHITE SQUARE WITH RIGHTWARDS TICK
027E6^⟦^\llbracket^\lBrack^O^mathopen^stmaryrd wrisym kpfonts fourier^= \Lbrack (mathbbol), = \lbag (oz -stmaryrd), MATHEMATICAL LEFT WHITE SQUARE BRACKET
027E7^⟧^\rrbracket^\rBrack^C^mathclose^stmaryrd wrisym kpfonts fourier^= \Rbrack (mathbbol), = \rbag (oz -stmaryrd), MATHEMATICAL RIGHT WHITE SQUARE BRACKET
027E8^⟨^\langle^\langle^O^mathopen^^MATHEMATICAL LEFT ANGLE BRACKET
027E9^⟩^\rangle^\rangle^C^mathclose^^MATHEMATICAL RIGHT ANGLE BRACKET
027EA^⟪^\lang^\lAngle^O^mathopen^oz^MATHEMATICAL LEFT DOUBLE ANGLE BRACKET, z notation left chevron bracket
027EB^⟫^\rang^\rAngle^C^mathclose^oz^MATHEMATICAL RIGHT DOUBLE ANGLE BRACKET, z notation right chevron bracket
027EC^⟬^^\Lbrbrak^O^mathopen^^MATHEMATICAL LEFT WHITE TORTOISE SHELL BRACKET
027ED^⟭^^\Rbrbrak^C^mathclose^^MATHEMATICAL RIGHT WHITE TORTOISE SHELL BRACKET
027EE^⟮^\lgroup^^O^mathopen^^MATHEMATICAL LEFT FLATTENED PARENTHESIS
027EF^⟯^\rgroup^^C^mathclose^^MATHEMATICAL RIGHT FLATTENED PARENTHESIS
027F0^⟰^^\UUparrow^R^mathrel^^UPWARDS QUADRUPLE ARROW
027F1^⟱^^\DDownarrow^R^mathrel^^DOWNWARDS QUADRUPLE ARROW
027F2^⟲^^\acwgapcirclearrow^R^mathrel^^ANTICLOCKWISE GAPPED CIRCLE ARROW
027F3^⟳^^\cwgapcirclearrow^R^mathrel^^CLOCKWISE GAPPED CIRCLE ARROW
027F4^⟴^^\rightarrowonoplus^R^mathrel^^RIGHT ARROW WITH CIRCLED PLUS
027F5^⟵^\longleftarrow^\longleftarrow^R^mathrel^^LONG LEFTWARDS ARROW
027F6^⟶^\longrightarrow^\longrightarrow^R^mathrel^^LONG RIGHTWARDS ARROW
027F7^⟷^\longleftrightarrow^\longleftrightarrow^R^mathrel^^LONG LEFT RIGHT ARROW
027F8^⟸^\Longleftarrow^\Longleftarrow^R^mathrel^^= \impliedby (amsmath), LONG LEFTWARDS DOUBLE ARROW
027F9^⟹^\Longrightarrow^\Longrightarrow^R^mathrel^^= \implies (amsmath), LONG RIGHTWARDS DOUBLE ARROW
027FA^⟺^\Longleftrightarrow^\Longleftrightarrow^R^mathrel^^= \iff (oz), LONG LEFT RIGHT DOUBLE ARROW
027FB^⟻^\longmapsfrom^\longmapsfrom^R^mathrel^stmaryrd^= \longmappedfrom (kpfonts), LONG LEFTWARDS ARROW FROM BAR
027FC^⟼^\longmapsto^\longmapsto^R^mathrel^^LONG RIGHTWARDS ARROW FROM BAR
027FD^⟽^\Longmapsfrom^\Longmapsfrom^R^mathrel^stmaryrd^= \Longmappedfrom (kpfonts), LONG LEFTWARDS DOUBLE ARROW FROM BAR
027FE^⟾^\Longmapsto^\Longmapsto^R^mathrel^stmaryrd^LONG RIGHTWARDS DOUBLE ARROW FROM BAR
027FF^⟿^^\longrightsquigarrow^R^mathrel^^LONG RIGHTWARDS SQUIGGLE ARROW
02900^⤀^\psur^\nvtwoheadrightarrow^R^mathrel^oz^= \psurj (oz), RIGHTWARDS TWO-HEADED ARROW WITH VERTICAL STROKE, z notation partial surjection
02901^⤁^^\nVtwoheadrightarrow^R^mathrel^^RIGHTWARDS TWO-HEADED ARROW WITH DOUBLE VERTICAL STROKE, z notation finite surjection
02902^⤂^^\nvLeftarrow^R^mathrel^^LEFTWARDS DOUBLE ARROW WITH VERTICAL STROKE
02903^⤃^^\nvRightarrow^R^mathrel^^RIGHTWARDS DOUBLE ARROW WITH VERTICAL STROKE
02904^⤄^^\nvLeftrightarrow^R^mathrel^^LEFT RIGHT DOUBLE ARROW WITH VERTICAL STROKE
02905^⤅^^\twoheadmapsto^R^mathrel^^RIGHTWARDS TWO-HEADED ARROW FROM BAR
02906^⤆^\Mapsfrom^\Mapsfrom^R^mathrel^stmaryrd^= \Mappedfrom (kpfonts), LEFTWARDS DOUBLE ARROW FROM BAR
02907^⤇^\Mapsto^\Mapsto^R^mathrel^stmaryrd^RIGHTWARDS DOUBLE ARROW FROM BAR
02908^⤈^^\downarrowbarred^R^mathrel^^DOWNWARDS ARROW WITH HORIZONTAL STROKE
02909^⤉^^\uparrowbarred^R^mathrel^^UPWARDS ARROW WITH HORIZONTAL STROKE
0290A^⤊^^\Uuparrow^R^mathrel^^UPWARDS TRIPLE ARROW
0290B^⤋^^\Ddownarrow^R^mathrel^^DOWNWARDS TRIPLE ARROW
0290C^⤌^^\leftbkarrow^R^mathrel^^LEFTWARDS DOUBLE DASH ARROW
0290D^⤍^^\rightbkarrow^R^mathrel^^RIGHTWARDS DOUBLE DASH ARROW
0290E^⤎^^\leftdbkarrow^R^mathrel^^LEFTWARDS TRIPLE DASH ARROW
0290F^⤏^^\dbkarow^R^mathrel^^RIGHTWARDS TRIPLE DASH ARROW
02910^⤐^^\drbkarow^R^mathrel^^RIGHTWARDS TWO-HEADED TRIPLE DASH ARROW
02911^⤑^^\rightdotarrow^R^mathrel^^RIGHTWARDS ARROW WITH DOTTED STEM
02912^⤒^\UpArrowBar^\baruparrow^R^mathrel^wrisym^UPWARDS ARROW TO BAR
02913^⤓^\DownArrowBar^\downarrowbar^R^mathrel^wrisym^DOWNWARDS ARROW TO BAR
02914^⤔^\pinj^\nvrightarrowtail^R^mathrel^oz^RIGHTWARDS ARROW WITH TAIL WITH VERTICAL STROKE, z notation partial injection
02915^⤕^\finj^\nVrightarrowtail^R^mathrel^oz^RIGHTWARDS ARROW WITH TAIL WITH DOUBLE VERTICAL STROKE, z notation finite injection
02916^⤖^\bij^\twoheadrightarrowtail^R^mathrel^oz^RIGHTWARDS TWO-HEADED ARROW WITH TAIL, z notation bijection
02917^⤗^^\nvtwoheadrightarrowtail^R^mathrel^^RIGHTWARDS TWO-HEADED ARROW WITH TAIL WITH VERTICAL STROKE, z notation surjective injection
02918^⤘^^\nVtwoheadrightarrowtail^R^mathrel^^RIGHTWARDS TWO-HEADED ARROW WITH TAIL WITH DOUBLE VERTICAL STROKE, z notation finite surjective injection
02919^⤙^^\lefttail^R^mathrel^^LEFTWARDS ARROW-TAIL
0291A^⤚^^\righttail^R^mathrel^^RIGHTWARDS ARROW-TAIL
0291B^⤛^^\leftdbltail^R^mathrel^^LEFTWARDS DOUBLE ARROW-TAIL
0291C^⤜^^\rightdbltail^R^mathrel^^RIGHTWARDS DOUBLE ARROW-TAIL
0291D^⤝^^\diamondleftarrow^R^mathrel^^LEFTWARDS ARROW TO BLACK DIAMOND
0291E^⤞^^\rightarrowdiamond^R^mathrel^^RIGHTWARDS ARROW TO BLACK DIAMOND
0291F^⤟^^\diamondleftarrowbar^R^mathrel^^LEFTWARDS ARROW FROM BAR TO BLACK DIAMOND
02920^⤠^^\barrightarrowdiamond^R^mathrel^^RIGHTWARDS ARROW FROM BAR TO BLACK DIAMOND
02921^⤡^^\nwsearrow^R^mathrel^^NORTH WEST AND SOUTH EAST ARROW
02922^⤢^^\neswarrow^R^mathrel^^NORTH EAST AND SOUTH WEST ARROW
02923^⤣^^\hknwarrow^R^mathrel^^NORTH WEST ARROW WITH HOOK
02924^⤤^^\hknearrow^R^mathrel^^NORTH EAST ARROW WITH HOOK
02925^⤥^^\hksearow^R^mathrel^^SOUTH EAST ARROW WITH HOOK
02926^⤦^^\hkswarow^R^mathrel^^SOUTH WEST ARROW WITH HOOK
02927^⤧^^\tona^R^mathrel^^NORTH WEST ARROW AND NORTH EAST ARROW
02928^⤨^^\toea^R^mathrel^^NORTH EAST ARROW AND SOUTH EAST ARROW
02929^⤩^^\tosa^R^mathrel^^SOUTH EAST ARROW AND SOUTH WEST ARROW
0292A^⤪^^\towa^R^mathrel^^SOUTH WEST ARROW AND NORTH WEST ARROW
0292B^⤫^^\rdiagovfdiag^R^mathord^^RISING DIAGONAL CROSSING FALLING DIAGONAL
0292C^⤬^^\fdiagovrdiag^R^mathord^^FALLING DIAGONAL CROSSING RISING DIAGONAL
0292D^⤭^^\seovnearrow^R^mathord^^SOUTH EAST ARROW CROSSING NORTH EAST ARROW
0292E^⤮^^\neovsearrow^R^mathord^^NORTH EAST ARROW CROSSING SOUTH EAST ARROW
0292F^⤯^^\fdiagovnearrow^R^mathord^^FALLING DIAGONAL CROSSING NORTH EAST ARROW
02930^⤰^^\rdiagovsearrow^R^mathord^^RISING DIAGONAL CROSSING SOUTH EAST ARROW
02931^⤱^^\neovnwarrow^R^mathord^^NORTH EAST ARROW CROSSING NORTH WEST ARROW
02932^⤲^^\nwovnearrow^R^mathord^^NORTH WEST ARROW CROSSING NORTH EAST ARROW
02933^⤳^\leadsto^\rightcurvedarrow^R^mathrel^txfonts^WAVE ARROW POINTING DIRECTLY RIGHT
02934^⤴^^\uprightcurvearrow^R^mathord^^ARROW POINTING RIGHTWARDS THEN CURVING UPWARDS
02935^⤵^^\downrightcurvedarrow^R^mathord^^ARROW POINTING RIGHTWARDS THEN CURVING DOWNWARDS
02936^⤶^^\leftdowncurvedarrow^R^mathrel^^ARROW POINTING DOWNWARDS THEN CURVING LEFTWARDS
02937^⤷^^\rightdowncurvedarrow^R^mathrel^^ARROW POINTING DOWNWARDS THEN CURVING RIGHTWARDS
02938^⤸^^\cwrightarcarrow^R^mathrel^^RIGHT-SIDE ARC CLOCKWISE ARROW
02939^⤹^^\acwleftarcarrow^R^mathrel^^LEFT-SIDE ARC ANTICLOCKWISE ARROW
0293A^⤺^^\acwoverarcarrow^R^mathrel^^TOP ARC ANTICLOCKWISE ARROW
0293B^⤻^^\acwunderarcarrow^R^mathrel^^BOTTOM ARC ANTICLOCKWISE ARROW
0293C^⤼^^\curvearrowrightminus^R^mathrel^^TOP ARC CLOCKWISE ARROW WITH MINUS
0293D^⤽^^\curvearrowleftplus^R^mathrel^^TOP ARC ANTICLOCKWISE ARROW WITH PLUS
0293E^⤾^^\cwundercurvearrow^R^mathrel^^LOWER RIGHT SEMICIRCULAR CLOCKWISE ARROW
0293F^⤿^^\ccwundercurvearrow^R^mathrel^^LOWER LEFT SEMICIRCULAR ANTICLOCKWISE ARROW
02940^⥀^^\acwcirclearrow^R^mathrel^^ANTICLOCKWISE CLOSED CIRCLE ARROW
02941^⥁^^\cwcirclearrow^R^mathrel^^CLOCKWISE CLOSED CIRCLE ARROW
02942^⥂^^\rightarrowshortleftarrow^R^mathrel^^RIGHTWARDS ARROW ABOVE SHORT LEFTWARDS ARROW
02943^⥃^^\leftarrowshortrightarrow^R^mathrel^^LEFTWARDS ARROW ABOVE SHORT RIGHTWARDS ARROW
02944^⥄^^\shortrightarrowleftarrow^R^mathrel^^SHORT RIGHTWARDS ARROW ABOVE LEFTWARDS ARROW
02945^⥅^^\rightarrowplus^R^mathrel^^RIGHTWARDS ARROW WITH PLUS BELOW
02946^⥆^^\leftarrowplus^R^mathrel^^LEFTWARDS ARROW WITH PLUS BELOW
02947^⥇^^\rightarrowx^R^mathrel^^RIGHTWARDS ARROW THROUGH X
02948^⥈^^\leftrightarrowcircle^R^mathrel^^LEFT RIGHT ARROW THROUGH SMALL CIRCLE
02949^⥉^^\twoheaduparrowcircle^R^mathrel^^UPWARDS TWO-HEADED ARROW FROM SMALL CIRCLE
0294A^⥊^\leftrightharpoon^\leftrightharpoonupdown^R^mathrel^mathabx^LEFT BARB UP RIGHT BARB DOWN HARPOON
0294B^⥋^\rightleftharpoon^\leftrightharpoondownup^R^mathrel^mathabx^LEFT BARB DOWN RIGHT BARB UP HARPOON
0294C^⥌^^\updownharpoonrightleft^R^mathrel^^UP BARB RIGHT DOWN BARB LEFT HARPOON
0294D^⥍^^\updownharpoonleftright^R^mathrel^^UP BARB LEFT DOWN BARB RIGHT HARPOON
0294E^⥎^\leftrightharpoonup^\leftrightharpoonupup^R^mathrel^wrisym^LEFT BARB UP RIGHT BARB UP HARPOON
0294F^⥏^\rightupdownharpoon^\updownharpoonrightright^R^mathrel^wrisym^UP BARB RIGHT DOWN BARB RIGHT HARPOON
02950^⥐^\leftrightharpoondown^\leftrightharpoondowndown^R^mathrel^wrisym^LEFT BARB DOWN RIGHT BARB DOWN HARPOON
02951^⥑^\leftupdownharpoon^\updownharpoonleftleft^R^mathrel^wrisym^UP BARB LEFT DOWN BARB LEFT HARPOON
02952^⥒^\LeftVectorBar^\barleftharpoonup^R^mathrel^wrisym^LEFTWARDS HARPOON WITH BARB UP TO BAR
02953^⥓^\RightVectorBar^\rightharpoonupbar^R^mathrel^wrisym^RIGHTWARDS HARPOON WITH BARB UP TO BAR
02954^⥔^\RightUpVectorBar^\barupharpoonright^R^mathrel^wrisym^UPWARDS HARPOON WITH BARB RIGHT TO BAR
02955^⥕^\RightDownVectorBar^\downharpoonrightbar^R^mathrel^wrisym^DOWNWARDS HARPOON WITH BARB RIGHT TO BAR
02956^⥖^\DownLeftVectorBar^\barleftharpoondown^R^mathrel^wrisym^LEFTWARDS HARPOON WITH BARB DOWN TO BAR
02957^⥗^\DownRightVectorBar^\rightharpoondownbar^R^mathrel^wrisym^RIGHTWARDS HARPOON WITH BARB DOWN TO BAR
02958^⥘^\LeftUpVectorBar^\barupharpoonleft^R^mathrel^wrisym^UPWARDS HARPOON WITH BARB LEFT TO BAR
02959^⥙^\LeftDownVectorBar^\downharpoonleftbar^R^mathrel^wrisym^DOWNWARDS HARPOON WITH BARB LEFT TO BAR
0295A^⥚^\LeftTeeVector^\leftharpoonupbar^R^mathrel^wrisym^LEFTWARDS HARPOON WITH BARB UP FROM BAR
0295B^⥛^\RightTeeVector^\barrightharpoonup^R^mathrel^wrisym^RIGHTWARDS HARPOON WITH BARB UP FROM BAR
0295C^⥜^\RightUpTeeVector^\upharpoonrightbar^R^mathrel^wrisym^UPWARDS HARPOON WITH BARB RIGHT FROM BAR
0295D^⥝^\RightDownTeeVector^\bardownharpoonright^R^mathrel^wrisym^DOWNWARDS HARPOON WITH BARB RIGHT FROM BAR
0295E^⥞^\DownLeftTeeVector^\leftharpoondownbar^R^mathrel^wrisym^LEFTWARDS HARPOON WITH BARB DOWN FROM BAR
0295F^⥟^\DownRightTeeVector^\barrightharpoondown^R^mathrel^wrisym^RIGHTWARDS HARPOON WITH BARB DOWN FROM BAR
02960^⥠^\LeftUpTeeVector^\upharpoonleftbar^R^mathrel^wrisym^UPWARDS HARPOON WITH BARB LEFT FROM BAR
02961^⥡^\LeftDownTeeVector^\bardownharpoonleft^R^mathrel^wrisym^DOWNWARDS HARPOON WITH BARB LEFT FROM BAR
02962^⥢^\leftleftharpoons^\leftharpoonsupdown^R^mathrel^mathabx^LEFTWARDS HARPOON WITH BARB UP ABOVE LEFTWARDS HARPOON WITH BARB DOWN
02963^⥣^\upupharpoons^\upharpoonsleftright^R^mathrel^mathabx^UPWARDS HARPOON WITH BARB LEFT BESIDE UPWARDS HARPOON WITH BARB RIGHT
02964^⥤^\rightrightharpoons^\rightharpoonsupdown^R^mathrel^mathabx^RIGHTWARDS HARPOON WITH BARB UP ABOVE RIGHTWARDS HARPOON WITH BARB DOWN
02965^⥥^\downdownharpoons^\downharpoonsleftright^R^mathrel^mathabx^DOWNWARDS HARPOON WITH BARB LEFT BESIDE DOWNWARDS HARPOON WITH BARB RIGHT
02966^⥦^^\leftrightharpoonsup^R^mathrel^^LEFTWARDS HARPOON WITH BARB UP ABOVE RIGHTWARDS HARPOON WITH BARB UP
02967^⥧^^\leftrightharpoonsdown^R^mathrel^^LEFTWARDS HARPOON WITH BARB DOWN ABOVE RIGHTWARDS HARPOON WITH BARB DOWN
02968^⥨^^\rightleftharpoonsup^R^mathrel^^RIGHTWARDS HARPOON WITH BARB UP ABOVE LEFTWARDS HARPOON WITH BARB UP
02969^⥩^^\rightleftharpoonsdown^R^mathrel^^RIGHTWARDS HARPOON WITH BARB DOWN ABOVE LEFTWARDS HARPOON WITH BARB DOWN
0296A^⥪^\leftbarharpoon^\leftharpoonupdash^R^mathrel^mathabx^LEFTWARDS HARPOON WITH BARB UP ABOVE LONG DASH
0296B^⥫^\barleftharpoon^\dashleftharpoondown^R^mathrel^mathabx^LEFTWARDS HARPOON WITH BARB DOWN BELOW LONG DASH
0296C^⥬^\rightbarharpoon^\rightharpoonupdash^R^mathrel^mathabx^RIGHTWARDS HARPOON WITH BARB UP ABOVE LONG DASH
0296D^⥭^\barrightharpoon^\dashrightharpoondown^R^mathrel^mathabx^RIGHTWARDS HARPOON WITH BARB DOWN BELOW LONG DASH
0296E^⥮^\updownharpoons^\updownharpoonsleftright^R^mathrel^mathabx^= \upequilibrium (wrisym), UPWARDS HARPOON WITH BARB LEFT BESIDE DOWNWARDS HARPOON WITH BARB RIGHT
0296F^⥯^\downupharpoons^\downupharpoonsleftright^R^mathrel^mathabx^= \uprevequilibrium (wrisym), DOWNWARDS HARPOON WITH BARB LEFT BESIDE UPWARDS HARPOON WITH BARB RIGHT
02970^⥰^^\rightimply^R^mathrel^^RIGHT DOUBLE ARROW WITH ROUNDED HEAD
02971^⥱^^\equalrightarrow^R^mathrel^^EQUALS SIGN ABOVE RIGHTWARDS ARROW
02972^⥲^^\similarrightarrow^R^mathrel^^TILDE OPERATOR ABOVE RIGHTWARDS ARROW
02973^⥳^^\leftarrowsimilar^R^mathrel^^LEFTWARDS ARROW ABOVE TILDE OPERATOR
02974^⥴^^\rightarrowsimilar^R^mathrel^^RIGHTWARDS ARROW ABOVE TILDE OPERATOR
02975^⥵^^\rightarrowapprox^R^mathrel^^RIGHTWARDS ARROW ABOVE ALMOST EQUAL TO
02976^⥶^^\ltlarr^R^mathrel^^LESS-THAN ABOVE LEFTWARDS ARROW
02977^⥷^^\leftarrowless^R^mathrel^^LEFTWARDS ARROW THROUGH LESS-THAN
02978^⥸^^\gtrarr^R^mathrel^^GREATER-THAN ABOVE RIGHTWARDS ARROW
02979^⥹^^\subrarr^R^mathrel^^SUBSET ABOVE RIGHTWARDS ARROW
0297A^⥺^^\leftarrowsubset^R^mathrel^^LEFTWARDS ARROW THROUGH SUBSET
0297B^⥻^^\suplarr^R^mathrel^^SUPERSET ABOVE LEFTWARDS ARROW
0297C^⥼^\strictfi^\leftfishtail^R^mathrel^txfonts^LEFT FISH TAIL
0297D^⥽^\strictif^\rightfishtail^R^mathrel^txfonts^RIGHT FISH TAIL
0297E^⥾^^\upfishtail^R^mathrel^^UP FISH TAIL
0297F^⥿^^\downfishtail^R^mathrel^^DOWN FISH TAIL
02980^⦀^\VERT^\Vvert^F^mathfence^fourier^TRIPLE VERTICAL BAR DELIMITER
02981^⦁^\spot^\mdsmblkcircle^N^mathord^oz^= \dot (oz), Z NOTATION SPOT
02982^⦂^^\typecolon^F^mathbin^^Z NOTATION TYPE COLON, (present in bbold font but no command)
02983^⦃^^\lBrace^O^mathopen^^LEFT WHITE CURLY BRACKET
02984^⦄^^\rBrace^C^mathclose^^RIGHT WHITE CURLY BRACKET
02985^⦅^\Lparen^\lParen^O^mathopen^mathbbol^LEFT WHITE PARENTHESIS
02986^⦆^\Rparen^\rParen^C^mathclose^mathbbol^RIGHT WHITE PARENTHESIS
02987^⦇^\limg^\llparenthesis^O^mathopen^oz^= \llparenthesis (stmaryrd), Z NOTATION LEFT IMAGE BRACKET
02988^⦈^\rimg^\rrparenthesis^C^mathclose^oz^= \rrparenthesis (stmaryrd), Z NOTATION RIGHT IMAGE BRACKET
02989^⦉^\lblot^\llangle^O^mathopen^oz^Z NOTATION LEFT BINDING BRACKET
0298A^⦊^\rblot^\rrangle^C^mathclose^oz^Z NOTATION RIGHT BINDING BRACKET
0298B^⦋^^\lbrackubar^O^mathopen^^LEFT SQUARE BRACKET WITH UNDERBAR
0298C^⦌^^\rbrackubar^C^mathclose^^RIGHT SQUARE BRACKET WITH UNDERBAR
0298D^⦍^^\lbrackultick^O^mathopen^^LEFT SQUARE BRACKET WITH TICK IN TOP CORNER
0298E^⦎^^\rbracklrtick^C^mathclose^^RIGHT SQUARE BRACKET WITH TICK IN BOTTOM CORNER
0298F^⦏^^\lbracklltick^O^mathopen^^LEFT SQUARE BRACKET WITH TICK IN BOTTOM CORNER
02990^⦐^^\rbrackurtick^C^mathclose^^RIGHT SQUARE BRACKET WITH TICK IN TOP CORNER
02991^⦑^^\langledot^O^mathopen^^LEFT ANGLE BRACKET WITH DOT
02992^⦒^^\rangledot^C^mathclose^^RIGHT ANGLE BRACKET WITH DOT
02993^⦓^^\lparenless^O^mathopen^^LEFT ARC LESS-THAN BRACKET
02994^⦔^^\rparengtr^C^mathclose^^RIGHT ARC GREATER-THAN BRACKET
02995^⦕^^\Lparengtr^O^mathopen^^DOUBLE LEFT ARC GREATER-THAN BRACKET
02996^⦖^^\Rparenless^C^mathclose^^DOUBLE RIGHT ARC LESS-THAN BRACKET
02997^⦗^^\lblkbrbrak^O^mathopen^^LEFT BLACK TORTOISE SHELL BRACKET
02998^⦘^^\rblkbrbrak^C^mathclose^^RIGHT BLACK TORTOISE SHELL BRACKET
02999^⦙^^\fourvdots^F^mathord^^DOTTED FENCE
0299A^⦚^^\vzigzag^F^mathord^^VERTICAL ZIGZAG LINE
0299B^⦛^^\measuredangleleft^N^mathord^^MEASURED ANGLE OPENING LEFT
0299C^⦜^^\rightanglesqr^N^mathord^^RIGHT ANGLE VARIANT WITH SQUARE
0299D^⦝^^\rightanglemdot^N^mathord^^MEASURED RIGHT ANGLE WITH DOT
0299E^⦞^^\angles^N^mathord^^ANGLE WITH S INSIDE
0299F^⦟^^\angdnr^N^mathord^^ACUTE ANGLE
029A0^⦠^^\gtlpar^N^mathord^^SPHERICAL ANGLE OPENING LEFT
029A1^⦡^^\sphericalangleup^N^mathord^^SPHERICAL ANGLE OPENING UP
029A2^⦢^^\turnangle^N^mathord^^TURNED ANGLE
029A3^⦣^^\revangle^N^mathord^^REVERSED ANGLE
029A4^⦤^^\angleubar^N^mathord^^ANGLE WITH UNDERBAR
029A5^⦥^^\revangleubar^N^mathord^^REVERSED ANGLE WITH UNDERBAR
029A6^⦦^^\wideangledown^N^mathord^^OBLIQUE ANGLE OPENING UP
029A7^⦧^^\wideangleup^N^mathord^^OBLIQUE ANGLE OPENING DOWN
029A8^⦨^^\measanglerutone^N^mathord^^MEASURED ANGLE WITH OPEN ARM ENDING IN ARROW POINTING UP AND RIGHT
029A9^⦩^^\measanglelutonw^N^mathord^^MEASURED ANGLE WITH OPEN ARM ENDING IN ARROW POINTING UP AND LEFT
029AA^⦪^^\measanglerdtose^N^mathord^^MEASURED ANGLE WITH OPEN ARM ENDING IN ARROW POINTING DOWN AND RIGHT
029AB^⦫^^\measangleldtosw^N^mathord^^MEASURED ANGLE WITH OPEN ARM ENDING IN ARROW POINTING DOWN AND LEFT
029AC^⦬^^\measangleurtone^N^mathord^^MEASURED ANGLE WITH OPEN ARM ENDING IN ARROW POINTING RIGHT AND UP
029AD^⦭^^\measangleultonw^N^mathord^^MEASURED ANGLE WITH OPEN ARM ENDING IN ARROW POINTING LEFT AND UP
029AE^⦮^^\measangledrtose^N^mathord^^MEASURED ANGLE WITH OPEN ARM ENDING IN ARROW POINTING RIGHT AND DOWN
029AF^⦯^^\measangledltosw^N^mathord^^MEASURED ANGLE WITH OPEN ARM ENDING IN ARROW POINTING LEFT AND DOWN
029B0^⦰^^\revemptyset^N^mathord^^REVERSED EMPTY SET
029B1^⦱^^\emptysetobar^N^mathord^^EMPTY SET WITH OVERBAR
029B2^⦲^^\emptysetocirc^N^mathord^^EMPTY SET WITH SMALL CIRCLE ABOVE
029B3^⦳^^\emptysetoarr^N^mathord^^EMPTY SET WITH RIGHT ARROW ABOVE
029B4^⦴^^\emptysetoarrl^N^mathord^^EMPTY SET WITH LEFT ARROW ABOVE
029B5^⦵^^\circlehbar^N^mathbin^^CIRCLE WITH HORIZONTAL BAR
029B6^⦶^^\circledvert^B^mathbin^^CIRCLED VERTICAL BAR
029B7^⦷^^\circledparallel^B^mathbin^^CIRCLED PARALLEL
029B8^⦸^\circledbslash^\obslash^B^mathbin^txfonts^CIRCLED REVERSE SOLIDUS
029B9^⦹^^\operp^B^mathbin^^CIRCLED PERPENDICULAR
029BA^⦺^^\obot^N^mathord^^CIRCLE DIVIDED BY HORIZONTAL BAR AND TOP HALF DIVIDED BY VERTICAL BAR
029BB^⦻^^\olcross^N^mathord^^CIRCLE WITH SUPERIMPOSED X
029BC^⦼^^\odotslashdot^N^mathord^^CIRCLED ANTICLOCKWISE-ROTATED DIVISION SIGN
029BD^⦽^^\uparrowoncircle^N^mathord^^UP ARROW THROUGH CIRCLE
029BE^⦾^^\circledwhitebullet^N^mathord^^CIRCLED WHITE BULLET
029BF^⦿^^\circledbullet^N^mathord^^CIRCLED BULLET
029C0^⧀^\circledless^\olessthan^B^mathbin^txfonts^CIRCLED LESS-THAN
029C1^⧁^\circledgtr^\ogreaterthan^B^mathbin^txfonts^CIRCLED GREATER-THAN
029C2^⧂^^\cirscir^N^mathord^^CIRCLE WITH SMALL CIRCLE TO THE RIGHT
029C3^⧃^^\cirE^N^mathord^^CIRCLE WITH TWO HORIZONTAL STROKES TO THE RIGHT
029C4^⧄^\boxslash^\boxdiag^B^mathbin^stmaryrd txfonts^SQUARED RISING DIAGONAL SLASH
029C5^⧅^\boxbslash^\boxbslash^B^mathbin^stmaryrd txfonts^SQUARED FALLING DIAGONAL SLASH
029C6^⧆^\boxast^\boxast^B^mathbin^stmaryrd txfonts^SQUARED ASTERISK
029C7^⧇^\boxcircle^\boxcircle^B^mathbin^stmaryrd^SQUARED SMALL CIRCLE
029C8^⧈^\boxbox^\boxbox^B^mathbin^stmaryrd^SQUARED SQUARE
029C9^⧉^^\boxonbox^N^mathord^^TWO JOINED SQUARES
029CA^⧊^^\triangleodot^N^mathord^^TRIANGLE WITH DOT ABOVE
029CB^⧋^^\triangleubar^N^mathord^^TRIANGLE WITH UNDERBAR
029CC^⧌^^\triangles^N^mathord^^S IN TRIANGLE
029CD^⧍^^\triangleserifs^N^mathbin^^TRIANGLE WITH SERIFS AT BOTTOM
029CE^⧎^^\rtriltri^R^mathrel^^RIGHT TRIANGLE ABOVE LEFT TRIANGLE
029CF^⧏^\LeftTriangleBar^\ltrivb^R^mathrel^wrisym^LEFT TRIANGLE BESIDE VERTICAL BAR
029D0^⧐^\RightTriangleBar^\vbrtri^R^mathrel^wrisym^VERTICAL BAR BESIDE RIGHT TRIANGLE
029D1^⧑^^\lfbowtie^R^mathrel^^left black bowtie
029D2^⧒^^\rfbowtie^R^mathrel^^right black bowtie
029D3^⧓^^\fbowtie^R^mathrel^^BLACK BOWTIE
029D4^⧔^^\lftimes^R^mathrel^^left black times
029D5^⧕^^\rftimes^R^mathrel^^right black times
029D6^⧖^^\hourglass^B^mathbin^^WHITE HOURGLASS
029D7^⧗^^\blackhourglass^B^mathbin^^BLACK HOURGLASS
029D8^⧘^^\lvzigzag^O^mathopen^^LEFT WIGGLY FENCE
029D9^⧙^^\rvzigzag^C^mathclose^^RIGHT WIGGLY FENCE
029DA^⧚^^\Lvzigzag^O^mathopen^^LEFT DOUBLE WIGGLY FENCE
029DB^⧛^^\Rvzigzag^C^mathclose^^RIGHT DOUBLE WIGGLY FENCE
029DC^⧜^^\iinfin^N^mathord^^INCOMPLETE INFINITY
029DD^⧝^^\tieinfty^N^mathord^^TIE OVER INFINITY
029DE^⧞^^\nvinfty^N^mathord^^INFINITY NEGATED WITH VERTICAL BAR
029DF^⧟^\multimapboth^\dualmap^R^mathrel^txfonts^DOUBLE-ENDED MULTIMAP
029E0^⧠^^\laplac^N^mathord^^SQUARE WITH CONTOURED OUTLINE
029E1^⧡^^\lrtriangleeq^R^mathrel^^INCREASES AS
029E2^⧢^^\shuffle^B^mathbin^^SHUFFLE PRODUCT
029E3^⧣^^\eparsl^R^mathrel^^EQUALS SIGN AND SLANTED PARALLEL
029E4^⧤^^\smeparsl^R^mathrel^^EQUALS SIGN AND SLANTED PARALLEL WITH TILDE ABOVE
029E5^⧥^^\eqvparsl^R^mathrel^^IDENTICAL TO AND SLANTED PARALLEL
029E6^⧦^^\gleichstark^R^mathrel^^GLEICH STARK
029E7^⧧^^\thermod^N^mathord^^THERMODYNAMIC
029E8^⧨^^\downtriangleleftblack^N^mathord^^DOWN-POINTING TRIANGLE WITH LEFT HALF BLACK
029E9^⧩^^\downtrianglerightblack^N^mathord^^DOWN-POINTING TRIANGLE WITH RIGHT HALF BLACK
029EA^⧪^^\blackdiamonddownarrow^N^mathord^^BLACK DIAMOND WITH DOWN ARROW
029EB^⧫^\blacklozenge^\mdlgblklozenge^B^mathbin^amssymb^BLACK LOZENGE
029EC^⧬^^\circledownarrow^N^mathord^^WHITE CIRCLE WITH DOWN ARROW
029ED^⧭^^\blackcircledownarrow^N^mathord^^BLACK CIRCLE WITH DOWN ARROW
029EE^⧮^^\errbarsquare^N^mathord^^ERROR-BARRED WHITE SQUARE
029EF^⧯^^\errbarblacksquare^N^mathord^^ERROR-BARRED BLACK SQUARE
029F0^⧰^^\errbardiamond^N^mathord^^ERROR-BARRED WHITE DIAMOND
029F1^⧱^^\errbarblackdiamond^N^mathord^^ERROR-BARRED BLACK DIAMOND
029F2^⧲^^\errbarcircle^N^mathord^^ERROR-BARRED WHITE CIRCLE
029F3^⧳^^\errbarblackcircle^N^mathord^^ERROR-BARRED BLACK CIRCLE
029F4^⧴^^\ruledelayed^R^mathrel^^RULE-DELAYED
029F5^⧵^\setminus^\setminus^B^mathbin^^REVERSE SOLIDUS OPERATOR
029F6^⧶^^\dsol^B^mathbin^^SOLIDUS WITH OVERBAR
029F7^⧷^^\rsolbar^B^mathbin^^REVERSE SOLIDUS WITH HORIZONTAL STROKE
029F8^⧸^^\xsol^L^mathop^^BIG SOLIDUS
029F9^⧹^\zhide^\xbsol^L^mathop^oz^= \hide (oz), BIG REVERSE SOLIDUS, z notation schema hiding
029FA^⧺^^\doubleplus^B^mathbin^^DOUBLE PLUS
029FB^⧻^^\tripleplus^B^mathbin^^TRIPLE PLUS
029FC^⧼^^\lcurvyangle^O^mathopen^^left pointing curved angle bracket
029FD^⧽^^\rcurvyangle^C^mathclose^^right pointing curved angle bracket
029FE^⧾^^\tplus^B^mathbin^^TINY
029FF^⧿^^\tminus^B^mathbin^^MINY
02A00^⨀^\bigodot^\bigodot^L^mathop^^N-ARY CIRCLED DOT OPERATOR
02A01^⨁^\bigoplus^\bigoplus^L^mathop^^N-ARY CIRCLED PLUS OPERATOR
02A02^⨂^\bigotimes^\bigotimes^L^mathop^^N-ARY CIRCLED TIMES OPERATOR
02A03^⨃^^\bigcupdot^L^mathop^^N-ARY UNION OPERATOR WITH DOT
02A04^⨄^\biguplus^\biguplus^L^mathop^^N-ARY UNION OPERATOR WITH PLUS
02A05^⨅^\bigsqcap^\bigsqcap^L^mathop^txfonts^N-ARY SQUARE INTERSECTION OPERATOR
02A06^⨆^\bigsqcup^\bigsqcup^L^mathop^^N-ARY SQUARE UNION OPERATOR
02A07^⨇^^\conjquant^L^mathop^^TWO LOGICAL AND OPERATOR
02A08^⨈^^\disjquant^L^mathop^^TWO LOGICAL OR OPERATOR
02A09^⨉^\varprod^\bigtimes^L^mathop^txfonts^N-ARY TIMES OPERATOR
02A0A^⨊^^\modtwosum^L^mathord^^MODULO TWO SUM
02A0B^⨋^^\sumint^L^mathop^^SUMMATION WITH INTEGRAL
02A0C^⨌^\iiiint^\iiiint^L^mathop^amsmath esint^QUADRUPLE INTEGRAL OPERATOR
02A0D^⨍^^\intbar^L^mathop^^FINITE PART INTEGRAL
02A0E^⨎^^\intBar^L^mathop^^INTEGRAL WITH DOUBLE STROKE
02A0F^⨏^\fint^\fint^L^mathop^esint wrisym^INTEGRAL AVERAGE WITH SLASH
02A10^⨐^^\cirfnint^L^mathop^^CIRCULATION FUNCTION
02A11^⨑^^\awint^L^mathop^^ANTICLOCKWISE INTEGRATION
02A12^⨒^^\rppolint^L^mathop^^LINE INTEGRATION WITH RECTANGULAR PATH AROUND POLE
02A13^⨓^^\scpolint^L^mathop^^LINE INTEGRATION WITH SEMICIRCULAR PATH AROUND POLE
02A14^⨔^^\npolint^L^mathop^^LINE INTEGRATION NOT INCLUDING THE POLE
02A15^⨕^^\pointint^L^mathop^^INTEGRAL AROUND A POINT OPERATOR
02A16^⨖^\sqint^\sqint^L^mathop^esint^= \sqrint (wrisym), QUATERNION INTEGRAL OPERATOR
02A17^⨗^^\intlarhk^L^mathop^^INTEGRAL WITH LEFTWARDS ARROW WITH HOOK
02A18^⨘^^\intx^L^mathop^^INTEGRAL WITH TIMES SIGN
02A19^⨙^^\intcap^L^mathop^^INTEGRAL WITH INTERSECTION
02A1A^⨚^^\intcup^L^mathop^^INTEGRAL WITH UNION
02A1B^⨛^^\upint^L^mathop^^INTEGRAL WITH OVERBAR
02A1C^⨜^^\lowint^L^mathop^^INTEGRAL WITH UNDERBAR
02A1D^⨝^\Join^\Join^L^mathop^amssymb^JOIN
02A1E^⨞^^\bigtriangleleft^L^mathop^^LARGE LEFT TRIANGLE OPERATOR
02A1F^⨟^\zcmp^\zcmp^L^mathop^oz^= \semi (oz), = \fatsemi (stmaryrd), Z NOTATION SCHEMA COMPOSITION
02A20^⨠^\zpipe^\zpipe^L^mathop^oz^Z NOTATION SCHEMA PIPING
02A21^⨡^\zproject^\zproject^L^mathop^oz^= \project (oz), Z NOTATION SCHEMA PROJECTION
02A22^⨢^^\ringplus^B^mathbin^^PLUS SIGN WITH SMALL CIRCLE ABOVE
02A23^⨣^^\plushat^B^mathbin^^PLUS SIGN WITH CIRCUMFLEX ACCENT ABOVE
02A24^⨤^^\simplus^B^mathbin^^PLUS SIGN WITH TILDE ABOVE
02A25^⨥^^\plusdot^B^mathbin^^PLUS SIGN WITH DOT BELOW
02A26^⨦^^\plussim^B^mathbin^^PLUS SIGN WITH TILDE BELOW
02A27^⨧^^\plussubtwo^B^mathbin^^PLUS SIGN WITH SUBSCRIPT TWO
02A28^⨨^^\plustrif^B^mathbin^^PLUS SIGN WITH BLACK TRIANGLE
02A29^⨩^^\commaminus^B^mathbin^^MINUS SIGN WITH COMMA ABOVE
02A2A^⨪^^\minusdot^B^mathbin^^MINUS SIGN WITH DOT BELOW
02A2B^⨫^^\minusfdots^B^mathbin^^MINUS SIGN WITH FALLING DOTS
02A2C^⨬^^\minusrdots^B^mathbin^^MINUS SIGN WITH RISING DOTS
02A2D^⨭^^\opluslhrim^B^mathbin^^PLUS SIGN IN LEFT HALF CIRCLE
02A2E^⨮^^\oplusrhrim^B^mathbin^^PLUS SIGN IN RIGHT HALF CIRCLE
02A2F^⨯^^\vectimes^B^mathbin^^# \times, VECTOR OR CROSS PRODUCT
02A30^⨰^^\dottimes^B^mathbin^^MULTIPLICATION SIGN WITH DOT ABOVE
02A31^⨱^^\timesbar^B^mathbin^^MULTIPLICATION SIGN WITH UNDERBAR
02A32^⨲^^\btimes^B^mathbin^^SEMIDIRECT PRODUCT WITH BOTTOM CLOSED
02A33^⨳^^\smashtimes^B^mathbin^^SMASH PRODUCT
02A34^⨴^^\otimeslhrim^B^mathbin^^MULTIPLICATION SIGN IN LEFT HALF CIRCLE
02A35^⨵^^\otimesrhrim^B^mathbin^^MULTIPLICATION SIGN IN RIGHT HALF CIRCLE
02A36^⨶^^\otimeshat^B^mathbin^^CIRCLED MULTIPLICATION SIGN WITH CIRCUMFLEX ACCENT
02A37^⨷^^\Otimes^B^mathbin^^MULTIPLICATION SIGN IN DOUBLE CIRCLE
02A38^⨸^^\odiv^B^mathbin^^CIRCLED DIVISION SIGN
02A39^⨹^^\triangleplus^B^mathbin^^PLUS SIGN IN TRIANGLE
02A3A^⨺^^\triangleminus^B^mathbin^^MINUS SIGN IN TRIANGLE
02A3B^⨻^^\triangletimes^B^mathbin^^MULTIPLICATION SIGN IN TRIANGLE
02A3C^⨼^^\intprod^B^mathbin^^INTERIOR PRODUCT
02A3D^⨽^^\intprodr^B^mathbin^^RIGHTHAND INTERIOR PRODUCT
02A3E^⨾^\fcmp^\fcmp^B^mathbin^oz^= \comp (oz), Z NOTATION RELATIONAL COMPOSITION
02A3F^⨿^\amalg^\amalg^B^mathbin^^AMALGAMATION OR COPRODUCT
02A40^⩀^^\capdot^B^mathbin^^INTERSECTION WITH DOT
02A41^⩁^^\uminus^B^mathbin^^UNION WITH MINUS SIGN, z notation bag subtraction
02A42^⩂^^\barcup^B^mathbin^^UNION WITH OVERBAR
02A43^⩃^^\barcap^B^mathbin^^INTERSECTION WITH OVERBAR
02A44^⩄^^\capwedge^B^mathbin^^INTERSECTION WITH LOGICAL AND
02A45^⩅^^\cupvee^B^mathbin^^UNION WITH LOGICAL OR
02A46^⩆^^\cupovercap^B^mathbin^^UNION ABOVE INTERSECTION
02A47^⩇^^\capovercup^B^mathbin^^INTERSECTION ABOVE UNION
02A48^⩈^^\cupbarcap^B^mathbin^^UNION ABOVE BAR ABOVE INTERSECTION
02A49^⩉^^\capbarcup^B^mathbin^^INTERSECTION ABOVE BAR ABOVE UNION
02A4A^⩊^^\twocups^B^mathbin^^UNION BESIDE AND JOINED WITH UNION
02A4B^⩋^^\twocaps^B^mathbin^^INTERSECTION BESIDE AND JOINED WITH INTERSECTION
02A4C^⩌^^\closedvarcup^B^mathbin^^CLOSED UNION WITH SERIFS
02A4D^⩍^^\closedvarcap^B^mathbin^^CLOSED INTERSECTION WITH SERIFS
02A4E^⩎^^\Sqcap^B^mathbin^^DOUBLE SQUARE INTERSECTION
02A4F^⩏^^\Sqcup^B^mathbin^^DOUBLE SQUARE UNION
02A50^⩐^^\closedvarcupsmashprod^B^mathbin^^CLOSED UNION WITH SERIFS AND SMASH PRODUCT
02A51^⩑^^\wedgeodot^B^mathbin^^LOGICAL AND WITH DOT ABOVE
02A52^⩒^^\veeodot^B^mathbin^^LOGICAL OR WITH DOT ABOVE
02A53^⩓^^\Wedge^B^mathbin^^DOUBLE LOGICAL AND
02A54^⩔^^\Vee^B^mathbin^^DOUBLE LOGICAL OR
02A55^⩕^^\wedgeonwedge^B^mathbin^^TWO INTERSECTING LOGICAL AND
02A56^⩖^^\veeonvee^B^mathbin^^TWO INTERSECTING LOGICAL OR
02A57^⩗^^\bigslopedvee^B^mathbin^^SLOPING LARGE OR
02A58^⩘^^\bigslopedwedge^B^mathbin^^SLOPING LARGE AND
02A59^⩙^^\veeonwedge^R^mathrel^^LOGICAL OR OVERLAPPING LOGICAL AND
02A5A^⩚^^\wedgemidvert^B^mathbin^^LOGICAL AND WITH MIDDLE STEM
02A5B^⩛^^\veemidvert^B^mathbin^^LOGICAL OR WITH MIDDLE STEM
02A5C^⩜^^\midbarwedge^B^mathbin^^ogical and with horizontal dash
02A5D^⩝^^\midbarvee^B^mathbin^^LOGICAL OR WITH HORIZONTAL DASH
02A5E^⩞^\doublebarwedge^\doublebarwedge^B^mathbin^amssymb^LOGICAL AND WITH DOUBLE OVERBAR
02A5F^⩟^^\wedgebar^B^mathbin^^LOGICAL AND WITH UNDERBAR
02A60^⩠^^\wedgedoublebar^B^mathbin^^LOGICAL AND WITH DOUBLE UNDERBAR
02A61^⩡^^\varveebar^B^mathbin^^SMALL VEE WITH UNDERBAR
02A62^⩢^^\doublebarvee^B^mathbin^^LOGICAL OR WITH DOUBLE OVERBAR
02A63^⩣^^\veedoublebar^B^mathbin^^LOGICAL OR WITH DOUBLE UNDERBAR
02A64^⩤^\dsub^\dsub^B^mathbin^oz^= \ndres (oz), Z NOTATION DOMAIN ANTIRESTRICTION
02A65^⩥^\rsub^\rsub^B^mathbin^oz^= \nrres (oz), Z NOTATION RANGE ANTIRESTRICTION
02A66^⩦^^\eqdot^R^mathrel^^EQUALS SIGN WITH DOT BELOW
02A67^⩧^^\dotequiv^R^mathrel^^IDENTICAL WITH DOT ABOVE
02A68^⩨^^\equivVert^R^mathrel^^TRIPLE HORIZONTAL BAR WITH DOUBLE VERTICAL STROKE
02A69^⩩^^\equivVvert^R^mathrel^^TRIPLE HORIZONTAL BAR WITH TRIPLE VERTICAL STROKE
02A6A^⩪^^\dotsim^R^mathrel^^TILDE OPERATOR WITH DOT ABOVE
02A6B^⩫^^\simrdots^R^mathrel^^TILDE OPERATOR WITH RISING DOTS
02A6C^⩬^^\simminussim^R^mathrel^^SIMILAR MINUS SIMILAR
02A6D^⩭^^\congdot^R^mathrel^^CONGRUENT WITH DOT ABOVE
02A6E^⩮^^\asteq^R^mathrel^^EQUALS WITH ASTERISK
02A6F^⩯^^\hatapprox^R^mathrel^^ALMOST EQUAL TO WITH CIRCUMFLEX ACCENT
02A70^⩰^^\approxeqq^R^mathrel^^APPROXIMATELY EQUAL OR EQUAL TO
02A71^⩱^^\eqqplus^B^mathbin^^EQUALS SIGN ABOVE PLUS SIGN
02A72^⩲^^\pluseqq^B^mathbin^^PLUS SIGN ABOVE EQUALS SIGN
02A73^⩳^^\eqqsim^R^mathrel^^EQUALS SIGN ABOVE TILDE OPERATOR
02A74^⩴^\Coloneqq^\Coloneq^R^mathrel^txfonts^# ::=, x \Coloneq (txfonts), DOUBLE COLON EQUAL
02A75^⩵^\Equal^\eqeq^R^mathrel^wrisym^# ==, TWO CONSECUTIVE EQUALS SIGNS
02A76^⩶^\Same^\eqeqeq^R^mathrel^wrisym^# ===, THREE CONSECUTIVE EQUALS SIGNS
02A77^⩷^^\ddotseq^R^mathrel^^EQUALS SIGN WITH TWO DOTS ABOVE AND TWO DOTS BELOW
02A78^⩸^^\equivDD^R^mathrel^^EQUIVALENT WITH FOUR DOTS ABOVE
02A79^⩹^^\ltcir^R^mathrel^^LESS-THAN WITH CIRCLE INSIDE
02A7A^⩺^^\gtcir^R^mathrel^^GREATER-THAN WITH CIRCLE INSIDE
02A7B^⩻^^\ltquest^R^mathrel^^LESS-THAN WITH QUESTION MARK ABOVE
02A7C^⩼^^\gtquest^R^mathrel^^GREATER-THAN WITH QUESTION MARK ABOVE
02A7D^⩽^\leqslant^\leqslant^R^mathrel^amssymb fourier^LESS-THAN OR SLANTED EQUAL TO
02A7E^⩾^\geqslant^\geqslant^R^mathrel^amssymb fourier^GREATER-THAN OR SLANTED EQUAL TO
02A7F^⩿^^\lesdot^R^mathrel^^LESS-THAN OR SLANTED EQUAL TO WITH DOT INSIDE
02A80^⪀^^\gesdot^R^mathrel^^GREATER-THAN OR SLANTED EQUAL TO WITH DOT INSIDE
02A81^⪁^^\lesdoto^R^mathrel^^LESS-THAN OR SLANTED EQUAL TO WITH DOT ABOVE
02A82^⪂^^\gesdoto^R^mathrel^^GREATER-THAN OR SLANTED EQUAL TO WITH DOT ABOVE
02A83^⪃^^\lesdotor^R^mathrel^^LESS-THAN OR SLANTED EQUAL TO WITH DOT ABOVE RIGHT
02A84^⪄^^\gesdotol^R^mathrel^^GREATER-THAN OR SLANTED EQUAL TO WITH DOT ABOVE LEFT
02A85^⪅^\lessapprox^\lessapprox^R^mathrel^amssymb^LESS-THAN OR APPROXIMATE
02A86^⪆^\gtrapprox^\gtrapprox^R^mathrel^amssymb^GREATER-THAN OR APPROXIMATE
02A87^⪇^\lneq^\lneq^R^mathrel^amssymb^LESS-THAN AND SINGLE-LINE NOT EQUAL TO
02A88^⪈^\gneq^\gneq^R^mathrel^amssymb^GREATER-THAN AND SINGLE-LINE NOT EQUAL TO
02A89^⪉^\lnapprox^\lnapprox^R^mathrel^amssymb^LESS-THAN AND NOT APPROXIMATE
02A8A^⪊^\gnapprox^\gnapprox^R^mathrel^amssymb^GREATER-THAN AND NOT APPROXIMATE
02A8B^⪋^\lesseqqgtr^\lesseqqgtr^R^mathrel^amssymb^LESS-THAN ABOVE DOUBLE-LINE EQUAL ABOVE GREATER-THAN
02A8C^⪌^\gtreqqless^\gtreqqless^R^mathrel^amssymb^GREATER-THAN ABOVE DOUBLE-LINE EQUAL ABOVE LESS-THAN
02A8D^⪍^^\lsime^R^mathrel^^LESS-THAN ABOVE SIMILAR OR EQUAL
02A8E^⪎^^\gsime^R^mathrel^^GREATER-THAN ABOVE SIMILAR OR EQUAL
02A8F^⪏^^\lsimg^R^mathrel^^LESS-THAN ABOVE SIMILAR ABOVE GREATER-THAN
02A90^⪐^^\gsiml^R^mathrel^^GREATER-THAN ABOVE SIMILAR ABOVE LESS-THAN
02A91^⪑^^\lgE^R^mathrel^^LESS-THAN ABOVE GREATER-THAN ABOVE DOUBLE-LINE EQUAL
02A92^⪒^^\glE^R^mathrel^^GREATER-THAN ABOVE LESS-THAN ABOVE DOUBLE-LINE EQUAL
02A93^⪓^^\lesges^R^mathrel^^LESS-THAN ABOVE SLANTED EQUAL ABOVE GREATER-THAN ABOVE SLANTED EQUAL
02A94^⪔^^\gesles^R^mathrel^^GREATER-THAN ABOVE SLANTED EQUAL ABOVE LESS-THAN ABOVE SLANTED EQUAL
02A95^⪕^\eqslantless^\eqslantless^R^mathrel^amssymb^SLANTED EQUAL TO OR LESS-THAN
02A96^⪖^\eqslantgtr^\eqslantgtr^R^mathrel^amssymb^SLANTED EQUAL TO OR GREATER-THAN
02A97^⪗^^\elsdot^R^mathrel^^SLANTED EQUAL TO OR LESS-THAN WITH DOT INSIDE
02A98^⪘^^\egsdot^R^mathrel^^SLANTED EQUAL TO OR GREATER-THAN WITH DOT INSIDE
02A99^⪙^^\eqqless^R^mathrel^^DOUBLE-LINE EQUAL TO OR LESS-THAN
02A9A^⪚^^\eqqgtr^R^mathrel^^DOUBLE-LINE EQUAL TO OR GREATER-THAN
02A9B^⪛^^\eqqslantless^R^mathrel^^DOUBLE-LINE SLANTED EQUAL TO OR LESS-THAN
02A9C^⪜^^\eqqslantgtr^R^mathrel^^DOUBLE-LINE SLANTED EQUAL TO OR GREATER-THAN
02A9D^⪝^^\simless^R^mathrel^^SIMILAR OR LESS-THAN
02A9E^⪞^^\simgtr^R^mathrel^^SIMILAR OR GREATER-THAN
02A9F^⪟^^\simlE^R^mathrel^^SIMILAR ABOVE LESS-THAN ABOVE EQUALS SIGN
02AA0^⪠^^\simgE^R^mathrel^^SIMILAR ABOVE GREATER-THAN ABOVE EQUALS SIGN
02AA1^⪡^\NestedLessLess^\Lt^R^mathrel^wrisym^= \lll (mathabx -amssymb), DOUBLE NESTED LESS-THAN
02AA2^⪢^\NestedGreaterGreater^\Gt^R^mathrel^wrisym^= \ggg (mathabx -amssymb), DOUBLE NESTED GREATER-THAN
02AA3^⪣^^\partialmeetcontraction^R^mathrel^^double less-than with underbar
02AA4^⪤^^\glj^R^mathrel^^GREATER-THAN OVERLAPPING LESS-THAN
02AA5^⪥^^\gla^R^mathrel^^GREATER-THAN BESIDE LESS-THAN
02AA6^⪦^\leftslice^\ltcc^R^mathrel^stmaryrd^LESS-THAN CLOSED BY CURVE
02AA7^⪧^\rightslice^\gtcc^R^mathrel^stmaryrd^GREATER-THAN CLOSED BY CURVE
02AA8^⪨^^\lescc^R^mathrel^^LESS-THAN CLOSED BY CURVE ABOVE SLANTED EQUAL
02AA9^⪩^^\gescc^R^mathrel^^GREATER-THAN CLOSED BY CURVE ABOVE SLANTED EQUAL
02AAA^⪪^^\smt^R^mathrel^^SMALLER THAN
02AAB^⪫^^\lat^R^mathrel^^LARGER THAN
02AAC^⪬^^\smte^R^mathrel^^SMALLER THAN OR EQUAL TO
02AAD^⪭^^\late^R^mathrel^^LARGER THAN OR EQUAL TO
02AAE^⪮^^\bumpeqq^R^mathrel^^EQUALS SIGN WITH BUMPY ABOVE
02AAF^⪯^\preceq^\preceq^R^mathrel^^PRECEDES ABOVE SINGLE-LINE EQUALS SIGN
02AB0^⪰^\succeq^\succeq^R^mathrel^^SUCCEEDS ABOVE SINGLE-LINE EQUALS SIGN
02AB1^⪱^^\precneq^R^mathrel^^PRECEDES ABOVE SINGLE-LINE NOT EQUAL TO
02AB2^⪲^^\succneq^R^mathrel^^SUCCEEDS ABOVE SINGLE-LINE NOT EQUAL TO
02AB3^⪳^\preceqq^\preceqq^R^mathrel^txfonts^PRECEDES ABOVE EQUALS SIGN
02AB4^⪴^\succeqq^\succeqq^R^mathrel^txfonts^SUCCEEDS ABOVE EQUALS SIGN
02AB5^⪵^^\precneqq^R^mathrel^amssymb^PRECEDES ABOVE NOT EQUAL TO
02AB6^⪶^^\succneqq^R^mathrel^amssymb^SUCCEEDS ABOVE NOT EQUAL TO
02AB7^⪷^\precapprox^\precapprox^R^mathrel^amssymb^PRECEDES ABOVE ALMOST EQUAL TO
02AB8^⪸^\succapprox^\succapprox^R^mathrel^amssymb^SUCCEEDS ABOVE ALMOST EQUAL TO
02AB9^⪹^\precnapprox^\precnapprox^R^mathrel^amssymb^PRECEDES ABOVE NOT ALMOST EQUAL TO
02ABA^⪺^\succnapprox^\succnapprox^R^mathrel^amssymb^SUCCEEDS ABOVE NOT ALMOST EQUAL TO
02ABB^⪻^\llcurly^\Prec^R^mathrel^mathabx^DOUBLE PRECEDES
02ABC^⪼^\ggcurly^\Succ^R^mathrel^mathabx^DOUBLE SUCCEEDS
02ABD^⪽^^\subsetdot^R^mathrel^^SUBSET WITH DOT
02ABE^⪾^^\supsetdot^R^mathrel^^SUPERSET WITH DOT
02ABF^⪿^^\subsetplus^R^mathrel^^SUBSET WITH PLUS SIGN BELOW
02AC0^⫀^^\supsetplus^R^mathrel^^SUPERSET WITH PLUS SIGN BELOW
02AC1^⫁^^\submult^R^mathrel^^SUBSET WITH MULTIPLICATION SIGN BELOW
02AC2^⫂^^\supmult^R^mathrel^^SUPERSET WITH MULTIPLICATION SIGN BELOW
02AC3^⫃^^\subedot^R^mathrel^^SUBSET OF OR EQUAL TO WITH DOT ABOVE
02AC4^⫄^^\supedot^R^mathrel^^SUPERSET OF OR EQUAL TO WITH DOT ABOVE
02AC5^⫅^\subseteqq^\subseteqq^R^mathrel^amssymb^SUBSET OF ABOVE EQUALS SIGN
02AC6^⫆^\supseteqq^\supseteqq^R^mathrel^amssymb^SUPERSET OF ABOVE EQUALS SIGN
02AC7^⫇^^\subsim^R^mathrel^^SUBSET OF ABOVE TILDE OPERATOR
02AC8^⫈^^\supsim^R^mathrel^^SUPERSET OF ABOVE TILDE OPERATOR
02AC9^⫉^^\subsetapprox^R^mathrel^^SUBSET OF ABOVE ALMOST EQUAL TO
02ACA^⫊^^\supsetapprox^R^mathrel^^SUPERSET OF ABOVE ALMOST EQUAL TO
02ACB^⫋^\subsetneqq^\subsetneqq^R^mathrel^amssymb^SUBSET OF ABOVE NOT EQUAL TO
02ACC^⫌^\supsetneqq^\supsetneqq^R^mathrel^amssymb^SUPERSET OF ABOVE NOT EQUAL TO
02ACD^⫍^^\lsqhook^R^mathrel^^SQUARE LEFT OPEN BOX OPERATOR
02ACE^⫎^^\rsqhook^R^mathrel^^SQUARE RIGHT OPEN BOX OPERATOR
02ACF^⫏^^\csub^R^mathrel^^CLOSED SUBSET
02AD0^⫐^^\csup^R^mathrel^^CLOSED SUPERSET
02AD1^⫑^^\csube^R^mathrel^^CLOSED SUBSET OR EQUAL TO
02AD2^⫒^^\csupe^R^mathrel^^CLOSED SUPERSET OR EQUAL TO
02AD3^⫓^^\subsup^R^mathrel^^SUBSET ABOVE SUPERSET
02AD4^⫔^^\supsub^R^mathrel^^SUPERSET ABOVE SUBSET
02AD5^⫕^^\subsub^R^mathrel^^SUBSET ABOVE SUBSET
02AD6^⫖^^\supsup^R^mathrel^^SUPERSET ABOVE SUPERSET
02AD7^⫗^^\suphsub^R^mathrel^^SUPERSET BESIDE SUBSET
02AD8^⫘^^\supdsub^R^mathrel^^SUPERSET BESIDE AND JOINED BY DASH WITH SUBSET
02AD9^⫙^^\forkv^R^mathrel^^ELEMENT OF OPENING DOWNWARDS
02ADA^⫚^^\topfork^R^mathrel^^PITCHFORK WITH TEE TOP
02ADB^⫛^^\mlcp^R^mathrel^^TRANSVERSAL INTERSECTION
02ADC^⫝̸^^\forks^R^mathrel^^FORKING
02ADD^⫝^^\forksnot^R^mathrel^^NONFORKING
02ADE^⫞^^\shortlefttack^R^mathrel^^SHORT LEFT TACK
02ADF^⫟^^\shortdowntack^R^mathrel^^SHORT DOWN TACK
02AE0^⫠^^\shortuptack^R^mathrel^^SHORT UP TACK
02AE1^⫡^^\perps^N^mathord^^PERPENDICULAR WITH S
02AE2^⫢^^\vDdash^R^mathrel^^VERTICAL BAR TRIPLE RIGHT TURNSTILE
02AE3^⫣^^\dashV^R^mathrel^^DOUBLE VERTICAL BAR LEFT TURNSTILE
02AE4^⫤^^\Dashv^R^mathrel^^VERTICAL BAR DOUBLE LEFT TURNSTILE
02AE5^⫥^^\DashV^R^mathrel^^DOUBLE VERTICAL BAR DOUBLE LEFT TURNSTILE
02AE6^⫦^^\varVdash^R^mathrel^^LONG DASH FROM LEFT MEMBER OF DOUBLE VERTICAL
02AE7^⫧^^\Barv^R^mathrel^^SHORT DOWN TACK WITH OVERBAR
02AE8^⫨^^\vBar^R^mathrel^^SHORT UP TACK WITH UNDERBAR
02AE9^⫩^^\vBarv^R^mathrel^^SHORT UP TACK ABOVE SHORT DOWN TACK
02AEA^⫪^\Top^\barV^R^mathrel^txfonts^DOUBLE DOWN TACK
02AEB^⫫^\Bot^\Vbar^R^mathrel^txfonts^= \Perp (txfonts), DOUBLE UP TACK
02AEC^⫬^^\Not^R^mathrel^^DOUBLE STROKE NOT SIGN
02AED^⫭^^\bNot^R^mathrel^^REVERSED DOUBLE STROKE NOT SIGN
02AEE^⫮^^\revnmid^R^mathrel^^DOES NOT DIVIDE WITH REVERSED NEGATION SLASH
02AEF^⫯^^\cirmid^R^mathrel^^VERTICAL LINE WITH CIRCLE ABOVE
02AF0^⫰^^\midcir^R^mathrel^^VERTICAL LINE WITH CIRCLE BELOW
02AF1^⫱^^\topcir^N^mathord^^DOWN TACK WITH CIRCLE BELOW
02AF2^⫲^^\nhpar^R^mathrel^^PARALLEL WITH HORIZONTAL STROKE
02AF3^⫳^^\parsim^R^mathrel^^PARALLEL WITH TILDE OPERATOR
02AF4^⫴^\interleave^\interleave^B^mathbin^stmaryrd^TRIPLE VERTICAL BAR BINARY RELATION
02AF5^⫵^^\nhVvert^B^mathbin^^TRIPLE VERTICAL BAR WITH HORIZONTAL STROKE
02AF6^⫶^^\threedotcolon^B^mathbin^^TRIPLE COLON OPERATOR
02AF7^⫷^^\lllnest^R^mathrel^^TRIPLE NESTED LESS-THAN
02AF8^⫸^^\gggnest^R^mathrel^^TRIPLE NESTED GREATER-THAN
02AF9^⫹^^\leqqslant^R^mathrel^^DOUBLE-LINE SLANTED LESS-THAN OR EQUAL TO
02AFA^⫺^^\geqqslant^R^mathrel^^DOUBLE-LINE SLANTED GREATER-THAN OR EQUAL TO
02AFB^⫻^^\trslash^B^mathbin^^TRIPLE SOLIDUS BINARY RELATION
02AFC^⫼^\biginterleave^\biginterleave^L^mathop^stmaryrd^LARGE TRIPLE VERTICAL BAR OPERATOR
02AFD^⫽^\sslash^\sslash^B^mathbin^stmaryrd^# \varparallel (txfonts), DOUBLE SOLIDUS OPERATOR
02AFE^⫾^\talloblong^\talloblong^B^mathbin^stmaryrd^WHITE VERTICAL BAR
02AFF^⫿^^\bigtalloblong^L^mathop^^N-ARY WHITE VERTICAL BAR
02B00^⬀^^^R^mathord^^NORTH EAST WHITE ARROW
02B01^⬁^^^R^mathord^^NORTH WEST WHITE ARROW
02B02^⬂^^^R^mathord^^SOUTH EAST WHITE ARROW
02B03^⬃^^^R^mathord^^SOUTH WEST WHITE ARROW
02B04^⬄^^^R^mathord^^LEFT RIGHT WHITE ARROW
02B05^⬅^^^R^mathord^^LEFTWARDS BLACK ARROW
02B06^⬆^^^R^mathord^^UPWARDS BLACK ARROW
02B07^⬇^^^R^mathord^^DOWNWARDS BLACK ARROW
02B08^⬈^^^R^mathord^^NORTH EAST BLACK ARROW
02B09^⬉^^^R^mathord^^NORTH WEST BLACK ARROW
02B0A^⬊^^^R^mathord^^SOUTH EAST BLACK ARROW
02B0B^⬋^^^R^mathord^^SOUTH WEST BLACK ARROW
02B0C^⬌^^^R^mathord^^LEFT RIGHT BLACK ARROW
02B0D^⬍^^^R^mathord^^UP DOWN BLACK ARROW
02B0E^⬎^^^R^mathord^^RIGHTWARDS ARROW WITH TIP DOWNWARDS
02B0F^⬏^^^R^mathord^^RIGHTWARDS ARROW WITH TIP UPWARDS
02B10^⬐^^^R^mathord^^LEFTWARDS ARROW WITH TIP DOWNWARDS
02B11^⬑^^^R^mathord^^LEFTWARDS ARROW WITH TIP UPWARDS
02B12^⬒^^\squaretopblack^N^mathord^^SQUARE WITH TOP HALF BLACK
02B13^⬓^^\squarebotblack^N^mathord^^SQUARE WITH BOTTOM HALF BLACK
02B14^⬔^^\squareurblack^N^mathord^^SQUARE WITH UPPER RIGHT DIAGONAL HALF BLACK
02B15^⬕^^\squarellblack^N^mathord^^SQUARE WITH LOWER LEFT DIAGONAL HALF BLACK
02B16^⬖^^\diamondleftblack^N^mathord^^DIAMOND WITH LEFT HALF BLACK
02B17^⬗^^\diamondrightblack^N^mathord^^DIAMOND WITH RIGHT HALF BLACK
02B18^⬘^^\diamondtopblack^N^mathord^^DIAMOND WITH TOP HALF BLACK
02B19^⬙^^\diamondbotblack^N^mathord^^DIAMOND WITH BOTTOM HALF BLACK
02B1A^⬚^^\dottedsquare^^mathord^^DOTTED SQUARE
02B1B^⬛^\blacksquare^\lgblksquare^N^mathord^fourier -amssymb^BLACK LARGE SQUARE
02B1C^⬜^\square^\lgwhtsquare^N^mathord^fourier -amssymb^WHITE LARGE SQUARE
02B1D^⬝^^\vysmblksquare^N^mathord^^# \centerdot (amssymb), t \Squaredot (marvosym), BLACK VERY SMALL SQUARE
02B1E^⬞^^\vysmwhtsquare^N^mathord^^WHITE VERY SMALL SQUARE
02B1F^⬟^^\pentagonblack^N^mathord^^BLACK PENTAGON
02B20^⬠^^\pentagon^N^mathord^^WHITE PENTAGON
02B21^⬡^^\varhexagon^N^mathord^^WHITE HEXAGON
02B22^⬢^^\varhexagonblack^N^mathord^^BLACK HEXAGON
02B23^⬣^^\hexagonblack^N^mathord^^HORIZONTAL BLACK HEXAGON
02B24^⬤^^\lgblkcircle^N^mathord^^BLACK LARGE CIRCLE
02B25^⬥^^\mdblkdiamond^N^mathord^^BLACK MEDIUM DIAMOND
02B26^⬦^^\mdwhtdiamond^N^mathord^^WHITE MEDIUM DIAMOND
02B27^⬧^^\mdblklozenge^N^mathord^^# \blacklozenge (amssymb), BLACK MEDIUM LOZENGE
02B28^⬨^^\mdwhtlozenge^N^mathord^^# \lozenge (amssymb), WHITE MEDIUM LOZENGE
02B29^⬩^^\smblkdiamond^N^mathord^^BLACK SMALL DIAMOND
02B2A^⬪^^\smblklozenge^N^mathord^^BLACK SMALL LOZENGE
02B2B^⬫^^\smwhtlozenge^N^mathord^^WHITE SMALL LOZENGE
02B2C^⬬^^\blkhorzoval^N^mathord^^BLACK HORIZONTAL ELLIPSE
02B2D^⬭^^\whthorzoval^N^mathord^^WHITE HORIZONTAL ELLIPSE
02B2E^⬮^^\blkvertoval^N^mathord^^BLACK VERTICAL ELLIPSE
02B2F^⬯^^\whtvertoval^N^mathord^^WHITE VERTICAL ELLIPSE
02B30^⬰^^\circleonleftarrow^R^mathrel^^LEFT ARROW WITH SMALL CIRCLE
02B31^⬱^^\leftthreearrows^R^mathrel^^THREE LEFTWARDS ARROWS
02B32^⬲^^\leftarrowonoplus^R^mathrel^^LEFT ARROW WITH CIRCLED PLUS
02B33^⬳^^\longleftsquigarrow^R^mathrel^^LONG LEFTWARDS SQUIGGLE ARROW
02B34^⬴^^\nvtwoheadleftarrow^R^mathrel^^LEFTWARDS TWO-HEADED ARROW WITH VERTICAL STROKE
02B35^⬵^^\nVtwoheadleftarrow^R^mathrel^^LEFTWARDS TWO-HEADED ARROW WITH DOUBLE VERTICAL STROKE
02B36^⬶^^\twoheadmapsfrom^R^mathrel^^LEFTWARDS TWO-HEADED ARROW FROM BAR
02B37^⬷^^\twoheadleftdbkarrow^R^mathrel^^leftwards two-headed triple-dash arrow
02B38^⬸^^\leftdotarrow^R^mathrel^^LEFTWARDS ARROW WITH DOTTED STEM
02B39^⬹^^\nvleftarrowtail^R^mathrel^^LEFTWARDS ARROW WITH TAIL WITH VERTICAL STROKE
02B3A^⬺^^\nVleftarrowtail^R^mathrel^^LEFTWARDS ARROW WITH TAIL WITH DOUBLE VERTICAL STROKE
02B3B^⬻^^\twoheadleftarrowtail^R^mathrel^^LEFTWARDS TWO-HEADED ARROW WITH TAIL
02B3C^⬼^^\nvtwoheadleftarrowtail^R^mathrel^^LEFTWARDS TWO-HEADED ARROW WITH TAIL WITH VERTICAL STROKE
02B3D^⬽^^\nVtwoheadleftarrowtail^R^mathrel^^LEFTWARDS TWO-HEADED ARROW WITH TAIL WITH DOUBLE VERTICAL STROKE
02B3E^⬾^^\leftarrowx^R^mathrel^^LEFTWARDS ARROW THROUGH X
02B3F^⬿^^\leftcurvedarrow^R^mathrel^^WAVE ARROW POINTING DIRECTLY LEFT
02B40^⭀^^\equalleftarrow^R^mathrel^^EQUALS SIGN ABOVE LEFTWARDS ARROW
02B41^⭁^^\bsimilarleftarrow^R^mathrel^^REVERSE TILDE OPERATOR ABOVE LEFTWARDS ARROW
02B42^⭂^^\leftarrowbackapprox^R^mathrel^^LEFTWARDS ARROW ABOVE REVERSE ALMOST EQUAL TO
02B43^⭃^^\rightarrowgtr^R^mathrel^^rightwards arrow through less-than
02B44^⭄^^\rightarrowsupset^R^mathrel^^rightwards arrow through subset
02B45^⭅^^\LLeftarrow^R^mathrel^^LEFTWARDS QUADRUPLE ARROW
02B46^⭆^^\RRightarrow^R^mathrel^^RIGHTWARDS QUADRUPLE ARROW
02B47^⭇^^\bsimilarrightarrow^R^mathrel^^REVERSE TILDE OPERATOR ABOVE RIGHTWARDS ARROW
02B48^⭈^^\rightarrowbackapprox^R^mathrel^^RIGHTWARDS ARROW ABOVE REVERSE ALMOST EQUAL TO
02B49^⭉^^\similarleftarrow^R^mathrel^^TILDE OPERATOR ABOVE LEFTWARDS ARROW
02B4A^⭊^^\leftarrowapprox^R^mathrel^^LEFTWARDS ARROW ABOVE ALMOST EQUAL TO
02B4B^⭋^^\leftarrowbsimilar^R^mathrel^^LEFTWARDS ARROW ABOVE REVERSE TILDE OPERATOR
02B4C^⭌^^\rightarrowbsimilar^R^mathrel^^righttwards arrow above reverse tilde operator
02B50^⭐^^\medwhitestar^N^mathord^^WHITE MEDIUM STAR
02B51^⭑^^\medblackstar^N^mathord^^black medium star
02B52^⭒^^\smwhitestar^N^mathord^^WHITE SMALL STAR
02B53^⭓^^\rightpentagonblack^N^mathord^^BLACK RIGHT-POINTING PENTAGON
02B54^⭔^^\rightpentagon^N^mathord^^WHITE RIGHT-POINTING PENTAGON
03008^〈^^^X^mathopen^^# \langle, LEFT ANGLE BRACKET (deprecated for math use)
03009^〉^^^X^mathclose^^# \rangle, RIGHT ANGLE BRACKET (deprecated for math use)
03012^〒^^\postalmark^^mathord^^POSTAL MARK
03014^〔^^\lbrbrak^^mathopen^^left broken bracket
03015^〕^^\rbrbrak^^mathclose^^right broken bracket
03018^〘^^\Lbrbrak^^mathopen^^LEFT WHITE TORTOISE SHELL BRACKET
03019^〙^^\Rbrbrak^^mathclose^^RIGHT WHITE TORTOISE SHELL BRACKET
0301A^〚^^^X^mathopen^^# \llbracket (stmaryrd), LEFT WHITE SQUARE BRACKET (deprecated for math use)
0301B^〛^^^X^mathclose^^# \rrbracket (stmaryrd), RIGHT WHITE SQUARE BRACKET (deprecated for math use)
03030^〰^^\hzigzag^^mathord^^zigzag
0306E^の^^^N^mathalpha^^HIRAGANA LETTER NO
0FB29^﬩^^^X^mathord^^HEBREW LETTER ALTERNATIVE PLUS SIGN (doesn't have cross shape)
0FE00^︀^^^D^mathaccent^^VARIATION SELECTOR-1
0FE61^﹡^^^X^^^SMALL ASTERISK
0FE62^﹢^^^X^mathord^^SMALL PLUS SIGN
0FE63^﹣^^^X^mathord^^SMALL HYPHEN-MINUS
0FE64^﹤^^^X^mathord^^SMALL LESS-THAN SIGN
0FE65^﹥^^^X^mathord^^SMALL GREATER-THAN SIGN
0FE66^﹦^^^X^mathord^^SMALL EQUALS SIGN
0FE68^﹨^^^X^^^SMALL REVERSE SOLIDUS
0FF0B^＋^^^X^mathord^^FULLWIDTH PLUS SIGN
0FF1C^＜^^^X^mathord^^FULLWIDTH LESS-THAN SIGN
0FF1D^＝^^^X^mathord^^FULLWIDTH EQUALS SIGN
0FF1E^＞^^^X^mathord^^FULLWIDTH GREATER-THAN SIGN
0FF3C^＼^^^X^^^FULLWIDTH REVERSE SOLIDUS
0FF3E^＾^^^X^mathord^^FULLWIDTH CIRCUMFLEX ACCENT
0FF5C^｜^^^X^mathord^^FULLWIDTH VERTICAL LINE
0FF5E^～^^^X^mathord^^FULLWIDTH TILDE
0FFE2^￢^^^X^mathord^^FULLWIDTH NOT SIGN
0FFE9^￩^^^X^mathord^^HALFWIDTH LEFTWARDS ARROW
0FFEA^￪^^^X^mathord^^HALFWIDTH UPWARDS ARROW
0FFEB^￫^^^X^mathord^^HALFWIDTH RIGHTWARDS ARROW
0FFEC^￬^^^X^mathord^^HALFWIDTH DOWNWARDS ARROW
1D400^𝐀^\mathbf{A}^\mbfA^A^mathalpha^^MATHEMATICAL BOLD CAPITAL A
1D401^𝐁^\mathbf{B}^\mbfB^A^mathalpha^^MATHEMATICAL BOLD CAPITAL B
1D402^𝐂^\mathbf{C}^\mbfC^A^mathalpha^^MATHEMATICAL BOLD CAPITAL C
1D403^𝐃^\mathbf{D}^\mbfD^A^mathalpha^^MATHEMATICAL BOLD CAPITAL D
1D404^𝐄^\mathbf{E}^\mbfE^A^mathalpha^^MATHEMATICAL BOLD CAPITAL E
1D405^𝐅^\mathbf{F}^\mbfF^A^mathalpha^^MATHEMATICAL BOLD CAPITAL F
1D406^𝐆^\mathbf{G}^\mbfG^A^mathalpha^^MATHEMATICAL BOLD CAPITAL G
1D407^𝐇^\mathbf{H}^\mbfH^A^mathalpha^^MATHEMATICAL BOLD CAPITAL H
1D408^𝐈^\mathbf{I}^\mbfI^A^mathalpha^^MATHEMATICAL BOLD CAPITAL I
1D409^𝐉^\mathbf{J}^\mbfJ^A^mathalpha^^MATHEMATICAL BOLD CAPITAL J
1D40A^𝐊^\mathbf{K}^\mbfK^A^mathalpha^^MATHEMATICAL BOLD CAPITAL K
1D40B^𝐋^\mathbf{L}^\mbfL^A^mathalpha^^MATHEMATICAL BOLD CAPITAL L
1D40C^𝐌^\mathbf{M}^\mbfM^A^mathalpha^^MATHEMATICAL BOLD CAPITAL M
1D40D^𝐍^\mathbf{N}^\mbfN^A^mathalpha^^MATHEMATICAL BOLD CAPITAL N
1D40E^𝐎^\mathbf{O}^\mbfO^A^mathalpha^^MATHEMATICAL BOLD CAPITAL O
1D40F^𝐏^\mathbf{P}^\mbfP^A^mathalpha^^MATHEMATICAL BOLD CAPITAL P
1D410^𝐐^\mathbf{Q}^\mbfQ^A^mathalpha^^MATHEMATICAL BOLD CAPITAL Q
1D411^𝐑^\mathbf{R}^\mbfR^A^mathalpha^^MATHEMATICAL BOLD CAPITAL R
1D412^𝐒^\mathbf{S}^\mbfS^A^mathalpha^^MATHEMATICAL BOLD CAPITAL S
1D413^𝐓^\mathbf{T}^\mbfT^A^mathalpha^^MATHEMATICAL BOLD CAPITAL T
1D414^𝐔^\mathbf{U}^\mbfU^A^mathalpha^^MATHEMATICAL BOLD CAPITAL U
1D415^𝐕^\mathbf{V}^\mbfV^A^mathalpha^^MATHEMATICAL BOLD CAPITAL V
1D416^𝐖^\mathbf{W}^\mbfW^A^mathalpha^^MATHEMATICAL BOLD CAPITAL W
1D417^𝐗^\mathbf{X}^\mbfX^A^mathalpha^^MATHEMATICAL BOLD CAPITAL X
1D418^𝐘^\mathbf{Y}^\mbfY^A^mathalpha^^MATHEMATICAL BOLD CAPITAL Y
1D419^𝐙^\mathbf{Z}^\mbfZ^A^mathalpha^^MATHEMATICAL BOLD CAPITAL Z
1D41A^𝐚^\mathbf{a}^\mbfa^A^mathalpha^^MATHEMATICAL BOLD SMALL A
1D41B^𝐛^\mathbf{b}^\mbfb^A^mathalpha^^MATHEMATICAL BOLD SMALL B
1D41C^𝐜^\mathbf{c}^\mbfc^A^mathalpha^^MATHEMATICAL BOLD SMALL C
1D41D^𝐝^\mathbf{d}^\mbfd^A^mathalpha^^MATHEMATICAL BOLD SMALL D
1D41E^𝐞^\mathbf{e}^\mbfe^A^mathalpha^^MATHEMATICAL BOLD SMALL E
1D41F^𝐟^\mathbf{f}^\mbff^A^mathalpha^^MATHEMATICAL BOLD SMALL F
1D420^𝐠^\mathbf{g}^\mbfg^A^mathalpha^^MATHEMATICAL BOLD SMALL G
1D421^𝐡^\mathbf{h}^\mbfh^A^mathalpha^^MATHEMATICAL BOLD SMALL H
1D422^𝐢^\mathbf{i}^\mbfi^A^mathalpha^^MATHEMATICAL BOLD SMALL I
1D423^𝐣^\mathbf{j}^\mbfj^A^mathalpha^^MATHEMATICAL BOLD SMALL J
1D424^𝐤^\mathbf{k}^\mbfk^A^mathalpha^^MATHEMATICAL BOLD SMALL K
1D425^𝐥^\mathbf{l}^\mbfl^A^mathalpha^^MATHEMATICAL BOLD SMALL L
1D426^𝐦^\mathbf{m}^\mbfm^A^mathalpha^^MATHEMATICAL BOLD SMALL M
1D427^𝐧^\mathbf{n}^\mbfn^A^mathalpha^^MATHEMATICAL BOLD SMALL N
1D428^𝐨^\mathbf{o}^\mbfo^A^mathalpha^^MATHEMATICAL BOLD SMALL O
1D429^𝐩^\mathbf{p}^\mbfp^A^mathalpha^^MATHEMATICAL BOLD SMALL P
1D42A^𝐪^\mathbf{q}^\mbfq^A^mathalpha^^MATHEMATICAL BOLD SMALL Q
1D42B^𝐫^\mathbf{r}^\mbfr^A^mathalpha^^MATHEMATICAL BOLD SMALL R
1D42C^𝐬^\mathbf{s}^\mbfs^A^mathalpha^^MATHEMATICAL BOLD SMALL S
1D42D^𝐭^\mathbf{t}^\mbft^A^mathalpha^^MATHEMATICAL BOLD SMALL T
1D42E^𝐮^\mathbf{u}^\mbfu^A^mathalpha^^MATHEMATICAL BOLD SMALL U
1D42F^𝐯^\mathbf{v}^\mbfv^A^mathalpha^^MATHEMATICAL BOLD SMALL V
1D430^𝐰^\mathbf{w}^\mbfw^A^mathalpha^^MATHEMATICAL BOLD SMALL W
1D431^𝐱^\mathbf{x}^\mbfx^A^mathalpha^^MATHEMATICAL BOLD SMALL X
1D432^𝐲^\mathbf{y}^\mbfy^A^mathalpha^^MATHEMATICAL BOLD SMALL Y
1D433^𝐳^\mathbf{z}^\mbfz^A^mathalpha^^MATHEMATICAL BOLD SMALL Z
1D434^𝐴^A^\mitA^A^mathalpha^-frenchstyle^= \mathit{A}, MATHEMATICAL ITALIC CAPITAL A
1D435^𝐵^B^\mitB^A^mathalpha^-frenchstyle^= \mathit{B}, MATHEMATICAL ITALIC CAPITAL B
1D436^𝐶^C^\mitC^A^mathalpha^-frenchstyle^= \mathit{C}, MATHEMATICAL ITALIC CAPITAL C
1D437^𝐷^D^\mitD^A^mathalpha^-frenchstyle^= \mathit{D}, MATHEMATICAL ITALIC CAPITAL D
1D438^𝐸^E^\mitE^A^mathalpha^-frenchstyle^= \mathit{E}, MATHEMATICAL ITALIC CAPITAL E
1D439^𝐹^F^\mitF^A^mathalpha^-frenchstyle^= \mathit{F}, MATHEMATICAL ITALIC CAPITAL F
1D43A^𝐺^G^\mitG^A^mathalpha^-frenchstyle^= \mathit{G}, MATHEMATICAL ITALIC CAPITAL G
1D43B^𝐻^H^\mitH^A^mathalpha^-frenchstyle^= \mathit{H}, MATHEMATICAL ITALIC CAPITAL H
1D43C^𝐼^I^\mitI^A^mathalpha^-frenchstyle^= \mathit{I}, MATHEMATICAL ITALIC CAPITAL I
1D43D^𝐽^J^\mitJ^A^mathalpha^-frenchstyle^= \mathit{J}, MATHEMATICAL ITALIC CAPITAL J
1D43E^𝐾^K^\mitK^A^mathalpha^-frenchstyle^= \mathit{K}, MATHEMATICAL ITALIC CAPITAL K
1D43F^𝐿^L^\mitL^A^mathalpha^-frenchstyle^= \mathit{L}, MATHEMATICAL ITALIC CAPITAL L
1D440^𝑀^M^\mitM^A^mathalpha^-frenchstyle^= \mathit{M}, MATHEMATICAL ITALIC CAPITAL M
1D441^𝑁^N^\mitN^A^mathalpha^-frenchstyle^= \mathit{N}, MATHEMATICAL ITALIC CAPITAL N
1D442^𝑂^O^\mitO^A^mathalpha^-frenchstyle^= \mathit{O}, MATHEMATICAL ITALIC CAPITAL O
1D443^𝑃^P^\mitP^A^mathalpha^-frenchstyle^= \mathit{P}, MATHEMATICAL ITALIC CAPITAL P
1D444^𝑄^Q^\mitQ^A^mathalpha^-frenchstyle^= \mathit{Q}, MATHEMATICAL ITALIC CAPITAL Q
1D445^𝑅^R^\mitR^A^mathalpha^-frenchstyle^= \mathit{R}, MATHEMATICAL ITALIC CAPITAL R
1D446^𝑆^S^\mitS^A^mathalpha^-frenchstyle^= \mathit{S}, MATHEMATICAL ITALIC CAPITAL S
1D447^𝑇^T^\mitT^A^mathalpha^-frenchstyle^= \mathit{T}, MATHEMATICAL ITALIC CAPITAL T
1D448^𝑈^U^\mitU^A^mathalpha^-frenchstyle^= \mathit{U}, MATHEMATICAL ITALIC CAPITAL U
1D449^𝑉^V^\mitV^A^mathalpha^-frenchstyle^= \mathit{V}, MATHEMATICAL ITALIC CAPITAL V
1D44A^𝑊^W^\mitW^A^mathalpha^-frenchstyle^= \mathit{W}, MATHEMATICAL ITALIC CAPITAL W
1D44B^𝑋^X^\mitX^A^mathalpha^-frenchstyle^= \mathit{X}, MATHEMATICAL ITALIC CAPITAL X
1D44C^𝑌^Y^\mitY^A^mathalpha^-frenchstyle^= \mathit{Y}, MATHEMATICAL ITALIC CAPITAL Y
1D44D^𝑍^Z^\mitZ^A^mathalpha^-frenchstyle^= \mathit{Z}, MATHEMATICAL ITALIC CAPITAL Z
1D44E^𝑎^a^\mita^A^mathalpha^-uprightstyle^= \mathit{a}, MATHEMATICAL ITALIC SMALL A
1D44F^𝑏^b^\mitb^A^mathalpha^-uprightstyle^= \mathit{b}, MATHEMATICAL ITALIC SMALL B
1D450^𝑐^c^\mitc^A^mathalpha^-uprightstyle^= \mathit{c}, MATHEMATICAL ITALIC SMALL C
1D451^𝑑^d^\mitd^A^mathalpha^-uprightstyle^= \mathit{d}, MATHEMATICAL ITALIC SMALL D
1D452^𝑒^e^\mite^A^mathalpha^-uprightstyle^= \mathit{e}, MATHEMATICAL ITALIC SMALL E
1D453^𝑓^f^\mitf^A^mathalpha^-uprightstyle^= \mathit{f}, MATHEMATICAL ITALIC SMALL F
1D454^𝑔^g^\mitg^A^mathalpha^-uprightstyle^= \mathit{g}, MATHEMATICAL ITALIC SMALL G
1D456^𝑖^i^\miti^A^mathalpha^-uprightstyle^= \mathit{i}, MATHEMATICAL ITALIC SMALL I
1D457^𝑗^j^\mitj^A^mathalpha^-uprightstyle^= \mathit{j}, MATHEMATICAL ITALIC SMALL J
1D458^𝑘^k^\mitk^A^mathalpha^-uprightstyle^= \mathit{k}, MATHEMATICAL ITALIC SMALL K
1D459^𝑙^l^\mitl^A^mathalpha^-uprightstyle^= \mathit{l}, MATHEMATICAL ITALIC SMALL L
1D45A^𝑚^m^\mitm^A^mathalpha^-uprightstyle^= \mathit{m}, MATHEMATICAL ITALIC SMALL M
1D45B^𝑛^n^\mitn^A^mathalpha^-uprightstyle^= \mathit{n}, MATHEMATICAL ITALIC SMALL N
1D45C^𝑜^o^\mito^A^mathalpha^-uprightstyle^= \mathit{o}, MATHEMATICAL ITALIC SMALL O
1D45D^𝑝^p^\mitp^A^mathalpha^-uprightstyle^= \mathit{p}, MATHEMATICAL ITALIC SMALL P
1D45E^𝑞^q^\mitq^A^mathalpha^-uprightstyle^= \mathit{q}, MATHEMATICAL ITALIC SMALL Q
1D45F^𝑟^r^\mitr^A^mathalpha^-uprightstyle^= \mathit{r}, MATHEMATICAL ITALIC SMALL R
1D460^𝑠^s^\mits^A^mathalpha^-uprightstyle^= \mathit{s}, MATHEMATICAL ITALIC SMALL S
1D461^𝑡^t^\mitt^A^mathalpha^-uprightstyle^= \mathit{t}, MATHEMATICAL ITALIC SMALL T
1D462^𝑢^u^\mitu^A^mathalpha^-uprightstyle^= \mathit{u}, MATHEMATICAL ITALIC SMALL U
1D463^𝑣^v^\mitv^A^mathalpha^-uprightstyle^= \mathit{v}, MATHEMATICAL ITALIC SMALL V
1D464^𝑤^w^\mitw^A^mathalpha^-uprightstyle^= \mathit{w}, MATHEMATICAL ITALIC SMALL W
1D465^𝑥^x^\mitx^A^mathalpha^-uprightstyle^= \mathit{x}, MATHEMATICAL ITALIC SMALL X
1D466^𝑦^y^\mity^A^mathalpha^-uprightstyle^= \mathit{y}, MATHEMATICAL ITALIC SMALL Y
1D467^𝑧^z^\mitz^A^mathalpha^-uprightstyle^= \mathit{z}, MATHEMATICAL ITALIC SMALL Z
1D468^𝑨^\mathbfit{A}^\mbfitA^A^mathalpha^isomath^= \mathbold{A} (fixmath), MATHEMATICAL BOLD ITALIC CAPITAL A
1D469^𝑩^\mathbfit{B}^\mbfitB^A^mathalpha^isomath^= \mathbold{B} (fixmath), MATHEMATICAL BOLD ITALIC CAPITAL B
1D46A^𝑪^\mathbfit{C}^\mbfitC^A^mathalpha^isomath^= \mathbold{C} (fixmath), MATHEMATICAL BOLD ITALIC CAPITAL C
1D46B^𝑫^\mathbfit{D}^\mbfitD^A^mathalpha^isomath^= \mathbold{D} (fixmath), MATHEMATICAL BOLD ITALIC CAPITAL D
1D46C^𝑬^\mathbfit{E}^\mbfitE^A^mathalpha^isomath^= \mathbold{E} (fixmath), MATHEMATICAL BOLD ITALIC CAPITAL E
1D46D^𝑭^\mathbfit{F}^\mbfitF^A^mathalpha^isomath^= \mathbold{F} (fixmath), MATHEMATICAL BOLD ITALIC CAPITAL F
1D46E^𝑮^\mathbfit{G}^\mbfitG^A^mathalpha^isomath^= \mathbold{G} (fixmath), MATHEMATICAL BOLD ITALIC CAPITAL G
1D46F^𝑯^\mathbfit{H}^\mbfitH^A^mathalpha^isomath^= \mathbold{H} (fixmath), MATHEMATICAL BOLD ITALIC CAPITAL H
1D470^𝑰^\mathbfit{I}^\mbfitI^A^mathalpha^isomath^= \mathbold{I} (fixmath), MATHEMATICAL BOLD ITALIC CAPITAL I
1D471^𝑱^\mathbfit{J}^\mbfitJ^A^mathalpha^isomath^= \mathbold{J} (fixmath), MATHEMATICAL BOLD ITALIC CAPITAL J
1D472^𝑲^\mathbfit{K}^\mbfitK^A^mathalpha^isomath^= \mathbold{K} (fixmath), MATHEMATICAL BOLD ITALIC CAPITAL K
1D473^𝑳^\mathbfit{L}^\mbfitL^A^mathalpha^isomath^= \mathbold{L} (fixmath), MATHEMATICAL BOLD ITALIC CAPITAL L
1D474^𝑴^\mathbfit{M}^\mbfitM^A^mathalpha^isomath^= \mathbold{M} (fixmath), MATHEMATICAL BOLD ITALIC CAPITAL M
1D475^𝑵^\mathbfit{N}^\mbfitN^A^mathalpha^isomath^= \mathbold{N} (fixmath), MATHEMATICAL BOLD ITALIC CAPITAL N
1D476^𝑶^\mathbfit{O}^\mbfitO^A^mathalpha^isomath^= \mathbold{O} (fixmath), MATHEMATICAL BOLD ITALIC CAPITAL O
1D477^𝑷^\mathbfit{P}^\mbfitP^A^mathalpha^isomath^= \mathbold{P} (fixmath), MATHEMATICAL BOLD ITALIC CAPITAL P
1D478^𝑸^\mathbfit{Q}^\mbfitQ^A^mathalpha^isomath^= \mathbold{Q} (fixmath), MATHEMATICAL BOLD ITALIC CAPITAL Q
1D479^𝑹^\mathbfit{R}^\mbfitR^A^mathalpha^isomath^= \mathbold{R} (fixmath), MATHEMATICAL BOLD ITALIC CAPITAL R
1D47A^𝑺^\mathbfit{S}^\mbfitS^A^mathalpha^isomath^= \mathbold{S} (fixmath), MATHEMATICAL BOLD ITALIC CAPITAL S
1D47B^𝑻^\mathbfit{T}^\mbfitT^A^mathalpha^isomath^= \mathbold{T} (fixmath), MATHEMATICAL BOLD ITALIC CAPITAL T
1D47C^𝑼^\mathbfit{U}^\mbfitU^A^mathalpha^isomath^= \mathbold{U} (fixmath), MATHEMATICAL BOLD ITALIC CAPITAL U
1D47D^𝑽^\mathbfit{V}^\mbfitV^A^mathalpha^isomath^= \mathbold{V} (fixmath), MATHEMATICAL BOLD ITALIC CAPITAL V
1D47E^𝑾^\mathbfit{W}^\mbfitW^A^mathalpha^isomath^= \mathbold{W} (fixmath), MATHEMATICAL BOLD ITALIC CAPITAL W
1D47F^𝑿^\mathbfit{X}^\mbfitX^A^mathalpha^isomath^= \mathbold{X} (fixmath), MATHEMATICAL BOLD ITALIC CAPITAL X
1D480^𝒀^\mathbfit{Y}^\mbfitY^A^mathalpha^isomath^= \mathbold{Y} (fixmath), MATHEMATICAL BOLD ITALIC CAPITAL Y
1D481^𝒁^\mathbfit{Z}^\mbfitZ^A^mathalpha^isomath^= \mathbold{Z} (fixmath), MATHEMATICAL BOLD ITALIC CAPITAL Z
1D482^𝒂^\mathbfit{a}^\mbfita^A^mathalpha^isomath^= \mathbold{a} (fixmath), MATHEMATICAL BOLD ITALIC SMALL A
1D483^𝒃^\mathbfit{b}^\mbfitb^A^mathalpha^isomath^= \mathbold{b} (fixmath), MATHEMATICAL BOLD ITALIC SMALL B
1D484^𝒄^\mathbfit{c}^\mbfitc^A^mathalpha^isomath^= \mathbold{c} (fixmath), MATHEMATICAL BOLD ITALIC SMALL C
1D485^𝒅^\mathbfit{d}^\mbfitd^A^mathalpha^isomath^= \mathbold{d} (fixmath), MATHEMATICAL BOLD ITALIC SMALL D
1D486^𝒆^\mathbfit{e}^\mbfite^A^mathalpha^isomath^= \mathbold{e} (fixmath), MATHEMATICAL BOLD ITALIC SMALL E
1D487^𝒇^\mathbfit{f}^\mbfitf^A^mathalpha^isomath^= \mathbold{f} (fixmath), MATHEMATICAL BOLD ITALIC SMALL F
1D488^𝒈^\mathbfit{g}^\mbfitg^A^mathalpha^isomath^= \mathbold{g} (fixmath), MATHEMATICAL BOLD ITALIC SMALL G
1D489^𝒉^\mathbfit{h}^\mbfith^A^mathalpha^isomath^= \mathbold{h} (fixmath), MATHEMATICAL BOLD ITALIC SMALL H
1D48A^𝒊^\mathbfit{i}^\mbfiti^A^mathalpha^isomath^= \mathbold{i} (fixmath), MATHEMATICAL BOLD ITALIC SMALL I
1D48B^𝒋^\mathbfit{j}^\mbfitj^A^mathalpha^isomath^= \mathbold{j} (fixmath), MATHEMATICAL BOLD ITALIC SMALL J
1D48C^𝒌^\mathbfit{k}^\mbfitk^A^mathalpha^isomath^= \mathbold{k} (fixmath), MATHEMATICAL BOLD ITALIC SMALL K
1D48D^𝒍^\mathbfit{l}^\mbfitl^A^mathalpha^isomath^= \mathbold{l} (fixmath), MATHEMATICAL BOLD ITALIC SMALL L
1D48E^𝒎^\mathbfit{m}^\mbfitm^A^mathalpha^isomath^= \mathbold{m} (fixmath), MATHEMATICAL BOLD ITALIC SMALL M
1D48F^𝒏^\mathbfit{n}^\mbfitn^A^mathalpha^isomath^= \mathbold{n} (fixmath), MATHEMATICAL BOLD ITALIC SMALL N
1D490^𝒐^\mathbfit{o}^\mbfito^A^mathalpha^isomath^= \mathbold{o} (fixmath), MATHEMATICAL BOLD ITALIC SMALL O
1D491^𝒑^\mathbfit{p}^\mbfitp^A^mathalpha^isomath^= \mathbold{p} (fixmath), MATHEMATICAL BOLD ITALIC SMALL P
1D492^𝒒^\mathbfit{q}^\mbfitq^A^mathalpha^isomath^= \mathbold{q} (fixmath), MATHEMATICAL BOLD ITALIC SMALL Q
1D493^𝒓^\mathbfit{r}^\mbfitr^A^mathalpha^isomath^= \mathbold{r} (fixmath), MATHEMATICAL BOLD ITALIC SMALL R
1D494^𝒔^\mathbfit{s}^\mbfits^A^mathalpha^isomath^= \mathbold{s} (fixmath), MATHEMATICAL BOLD ITALIC SMALL S
1D495^𝒕^\mathbfit{t}^\mbfitt^A^mathalpha^isomath^= \mathbold{t} (fixmath), MATHEMATICAL BOLD ITALIC SMALL T
1D496^𝒖^\mathbfit{u}^\mbfitu^A^mathalpha^isomath^= \mathbold{u} (fixmath), MATHEMATICAL BOLD ITALIC SMALL U
1D497^𝒗^\mathbfit{v}^\mbfitv^A^mathalpha^isomath^= \mathbold{v} (fixmath), MATHEMATICAL BOLD ITALIC SMALL V
1D498^𝒘^\mathbfit{w}^\mbfitw^A^mathalpha^isomath^= \mathbold{w} (fixmath), MATHEMATICAL BOLD ITALIC SMALL W
1D499^𝒙^\mathbfit{x}^\mbfitx^A^mathalpha^isomath^= \mathbold{x} (fixmath), MATHEMATICAL BOLD ITALIC SMALL X
1D49A^𝒚^\mathbfit{y}^\mbfity^A^mathalpha^isomath^= \mathbold{y} (fixmath), MATHEMATICAL BOLD ITALIC SMALL Y
1D49B^𝒛^\mathbfit{z}^\mbfitz^A^mathalpha^isomath^= \mathbold{z} (fixmath), MATHEMATICAL BOLD ITALIC SMALL Z
1D49C^𝒜^\mathcal{A}^\mscrA^A^mathalpha^^MATHEMATICAL SCRIPT CAPITAL A
1D49E^𝒞^\mathcal{C}^\mscrC^A^mathalpha^^MATHEMATICAL SCRIPT CAPITAL C
1D49F^𝒟^\mathcal{D}^\mscrD^A^mathalpha^^MATHEMATICAL SCRIPT CAPITAL D
1D4A2^𝒢^\mathcal{G}^\mscrG^A^mathalpha^^MATHEMATICAL SCRIPT CAPITAL G
1D4A5^𝒥^\mathcal{J}^\mscrJ^A^mathalpha^^MATHEMATICAL SCRIPT CAPITAL J
1D4A6^𝒦^\mathcal{K}^\mscrK^A^mathalpha^^MATHEMATICAL SCRIPT CAPITAL K
1D4A9^𝒩^\mathcal{N}^\mscrN^A^mathalpha^^MATHEMATICAL SCRIPT CAPITAL N
1D4AA^𝒪^\mathcal{O}^\mscrO^A^mathalpha^^MATHEMATICAL SCRIPT CAPITAL O
1D4AB^𝒫^\mathcal{P}^\mscrP^A^mathalpha^^MATHEMATICAL SCRIPT CAPITAL P
1D4AC^𝒬^\mathcal{Q}^\mscrQ^A^mathalpha^^MATHEMATICAL SCRIPT CAPITAL Q
1D4AE^𝒮^\mathcal{S}^\mscrS^A^mathalpha^^MATHEMATICAL SCRIPT CAPITAL S
1D4AF^𝒯^\mathcal{T}^\mscrT^A^mathalpha^^MATHEMATICAL SCRIPT CAPITAL T
1D4B0^𝒰^\mathcal{U}^\mscrU^A^mathalpha^^MATHEMATICAL SCRIPT CAPITAL U
1D4B1^𝒱^\mathcal{V}^\mscrV^A^mathalpha^^MATHEMATICAL SCRIPT CAPITAL V
1D4B2^𝒲^\mathcal{W}^\mscrW^A^mathalpha^^MATHEMATICAL SCRIPT CAPITAL W
1D4B3^𝒳^\mathcal{X}^\mscrX^A^mathalpha^^MATHEMATICAL SCRIPT CAPITAL X
1D4B4^𝒴^\mathcal{Y}^\mscrY^A^mathalpha^^MATHEMATICAL SCRIPT CAPITAL Y
1D4B5^𝒵^\mathcal{Z}^\mscrZ^A^mathalpha^^MATHEMATICAL SCRIPT CAPITAL Z
1D4B6^𝒶^\mathcal{a}^\mscra^A^mathalpha^urwchancal^MATHEMATICAL SCRIPT SMALL A
1D4B7^𝒷^\mathcal{b}^\mscrb^A^mathalpha^urwchancal^MATHEMATICAL SCRIPT SMALL B
1D4B8^𝒸^\mathcal{c}^\mscrc^A^mathalpha^urwchancal^MATHEMATICAL SCRIPT SMALL C
1D4B9^𝒹^\mathcal{d}^\mscrd^A^mathalpha^urwchancal^MATHEMATICAL SCRIPT SMALL D
1D4BB^𝒻^\mathcal{f}^\mscrf^A^mathalpha^urwchancal^MATHEMATICAL SCRIPT SMALL F
1D4BD^𝒽^\mathcal{h}^\mscrh^A^mathalpha^urwchancal^MATHEMATICAL SCRIPT SMALL H
1D4BE^𝒾^\mathcal{i}^\mscri^A^mathalpha^urwchancal^MATHEMATICAL SCRIPT SMALL I
1D4BF^𝒿^\mathcal{j}^\mscrj^A^mathalpha^urwchancal^MATHEMATICAL SCRIPT SMALL J
1D4C0^𝓀^\mathcal{k}^\mscrk^A^mathalpha^urwchancal^MATHEMATICAL SCRIPT SMALL K
1D4C1^𝓁^\mathcal{l}^\mscrl^A^mathalpha^urwchancal^MATHEMATICAL SCRIPT SMALL L
1D4C2^𝓂^\mathcal{m}^\mscrm^A^mathalpha^urwchancal^MATHEMATICAL SCRIPT SMALL M
1D4C3^𝓃^\mathcal{n}^\mscrn^A^mathalpha^urwchancal^MATHEMATICAL SCRIPT SMALL N
1D4C5^𝓅^\mathcal{p}^\mscrp^A^mathalpha^urwchancal^MATHEMATICAL SCRIPT SMALL P
1D4C6^𝓆^\mathcal{q}^\mscrq^A^mathalpha^urwchancal^MATHEMATICAL SCRIPT SMALL Q
1D4C7^𝓇^\mathcal{r}^\mscrr^A^mathalpha^urwchancal^MATHEMATICAL SCRIPT SMALL R
1D4C8^𝓈^\mathcal{s}^\mscrs^A^mathalpha^urwchancal^MATHEMATICAL SCRIPT SMALL S
1D4C9^𝓉^\mathcal{t}^\mscrt^A^mathalpha^urwchancal^MATHEMATICAL SCRIPT SMALL T
1D4CA^𝓊^\mathcal{u}^\mscru^A^mathalpha^urwchancal^MATHEMATICAL SCRIPT SMALL U
1D4CB^𝓋^\mathcal{v}^\mscrv^A^mathalpha^urwchancal^MATHEMATICAL SCRIPT SMALL V
1D4CC^𝓌^\mathcal{w}^\mscrw^A^mathalpha^urwchancal^MATHEMATICAL SCRIPT SMALL W
1D4CD^𝓍^\mathcal{x}^\mscrx^A^mathalpha^urwchancal^MATHEMATICAL SCRIPT SMALL X
1D4CE^𝓎^\mathcal{y}^\mscry^A^mathalpha^urwchancal^MATHEMATICAL SCRIPT SMALL Y
1D4CF^𝓏^\mathcal{z}^\mscrz^A^mathalpha^urwchancal^MATHEMATICAL SCRIPT SMALL Z
1D4D0^𝓐^^\mbfscrA^A^mathalpha^^MATHEMATICAL BOLD SCRIPT CAPITAL A
1D4D1^𝓑^^\mbfscrB^A^mathalpha^^MATHEMATICAL BOLD SCRIPT CAPITAL B
1D4D2^𝓒^^\mbfscrC^A^mathalpha^^MATHEMATICAL BOLD SCRIPT CAPITAL C
1D4D3^𝓓^^\mbfscrD^A^mathalpha^^MATHEMATICAL BOLD SCRIPT CAPITAL D
1D4D4^𝓔^^\mbfscrE^A^mathalpha^^MATHEMATICAL BOLD SCRIPT CAPITAL E
1D4D5^𝓕^^\mbfscrF^A^mathalpha^^MATHEMATICAL BOLD SCRIPT CAPITAL F
1D4D6^𝓖^^\mbfscrG^A^mathalpha^^MATHEMATICAL BOLD SCRIPT CAPITAL G
1D4D7^𝓗^^\mbfscrH^A^mathalpha^^MATHEMATICAL BOLD SCRIPT CAPITAL H
1D4D8^𝓘^^\mbfscrI^A^mathalpha^^MATHEMATICAL BOLD SCRIPT CAPITAL I
1D4D9^𝓙^^\mbfscrJ^A^mathalpha^^MATHEMATICAL BOLD SCRIPT CAPITAL J
1D4DA^𝓚^^\mbfscrK^A^mathalpha^^MATHEMATICAL BOLD SCRIPT CAPITAL K
1D4DB^𝓛^^\mbfscrL^A^mathalpha^^MATHEMATICAL BOLD SCRIPT CAPITAL L
1D4DC^𝓜^^\mbfscrM^A^mathalpha^^MATHEMATICAL BOLD SCRIPT CAPITAL M
1D4DD^𝓝^^\mbfscrN^A^mathalpha^^MATHEMATICAL BOLD SCRIPT CAPITAL N
1D4DE^𝓞^^\mbfscrO^A^mathalpha^^MATHEMATICAL BOLD SCRIPT CAPITAL O
1D4DF^𝓟^^\mbfscrP^A^mathalpha^^MATHEMATICAL BOLD SCRIPT CAPITAL P
1D4E0^𝓠^^\mbfscrQ^A^mathalpha^^MATHEMATICAL BOLD SCRIPT CAPITAL Q
1D4E1^𝓡^^\mbfscrR^A^mathalpha^^MATHEMATICAL BOLD SCRIPT CAPITAL R
1D4E2^𝓢^^\mbfscrS^A^mathalpha^^MATHEMATICAL BOLD SCRIPT CAPITAL S
1D4E3^𝓣^^\mbfscrT^A^mathalpha^^MATHEMATICAL BOLD SCRIPT CAPITAL T
1D4E4^𝓤^^\mbfscrU^A^mathalpha^^MATHEMATICAL BOLD SCRIPT CAPITAL U
1D4E5^𝓥^^\mbfscrV^A^mathalpha^^MATHEMATICAL BOLD SCRIPT CAPITAL V
1D4E6^𝓦^^\mbfscrW^A^mathalpha^^MATHEMATICAL BOLD SCRIPT CAPITAL W
1D4E7^𝓧^^\mbfscrX^A^mathalpha^^MATHEMATICAL BOLD SCRIPT CAPITAL X
1D4E8^𝓨^^\mbfscrY^A^mathalpha^^MATHEMATICAL BOLD SCRIPT CAPITAL Y
1D4E9^𝓩^^\mbfscrZ^A^mathalpha^^MATHEMATICAL BOLD SCRIPT CAPITAL Z
1D4EA^𝓪^^\mbfscra^A^mathalpha^^MATHEMATICAL BOLD SCRIPT SMALL A
1D4EB^𝓫^^\mbfscrb^A^mathalpha^^MATHEMATICAL BOLD SCRIPT SMALL B
1D4EC^𝓬^^\mbfscrc^A^mathalpha^^MATHEMATICAL BOLD SCRIPT SMALL C
1D4ED^𝓭^^\mbfscrd^A^mathalpha^^MATHEMATICAL BOLD SCRIPT SMALL D
1D4EE^𝓮^^\mbfscre^A^mathalpha^^MATHEMATICAL BOLD SCRIPT SMALL E
1D4EF^𝓯^^\mbfscrf^A^mathalpha^^MATHEMATICAL BOLD SCRIPT SMALL F
1D4F0^𝓰^^\mbfscrg^A^mathalpha^^MATHEMATICAL BOLD SCRIPT SMALL G
1D4F1^𝓱^^\mbfscrh^A^mathalpha^^MATHEMATICAL BOLD SCRIPT SMALL H
1D4F2^𝓲^^\mbfscri^A^mathalpha^^MATHEMATICAL BOLD SCRIPT SMALL I
1D4F3^𝓳^^\mbfscrj^A^mathalpha^^MATHEMATICAL BOLD SCRIPT SMALL J
1D4F4^𝓴^^\mbfscrk^A^mathalpha^^MATHEMATICAL BOLD SCRIPT SMALL K
1D4F5^𝓵^^\mbfscrl^A^mathalpha^^MATHEMATICAL BOLD SCRIPT SMALL L
1D4F6^𝓶^^\mbfscrm^A^mathalpha^^MATHEMATICAL BOLD SCRIPT SMALL M
1D4F7^𝓷^^\mbfscrn^A^mathalpha^^MATHEMATICAL BOLD SCRIPT SMALL N
1D4F8^𝓸^^\mbfscro^A^mathalpha^^MATHEMATICAL BOLD SCRIPT SMALL O
1D4F9^𝓹^^\mbfscrp^A^mathalpha^^MATHEMATICAL BOLD SCRIPT SMALL P
1D4FA^𝓺^^\mbfscrq^A^mathalpha^^MATHEMATICAL BOLD SCRIPT SMALL Q
1D4FB^𝓻^^\mbfscrr^A^mathalpha^^MATHEMATICAL BOLD SCRIPT SMALL R
1D4FC^𝓼^^\mbfscrs^A^mathalpha^^MATHEMATICAL BOLD SCRIPT SMALL S
1D4FD^𝓽^^\mbfscrt^A^mathalpha^^MATHEMATICAL BOLD SCRIPT SMALL T
1D4FE^𝓾^^\mbfscru^A^mathalpha^^MATHEMATICAL BOLD SCRIPT SMALL U
1D4FF^𝓿^^\mbfscrv^A^mathalpha^^MATHEMATICAL BOLD SCRIPT SMALL V
1D500^𝔀^^\mbfscrw^A^mathalpha^^MATHEMATICAL BOLD SCRIPT SMALL W
1D501^𝔁^^\mbfscrx^A^mathalpha^^MATHEMATICAL BOLD SCRIPT SMALL X
1D502^𝔂^^\mbfscry^A^mathalpha^^MATHEMATICAL BOLD SCRIPT SMALL Y
1D503^𝔃^^\mbfscrz^A^mathalpha^^MATHEMATICAL BOLD SCRIPT SMALL Z
1D504^𝔄^\mathfrak{A}^\mfrakA^A^mathalpha^eufrak^MATHEMATICAL FRAKTUR CAPITAL A
1D505^𝔅^\mathfrak{B}^\mfrakB^A^mathalpha^eufrak^MATHEMATICAL FRAKTUR CAPITAL B
1D507^𝔇^\mathfrak{D}^\mfrakD^A^mathalpha^eufrak^MATHEMATICAL FRAKTUR CAPITAL D
1D508^𝔈^\mathfrak{E}^\mfrakE^A^mathalpha^eufrak^MATHEMATICAL FRAKTUR CAPITAL E
1D509^𝔉^\mathfrak{F}^\mfrakF^A^mathalpha^eufrak^MATHEMATICAL FRAKTUR CAPITAL F
1D50A^𝔊^\mathfrak{G}^\mfrakG^A^mathalpha^eufrak^MATHEMATICAL FRAKTUR CAPITAL G
1D50D^𝔍^\mathfrak{J}^\mfrakJ^A^mathalpha^eufrak^MATHEMATICAL FRAKTUR CAPITAL J
1D50E^𝔎^\mathfrak{K}^\mfrakK^A^mathalpha^eufrak^MATHEMATICAL FRAKTUR CAPITAL K
1D50F^𝔏^\mathfrak{L}^\mfrakL^A^mathalpha^eufrak^MATHEMATICAL FRAKTUR CAPITAL L
1D510^𝔐^\mathfrak{M}^\mfrakM^A^mathalpha^eufrak^MATHEMATICAL FRAKTUR CAPITAL M
1D511^𝔑^\mathfrak{N}^\mfrakN^A^mathalpha^eufrak^MATHEMATICAL FRAKTUR CAPITAL N
1D512^𝔒^\mathfrak{O}^\mfrakO^A^mathalpha^eufrak^MATHEMATICAL FRAKTUR CAPITAL O
1D513^𝔓^\mathfrak{P}^\mfrakP^A^mathalpha^eufrak^MATHEMATICAL FRAKTUR CAPITAL P
1D514^𝔔^\mathfrak{Q}^\mfrakQ^A^mathalpha^eufrak^MATHEMATICAL FRAKTUR CAPITAL Q
1D516^𝔖^\mathfrak{S}^\mfrakS^A^mathalpha^eufrak^MATHEMATICAL FRAKTUR CAPITAL S
1D517^𝔗^\mathfrak{T}^\mfrakT^A^mathalpha^eufrak^MATHEMATICAL FRAKTUR CAPITAL T
1D518^𝔘^\mathfrak{U}^\mfrakU^A^mathalpha^eufrak^MATHEMATICAL FRAKTUR CAPITAL U
1D519^𝔙^\mathfrak{V}^\mfrakV^A^mathalpha^eufrak^MATHEMATICAL FRAKTUR CAPITAL V
1D51A^𝔚^\mathfrak{W}^\mfrakW^A^mathalpha^eufrak^MATHEMATICAL FRAKTUR CAPITAL W
1D51B^𝔛^\mathfrak{X}^\mfrakX^A^mathalpha^eufrak^MATHEMATICAL FRAKTUR CAPITAL X
1D51C^𝔜^\mathfrak{Y}^\mfrakY^A^mathalpha^eufrak^MATHEMATICAL FRAKTUR CAPITAL Y
1D51E^𝔞^\mathfrak{a}^\mfraka^A^mathalpha^eufrak^MATHEMATICAL FRAKTUR SMALL A
1D51F^𝔟^\mathfrak{b}^\mfrakb^A^mathalpha^eufrak^MATHEMATICAL FRAKTUR SMALL B
1D520^𝔠^\mathfrak{c}^\mfrakc^A^mathalpha^eufrak^MATHEMATICAL FRAKTUR SMALL C
1D521^𝔡^\mathfrak{d}^\mfrakd^A^mathalpha^eufrak^MATHEMATICAL FRAKTUR SMALL D
1D522^𝔢^\mathfrak{e}^\mfrake^A^mathalpha^eufrak^MATHEMATICAL FRAKTUR SMALL E
1D523^𝔣^\mathfrak{f}^\mfrakf^A^mathalpha^eufrak^MATHEMATICAL FRAKTUR SMALL F
1D524^𝔤^\mathfrak{g}^\mfrakg^A^mathalpha^eufrak^MATHEMATICAL FRAKTUR SMALL G
1D525^𝔥^\mathfrak{h}^\mfrakh^A^mathalpha^eufrak^MATHEMATICAL FRAKTUR SMALL H
1D526^𝔦^\mathfrak{i}^\mfraki^A^mathalpha^eufrak^MATHEMATICAL FRAKTUR SMALL I
1D527^𝔧^\mathfrak{j}^\mfrakj^A^mathalpha^eufrak^MATHEMATICAL FRAKTUR SMALL J
1D528^𝔨^\mathfrak{k}^\mfrakk^A^mathalpha^eufrak^MATHEMATICAL FRAKTUR SMALL K
1D529^𝔩^\mathfrak{l}^\mfrakl^A^mathalpha^eufrak^MATHEMATICAL FRAKTUR SMALL L
1D52A^𝔪^\mathfrak{m}^\mfrakm^A^mathalpha^eufrak^MATHEMATICAL FRAKTUR SMALL M
1D52B^𝔫^\mathfrak{n}^\mfrakn^A^mathalpha^eufrak^MATHEMATICAL FRAKTUR SMALL N
1D52C^𝔬^\mathfrak{o}^\mfrako^A^mathalpha^eufrak^MATHEMATICAL FRAKTUR SMALL O
1D52D^𝔭^\mathfrak{p}^\mfrakp^A^mathalpha^eufrak^MATHEMATICAL FRAKTUR SMALL P
1D52E^𝔮^\mathfrak{q}^\mfrakq^A^mathalpha^eufrak^MATHEMATICAL FRAKTUR SMALL Q
1D52F^𝔯^\mathfrak{r}^\mfrakr^A^mathalpha^eufrak^MATHEMATICAL FRAKTUR SMALL R
1D530^𝔰^\mathfrak{s}^\mfraks^A^mathalpha^eufrak^MATHEMATICAL FRAKTUR SMALL S
1D531^𝔱^\mathfrak{t}^\mfrakt^A^mathalpha^eufrak^MATHEMATICAL FRAKTUR SMALL T
1D532^𝔲^\mathfrak{u}^\mfraku^A^mathalpha^eufrak^MATHEMATICAL FRAKTUR SMALL U
1D533^𝔳^\mathfrak{v}^\mfrakv^A^mathalpha^eufrak^MATHEMATICAL FRAKTUR SMALL V
1D534^𝔴^\mathfrak{w}^\mfrakw^A^mathalpha^eufrak^MATHEMATICAL FRAKTUR SMALL W
1D535^𝔵^\mathfrak{x}^\mfrakx^A^mathalpha^eufrak^MATHEMATICAL FRAKTUR SMALL X
1D536^𝔶^\mathfrak{y}^\mfraky^A^mathalpha^eufrak^MATHEMATICAL FRAKTUR SMALL Y
1D537^𝔷^\mathfrak{z}^\mfrakz^A^mathalpha^eufrak^MATHEMATICAL FRAKTUR SMALL Z
1D538^𝔸^\mathbb{A}^\BbbA^A^mathalpha^mathbb^= \mathds{A} (dsfont), MATHEMATICAL DOUBLE-STRUCK CAPITAL A
1D539^𝔹^\mathbb{B}^\BbbB^A^mathalpha^mathbb^= \mathds{B} (dsfont), matMATHEMATICAL DOUBLE-STRUCK CAPITAL B
1D53B^𝔻^\mathbb{D}^\BbbD^A^mathalpha^mathbb^= \mathds{D} (dsfont), matMATHEMATICAL DOUBLE-STRUCK CAPITAL D
1D53C^𝔼^\mathbb{E}^\BbbE^A^mathalpha^mathbb^= \mathds{E} (dsfont), matMATHEMATICAL DOUBLE-STRUCK CAPITAL E
1D53D^𝔽^\mathbb{F}^\BbbF^A^mathalpha^mathbb^= \mathds{F} (dsfont), matMATHEMATICAL DOUBLE-STRUCK CAPITAL F
1D53E^𝔾^\mathbb{G}^\BbbG^A^mathalpha^mathbb^= \mathds{G} (dsfont), matMATHEMATICAL DOUBLE-STRUCK CAPITAL G
1D540^𝕀^\mathbb{I}^\BbbI^A^mathalpha^mathbb^= \mathds{I} (dsfont), matMATHEMATICAL DOUBLE-STRUCK CAPITAL I
1D541^𝕁^\mathbb{J}^\BbbJ^A^mathalpha^mathbb^= \mathds{J} (dsfont), matMATHEMATICAL DOUBLE-STRUCK CAPITAL J
1D542^𝕂^\mathbb{K}^\BbbK^A^mathalpha^mathbb^= \mathds{K} (dsfont), matMATHEMATICAL DOUBLE-STRUCK CAPITAL K
1D543^𝕃^\mathbb{L}^\BbbL^A^mathalpha^mathbb^= \mathds{L} (dsfont), matMATHEMATICAL DOUBLE-STRUCK CAPITAL L
1D544^𝕄^\mathbb{M}^\BbbM^A^mathalpha^mathbb^= \mathds{M} (dsfont), matMATHEMATICAL DOUBLE-STRUCK CAPITAL M
1D546^𝕆^\mathbb{O}^\BbbO^A^mathalpha^mathbb^= \mathds{O} (dsfont), matMATHEMATICAL DOUBLE-STRUCK CAPITAL O
1D54A^𝕊^\mathbb{S}^\BbbS^A^mathalpha^mathbb^= \mathds{S} (dsfont), matMATHEMATICAL DOUBLE-STRUCK CAPITAL S
1D54B^𝕋^\mathbb{T}^\BbbT^A^mathalpha^mathbb^= \mathds{T} (dsfont), matMATHEMATICAL DOUBLE-STRUCK CAPITAL T
1D54C^𝕌^\mathbb{U}^\BbbU^A^mathalpha^mathbb^= \mathds{U} (dsfont), matMATHEMATICAL DOUBLE-STRUCK CAPITAL U
1D54D^𝕍^\mathbb{V}^\BbbV^A^mathalpha^mathbb^= \mathds{V} (dsfont), matMATHEMATICAL DOUBLE-STRUCK CAPITAL V
1D54E^𝕎^\mathbb{W}^\BbbW^A^mathalpha^mathbb^= \mathds{W} (dsfont), matMATHEMATICAL DOUBLE-STRUCK CAPITAL W
1D54F^𝕏^\mathbb{X}^\BbbX^A^mathalpha^mathbb^= \mathds{X} (dsfont), matMATHEMATICAL DOUBLE-STRUCK CAPITAL X
1D550^𝕐^\mathbb{Y}^\BbbY^A^mathalpha^mathbb^= \mathds{Y} (dsfont), matMATHEMATICAL DOUBLE-STRUCK CAPITAL Y
1D552^𝕒^\mathbb{a}^\Bbba^A^mathalpha^bbold^MATHEMATICAL DOUBLE-STRUCK SMALL A
1D553^𝕓^\mathbb{b}^\Bbbb^A^mathalpha^bbold^MATHEMATICAL DOUBLE-STRUCK SMALL B
1D554^𝕔^\mathbb{c}^\Bbbc^A^mathalpha^bbold^MATHEMATICAL DOUBLE-STRUCK SMALL C
1D555^𝕕^\mathbb{d}^\Bbbd^A^mathalpha^bbold^MATHEMATICAL DOUBLE-STRUCK SMALL D
1D556^𝕖^\mathbb{e}^\Bbbe^A^mathalpha^bbold^MATHEMATICAL DOUBLE-STRUCK SMALL E
1D557^𝕗^\mathbb{f}^\Bbbf^A^mathalpha^bbold^MATHEMATICAL DOUBLE-STRUCK SMALL F
1D558^𝕘^\mathbb{g}^\Bbbg^A^mathalpha^bbold^MATHEMATICAL DOUBLE-STRUCK SMALL G
1D559^𝕙^\mathbb{h}^\Bbbh^A^mathalpha^bbold^MATHEMATICAL DOUBLE-STRUCK SMALL H
1D55A^𝕚^\mathbb{i}^\Bbbi^A^mathalpha^bbold^MATHEMATICAL DOUBLE-STRUCK SMALL I
1D55B^𝕛^\mathbb{j}^\Bbbj^A^mathalpha^bbold^MATHEMATICAL DOUBLE-STRUCK SMALL J
1D55C^𝕜^\mathbb{k}^\Bbbk^A^mathalpha^bbold fourier^= \Bbbk (amssymb), MATHEMATICAL DOUBLE-STRUCK SMALL K
1D55D^𝕝^\mathbb{l}^\Bbbl^A^mathalpha^bbold^MATHEMATICAL DOUBLE-STRUCK SMALL L
1D55E^𝕞^\mathbb{m}^\Bbbm^A^mathalpha^bbold^MATHEMATICAL DOUBLE-STRUCK SMALL M
1D55F^𝕟^\mathbb{n}^\Bbbn^A^mathalpha^bbold^MATHEMATICAL DOUBLE-STRUCK SMALL N
1D560^𝕠^\mathbb{o}^\Bbbo^A^mathalpha^bbold^MATHEMATICAL DOUBLE-STRUCK SMALL O
1D561^𝕡^\mathbb{p}^\Bbbp^A^mathalpha^bbold^MATHEMATICAL DOUBLE-STRUCK SMALL P
1D562^𝕢^\mathbb{q}^\Bbbq^A^mathalpha^bbold^MATHEMATICAL DOUBLE-STRUCK SMALL Q
1D563^𝕣^\mathbb{r}^\Bbbr^A^mathalpha^bbold^MATHEMATICAL DOUBLE-STRUCK SMALL R
1D564^𝕤^\mathbb{s}^\Bbbs^A^mathalpha^bbold^MATHEMATICAL DOUBLE-STRUCK SMALL S
1D565^𝕥^\mathbb{t}^\Bbbt^A^mathalpha^bbold^MATHEMATICAL DOUBLE-STRUCK SMALL T
1D566^𝕦^\mathbb{u}^\Bbbu^A^mathalpha^bbold^MATHEMATICAL DOUBLE-STRUCK SMALL U
1D567^𝕧^\mathbb{v}^\Bbbv^A^mathalpha^bbold^MATHEMATICAL DOUBLE-STRUCK SMALL V
1D568^𝕨^\mathbb{w}^\Bbbw^A^mathalpha^bbold^MATHEMATICAL DOUBLE-STRUCK SMALL W
1D569^𝕩^\mathbb{x}^\Bbbx^A^mathalpha^bbold^MATHEMATICAL DOUBLE-STRUCK SMALL X
1D56A^𝕪^\mathbb{y}^\Bbby^A^mathalpha^bbold^MATHEMATICAL DOUBLE-STRUCK SMALL Y
1D56B^𝕫^\mathbb{z}^\Bbbz^A^mathalpha^bbold^MATHEMATICAL DOUBLE-STRUCK SMALL Z
1D56C^𝕬^^\mbffrakA^A^mathalpha^^MATHEMATICAL BOLD FRAKTUR CAPITAL A
1D56D^𝕭^^\mbffrakB^A^mathalpha^^MATHEMATICAL BOLD FRAKTUR CAPITAL B
1D56E^𝕮^^\mbffrakC^A^mathalpha^^MATHEMATICAL BOLD FRAKTUR CAPITAL C
1D56F^𝕯^^\mbffrakD^A^mathalpha^^MATHEMATICAL BOLD FRAKTUR CAPITAL D
1D570^𝕰^^\mbffrakE^A^mathalpha^^MATHEMATICAL BOLD FRAKTUR CAPITAL E
1D571^𝕱^^\mbffrakF^A^mathalpha^^MATHEMATICAL BOLD FRAKTUR CAPITAL F
1D572^𝕲^^\mbffrakG^A^mathalpha^^MATHEMATICAL BOLD FRAKTUR CAPITAL G
1D573^𝕳^^\mbffrakH^A^mathalpha^^MATHEMATICAL BOLD FRAKTUR CAPITAL H
1D574^𝕴^^\mbffrakI^A^mathalpha^^MATHEMATICAL BOLD FRAKTUR CAPITAL I
1D575^𝕵^^\mbffrakJ^A^mathalpha^^MATHEMATICAL BOLD FRAKTUR CAPITAL J
1D576^𝕶^^\mbffrakK^A^mathalpha^^MATHEMATICAL BOLD FRAKTUR CAPITAL K
1D577^𝕷^^\mbffrakL^A^mathalpha^^MATHEMATICAL BOLD FRAKTUR CAPITAL L
1D578^𝕸^^\mbffrakM^A^mathalpha^^MATHEMATICAL BOLD FRAKTUR CAPITAL M
1D579^𝕹^^\mbffrakN^A^mathalpha^^MATHEMATICAL BOLD FRAKTUR CAPITAL N
1D57A^𝕺^^\mbffrakO^A^mathalpha^^MATHEMATICAL BOLD FRAKTUR CAPITAL O
1D57B^𝕻^^\mbffrakP^A^mathalpha^^MATHEMATICAL BOLD FRAKTUR CAPITAL P
1D57C^𝕼^^\mbffrakQ^A^mathalpha^^MATHEMATICAL BOLD FRAKTUR CAPITAL Q
1D57D^𝕽^^\mbffrakR^A^mathalpha^^MATHEMATICAL BOLD FRAKTUR CAPITAL R
1D57E^𝕾^^\mbffrakS^A^mathalpha^^MATHEMATICAL BOLD FRAKTUR CAPITAL S
1D57F^𝕿^^\mbffrakT^A^mathalpha^^MATHEMATICAL BOLD FRAKTUR CAPITAL T
1D580^𝖀^^\mbffrakU^A^mathalpha^^MATHEMATICAL BOLD FRAKTUR CAPITAL U
1D581^𝖁^^\mbffrakV^A^mathalpha^^MATHEMATICAL BOLD FRAKTUR CAPITAL V
1D582^𝖂^^\mbffrakW^A^mathalpha^^MATHEMATICAL BOLD FRAKTUR CAPITAL W
1D583^𝖃^^\mbffrakX^A^mathalpha^^MATHEMATICAL BOLD FRAKTUR CAPITAL X
1D584^𝖄^^\mbffrakY^A^mathalpha^^MATHEMATICAL BOLD FRAKTUR CAPITAL Y
1D585^𝖅^^\mbffrakZ^A^mathalpha^^MATHEMATICAL BOLD FRAKTUR CAPITAL Z
1D586^𝖆^^\mbffraka^A^mathalpha^^MATHEMATICAL BOLD FRAKTUR SMALL A
1D587^𝖇^^\mbffrakb^A^mathalpha^^MATHEMATICAL BOLD FRAKTUR SMALL B
1D588^𝖈^^\mbffrakc^A^mathalpha^^MATHEMATICAL BOLD FRAKTUR SMALL C
1D589^𝖉^^\mbffrakd^A^mathalpha^^MATHEMATICAL BOLD FRAKTUR SMALL D
1D58A^𝖊^^\mbffrake^A^mathalpha^^MATHEMATICAL BOLD FRAKTUR SMALL E
1D58B^𝖋^^\mbffrakf^A^mathalpha^^MATHEMATICAL BOLD FRAKTUR SMALL F
1D58C^𝖌^^\mbffrakg^A^mathalpha^^MATHEMATICAL BOLD FRAKTUR SMALL G
1D58D^𝖍^^\mbffrakh^A^mathalpha^^MATHEMATICAL BOLD FRAKTUR SMALL H
1D58E^𝖎^^\mbffraki^A^mathalpha^^MATHEMATICAL BOLD FRAKTUR SMALL I
1D58F^𝖏^^\mbffrakj^A^mathalpha^^MATHEMATICAL BOLD FRAKTUR SMALL J
1D590^𝖐^^\mbffrakk^A^mathalpha^^MATHEMATICAL BOLD FRAKTUR SMALL K
1D591^𝖑^^\mbffrakl^A^mathalpha^^MATHEMATICAL BOLD FRAKTUR SMALL L
1D592^𝖒^^\mbffrakm^A^mathalpha^^MATHEMATICAL BOLD FRAKTUR SMALL M
1D593^𝖓^^\mbffrakn^A^mathalpha^^MATHEMATICAL BOLD FRAKTUR SMALL N
1D594^𝖔^^\mbffrako^A^mathalpha^^MATHEMATICAL BOLD FRAKTUR SMALL O
1D595^𝖕^^\mbffrakp^A^mathalpha^^MATHEMATICAL BOLD FRAKTUR SMALL P
1D596^𝖖^^\mbffrakq^A^mathalpha^^MATHEMATICAL BOLD FRAKTUR SMALL Q
1D597^𝖗^^\mbffrakr^A^mathalpha^^MATHEMATICAL BOLD FRAKTUR SMALL R
1D598^𝖘^^\mbffraks^A^mathalpha^^MATHEMATICAL BOLD FRAKTUR SMALL S
1D599^𝖙^^\mbffrakt^A^mathalpha^^MATHEMATICAL BOLD FRAKTUR SMALL T
1D59A^𝖚^^\mbffraku^A^mathalpha^^MATHEMATICAL BOLD FRAKTUR SMALL U
1D59B^𝖛^^\mbffrakv^A^mathalpha^^MATHEMATICAL BOLD FRAKTUR SMALL V
1D59C^𝖜^^\mbffrakw^A^mathalpha^^MATHEMATICAL BOLD FRAKTUR SMALL W
1D59D^𝖝^^\mbffrakx^A^mathalpha^^MATHEMATICAL BOLD FRAKTUR SMALL X
1D59E^𝖞^^\mbffraky^A^mathalpha^^MATHEMATICAL BOLD FRAKTUR SMALL Y
1D59F^𝖟^^\mbffrakz^A^mathalpha^^MATHEMATICAL BOLD FRAKTUR SMALL Z
1D5A0^𝖠^\mathsf{A}^\msansA^A^mathalpha^^MATHEMATICAL SANS-SERIF CAPITAL A
1D5A1^𝖡^\mathsf{B}^\msansB^A^mathalpha^^MATHEMATICAL SANS-SERIF CAPITAL B
1D5A2^𝖢^\mathsf{C}^\msansC^A^mathalpha^^MATHEMATICAL SANS-SERIF CAPITAL C
1D5A3^𝖣^\mathsf{D}^\msansD^A^mathalpha^^MATHEMATICAL SANS-SERIF CAPITAL D
1D5A4^𝖤^\mathsf{E}^\msansE^A^mathalpha^^MATHEMATICAL SANS-SERIF CAPITAL E
1D5A5^𝖥^\mathsf{F}^\msansF^A^mathalpha^^MATHEMATICAL SANS-SERIF CAPITAL F
1D5A6^𝖦^\mathsf{G}^\msansG^A^mathalpha^^MATHEMATICAL SANS-SERIF CAPITAL G
1D5A7^𝖧^\mathsf{H}^\msansH^A^mathalpha^^MATHEMATICAL SANS-SERIF CAPITAL H
1D5A8^𝖨^\mathsf{I}^\msansI^A^mathalpha^^MATHEMATICAL SANS-SERIF CAPITAL I
1D5A9^𝖩^\mathsf{J}^\msansJ^A^mathalpha^^MATHEMATICAL SANS-SERIF CAPITAL J
1D5AA^𝖪^\mathsf{K}^\msansK^A^mathalpha^^MATHEMATICAL SANS-SERIF CAPITAL K
1D5AB^𝖫^\mathsf{L}^\msansL^A^mathalpha^^MATHEMATICAL SANS-SERIF CAPITAL L
1D5AC^𝖬^\mathsf{M}^\msansM^A^mathalpha^^MATHEMATICAL SANS-SERIF CAPITAL M
1D5AD^𝖭^\mathsf{N}^\msansN^A^mathalpha^^MATHEMATICAL SANS-SERIF CAPITAL N
1D5AE^𝖮^\mathsf{O}^\msansO^A^mathalpha^^MATHEMATICAL SANS-SERIF CAPITAL O
1D5AF^𝖯^\mathsf{P}^\msansP^A^mathalpha^^MATHEMATICAL SANS-SERIF CAPITAL P
1D5B0^𝖰^\mathsf{Q}^\msansQ^A^mathalpha^^MATHEMATICAL SANS-SERIF CAPITAL Q
1D5B1^𝖱^\mathsf{R}^\msansR^A^mathalpha^^MATHEMATICAL SANS-SERIF CAPITAL R
1D5B2^𝖲^\mathsf{S}^\msansS^A^mathalpha^^MATHEMATICAL SANS-SERIF CAPITAL S
1D5B3^𝖳^\mathsf{T}^\msansT^A^mathalpha^^MATHEMATICAL SANS-SERIF CAPITAL T
1D5B4^𝖴^\mathsf{U}^\msansU^A^mathalpha^^MATHEMATICAL SANS-SERIF CAPITAL U
1D5B5^𝖵^\mathsf{V}^\msansV^A^mathalpha^^MATHEMATICAL SANS-SERIF CAPITAL V
1D5B6^𝖶^\mathsf{W}^\msansW^A^mathalpha^^MATHEMATICAL SANS-SERIF CAPITAL W
1D5B7^𝖷^\mathsf{X}^\msansX^A^mathalpha^^MATHEMATICAL SANS-SERIF CAPITAL X
1D5B8^𝖸^\mathsf{Y}^\msansY^A^mathalpha^^MATHEMATICAL SANS-SERIF CAPITAL Y
1D5B9^𝖹^\mathsf{Z}^\msansZ^A^mathalpha^^MATHEMATICAL SANS-SERIF CAPITAL Z
1D5BA^𝖺^\mathsf{a}^\msansa^A^mathalpha^^MATHEMATICAL SANS-SERIF SMALL A
1D5BB^𝖻^\mathsf{b}^\msansb^A^mathalpha^^MATHEMATICAL SANS-SERIF SMALL B
1D5BC^𝖼^\mathsf{c}^\msansc^A^mathalpha^^MATHEMATICAL SANS-SERIF SMALL C
1D5BD^𝖽^\mathsf{d}^\msansd^A^mathalpha^^MATHEMATICAL SANS-SERIF SMALL D
1D5BE^𝖾^\mathsf{e}^\msanse^A^mathalpha^^MATHEMATICAL SANS-SERIF SMALL E
1D5BF^𝖿^\mathsf{f}^\msansf^A^mathalpha^^MATHEMATICAL SANS-SERIF SMALL F
1D5C0^𝗀^\mathsf{g}^\msansg^A^mathalpha^^MATHEMATICAL SANS-SERIF SMALL G
1D5C1^𝗁^\mathsf{h}^\msansh^A^mathalpha^^MATHEMATICAL SANS-SERIF SMALL H
1D5C2^𝗂^\mathsf{i}^\msansi^A^mathalpha^^MATHEMATICAL SANS-SERIF SMALL I
1D5C3^𝗃^\mathsf{j}^\msansj^A^mathalpha^^MATHEMATICAL SANS-SERIF SMALL J
1D5C4^𝗄^\mathsf{k}^\msansk^A^mathalpha^^MATHEMATICAL SANS-SERIF SMALL K
1D5C5^𝗅^\mathsf{l}^\msansl^A^mathalpha^^MATHEMATICAL SANS-SERIF SMALL L
1D5C6^𝗆^\mathsf{m}^\msansm^A^mathalpha^^MATHEMATICAL SANS-SERIF SMALL M
1D5C7^𝗇^\mathsf{n}^\msansn^A^mathalpha^^MATHEMATICAL SANS-SERIF SMALL N
1D5C8^𝗈^\mathsf{o}^\msanso^A^mathalpha^^MATHEMATICAL SANS-SERIF SMALL O
1D5C9^𝗉^\mathsf{p}^\msansp^A^mathalpha^^MATHEMATICAL SANS-SERIF SMALL P
1D5CA^𝗊^\mathsf{q}^\msansq^A^mathalpha^^MATHEMATICAL SANS-SERIF SMALL Q
1D5CB^𝗋^\mathsf{r}^\msansr^A^mathalpha^^MATHEMATICAL SANS-SERIF SMALL R
1D5CC^𝗌^\mathsf{s}^\msanss^A^mathalpha^^MATHEMATICAL SANS-SERIF SMALL S
1D5CD^𝗍^\mathsf{t}^\msanst^A^mathalpha^^MATHEMATICAL SANS-SERIF SMALL T
1D5CE^𝗎^\mathsf{u}^\msansu^A^mathalpha^^MATHEMATICAL SANS-SERIF SMALL U
1D5CF^𝗏^\mathsf{v}^\msansv^A^mathalpha^^MATHEMATICAL SANS-SERIF SMALL V
1D5D0^𝗐^\mathsf{w}^\msansw^A^mathalpha^^MATHEMATICAL SANS-SERIF SMALL W
1D5D1^𝗑^\mathsf{x}^\msansx^A^mathalpha^^MATHEMATICAL SANS-SERIF SMALL X
1D5D2^𝗒^\mathsf{y}^\msansy^A^mathalpha^^MATHEMATICAL SANS-SERIF SMALL Y
1D5D3^𝗓^\mathsf{z}^\msansz^A^mathalpha^^MATHEMATICAL SANS-SERIF SMALL Z
1D5D4^𝗔^\mathsfbf{A}^\mbfsansA^A^mathalpha^mathsfbf^MATHEMATICAL SANS-SERIF BOLD CAPITAL A
1D5D5^𝗕^\mathsfbf{B}^\mbfsansB^A^mathalpha^mathsfbf^MATHEMATICAL SANS-SERIF BOLD CAPITAL B
1D5D6^𝗖^\mathsfbf{C}^\mbfsansC^A^mathalpha^mathsfbf^MATHEMATICAL SANS-SERIF BOLD CAPITAL C
1D5D7^𝗗^\mathsfbf{D}^\mbfsansD^A^mathalpha^mathsfbf^MATHEMATICAL SANS-SERIF BOLD CAPITAL D
1D5D8^𝗘^\mathsfbf{E}^\mbfsansE^A^mathalpha^mathsfbf^MATHEMATICAL SANS-SERIF BOLD CAPITAL E
1D5D9^𝗙^\mathsfbf{F}^\mbfsansF^A^mathalpha^mathsfbf^MATHEMATICAL SANS-SERIF BOLD CAPITAL F
1D5DA^𝗚^\mathsfbf{G}^\mbfsansG^A^mathalpha^mathsfbf^MATHEMATICAL SANS-SERIF BOLD CAPITAL G
1D5DB^𝗛^\mathsfbf{H}^\mbfsansH^A^mathalpha^mathsfbf^MATHEMATICAL SANS-SERIF BOLD CAPITAL H
1D5DC^𝗜^\mathsfbf{I}^\mbfsansI^A^mathalpha^mathsfbf^MATHEMATICAL SANS-SERIF BOLD CAPITAL I
1D5DD^𝗝^\mathsfbf{J}^\mbfsansJ^A^mathalpha^mathsfbf^MATHEMATICAL SANS-SERIF BOLD CAPITAL J
1D5DE^𝗞^\mathsfbf{K}^\mbfsansK^A^mathalpha^mathsfbf^MATHEMATICAL SANS-SERIF BOLD CAPITAL K
1D5DF^𝗟^\mathsfbf{L}^\mbfsansL^A^mathalpha^mathsfbf^MATHEMATICAL SANS-SERIF BOLD CAPITAL L
1D5E0^𝗠^\mathsfbf{M}^\mbfsansM^A^mathalpha^mathsfbf^MATHEMATICAL SANS-SERIF BOLD CAPITAL M
1D5E1^𝗡^\mathsfbf{N}^\mbfsansN^A^mathalpha^mathsfbf^MATHEMATICAL SANS-SERIF BOLD CAPITAL N
1D5E2^𝗢^\mathsfbf{O}^\mbfsansO^A^mathalpha^mathsfbf^MATHEMATICAL SANS-SERIF BOLD CAPITAL O
1D5E3^𝗣^\mathsfbf{P}^\mbfsansP^A^mathalpha^mathsfbf^MATHEMATICAL SANS-SERIF BOLD CAPITAL P
1D5E4^𝗤^\mathsfbf{Q}^\mbfsansQ^A^mathalpha^mathsfbf^MATHEMATICAL SANS-SERIF BOLD CAPITAL Q
1D5E5^𝗥^\mathsfbf{R}^\mbfsansR^A^mathalpha^mathsfbf^MATHEMATICAL SANS-SERIF BOLD CAPITAL R
1D5E6^𝗦^\mathsfbf{S}^\mbfsansS^A^mathalpha^mathsfbf^MATHEMATICAL SANS-SERIF BOLD CAPITAL S
1D5E7^𝗧^\mathsfbf{T}^\mbfsansT^A^mathalpha^mathsfbf^MATHEMATICAL SANS-SERIF BOLD CAPITAL T
1D5E8^𝗨^\mathsfbf{U}^\mbfsansU^A^mathalpha^mathsfbf^MATHEMATICAL SANS-SERIF BOLD CAPITAL U
1D5E9^𝗩^\mathsfbf{V}^\mbfsansV^A^mathalpha^mathsfbf^MATHEMATICAL SANS-SERIF BOLD CAPITAL V
1D5EA^𝗪^\mathsfbf{W}^\mbfsansW^A^mathalpha^mathsfbf^MATHEMATICAL SANS-SERIF BOLD CAPITAL W
1D5EB^𝗫^\mathsfbf{X}^\mbfsansX^A^mathalpha^mathsfbf^MATHEMATICAL SANS-SERIF BOLD CAPITAL X
1D5EC^𝗬^\mathsfbf{Y}^\mbfsansY^A^mathalpha^mathsfbf^MATHEMATICAL SANS-SERIF BOLD CAPITAL Y
1D5ED^𝗭^\mathsfbf{Z}^\mbfsansZ^A^mathalpha^mathsfbf^MATHEMATICAL SANS-SERIF BOLD CAPITAL Z
1D5EE^𝗮^\mathsfbf{a}^\mbfsansa^A^mathalpha^mathsfbf^MATHEMATICAL SANS-SERIF BOLD SMALL A
1D5EF^𝗯^\mathsfbf{b}^\mbfsansb^A^mathalpha^mathsfbf^MATHEMATICAL SANS-SERIF BOLD SMALL B
1D5F0^𝗰^\mathsfbf{c}^\mbfsansc^A^mathalpha^mathsfbf^MATHEMATICAL SANS-SERIF BOLD SMALL C
1D5F1^𝗱^\mathsfbf{d}^\mbfsansd^A^mathalpha^mathsfbf^MATHEMATICAL SANS-SERIF BOLD SMALL D
1D5F2^𝗲^\mathsfbf{e}^\mbfsanse^A^mathalpha^mathsfbf^MATHEMATICAL SANS-SERIF BOLD SMALL E
1D5F3^𝗳^\mathsfbf{f}^\mbfsansf^A^mathalpha^mathsfbf^MATHEMATICAL SANS-SERIF BOLD SMALL F
1D5F4^𝗴^\mathsfbf{g}^\mbfsansg^A^mathalpha^mathsfbf^MATHEMATICAL SANS-SERIF BOLD SMALL G
1D5F5^𝗵^\mathsfbf{h}^\mbfsansh^A^mathalpha^mathsfbf^MATHEMATICAL SANS-SERIF BOLD SMALL H
1D5F6^𝗶^\mathsfbf{i}^\mbfsansi^A^mathalpha^mathsfbf^MATHEMATICAL SANS-SERIF BOLD SMALL I
1D5F7^𝗷^\mathsfbf{j}^\mbfsansj^A^mathalpha^mathsfbf^MATHEMATICAL SANS-SERIF BOLD SMALL J
1D5F8^𝗸^\mathsfbf{k}^\mbfsansk^A^mathalpha^mathsfbf^MATHEMATICAL SANS-SERIF BOLD SMALL K
1D5F9^𝗹^\mathsfbf{l}^\mbfsansl^A^mathalpha^mathsfbf^MATHEMATICAL SANS-SERIF BOLD SMALL L
1D5FA^𝗺^\mathsfbf{m}^\mbfsansm^A^mathalpha^mathsfbf^MATHEMATICAL SANS-SERIF BOLD SMALL M
1D5FB^𝗻^\mathsfbf{n}^\mbfsansn^A^mathalpha^mathsfbf^MATHEMATICAL SANS-SERIF BOLD SMALL N
1D5FC^𝗼^\mathsfbf{o}^\mbfsanso^A^mathalpha^mathsfbf^MATHEMATICAL SANS-SERIF BOLD SMALL O
1D5FD^𝗽^\mathsfbf{p}^\mbfsansp^A^mathalpha^mathsfbf^MATHEMATICAL SANS-SERIF BOLD SMALL P
1D5FE^𝗾^\mathsfbf{q}^\mbfsansq^A^mathalpha^mathsfbf^MATHEMATICAL SANS-SERIF BOLD SMALL Q
1D5FF^𝗿^\mathsfbf{r}^\mbfsansr^A^mathalpha^mathsfbf^MATHEMATICAL SANS-SERIF BOLD SMALL R
1D600^𝘀^\mathsfbf{s}^\mbfsanss^A^mathalpha^mathsfbf^MATHEMATICAL SANS-SERIF BOLD SMALL S
1D601^𝘁^\mathsfbf{t}^\mbfsanst^A^mathalpha^mathsfbf^MATHEMATICAL SANS-SERIF BOLD SMALL T
1D602^𝘂^\mathsfbf{u}^\mbfsansu^A^mathalpha^mathsfbf^MATHEMATICAL SANS-SERIF BOLD SMALL U
1D603^𝘃^\mathsfbf{v}^\mbfsansv^A^mathalpha^mathsfbf^MATHEMATICAL SANS-SERIF BOLD SMALL V
1D604^𝘄^\mathsfbf{w}^\mbfsansw^A^mathalpha^mathsfbf^MATHEMATICAL SANS-SERIF BOLD SMALL W
1D605^𝘅^\mathsfbf{x}^\mbfsansx^A^mathalpha^mathsfbf^MATHEMATICAL SANS-SERIF BOLD SMALL X
1D606^𝘆^\mathsfbf{y}^\mbfsansy^A^mathalpha^mathsfbf^MATHEMATICAL SANS-SERIF BOLD SMALL Y
1D607^𝘇^\mathsfbf{z}^\mbfsansz^A^mathalpha^mathsfbf^MATHEMATICAL SANS-SERIF BOLD SMALL Z
1D608^𝘈^\mathsfit{A}^\mitsansA^A^mathalpha^omlmathsfit^MATHEMATICAL SANS-SERIF ITALIC CAPITAL A
1D609^𝘉^\mathsfit{B}^\mitsansB^A^mathalpha^omlmathsfit^MATHEMATICAL SANS-SERIF ITALIC CAPITAL B
1D60A^𝘊^\mathsfit{C}^\mitsansC^A^mathalpha^omlmathsfit^MATHEMATICAL SANS-SERIF ITALIC CAPITAL C
1D60B^𝘋^\mathsfit{D}^\mitsansD^A^mathalpha^omlmathsfit^MATHEMATICAL SANS-SERIF ITALIC CAPITAL D
1D60C^𝘌^\mathsfit{E}^\mitsansE^A^mathalpha^omlmathsfit^MATHEMATICAL SANS-SERIF ITALIC CAPITAL E
1D60D^𝘍^\mathsfit{F}^\mitsansF^A^mathalpha^omlmathsfit^MATHEMATICAL SANS-SERIF ITALIC CAPITAL F
1D60E^𝘎^\mathsfit{G}^\mitsansG^A^mathalpha^omlmathsfit^MATHEMATICAL SANS-SERIF ITALIC CAPITAL G
1D60F^𝘏^\mathsfit{H}^\mitsansH^A^mathalpha^omlmathsfit^MATHEMATICAL SANS-SERIF ITALIC CAPITAL H
1D610^𝘐^\mathsfit{I}^\mitsansI^A^mathalpha^omlmathsfit^MATHEMATICAL SANS-SERIF ITALIC CAPITAL I
1D611^𝘑^\mathsfit{J}^\mitsansJ^A^mathalpha^omlmathsfit^MATHEMATICAL SANS-SERIF ITALIC CAPITAL J
1D612^𝘒^\mathsfit{K}^\mitsansK^A^mathalpha^omlmathsfit^MATHEMATICAL SANS-SERIF ITALIC CAPITAL K
1D613^𝘓^\mathsfit{L}^\mitsansL^A^mathalpha^omlmathsfit^MATHEMATICAL SANS-SERIF ITALIC CAPITAL L
1D614^𝘔^\mathsfit{M}^\mitsansM^A^mathalpha^omlmathsfit^MATHEMATICAL SANS-SERIF ITALIC CAPITAL M
1D615^𝘕^\mathsfit{N}^\mitsansN^A^mathalpha^omlmathsfit^MATHEMATICAL SANS-SERIF ITALIC CAPITAL N
1D616^𝘖^\mathsfit{O}^\mitsansO^A^mathalpha^omlmathsfit^MATHEMATICAL SANS-SERIF ITALIC CAPITAL O
1D617^𝘗^\mathsfit{P}^\mitsansP^A^mathalpha^omlmathsfit^MATHEMATICAL SANS-SERIF ITALIC CAPITAL P
1D618^𝘘^\mathsfit{Q}^\mitsansQ^A^mathalpha^omlmathsfit^MATHEMATICAL SANS-SERIF ITALIC CAPITAL Q
1D619^𝘙^\mathsfit{R}^\mitsansR^A^mathalpha^omlmathsfit^MATHEMATICAL SANS-SERIF ITALIC CAPITAL R
1D61A^𝘚^\mathsfit{S}^\mitsansS^A^mathalpha^omlmathsfit^MATHEMATICAL SANS-SERIF ITALIC CAPITAL S
1D61B^𝘛^\mathsfit{T}^\mitsansT^A^mathalpha^omlmathsfit^MATHEMATICAL SANS-SERIF ITALIC CAPITAL T
1D61C^𝘜^\mathsfit{U}^\mitsansU^A^mathalpha^omlmathsfit^MATHEMATICAL SANS-SERIF ITALIC CAPITAL U
1D61D^𝘝^\mathsfit{V}^\mitsansV^A^mathalpha^omlmathsfit^MATHEMATICAL SANS-SERIF ITALIC CAPITAL V
1D61E^𝘞^\mathsfit{W}^\mitsansW^A^mathalpha^omlmathsfit^MATHEMATICAL SANS-SERIF ITALIC CAPITAL W
1D61F^𝘟^\mathsfit{X}^\mitsansX^A^mathalpha^omlmathsfit^MATHEMATICAL SANS-SERIF ITALIC CAPITAL X
1D620^𝘠^\mathsfit{Y}^\mitsansY^A^mathalpha^omlmathsfit^MATHEMATICAL SANS-SERIF ITALIC CAPITAL Y
1D621^𝘡^\mathsfit{Z}^\mitsansZ^A^mathalpha^omlmathsfit^MATHEMATICAL SANS-SERIF ITALIC CAPITAL Z
1D622^𝘢^\mathsfit{a}^\mitsansa^A^mathalpha^omlmathsfit^MATHEMATICAL SANS-SERIF ITALIC SMALL A
1D623^𝘣^\mathsfit{b}^\mitsansb^A^mathalpha^omlmathsfit^MATHEMATICAL SANS-SERIF ITALIC SMALL B
1D624^𝘤^\mathsfit{c}^\mitsansc^A^mathalpha^omlmathsfit^MATHEMATICAL SANS-SERIF ITALIC SMALL C
1D625^𝘥^\mathsfit{d}^\mitsansd^A^mathalpha^omlmathsfit^MATHEMATICAL SANS-SERIF ITALIC SMALL D
1D626^𝘦^\mathsfit{e}^\mitsanse^A^mathalpha^omlmathsfit^MATHEMATICAL SANS-SERIF ITALIC SMALL E
1D627^𝘧^\mathsfit{f}^\mitsansf^A^mathalpha^omlmathsfit^MATHEMATICAL SANS-SERIF ITALIC SMALL F
1D628^𝘨^\mathsfit{g}^\mitsansg^A^mathalpha^omlmathsfit^MATHEMATICAL SANS-SERIF ITALIC SMALL G
1D629^𝘩^\mathsfit{h}^\mitsansh^A^mathalpha^omlmathsfit^MATHEMATICAL SANS-SERIF ITALIC SMALL H
1D62A^𝘪^\mathsfit{i}^\mitsansi^A^mathalpha^omlmathsfit^MATHEMATICAL SANS-SERIF ITALIC SMALL I
1D62B^𝘫^\mathsfit{j}^\mitsansj^A^mathalpha^omlmathsfit^MATHEMATICAL SANS-SERIF ITALIC SMALL J
1D62C^𝘬^\mathsfit{k}^\mitsansk^A^mathalpha^omlmathsfit^MATHEMATICAL SANS-SERIF ITALIC SMALL K
1D62D^𝘭^\mathsfit{l}^\mitsansl^A^mathalpha^omlmathsfit^MATHEMATICAL SANS-SERIF ITALIC SMALL L
1D62E^𝘮^\mathsfit{m}^\mitsansm^A^mathalpha^omlmathsfit^MATHEMATICAL SANS-SERIF ITALIC SMALL M
1D62F^𝘯^\mathsfit{n}^\mitsansn^A^mathalpha^omlmathsfit^MATHEMATICAL SANS-SERIF ITALIC SMALL N
1D630^𝘰^\mathsfit{o}^\mitsanso^A^mathalpha^omlmathsfit^MATHEMATICAL SANS-SERIF ITALIC SMALL O
1D631^𝘱^\mathsfit{p}^\mitsansp^A^mathalpha^omlmathsfit^MATHEMATICAL SANS-SERIF ITALIC SMALL P
1D632^𝘲^\mathsfit{q}^\mitsansq^A^mathalpha^omlmathsfit^MATHEMATICAL SANS-SERIF ITALIC SMALL Q
1D633^𝘳^\mathsfit{r}^\mitsansr^A^mathalpha^omlmathsfit^MATHEMATICAL SANS-SERIF ITALIC SMALL R
1D634^𝘴^\mathsfit{s}^\mitsanss^A^mathalpha^omlmathsfit^MATHEMATICAL SANS-SERIF ITALIC SMALL S
1D635^𝘵^\mathsfit{t}^\mitsanst^A^mathalpha^omlmathsfit^MATHEMATICAL SANS-SERIF ITALIC SMALL T
1D636^𝘶^\mathsfit{u}^\mitsansu^A^mathalpha^omlmathsfit^MATHEMATICAL SANS-SERIF ITALIC SMALL U
1D637^𝘷^\mathsfit{v}^\mitsansv^A^mathalpha^omlmathsfit^MATHEMATICAL SANS-SERIF ITALIC SMALL V
1D638^𝘸^\mathsfit{w}^\mitsansw^A^mathalpha^omlmathsfit^MATHEMATICAL SANS-SERIF ITALIC SMALL W
1D639^𝘹^\mathsfit{x}^\mitsansx^A^mathalpha^omlmathsfit^MATHEMATICAL SANS-SERIF ITALIC SMALL X
1D63A^𝘺^\mathsfit{y}^\mitsansy^A^mathalpha^omlmathsfit^MATHEMATICAL SANS-SERIF ITALIC SMALL Y
1D63B^𝘻^\mathsfit{z}^\mitsansz^A^mathalpha^omlmathsfit^MATHEMATICAL SANS-SERIF ITALIC SMALL Z
1D63C^𝘼^\mathsfbfit{A}^\mbfitsansA^A^mathalpha^isomath^MATHEMATICAL SANS-SERIF BOLD ITALIC CAPITAL A
1D63D^𝘽^\mathsfbfit{B}^\mbfitsansB^A^mathalpha^isomath^MATHEMATICAL SANS-SERIF BOLD ITALIC CAPITAL B
1D63E^𝘾^\mathsfbfit{C}^\mbfitsansC^A^mathalpha^isomath^MATHEMATICAL SANS-SERIF BOLD ITALIC CAPITAL C
1D63F^𝘿^\mathsfbfit{D}^\mbfitsansD^A^mathalpha^isomath^MATHEMATICAL SANS-SERIF BOLD ITALIC CAPITAL D
1D640^𝙀^\mathsfbfit{E}^\mbfitsansE^A^mathalpha^isomath^MATHEMATICAL SANS-SERIF BOLD ITALIC CAPITAL E
1D641^𝙁^\mathsfbfit{F}^\mbfitsansF^A^mathalpha^isomath^MATHEMATICAL SANS-SERIF BOLD ITALIC CAPITAL F
1D642^𝙂^\mathsfbfit{G}^\mbfitsansG^A^mathalpha^isomath^MATHEMATICAL SANS-SERIF BOLD ITALIC CAPITAL G
1D643^𝙃^\mathsfbfit{H}^\mbfitsansH^A^mathalpha^isomath^MATHEMATICAL SANS-SERIF BOLD ITALIC CAPITAL H
1D644^𝙄^\mathsfbfit{I}^\mbfitsansI^A^mathalpha^isomath^MATHEMATICAL SANS-SERIF BOLD ITALIC CAPITAL I
1D645^𝙅^\mathsfbfit{J}^\mbfitsansJ^A^mathalpha^isomath^MATHEMATICAL SANS-SERIF BOLD ITALIC CAPITAL J
1D646^𝙆^\mathsfbfit{K}^\mbfitsansK^A^mathalpha^isomath^MATHEMATICAL SANS-SERIF BOLD ITALIC CAPITAL K
1D647^𝙇^\mathsfbfit{L}^\mbfitsansL^A^mathalpha^isomath^MATHEMATICAL SANS-SERIF BOLD ITALIC CAPITAL L
1D648^𝙈^\mathsfbfit{M}^\mbfitsansM^A^mathalpha^isomath^MATHEMATICAL SANS-SERIF BOLD ITALIC CAPITAL M
1D649^𝙉^\mathsfbfit{N}^\mbfitsansN^A^mathalpha^isomath^MATHEMATICAL SANS-SERIF BOLD ITALIC CAPITAL N
1D64A^𝙊^\mathsfbfit{O}^\mbfitsansO^A^mathalpha^isomath^MATHEMATICAL SANS-SERIF BOLD ITALIC CAPITAL O
1D64B^𝙋^\mathsfbfit{P}^\mbfitsansP^A^mathalpha^isomath^MATHEMATICAL SANS-SERIF BOLD ITALIC CAPITAL P
1D64C^𝙌^\mathsfbfit{Q}^\mbfitsansQ^A^mathalpha^isomath^MATHEMATICAL SANS-SERIF BOLD ITALIC CAPITAL Q
1D64D^𝙍^\mathsfbfit{R}^\mbfitsansR^A^mathalpha^isomath^MATHEMATICAL SANS-SERIF BOLD ITALIC CAPITAL R
1D64E^𝙎^\mathsfbfit{S}^\mbfitsansS^A^mathalpha^isomath^MATHEMATICAL SANS-SERIF BOLD ITALIC CAPITAL S
1D64F^𝙏^\mathsfbfit{T}^\mbfitsansT^A^mathalpha^isomath^MATHEMATICAL SANS-SERIF BOLD ITALIC CAPITAL T
1D650^𝙐^\mathsfbfit{U}^\mbfitsansU^A^mathalpha^isomath^MATHEMATICAL SANS-SERIF BOLD ITALIC CAPITAL U
1D651^𝙑^\mathsfbfit{V}^\mbfitsansV^A^mathalpha^isomath^MATHEMATICAL SANS-SERIF BOLD ITALIC CAPITAL V
1D652^𝙒^\mathsfbfit{W}^\mbfitsansW^A^mathalpha^isomath^MATHEMATICAL SANS-SERIF BOLD ITALIC CAPITAL W
1D653^𝙓^\mathsfbfit{X}^\mbfitsansX^A^mathalpha^isomath^MATHEMATICAL SANS-SERIF BOLD ITALIC CAPITAL X
1D654^𝙔^\mathsfbfit{Y}^\mbfitsansY^A^mathalpha^isomath^MATHEMATICAL SANS-SERIF BOLD ITALIC CAPITAL Y
1D655^𝙕^\mathsfbfit{Z}^\mbfitsansZ^A^mathalpha^isomath^MATHEMATICAL SANS-SERIF BOLD ITALIC CAPITAL Z
1D656^𝙖^\mathsfbfit{a}^\mbfitsansa^A^mathalpha^isomath^MATHEMATICAL SANS-SERIF BOLD ITALIC SMALL A
1D657^𝙗^\mathsfbfit{b}^\mbfitsansb^A^mathalpha^isomath^MATHEMATICAL SANS-SERIF BOLD ITALIC SMALL B
1D658^𝙘^\mathsfbfit{c}^\mbfitsansc^A^mathalpha^isomath^MATHEMATICAL SANS-SERIF BOLD ITALIC SMALL C
1D659^𝙙^\mathsfbfit{d}^\mbfitsansd^A^mathalpha^isomath^MATHEMATICAL SANS-SERIF BOLD ITALIC SMALL D
1D65A^𝙚^\mathsfbfit{e}^\mbfitsanse^A^mathalpha^isomath^MATHEMATICAL SANS-SERIF BOLD ITALIC SMALL E
1D65B^𝙛^\mathsfbfit{f}^\mbfitsansf^A^mathalpha^isomath^MATHEMATICAL SANS-SERIF BOLD ITALIC SMALL F
1D65C^𝙜^\mathsfbfit{g}^\mbfitsansg^A^mathalpha^isomath^MATHEMATICAL SANS-SERIF BOLD ITALIC SMALL G
1D65D^𝙝^\mathsfbfit{h}^\mbfitsansh^A^mathalpha^isomath^MATHEMATICAL SANS-SERIF BOLD ITALIC SMALL H
1D65E^𝙞^\mathsfbfit{i}^\mbfitsansi^A^mathalpha^isomath^MATHEMATICAL SANS-SERIF BOLD ITALIC SMALL I
1D65F^𝙟^\mathsfbfit{j}^\mbfitsansj^A^mathalpha^isomath^MATHEMATICAL SANS-SERIF BOLD ITALIC SMALL J
1D660^𝙠^\mathsfbfit{k}^\mbfitsansk^A^mathalpha^isomath^MATHEMATICAL SANS-SERIF BOLD ITALIC SMALL K
1D661^𝙡^\mathsfbfit{l}^\mbfitsansl^A^mathalpha^isomath^MATHEMATICAL SANS-SERIF BOLD ITALIC SMALL L
1D662^𝙢^\mathsfbfit{m}^\mbfitsansm^A^mathalpha^isomath^MATHEMATICAL SANS-SERIF BOLD ITALIC SMALL M
1D663^𝙣^\mathsfbfit{n}^\mbfitsansn^A^mathalpha^isomath^MATHEMATICAL SANS-SERIF BOLD ITALIC SMALL N
1D664^𝙤^\mathsfbfit{o}^\mbfitsanso^A^mathalpha^isomath^MATHEMATICAL SANS-SERIF BOLD ITALIC SMALL O
1D665^𝙥^\mathsfbfit{p}^\mbfitsansp^A^mathalpha^isomath^MATHEMATICAL SANS-SERIF BOLD ITALIC SMALL P
1D666^𝙦^\mathsfbfit{q}^\mbfitsansq^A^mathalpha^isomath^MATHEMATICAL SANS-SERIF BOLD ITALIC SMALL Q
1D667^𝙧^\mathsfbfit{r}^\mbfitsansr^A^mathalpha^isomath^MATHEMATICAL SANS-SERIF BOLD ITALIC SMALL R
1D668^𝙨^\mathsfbfit{s}^\mbfitsanss^A^mathalpha^isomath^MATHEMATICAL SANS-SERIF BOLD ITALIC SMALL S
1D669^𝙩^\mathsfbfit{t}^\mbfitsanst^A^mathalpha^isomath^MATHEMATICAL SANS-SERIF BOLD ITALIC SMALL T
1D66A^𝙪^\mathsfbfit{u}^\mbfitsansu^A^mathalpha^isomath^MATHEMATICAL SANS-SERIF BOLD ITALIC SMALL U
1D66B^𝙫^\mathsfbfit{v}^\mbfitsansv^A^mathalpha^isomath^MATHEMATICAL SANS-SERIF BOLD ITALIC SMALL V
1D66C^𝙬^\mathsfbfit{w}^\mbfitsansw^A^mathalpha^isomath^MATHEMATICAL SANS-SERIF BOLD ITALIC SMALL W
1D66D^𝙭^\mathsfbfit{x}^\mbfitsansx^A^mathalpha^isomath^MATHEMATICAL SANS-SERIF BOLD ITALIC SMALL X
1D66E^𝙮^\mathsfbfit{y}^\mbfitsansy^A^mathalpha^isomath^MATHEMATICAL SANS-SERIF BOLD ITALIC SMALL Y
1D66F^𝙯^\mathsfbfit{z}^\mbfitsansz^A^mathalpha^isomath^MATHEMATICAL SANS-SERIF BOLD ITALIC SMALL Z
1D670^𝙰^\mathtt{A}^\mttA^A^mathalpha^^MATHEMATICAL MONOSPACE CAPITAL A
1D671^𝙱^\mathtt{B}^\mttB^A^mathalpha^^MATHEMATICAL MONOSPACE CAPITAL B
1D672^𝙲^\mathtt{C}^\mttC^A^mathalpha^^MATHEMATICAL MONOSPACE CAPITAL C
1D673^𝙳^\mathtt{D}^\mttD^A^mathalpha^^MATHEMATICAL MONOSPACE CAPITAL D
1D674^𝙴^\mathtt{E}^\mttE^A^mathalpha^^MATHEMATICAL MONOSPACE CAPITAL E
1D675^𝙵^\mathtt{F}^\mttF^A^mathalpha^^MATHEMATICAL MONOSPACE CAPITAL F
1D676^𝙶^\mathtt{G}^\mttG^A^mathalpha^^MATHEMATICAL MONOSPACE CAPITAL G
1D677^𝙷^\mathtt{H}^\mttH^A^mathalpha^^MATHEMATICAL MONOSPACE CAPITAL H
1D678^𝙸^\mathtt{I}^\mttI^A^mathalpha^^MATHEMATICAL MONOSPACE CAPITAL I
1D679^𝙹^\mathtt{J}^\mttJ^A^mathalpha^^MATHEMATICAL MONOSPACE CAPITAL J
1D67A^𝙺^\mathtt{K}^\mttK^A^mathalpha^^MATHEMATICAL MONOSPACE CAPITAL K
1D67B^𝙻^\mathtt{L}^\mttL^A^mathalpha^^MATHEMATICAL MONOSPACE CAPITAL L
1D67C^𝙼^\mathtt{M}^\mttM^A^mathalpha^^MATHEMATICAL MONOSPACE CAPITAL M
1D67D^𝙽^\mathtt{N}^\mttN^A^mathalpha^^MATHEMATICAL MONOSPACE CAPITAL N
1D67E^𝙾^\mathtt{O}^\mttO^A^mathalpha^^MATHEMATICAL MONOSPACE CAPITAL O
1D67F^𝙿^\mathtt{P}^\mttP^A^mathalpha^^MATHEMATICAL MONOSPACE CAPITAL P
1D680^𝚀^\mathtt{Q}^\mttQ^A^mathalpha^^MATHEMATICAL MONOSPACE CAPITAL Q
1D681^𝚁^\mathtt{R}^\mttR^A^mathalpha^^MATHEMATICAL MONOSPACE CAPITAL R
1D682^𝚂^\mathtt{S}^\mttS^A^mathalpha^^MATHEMATICAL MONOSPACE CAPITAL S
1D683^𝚃^\mathtt{T}^\mttT^A^mathalpha^^MATHEMATICAL MONOSPACE CAPITAL T
1D684^𝚄^\mathtt{U}^\mttU^A^mathalpha^^MATHEMATICAL MONOSPACE CAPITAL U
1D685^𝚅^\mathtt{V}^\mttV^A^mathalpha^^MATHEMATICAL MONOSPACE CAPITAL V
1D686^𝚆^\mathtt{W}^\mttW^A^mathalpha^^MATHEMATICAL MONOSPACE CAPITAL W
1D687^𝚇^\mathtt{X}^\mttX^A^mathalpha^^MATHEMATICAL MONOSPACE CAPITAL X
1D688^𝚈^\mathtt{Y}^\mttY^A^mathalpha^^MATHEMATICAL MONOSPACE CAPITAL Y
1D689^𝚉^\mathtt{Z}^\mttZ^A^mathalpha^^MATHEMATICAL MONOSPACE CAPITAL Z
1D68A^𝚊^\mathtt{a}^\mtta^A^mathalpha^^MATHEMATICAL MONOSPACE SMALL A
1D68B^𝚋^\mathtt{b}^\mttb^A^mathalpha^^MATHEMATICAL MONOSPACE SMALL B
1D68C^𝚌^\mathtt{c}^\mttc^A^mathalpha^^MATHEMATICAL MONOSPACE SMALL C
1D68D^𝚍^\mathtt{d}^\mttd^A^mathalpha^^MATHEMATICAL MONOSPACE SMALL D
1D68E^𝚎^\mathtt{e}^\mtte^A^mathalpha^^MATHEMATICAL MONOSPACE SMALL E
1D68F^𝚏^\mathtt{f}^\mttf^A^mathalpha^^MATHEMATICAL MONOSPACE SMALL F
1D690^𝚐^\mathtt{g}^\mttg^A^mathalpha^^MATHEMATICAL MONOSPACE SMALL G
1D691^𝚑^\mathtt{h}^\mtth^A^mathalpha^^MATHEMATICAL MONOSPACE SMALL H
1D692^𝚒^\mathtt{i}^\mtti^A^mathalpha^^MATHEMATICAL MONOSPACE SMALL I
1D693^𝚓^\mathtt{j}^\mttj^A^mathalpha^^MATHEMATICAL MONOSPACE SMALL J
1D694^𝚔^\mathtt{k}^\mttk^A^mathalpha^^MATHEMATICAL MONOSPACE SMALL K
1D695^𝚕^\mathtt{l}^\mttl^A^mathalpha^^MATHEMATICAL MONOSPACE SMALL L
1D696^𝚖^\mathtt{m}^\mttm^A^mathalpha^^MATHEMATICAL MONOSPACE SMALL M
1D697^𝚗^\mathtt{n}^\mttn^A^mathalpha^^MATHEMATICAL MONOSPACE SMALL N
1D698^𝚘^\mathtt{o}^\mtto^A^mathalpha^^MATHEMATICAL MONOSPACE SMALL O
1D699^𝚙^\mathtt{p}^\mttp^A^mathalpha^^MATHEMATICAL MONOSPACE SMALL P
1D69A^𝚚^\mathtt{q}^\mttq^A^mathalpha^^MATHEMATICAL MONOSPACE SMALL Q
1D69B^𝚛^\mathtt{r}^\mttr^A^mathalpha^^MATHEMATICAL MONOSPACE SMALL R
1D69C^𝚜^\mathtt{s}^\mtts^A^mathalpha^^MATHEMATICAL MONOSPACE SMALL S
1D69D^𝚝^\mathtt{t}^\mttt^A^mathalpha^^MATHEMATICAL MONOSPACE SMALL T
1D69E^𝚞^\mathtt{u}^\mttu^A^mathalpha^^MATHEMATICAL MONOSPACE SMALL U
1D69F^𝚟^\mathtt{v}^\mttv^A^mathalpha^^MATHEMATICAL MONOSPACE SMALL V
1D6A0^𝚠^\mathtt{w}^\mttw^A^mathalpha^^MATHEMATICAL MONOSPACE SMALL W
1D6A1^𝚡^\mathtt{x}^\mttx^A^mathalpha^^MATHEMATICAL MONOSPACE SMALL X
1D6A2^𝚢^\mathtt{y}^\mtty^A^mathalpha^^MATHEMATICAL MONOSPACE SMALL Y
1D6A3^𝚣^\mathtt{z}^\mttz^A^mathalpha^^MATHEMATICAL MONOSPACE SMALL Z
1D6A4^𝚤^\imath^\imath^A^mathalpha^^MATHEMATICAL ITALIC SMALL DOTLESS I
1D6A5^𝚥^\jmath^\jmath^A^mathalpha^^MATHEMATICAL ITALIC SMALL DOTLESS J
1D6A8^𝚨^^\mbfAlpha^A^mathalpha^^MATHEMATICAL BOLD CAPITAL ALPHA
1D6A9^𝚩^^\mbfBeta^A^mathalpha^^MATHEMATICAL BOLD CAPITAL BETA
1D6AA^𝚪^\mathbf{\Gamma}^\mbfGamma^A^mathalpha^-fourier^MATHEMATICAL BOLD CAPITAL GAMMA
1D6AB^𝚫^\mathbf{\Delta}^\mbfDelta^A^mathalpha^-fourier^MATHEMATICAL BOLD CAPITAL DELTA
1D6AC^𝚬^^\mbfEpsilon^A^mathalpha^^MATHEMATICAL BOLD CAPITAL EPSILON
1D6AD^𝚭^^\mbfZeta^A^mathalpha^^MATHEMATICAL BOLD CAPITAL ZETA
1D6AE^𝚮^^\mbfEta^A^mathalpha^^MATHEMATICAL BOLD CAPITAL ETA
1D6AF^𝚯^\mathbf{\Theta}^\mbfTheta^A^mathalpha^-fourier^MATHEMATICAL BOLD CAPITAL THETA
1D6B0^𝚰^^\mbfIota^A^mathalpha^^MATHEMATICAL BOLD CAPITAL IOTA
1D6B1^𝚱^^\mbfKappa^A^mathalpha^^MATHEMATICAL BOLD CAPITAL KAPPA
1D6B2^𝚲^\mathbf{\Lambda}^\mbfLambda^A^mathalpha^-fourier^mathematical bold capital lambda
1D6B3^𝚳^^\mbfMu^A^mathalpha^^MATHEMATICAL BOLD CAPITAL MU
1D6B4^𝚴^^\mbfNu^A^mathalpha^^MATHEMATICAL BOLD CAPITAL NU
1D6B5^𝚵^\mathbf{\Xi}^\mbfXi^A^mathalpha^-fourier^MATHEMATICAL BOLD CAPITAL XI
1D6B6^𝚶^^\mbfOmicron^A^mathalpha^^MATHEMATICAL BOLD CAPITAL OMICRON
1D6B7^𝚷^\mathbf{\Pi}^\mbfPi^A^mathalpha^-fourier^MATHEMATICAL BOLD CAPITAL PI
1D6B8^𝚸^^\mbfRho^A^mathalpha^^MATHEMATICAL BOLD CAPITAL RHO
1D6B9^𝚹^^\mbfvarTheta^A^mathalpha^^MATHEMATICAL BOLD CAPITAL THETA SYMBOL
1D6BA^𝚺^\mathbf{\Sigma}^\mbfSigma^A^mathalpha^-fourier^MATHEMATICAL BOLD CAPITAL SIGMA
1D6BB^𝚻^^\mbfTau^A^mathalpha^^MATHEMATICAL BOLD CAPITAL TAU
1D6BC^𝚼^\mathbf{\Upsilon}^\mbfUpsilon^A^mathalpha^-fourier^MATHEMATICAL BOLD CAPITAL UPSILON
1D6BD^𝚽^\mathbf{\Phi}^\mbfPhi^A^mathalpha^-fourier^MATHEMATICAL BOLD CAPITAL PHI
1D6BE^𝚾^^\mbfChi^A^mathalpha^^MATHEMATICAL BOLD CAPITAL CHI
1D6BF^𝚿^\mathbf{\Psi}^\mbfPsi^A^mathalpha^-fourier^MATHEMATICAL BOLD CAPITAL PSI
1D6C0^𝛀^\mathbf{\Omega}^\mbfOmega^A^mathalpha^-fourier^MATHEMATICAL BOLD CAPITAL OMEGA
1D6C1^𝛁^^\mbfnabla^A^mathord^^MATHEMATICAL BOLD NABLA
1D6C2^𝛂^\mathbf{\alpha}^\mbfalpha^A^mathalpha^omlmathbf^MATHEMATICAL BOLD SMALL ALPHA
1D6C3^𝛃^\mathbf{\beta}^\mbfbeta^A^mathalpha^omlmathbf^MATHEMATICAL BOLD SMALL BETA
1D6C4^𝛄^\mathbf{\gamma}^\mbfgamma^A^mathalpha^omlmathbf^MATHEMATICAL BOLD SMALL GAMMA
1D6C5^𝛅^\mathbf{\delta}^\mbfdelta^A^mathalpha^omlmathbf^MATHEMATICAL BOLD SMALL DELTA
1D6C6^𝛆^\mathbf{\varepsilon}^\mbfepsilon^A^mathalpha^omlmathbf^MATHEMATICAL BOLD SMALL EPSILON
1D6C7^𝛇^\mathbf{\zeta}^\mbfzeta^A^mathalpha^omlmathbf^MATHEMATICAL BOLD SMALL ZETA
1D6C8^𝛈^\mathbf{\eta}^\mbfeta^A^mathalpha^omlmathbf^MATHEMATICAL BOLD SMALL ETA
1D6C9^𝛉^\mathbf{\theta}^\mbftheta^A^mathalpha^omlmathbf^MATHEMATICAL BOLD SMALL THETA
1D6CA^𝛊^\mathbf{\iota}^\mbfiota^A^mathalpha^omlmathbf^MATHEMATICAL BOLD SMALL IOTA
1D6CB^𝛋^\mathbf{\kappa}^\mbfkappa^A^mathalpha^omlmathbf^MATHEMATICAL BOLD SMALL KAPPA
1D6CC^𝛌^\mathbf{\lambda}^\mbflambda^A^mathalpha^omlmathbf^mathematical bold small lambda
1D6CD^𝛍^\mathbf{\mu}^\mbfmu^A^mathalpha^omlmathbf^MATHEMATICAL BOLD SMALL MU
1D6CE^𝛎^\mathbf{\nu}^\mbfnu^A^mathalpha^omlmathbf^MATHEMATICAL BOLD SMALL NU
1D6CF^𝛏^\mathbf{\xi}^\mbfxi^A^mathalpha^omlmathbf^MATHEMATICAL BOLD SMALL XI
1D6D0^𝛐^^\mbfomicron^A^mathalpha^^MATHEMATICAL BOLD SMALL OMICRON
1D6D1^𝛑^\mathbf{\pi}^\mbfpi^A^mathalpha^omlmathbf^MATHEMATICAL BOLD SMALL PI
1D6D2^𝛒^\mathbf{\rho}^\mbfrho^A^mathalpha^omlmathbf^MATHEMATICAL BOLD SMALL RHO
1D6D3^𝛓^\mathbf{\varsigma}^\mbfvarsigma^A^mathalpha^omlmathbf^MATHEMATICAL BOLD SMALL FINAL SIGMA
1D6D4^𝛔^\mathbf{\sigma}^\mbfsigma^A^mathalpha^omlmathbf^MATHEMATICAL BOLD SMALL SIGMA
1D6D5^𝛕^\mathbf{\tau}^\mbftau^A^mathalpha^omlmathbf^MATHEMATICAL BOLD SMALL TAU
1D6D6^𝛖^\mathbf{\upsilon}^\mbfupsilon^A^mathalpha^omlmathbf^MATHEMATICAL BOLD SMALL UPSILON
1D6D7^𝛗^\mathbf{\varphi}^\mbfvarphi^A^mathalpha^omlmathbf^MATHEMATICAL BOLD SMALL PHI
1D6D8^𝛘^\mathbf{\chi}^\mbfchi^A^mathalpha^omlmathbf^MATHEMATICAL BOLD SMALL CHI
1D6D9^𝛙^\mathbf{\psi}^\mbfpsi^A^mathalpha^omlmathbf^MATHEMATICAL BOLD SMALL PSI
1D6DA^𝛚^\mathbf{\omega}^\mbfomega^A^mathalpha^omlmathbf^MATHEMATICAL BOLD SMALL OMEGA
1D6DB^𝛛^^\mbfpartial^A^mathord^^MATHEMATICAL BOLD PARTIAL DIFFERENTIAL
1D6DC^𝛜^\mathbf{\epsilon}^\mbfvarepsilon^A^mathalpha^omlmathbf^MATHEMATICAL BOLD EPSILON SYMBOL
1D6DD^𝛝^\mathbf{\vartheta}^\mbfvartheta^A^mathalpha^omlmathbf^MATHEMATICAL BOLD THETA SYMBOL
1D6DE^𝛞^^\mbfvarkappa^A^mathalpha^^MATHEMATICAL BOLD KAPPA SYMBOL
1D6DF^𝛟^\mathbf{\phi}^\mbfphi^A^mathalpha^omlmathbf^MATHEMATICAL BOLD PHI SYMBOL
1D6E0^𝛠^\mathbf{\varrho}^\mbfvarrho^A^mathalpha^omlmathbf^MATHEMATICAL BOLD RHO SYMBOL
1D6E1^𝛡^\mathbf{\varpi}^\mbfvarpi^A^mathalpha^omlmathbf^MATHEMATICAL BOLD PI SYMBOL
1D6E2^𝛢^^\mitAlpha^A^mathalpha^^MATHEMATICAL ITALIC CAPITAL ALPHA
1D6E3^𝛣^^\mitBeta^A^mathalpha^^MATHEMATICAL ITALIC CAPITAL BETA
1D6E4^𝛤^\Gamma^\mitGamma^A^mathalpha^slantedGreek^= \mathit{\Gamma} (-fourier), = \varGamma (amsmath fourier), MATHEMATICAL ITALIC CAPITAL GAMMA
1D6E5^𝛥^\Delta^\mitDelta^A^mathalpha^slantedGreek^= \mathit{\Delta} (-fourier), = \varDelta (amsmath fourier), MATHEMATICAL ITALIC CAPITAL DELTA
1D6E6^𝛦^^\mitEpsilon^A^mathalpha^^MATHEMATICAL ITALIC CAPITAL EPSILON
1D6E7^𝛧^^\mitZeta^A^mathalpha^^MATHEMATICAL ITALIC CAPITAL ZETA
1D6E8^𝛨^^\mitEta^A^mathalpha^^MATHEMATICAL ITALIC CAPITAL ETA
1D6E9^𝛩^\Theta^\mitTheta^A^mathalpha^slantedGreek^= \mathit{\Theta} (-fourier), = \varTheta (amsmath fourier), MATHEMATICAL ITALIC CAPITAL THETA
1D6EA^𝛪^^\mitIota^A^mathalpha^^MATHEMATICAL ITALIC CAPITAL IOTA
1D6EB^𝛫^^\mitKappa^A^mathalpha^^MATHEMATICAL ITALIC CAPITAL KAPPA
1D6EC^𝛬^\Lambda^\mitLambda^A^mathalpha^slantedGreek^= \mathit{\Lambda} (-fourier), = \varLambda (amsmath fourier), mathematical italic capital lambda
1D6ED^𝛭^^\mitMu^A^mathalpha^^MATHEMATICAL ITALIC CAPITAL MU
1D6EE^𝛮^^\mitNu^A^mathalpha^^MATHEMATICAL ITALIC CAPITAL NU
1D6EF^𝛯^\Xi^\mitXi^A^mathalpha^slantedGreek^= \mathit{\Xi} (-fourier), = \varXi (amsmath fourier), MATHEMATICAL ITALIC CAPITAL XI
1D6F0^𝛰^^\mitOmicron^A^mathalpha^^MATHEMATICAL ITALIC CAPITAL OMICRON
1D6F1^𝛱^\Pi^\mitPi^A^mathalpha^slantedGreek^= \mathit{\Pi} (-fourier), = \varPi (amsmath fourier), MATHEMATICAL ITALIC CAPITAL PI
1D6F2^𝛲^^\mitRho^A^mathalpha^^MATHEMATICAL ITALIC CAPITAL RHO
1D6F3^𝛳^^\mitvarTheta^A^mathalpha^^MATHEMATICAL ITALIC CAPITAL THETA SYMBOL
1D6F4^𝛴^\Sigma^\mitSigma^A^mathalpha^slantedGreek^= \mathit{\Sigma} (-fourier), = \varSigma (amsmath fourier), MATHEMATICAL ITALIC CAPITAL SIGMA
1D6F5^𝛵^^\mitTau^A^mathalpha^^MATHEMATICAL ITALIC CAPITAL TAU
1D6F6^𝛶^\Upsilon^\mitUpsilon^A^mathalpha^slantedGreek^= \mathit{\Upsilon} (-fourier), = \varUpsilon (amsmath fourier), MATHEMATICAL ITALIC CAPITAL UPSILON
1D6F7^𝛷^\Phi^\mitPhi^A^mathalpha^slantedGreek^= \mathit{\Phi} (-fourier), = \varPhi (amsmath fourier), MATHEMATICAL ITALIC CAPITAL PHI
1D6F8^𝛸^^\mitChi^A^mathalpha^^MATHEMATICAL ITALIC CAPITAL CHI
1D6F9^𝛹^\Psi^\mitPsi^A^mathalpha^slantedGreek^= \mathit{\Psi} (-fourier), = \varPsi (amsmath fourier), MATHEMATICAL ITALIC CAPITAL PSI
1D6FA^𝛺^\Omega^\mitOmega^A^mathalpha^slantedGreek^= \mathit{\Omega} (-fourier), = \varOmega (amsmath fourier), MATHEMATICAL ITALIC CAPITAL OMEGA
1D6FB^𝛻^^\mitnabla^A^mathord^^MATHEMATICAL ITALIC NABLA
1D6FC^𝛼^\alpha^\mitalpha^A^mathalpha^^= \mathit{\alpha} (omlmathit), MATHEMATICAL ITALIC SMALL ALPHA
1D6FD^𝛽^\beta^\mitbeta^A^mathalpha^^= \mathit{\beta} (omlmathit), MATHEMATICAL ITALIC SMALL BETA
1D6FE^𝛾^\gamma^\mitgamma^A^mathalpha^^= \mathit{\gamma} (omlmathit), MATHEMATICAL ITALIC SMALL GAMMA
1D6FF^𝛿^\delta^\mitdelta^A^mathalpha^^= \mathit{\delta} (omlmathit), MATHEMATICAL ITALIC SMALL DELTA
1D700^𝜀^\varepsilon^\mitepsilon^A^mathalpha^^= \mathit{\varepsilon} (omlmathit), MATHEMATICAL ITALIC SMALL EPSILON
1D701^𝜁^\zeta^\mitzeta^A^mathalpha^^= \mathit{\zeta} (omlmathit), MATHEMATICAL ITALIC SMALL ZETA
1D702^𝜂^\eta^\miteta^A^mathalpha^^= \mathit{\eta} (omlmathit), MATHEMATICAL ITALIC SMALL ETA
1D703^𝜃^\theta^\mittheta^A^mathalpha^^= \mathit{\theta} (omlmathit), MATHEMATICAL ITALIC SMALL THETA
1D704^𝜄^\iota^\mitiota^A^mathalpha^^= \mathit{\iota} (omlmathit), MATHEMATICAL ITALIC SMALL IOTA
1D705^𝜅^\kappa^\mitkappa^A^mathalpha^^= \mathit{\kappa} (omlmathit), MATHEMATICAL ITALIC SMALL KAPPA
1D706^𝜆^\lambda^\mitlambda^A^mathalpha^^= \mathit{\lambda} (omlmathit), mathematical italic small lambda
1D707^𝜇^\mu^\mitmu^A^mathalpha^^= \mathit{\mu} (omlmathit), MATHEMATICAL ITALIC SMALL MU
1D708^𝜈^\nu^\mitnu^A^mathalpha^^= \mathit{\nu} (omlmathit), MATHEMATICAL ITALIC SMALL NU
1D709^𝜉^\xi^\mitxi^A^mathalpha^^= \mathit{\xi} (omlmathit), MATHEMATICAL ITALIC SMALL XI
1D70A^𝜊^^\mitomicron^A^mathalpha^^MATHEMATICAL ITALIC SMALL OMICRON
1D70B^𝜋^\pi^\mitpi^A^mathalpha^^= \mathit{\pi} (omlmathit), MATHEMATICAL ITALIC SMALL PI
1D70C^𝜌^\rho^\mitrho^A^mathalpha^^= \mathit{\rho} (omlmathit), MATHEMATICAL ITALIC SMALL RHO
1D70D^𝜍^\varsigma^\mitvarsigma^A^mathalpha^^= \mathit{\varsigma} (omlmathit), MATHEMATICAL ITALIC SMALL FINAL SIGMA
1D70E^𝜎^\sigma^\mitsigma^A^mathalpha^^= \mathit{\sigma} (omlmathit), MATHEMATICAL ITALIC SMALL SIGMA
1D70F^𝜏^\tau^\mittau^A^mathalpha^^= \mathit{\tau} (omlmathit), MATHEMATICAL ITALIC SMALL TAU
1D710^𝜐^\upsilon^\mitupsilon^A^mathalpha^^= \mathit{\upsilon} (omlmathit), MATHEMATICAL ITALIC SMALL UPSILON
1D711^𝜑^\varphi^\mitphi^A^mathalpha^^= \mathit{\varphi} (omlmathit), MATHEMATICAL ITALIC SMALL PHI
1D712^𝜒^\chi^\mitchi^A^mathalpha^^= \mathit{\chi} (omlmathit), MATHEMATICAL ITALIC SMALL CHI
1D713^𝜓^\psi^\mitpsi^A^mathalpha^^= \mathit{\psi} (omlmathit), MATHEMATICAL ITALIC SMALL PSI
1D714^𝜔^\omega^\mitomega^A^mathalpha^^= \mathit{\omega} (omlmathit), MATHEMATICAL ITALIC SMALL OMEGA
1D715^𝜕^\partial^\mitpartial^A^mathord^^= \mathit{\partial} (omlmathit), MATHEMATICAL ITALIC PARTIAL DIFFERENTIAL
1D716^𝜖^\epsilon^\mitvarepsilon^A^mathalpha^^= \mathit{\epsilon} (omlmathit), MATHEMATICAL ITALIC EPSILON SYMBOL
1D717^𝜗^\vartheta^\mitvartheta^A^mathalpha^^= \mathit{\vartheta} (omlmathit), MATHEMATICAL ITALIC THETA SYMBOL
1D718^𝜘^\varkappa^\mitvarkappa^A^mathalpha^amssymb^MATHEMATICAL ITALIC KAPPA SYMBOL
1D719^𝜙^\phi^\mitvarphi^A^mathalpha^^= \mathit{\phi} (omlmathit), MATHEMATICAL ITALIC PHI SYMBOL
1D71A^𝜚^\varrho^\mitvarrho^A^mathalpha^^= \mathit{\varrho} (omlmathit), MATHEMATICAL ITALIC RHO SYMBOL
1D71B^𝜛^\varpi^\mitvarpi^A^mathalpha^^= \mathit{\varpi} (omlmathit), MATHEMATICAL ITALIC PI SYMBOL
1D71C^𝜜^^\mbfitAlpha^A^mathalpha^^MATHEMATICAL BOLD ITALIC CAPITAL ALPHA
1D71D^𝜝^^\mbfitBeta^A^mathalpha^^MATHEMATICAL BOLD ITALIC CAPITAL BETA
1D71E^𝜞^\mathbfit{\Gamma}^\mbfitGamma^A^mathalpha^isomath^= \mathbold{\Gamma} (fixmath), MATHEMATICAL BOLD ITALIC CAPITAL GAMMA
1D71F^𝜟^\mathbfit{\Delta}^\mbfitDelta^A^mathalpha^isomath^= \mathbold{\Delta} (fixmath), MATHEMATICAL BOLD ITALIC CAPITAL DELTA
1D720^𝜠^^\mbfitEpsilon^A^mathalpha^^MATHEMATICAL BOLD ITALIC CAPITAL EPSILON
1D721^𝜡^^\mbfitZeta^A^mathalpha^^MATHEMATICAL BOLD ITALIC CAPITAL ZETA
1D722^𝜢^^\mbfitEta^A^mathalpha^^MATHEMATICAL BOLD ITALIC CAPITAL ETA
1D723^𝜣^\mathbfit{\Theta}^\mbfitTheta^A^mathalpha^isomath^= \mathbold{\Theta} (fixmath), MATHEMATICAL BOLD ITALIC CAPITAL THETA
1D724^𝜤^^\mbfitIota^A^mathalpha^^MATHEMATICAL BOLD ITALIC CAPITAL IOTA
1D725^𝜥^^\mbfitKappa^A^mathalpha^^MATHEMATICAL BOLD ITALIC CAPITAL KAPPA
1D726^𝜦^\mathbfit{\Lambda}^\mbfitLambda^A^mathalpha^isomath^= \mathbold{\Lambda} (fixmath), mathematical bold italic capital lambda
1D727^𝜧^^\mbfitMu^A^mathalpha^^MATHEMATICAL BOLD ITALIC CAPITAL MU
1D728^𝜨^^\mbfitNu^A^mathalpha^^MATHEMATICAL BOLD ITALIC CAPITAL NU
1D729^𝜩^\mathbfit{\Xi}^\mbfitXi^A^mathalpha^isomath^= \mathbold{\Xi} (fixmath), MATHEMATICAL BOLD ITALIC CAPITAL XI
1D72A^𝜪^^\mbfitOmicron^A^mathalpha^^MATHEMATICAL BOLD ITALIC CAPITAL OMICRON
1D72B^𝜫^\mathbfit{\Pi}^\mbfitPi^A^mathalpha^isomath^= \mathbold{\Pi} (fixmath), MATHEMATICAL BOLD ITALIC CAPITAL PI
1D72C^𝜬^^\mbfitRho^A^mathalpha^^MATHEMATICAL BOLD ITALIC CAPITAL RHO
1D72D^𝜭^^\mbfitvarTheta^A^mathalpha^^MATHEMATICAL BOLD ITALIC CAPITAL THETA SYMBOL
1D72E^𝜮^\mathbfit{\Sigma}^\mbfitSigma^A^mathalpha^isomath^= \mathbold{\Sigma} (fixmath), MATHEMATICAL BOLD ITALIC CAPITAL SIGMA
1D72F^𝜯^^\mbfitTau^A^mathalpha^^MATHEMATICAL BOLD ITALIC CAPITAL TAU
1D730^𝜰^\mathbfit{\Upsilon}^\mbfitUpsilon^A^mathalpha^isomath^= \mathbold{\Upsilon} (fixmath), MATHEMATICAL BOLD ITALIC CAPITAL UPSILON
1D731^𝜱^\mathbfit{\Phi}^\mbfitPhi^A^mathalpha^isomath^= \mathbold{\Phi} (fixmath), MATHEMATICAL BOLD ITALIC CAPITAL PHI
1D732^𝜲^^\mbfitChi^A^mathalpha^^MATHEMATICAL BOLD ITALIC CAPITAL CHI
1D733^𝜳^\mathbfit{\Psi}^\mbfitPsi^A^mathalpha^isomath^= \mathbold{\Psi} (fixmath), MATHEMATICAL BOLD ITALIC CAPITAL PSI
1D734^𝜴^\mathbfit{\Omega}^\mbfitOmega^A^mathalpha^isomath^= \mathbold{\Omega} (fixmath), MATHEMATICAL BOLD ITALIC CAPITAL OMEGA
1D735^𝜵^^\mbfitnabla^A^mathord^^MATHEMATICAL BOLD ITALIC NABLA
1D736^𝜶^\mathbfit{\alpha}^\mbfitalpha^A^mathalpha^isomath^= \mathbold{\alpha} (fixmath), MATHEMATICAL BOLD ITALIC SMALL ALPHA
1D737^𝜷^\mathbfit{\beta}^\mbfitbeta^A^mathalpha^isomath^= \mathbold{\beta} (fixmath), MATHEMATICAL BOLD ITALIC SMALL BETA
1D738^𝜸^\mathbfit{\gamma}^\mbfitgamma^A^mathalpha^isomath^= \mathbold{\gamma} (fixmath), MATHEMATICAL BOLD ITALIC SMALL GAMMA
1D739^𝜹^\mathbfit{\delta}^\mbfitdelta^A^mathalpha^isomath^= \mathbold{\delta} (fixmath), MATHEMATICAL BOLD ITALIC SMALL DELTA
1D73A^𝜺^\mathbfit{\varepsilon}^\mbfitepsilon^A^mathalpha^isomath^= \mathbold{\varepsilon} (fixmath), MATHEMATICAL BOLD ITALIC SMALL EPSILON
1D73B^𝜻^\mathbfit{\zeta}^\mbfitzeta^A^mathalpha^isomath^= \mathbold{\zeta} (fixmath), MATHEMATICAL BOLD ITALIC SMALL ZETA
1D73C^𝜼^\mathbfit{\eta}^\mbfiteta^A^mathalpha^isomath^= \mathbold{\eta} (fixmath), MATHEMATICAL BOLD ITALIC SMALL ETA
1D73D^𝜽^\mathbfit{\theta}^\mbfittheta^A^mathalpha^isomath^= \mathbold{\theta} (fixmath), MATHEMATICAL BOLD ITALIC SMALL THETA
1D73E^𝜾^\mathbfit{\iota}^\mbfitiota^A^mathalpha^isomath^= \mathbold{\iota} (fixmath), MATHEMATICAL BOLD ITALIC SMALL IOTA
1D73F^𝜿^\mathbfit{\kappa}^\mbfitkappa^A^mathalpha^isomath^= \mathbold{\kappa} (fixmath), MATHEMATICAL BOLD ITALIC SMALL KAPPA
1D740^𝝀^\mathbfit{\lambda}^\mbfitlambda^A^mathalpha^isomath^= \mathbold{\lambda} (fixmath), mathematical bold italic small lambda
1D741^𝝁^\mathbfit{\mu}^\mbfitmu^A^mathalpha^isomath^= \mathbold{\mu} (fixmath), MATHEMATICAL BOLD ITALIC SMALL MU
1D742^𝝂^\mathbfit{\nu}^\mbfitnu^A^mathalpha^isomath^= \mathbold{\nu} (fixmath), MATHEMATICAL BOLD ITALIC SMALL NU
1D743^𝝃^\mathbfit{\xi}^\mbfitxi^A^mathalpha^isomath^= \mathbold{\xi} (fixmath), MATHEMATICAL BOLD ITALIC SMALL XI
1D744^𝝄^^\mbfitomicron^A^mathalpha^^MATHEMATICAL BOLD ITALIC SMALL OMICRON
1D745^𝝅^\mathbfit{\pi}^\mbfitpi^A^mathalpha^isomath^= \mathbold{\pi} (fixmath), MATHEMATICAL BOLD ITALIC SMALL PI
1D746^𝝆^\mathbfit{\rho}^\mbfitrho^A^mathalpha^isomath^= \mathbold{\rho} (fixmath), MATHEMATICAL BOLD ITALIC SMALL RHO
1D747^𝝇^\mathbfit{\varsigma}^\mbfitvarsigma^A^mathalpha^isomath^= \mathbold{\varsigma} (fixmath), MATHEMATICAL BOLD ITALIC SMALL FINAL SIGMA
1D748^𝝈^\mathbfit{\sigma}^\mbfitsigma^A^mathalpha^isomath^= \mathbold{\sigma} (fixmath), MATHEMATICAL BOLD ITALIC SMALL SIGMA
1D749^𝝉^\mathbfit{\tau}^\mbfittau^A^mathalpha^isomath^= \mathbold{\tau} (fixmath), MATHEMATICAL BOLD ITALIC SMALL TAU
1D74A^𝝊^\mathbfit{\upsilon}^\mbfitupsilon^A^mathalpha^isomath^= \mathbold{\upsilon} (fixmath), MATHEMATICAL BOLD ITALIC SMALL UPSILON
1D74B^𝝋^\mathbfit{\varphi}^\mbfitphi^A^mathalpha^isomath^= \mathbold{\varphi} (fixmath), MATHEMATICAL BOLD ITALIC SMALL PHI
1D74C^𝝌^\mathbfit{\chi}^\mbfitchi^A^mathalpha^isomath^= \mathbold{\chi} (fixmath), MATHEMATICAL BOLD ITALIC SMALL CHI
1D74D^𝝍^\mathbfit{\psi}^\mbfitpsi^A^mathalpha^isomath^= \mathbold{\psi} (fixmath), MATHEMATICAL BOLD ITALIC SMALL PSI
1D74E^𝝎^\mathbfit{\omega}^\mbfitomega^A^mathalpha^isomath^= \mathbold{\omega} (fixmath), MATHEMATICAL BOLD ITALIC SMALL OMEGA
1D74F^𝝏^^\mbfitpartial^A^mathord^^MATHEMATICAL BOLD ITALIC PARTIAL DIFFERENTIAL
1D750^𝝐^\mathbfit{\epsilon}^\mbfitvarepsilon^A^mathalpha^isomath^= \mathbold{\epsilon} (fixmath), MATHEMATICAL BOLD ITALIC EPSILON SYMBOL
1D751^𝝑^\mathbfit{\vartheta}^\mbfitvartheta^A^mathalpha^isomath^= \mathbold{\vartheta} (fixmath), MATHEMATICAL BOLD ITALIC THETA SYMBOL
1D752^𝝒^^\mbfitvarkappa^A^mathalpha^^MATHEMATICAL BOLD ITALIC KAPPA SYMBOL
1D753^𝝓^\mathbfit{\phi}^\mbfitvarphi^A^mathalpha^isomath^= \mathbold{\phi} (fixmath), MATHEMATICAL BOLD ITALIC PHI SYMBOL
1D754^𝝔^\mathbfit{\varrho}^\mbfitvarrho^A^mathalpha^isomath^= \mathbold{\varrho} (fixmath), MATHEMATICAL BOLD ITALIC RHO SYMBOL
1D755^𝝕^\mathbfit{\varpi}^\mbfitvarpi^A^mathalpha^isomath^= \mathbold{\varpi} (fixmath), MATHEMATICAL BOLD ITALIC PI SYMBOL
1D756^𝝖^^\mbfsansAlpha^A^mathalpha^^MATHEMATICAL SANS-SERIF BOLD CAPITAL ALPHA
1D757^𝝗^^\mbfsansBeta^A^mathalpha^^MATHEMATICAL SANS-SERIF BOLD CAPITAL BETA
1D758^𝝘^\mathsfbf{\Gamma}^\mbfsansGamma^A^mathalpha^mathsfbf^MATHEMATICAL SANS-SERIF BOLD CAPITAL GAMMA
1D759^𝝙^\mathsfbf{\Delta}^\mbfsansDelta^A^mathalpha^mathsfbf^MATHEMATICAL SANS-SERIF BOLD CAPITAL DELTA
1D75A^𝝚^^\mbfsansEpsilon^A^mathalpha^^MATHEMATICAL SANS-SERIF BOLD CAPITAL EPSILON
1D75B^𝝛^^\mbfsansZeta^A^mathalpha^^MATHEMATICAL SANS-SERIF BOLD CAPITAL ZETA
1D75C^𝝜^^\mbfsansEta^A^mathalpha^^MATHEMATICAL SANS-SERIF BOLD CAPITAL ETA
1D75D^𝝝^\mathsfbf{\Theta}^\mbfsansTheta^A^mathalpha^mathsfbf^MATHEMATICAL SANS-SERIF BOLD CAPITAL THETA
1D75E^𝝞^^\mbfsansIota^A^mathalpha^^MATHEMATICAL SANS-SERIF BOLD CAPITAL IOTA
1D75F^𝝟^^\mbfsansKappa^A^mathalpha^^MATHEMATICAL SANS-SERIF BOLD CAPITAL KAPPA
1D760^𝝠^\mathsfbf{\Lambda}^\mbfsansLambda^A^mathalpha^mathsfbf^mathematical sans-serif bold capital lambda
1D761^𝝡^^\mbfsansMu^A^mathalpha^^MATHEMATICAL SANS-SERIF BOLD CAPITAL MU
1D762^𝝢^^\mbfsansNu^A^mathalpha^^MATHEMATICAL SANS-SERIF BOLD CAPITAL NU
1D763^𝝣^\mathsfbf{\Xi}^\mbfsansXi^A^mathalpha^mathsfbf^MATHEMATICAL SANS-SERIF BOLD CAPITAL XI
1D764^𝝤^^\mbfsansOmicron^A^mathalpha^^MATHEMATICAL SANS-SERIF BOLD CAPITAL OMICRON
1D765^𝝥^\mathsfbf{\Pi}^\mbfsansPi^A^mathalpha^mathsfbf^MATHEMATICAL SANS-SERIF BOLD CAPITAL PI
1D766^𝝦^^\mbfsansRho^A^mathalpha^^MATHEMATICAL SANS-SERIF BOLD CAPITAL RHO
1D767^𝝧^^\mbfsansvarTheta^A^mathalpha^^MATHEMATICAL SANS-SERIF BOLD CAPITAL THETA SYMBOL
1D768^𝝨^\mathsfbf{\Sigma}^\mbfsansSigma^A^mathalpha^mathsfbf^MATHEMATICAL SANS-SERIF BOLD CAPITAL SIGMA
1D769^𝝩^^\mbfsansTau^A^mathalpha^^MATHEMATICAL SANS-SERIF BOLD CAPITAL TAU
1D76A^𝝪^\mathsfbf{\Upsilon}^\mbfsansUpsilon^A^mathalpha^mathsfbf^MATHEMATICAL SANS-SERIF BOLD CAPITAL UPSILON
1D76B^𝝫^\mathsfbf{\Phi}^\mbfsansPhi^A^mathalpha^mathsfbf^MATHEMATICAL SANS-SERIF BOLD CAPITAL PHI
1D76C^𝝬^^\mbfsansChi^A^mathalpha^^MATHEMATICAL SANS-SERIF BOLD CAPITAL CHI
1D76D^𝝭^\mathsfbf{\Psi}^\mbfsansPsi^A^mathalpha^mathsfbf^MATHEMATICAL SANS-SERIF BOLD CAPITAL PSI
1D76E^𝝮^\mathsfbf{\Omega}^\mbfsansOmega^A^mathalpha^mathsfbf^MATHEMATICAL SANS-SERIF BOLD CAPITAL OMEGA
1D76F^𝝯^^\mbfsansnabla^A^mathord^^MATHEMATICAL SANS-SERIF BOLD NABLA
1D770^𝝰^\mathsfbf{\alpha}^\mbfsansalpha^A^mathalpha^omlmathsfbf^MATHEMATICAL SANS-SERIF BOLD SMALL ALPHA
1D771^𝝱^\mathsfbf{\beta}^\mbfsansbeta^A^mathalpha^omlmathsfbf^MATHEMATICAL SANS-SERIF BOLD SMALL BETA
1D772^𝝲^\mathsfbf{\gamma}^\mbfsansgamma^A^mathalpha^omlmathsfbf^MATHEMATICAL SANS-SERIF BOLD SMALL GAMMA
1D773^𝝳^\mathsfbf{\delta}^\mbfsansdelta^A^mathalpha^omlmathsfbf^MATHEMATICAL SANS-SERIF BOLD SMALL DELTA
1D774^𝝴^\mathsfbf{\varepsilon}^\mbfsansepsilon^A^mathalpha^omlmathsfbf^MATHEMATICAL SANS-SERIF BOLD SMALL EPSILON
1D775^𝝵^\mathsfbf{\zeta}^\mbfsanszeta^A^mathalpha^omlmathsfbf^MATHEMATICAL SANS-SERIF BOLD SMALL ZETA
1D776^𝝶^\mathsfbf{\eta}^\mbfsanseta^A^mathalpha^omlmathsfbf^MATHEMATICAL SANS-SERIF BOLD SMALL ETA
1D777^𝝷^\mathsfbf{\theta}^\mbfsanstheta^A^mathalpha^omlmathsfbf^MATHEMATICAL SANS-SERIF BOLD SMALL THETA
1D778^𝝸^\mathsfbf{\iota}^\mbfsansiota^A^mathalpha^omlmathsfbf^MATHEMATICAL SANS-SERIF BOLD SMALL IOTA
1D779^𝝹^\mathsfbf{\kappa}^\mbfsanskappa^A^mathalpha^omlmathsfbf^MATHEMATICAL SANS-SERIF BOLD SMALL KAPPA
1D77A^𝝺^\mathsfbf{\lambda}^\mbfsanslambda^A^mathalpha^omlmathsfbf^mathematical sans-serif bold small lambda
1D77B^𝝻^\mathsfbf{\mu}^\mbfsansmu^A^mathalpha^omlmathsfbf^MATHEMATICAL SANS-SERIF BOLD SMALL MU
1D77C^𝝼^\mathsfbf{\nu}^\mbfsansnu^A^mathalpha^omlmathsfbf^MATHEMATICAL SANS-SERIF BOLD SMALL NU
1D77D^𝝽^\mathsfbf{\xi}^\mbfsansxi^A^mathalpha^omlmathsfbf^MATHEMATICAL SANS-SERIF BOLD SMALL XI
1D77E^𝝾^^\mbfsansomicron^A^mathalpha^^MATHEMATICAL SANS-SERIF BOLD SMALL OMICRON
1D77F^𝝿^\mathsfbf{\pi}^\mbfsanspi^A^mathalpha^omlmathsfbf^MATHEMATICAL SANS-SERIF BOLD SMALL PI
1D780^𝞀^\mathsfbf{\rho}^\mbfsansrho^A^mathalpha^omlmathsfbf^MATHEMATICAL SANS-SERIF BOLD SMALL RHO
1D781^𝞁^\mathsfbf{\varsigma}^\mbfsansvarsigma^A^mathalpha^omlmathsfbf^MATHEMATICAL SANS-SERIF BOLD SMALL FINAL SIGMA
1D782^𝞂^\mathsfbf{\sigma}^\mbfsanssigma^A^mathalpha^omlmathsfbf^MATHEMATICAL SANS-SERIF BOLD SMALL SIGMA
1D783^𝞃^\mathsfbf{\tau}^\mbfsanstau^A^mathalpha^omlmathsfbf^MATHEMATICAL SANS-SERIF BOLD SMALL TAU
1D784^𝞄^\mathsfbf{\upsilon}^\mbfsansupsilon^A^mathalpha^omlmathsfbf^MATHEMATICAL SANS-SERIF BOLD SMALL UPSILON
1D785^𝞅^\mathsfbf{\varphi}^\mbfsansphi^A^mathalpha^omlmathsfbf^MATHEMATICAL SANS-SERIF BOLD SMALL PHI
1D786^𝞆^\mathsfbf{\chi}^\mbfsanschi^A^mathalpha^omlmathsfbf^MATHEMATICAL SANS-SERIF BOLD SMALL CHI
1D787^𝞇^\mathsfbf{\psi}^\mbfsanspsi^A^mathalpha^omlmathsfbf^MATHEMATICAL SANS-SERIF BOLD SMALL PSI
1D788^𝞈^\mathsfbf{\omega}^\mbfsansomega^A^mathalpha^omlmathsfbf^MATHEMATICAL SANS-SERIF BOLD SMALL OMEGA
1D789^𝞉^^\mbfsanspartial^A^mathord^^MATHEMATICAL SANS-SERIF BOLD PARTIAL DIFFERENTIAL
1D78A^𝞊^\mathsfbf{\epsilon}^\mbfsansvarepsilon^A^mathalpha^omlmathsfbf^MATHEMATICAL SANS-SERIF BOLD EPSILON SYMBOL
1D78B^𝞋^\mathsfbf{\vartheta}^\mbfsansvartheta^A^mathalpha^omlmathsfbf^MATHEMATICAL SANS-SERIF BOLD THETA SYMBOL
1D78C^𝞌^^\mbfsansvarkappa^A^mathalpha^^MATHEMATICAL SANS-SERIF BOLD KAPPA SYMBOL
1D78D^𝞍^\mathsfbf{\phi}^\mbfsansvarphi^A^mathalpha^omlmathsfbf^MATHEMATICAL SANS-SERIF BOLD PHI SYMBOL
1D78E^𝞎^\mathsfbf{\varrho}^\mbfsansvarrho^A^mathalpha^omlmathsfbf^MATHEMATICAL SANS-SERIF BOLD RHO SYMBOL
1D78F^𝞏^\mathsfbf{\varpi}^\mbfsansvarpi^A^mathalpha^omlmathsfbf^MATHEMATICAL SANS-SERIF BOLD PI SYMBOL
1D790^𝞐^^\mbfitsansAlpha^A^mathalpha^^MATHEMATICAL SANS-SERIF BOLD ITALIC CAPITAL ALPHA
1D791^𝞑^^\mbfitsansBeta^A^mathalpha^^MATHEMATICAL SANS-SERIF BOLD ITALIC CAPITAL BETA
1D792^𝞒^\mathsfbfit{\Gamma}^\mbfitsansGamma^A^mathalpha^isomath^MATHEMATICAL SANS-SERIF BOLD ITALIC CAPITAL GAMMA
1D793^𝞓^\mathsfbfit{\Delta}^\mbfitsansDelta^A^mathalpha^isomath^MATHEMATICAL SANS-SERIF BOLD ITALIC CAPITAL DELTA
1D794^𝞔^^\mbfitsansEpsilon^A^mathalpha^^MATHEMATICAL SANS-SERIF BOLD ITALIC CAPITAL EPSILON
1D795^𝞕^^\mbfitsansZeta^A^mathalpha^^MATHEMATICAL SANS-SERIF BOLD ITALIC CAPITAL ZETA
1D796^𝞖^^\mbfitsansEta^A^mathalpha^^MATHEMATICAL SANS-SERIF BOLD ITALIC CAPITAL ETA
1D797^𝞗^\mathsfbfit{\Theta}^\mbfitsansTheta^A^mathalpha^isomath^MATHEMATICAL SANS-SERIF BOLD ITALIC CAPITAL THETA
1D798^𝞘^^\mbfitsansIota^A^mathalpha^^MATHEMATICAL SANS-SERIF BOLD ITALIC CAPITAL IOTA
1D799^𝞙^^\mbfitsansKappa^A^mathalpha^^MATHEMATICAL SANS-SERIF BOLD ITALIC CAPITAL KAPPA
1D79A^𝞚^\mathsfbfit{\Lambda}^\mbfitsansLambda^A^mathalpha^isomath^mathematical sans-serif bold italic capital lambda
1D79B^𝞛^^\mbfitsansMu^A^mathalpha^^MATHEMATICAL SANS-SERIF BOLD ITALIC CAPITAL MU
1D79C^𝞜^^\mbfitsansNu^A^mathalpha^^MATHEMATICAL SANS-SERIF BOLD ITALIC CAPITAL NU
1D79D^𝞝^\mathsfbfit{\Xi}^\mbfitsansXi^A^mathalpha^isomath^MATHEMATICAL SANS-SERIF BOLD ITALIC CAPITAL XI
1D79E^𝞞^^\mbfitsansOmicron^A^mathalpha^^MATHEMATICAL SANS-SERIF BOLD ITALIC CAPITAL OMICRON
1D79F^𝞟^\mathsfbfit{\Pi}^\mbfitsansPi^A^mathalpha^isomath^MATHEMATICAL SANS-SERIF BOLD ITALIC CAPITAL PI
1D7A0^𝞠^^\mbfitsansRho^A^mathalpha^^MATHEMATICAL SANS-SERIF BOLD ITALIC CAPITAL RHO
1D7A1^𝞡^^\mbfitsansvarTheta^A^mathalpha^^MATHEMATICAL SANS-SERIF BOLD ITALIC CAPITAL THETA SYMBOL
1D7A2^𝞢^\mathsfbfit{\Sigma}^\mbfitsansSigma^A^mathalpha^isomath^MATHEMATICAL SANS-SERIF BOLD ITALIC CAPITAL SIGMA
1D7A3^𝞣^^\mbfitsansTau^A^mathalpha^^MATHEMATICAL SANS-SERIF BOLD ITALIC CAPITAL TAU
1D7A4^𝞤^\mathsfbfit{\Upsilon}^\mbfitsansUpsilon^A^mathalpha^isomath^MATHEMATICAL SANS-SERIF BOLD ITALIC CAPITAL UPSILON
1D7A5^𝞥^\mathsfbfit{\Phi}^\mbfitsansPhi^A^mathalpha^isomath^MATHEMATICAL SANS-SERIF BOLD ITALIC CAPITAL PHI
1D7A6^𝞦^^\mbfitsansChi^A^mathalpha^^MATHEMATICAL SANS-SERIF BOLD ITALIC CAPITAL CHI
1D7A7^𝞧^\mathsfbfit{\Psi}^\mbfitsansPsi^A^mathalpha^isomath^MATHEMATICAL SANS-SERIF BOLD ITALIC CAPITAL PSI
1D7A8^𝞨^\mathsfbfit{\Omega}^\mbfitsansOmega^A^mathalpha^isomath^MATHEMATICAL SANS-SERIF BOLD ITALIC CAPITAL OMEGA
1D7A9^𝞩^^\mbfitsansnabla^A^mathord^^MATHEMATICAL SANS-SERIF BOLD ITALIC NABLA
1D7AA^𝞪^\mathsfbfit{\alpha}^\mbfitsansalpha^A^mathalpha^isomath^MATHEMATICAL SANS-SERIF BOLD ITALIC SMALL ALPHA
1D7AB^𝞫^\mathsfbfit{\beta}^\mbfitsansbeta^A^mathalpha^isomath^MATHEMATICAL SANS-SERIF BOLD ITALIC SMALL BETA
1D7AC^𝞬^\mathsfbfit{\gamma}^\mbfitsansgamma^A^mathalpha^isomath^MATHEMATICAL SANS-SERIF BOLD ITALIC SMALL GAMMA
1D7AD^𝞭^\mathsfbfit{\delta}^\mbfitsansdelta^A^mathalpha^isomath^MATHEMATICAL SANS-SERIF BOLD ITALIC SMALL DELTA
1D7AE^𝞮^\mathsfbfit{\varepsilon}^\mbfitsansepsilon^A^mathalpha^isomath^MATHEMATICAL SANS-SERIF BOLD ITALIC SMALL EPSILON
1D7AF^𝞯^\mathsfbfit{\zeta}^\mbfitsanszeta^A^mathalpha^isomath^MATHEMATICAL SANS-SERIF BOLD ITALIC SMALL ZETA
1D7B0^𝞰^\mathsfbfit{\eta}^\mbfitsanseta^A^mathalpha^isomath^MATHEMATICAL SANS-SERIF BOLD ITALIC SMALL ETA
1D7B1^𝞱^\mathsfbfit{\theta}^\mbfitsanstheta^A^mathalpha^isomath^MATHEMATICAL SANS-SERIF BOLD ITALIC SMALL THETA
1D7B2^𝞲^\mathsfbfit{\iota}^\mbfitsansiota^A^mathalpha^isomath^MATHEMATICAL SANS-SERIF BOLD ITALIC SMALL IOTA
1D7B3^𝞳^\mathsfbfit{\kappa}^\mbfitsanskappa^A^mathalpha^isomath^MATHEMATICAL SANS-SERIF BOLD ITALIC SMALL KAPPA
1D7B4^𝞴^\mathsfbfit{\lambda}^\mbfitsanslambda^A^mathalpha^isomath^mathematical sans-serif bold italic small lambda
1D7B5^𝞵^\mathsfbfit{\mu}^\mbfitsansmu^A^mathalpha^isomath^MATHEMATICAL SANS-SERIF BOLD ITALIC SMALL MU
1D7B6^𝞶^\mathsfbfit{\nu}^\mbfitsansnu^A^mathalpha^isomath^MATHEMATICAL SANS-SERIF BOLD ITALIC SMALL NU
1D7B7^𝞷^\mathsfbfit{\xi}^\mbfitsansxi^A^mathalpha^isomath^MATHEMATICAL SANS-SERIF BOLD ITALIC SMALL XI
1D7B8^𝞸^^\mbfitsansomicron^A^mathalpha^^MATHEMATICAL SANS-SERIF BOLD ITALIC SMALL OMICRON
1D7B9^𝞹^\mathsfbfit{\pi}^\mbfitsanspi^A^mathalpha^isomath^MATHEMATICAL SANS-SERIF BOLD ITALIC SMALL PI
1D7BA^𝞺^\mathsfbfit{\rho}^\mbfitsansrho^A^mathalpha^isomath^MATHEMATICAL SANS-SERIF BOLD ITALIC SMALL RHO
1D7BB^𝞻^\mathsfbfit{\varsigma}^\mbfitsansvarsigma^A^mathalpha^isomath^MATHEMATICAL SANS-SERIF BOLD ITALIC SMALL FINAL SIGMA
1D7BC^𝞼^\mathsfbfit{\sigma}^\mbfitsanssigma^A^mathalpha^isomath^MATHEMATICAL SANS-SERIF BOLD ITALIC SMALL SIGMA
1D7BD^𝞽^\mathsfbfit{\tau}^\mbfitsanstau^A^mathalpha^isomath^MATHEMATICAL SANS-SERIF BOLD ITALIC SMALL TAU
1D7BE^𝞾^\mathsfbfit{\upsilon}^\mbfitsansupsilon^A^mathalpha^isomath^MATHEMATICAL SANS-SERIF BOLD ITALIC SMALL UPSILON
1D7BF^𝞿^\mathsfbfit{\varphi}^\mbfitsansphi^A^mathalpha^isomath^MATHEMATICAL SANS-SERIF BOLD ITALIC SMALL PHI
1D7C0^𝟀^\mathsfbfit{\chi}^\mbfitsanschi^A^mathalpha^isomath^MATHEMATICAL SANS-SERIF BOLD ITALIC SMALL CHI
1D7C1^𝟁^\mathsfbfit{\psi}^\mbfitsanspsi^A^mathalpha^isomath^MATHEMATICAL SANS-SERIF BOLD ITALIC SMALL PSI
1D7C2^𝟂^\mathsfbfit{\omega}^\mbfitsansomega^A^mathalpha^isomath^MATHEMATICAL SANS-SERIF BOLD ITALIC SMALL OMEGA
1D7C3^𝟃^^\mbfitsanspartial^A^mathord^^MATHEMATICAL SANS-SERIF BOLD ITALIC PARTIAL DIFFERENTIAL
1D7C4^𝟄^\mathsfbfit{\epsilon}^\mbfitsansvarepsilon^A^mathalpha^isomath^MATHEMATICAL SANS-SERIF BOLD ITALIC EPSILON SYMBOL
1D7C5^𝟅^\mathsfbfit{\vartheta}^\mbfitsansvartheta^A^mathalpha^isomath^MATHEMATICAL SANS-SERIF BOLD ITALIC THETA SYMBOL
1D7C6^𝟆^^\mbfitsansvarkappa^A^mathalpha^^MATHEMATICAL SANS-SERIF BOLD ITALIC KAPPA SYMBOL
1D7C7^𝟇^\mathsfbfit{\phi}^\mbfitsansvarphi^A^mathalpha^isomath^MATHEMATICAL SANS-SERIF BOLD ITALIC PHI SYMBOL
1D7C8^𝟈^\mathsfbfit{\varrho}^\mbfitsansvarrho^A^mathalpha^isomath^MATHEMATICAL SANS-SERIF BOLD ITALIC RHO SYMBOL
1D7C9^𝟉^\mathsfbfit{\varpi}^\mbfitsansvarpi^A^mathalpha^isomath^MATHEMATICAL SANS-SERIF BOLD ITALIC PI SYMBOL
1D7CA^𝟊^^\mbfDigamma^A^mathalpha^^MATHEMATICAL BOLD CAPITAL DIGAMMA
1D7CB^𝟋^^\mbfdigamma^A^mathalpha^^MATHEMATICAL BOLD SMALL DIGAMMA
1D7CE^𝟎^\mathbf{0}^^N^mathord^^mathematical bold digit 0
1D7CF^𝟏^\mathbf{1}^^N^mathord^^mathematical bold digit 1
1D7D0^𝟐^\mathbf{2}^^N^mathord^^mathematical bold digit 2
1D7D1^𝟑^\mathbf{3}^^N^mathord^^mathematical bold digit 3
1D7D2^𝟒^\mathbf{4}^^N^mathord^^mathematical bold digit 4
1D7D3^𝟓^\mathbf{5}^^N^mathord^^mathematical bold digit 5
1D7D4^𝟔^\mathbf{6}^^N^mathord^^mathematical bold digit 6
1D7D5^𝟕^\mathbf{7}^^N^mathord^^mathematical bold digit 7
1D7D6^𝟖^\mathbf{8}^^N^mathord^^mathematical bold digit 8
1D7D7^𝟗^\mathbf{9}^^N^mathord^^mathematical bold digit 9
1D7D8^𝟘^\mathbb{0}^\Bbbzero^N^mathord^bbold^mathematical double-struck digit 0
1D7D9^𝟙^\mathbb{1}^\Bbbone^N^mathord^bbold fourier^= \mathds{1} (dsfont), mathematical double-struck digit 1
1D7DA^𝟚^\mathbb{2}^\Bbbtwo^N^mathord^bbold^mathematical double-struck digit 2
1D7DB^𝟛^\mathbb{3}^\Bbbthree^N^mathord^bbold^mathematical double-struck digit 3
1D7DC^𝟜^\mathbb{4}^\Bbbfour^N^mathord^bbold^mathematical double-struck digit 4
1D7DD^𝟝^\mathbb{5}^\Bbbfive^N^mathord^bbold^mathematical double-struck digit 5
1D7DE^𝟞^\mathbb{6}^\Bbbsix^N^mathord^bbold^mathematical double-struck digit 6
1D7DF^𝟟^\mathbb{7}^\Bbbseven^N^mathord^bbold^mathematical double-struck digit 7
1D7E0^𝟠^\mathbb{8}^\Bbbeight^N^mathord^bbold^mathematical double-struck digit 8
1D7E1^𝟡^\mathbb{9}^\Bbbnine^N^mathord^bbold^mathematical double-struck digit 9
1D7E2^𝟢^\mathsf{0}^\msanszero^N^mathord^^mathematical sans-serif digit 0
1D7E3^𝟣^\mathsf{1}^\msansone^N^mathord^^mathematical sans-serif digit 1
1D7E4^𝟤^\mathsf{2}^\msanstwo^N^mathord^^mathematical sans-serif digit 2
1D7E5^𝟥^\mathsf{3}^\msansthree^N^mathord^^mathematical sans-serif digit 3
1D7E6^𝟦^\mathsf{4}^\msansfour^N^mathord^^mathematical sans-serif digit 4
1D7E7^𝟧^\mathsf{5}^\msansfive^N^mathord^^mathematical sans-serif digit 5
1D7E8^𝟨^\mathsf{6}^\msanssix^N^mathord^^mathematical sans-serif digit 6
1D7E9^𝟩^\mathsf{7}^\msansseven^N^mathord^^mathematical sans-serif digit 7
1D7EA^𝟪^\mathsf{8}^\msanseight^N^mathord^^mathematical sans-serif digit 8
1D7EB^𝟫^\mathsf{9}^\msansnine^N^mathord^^mathematical sans-serif digit 9
1D7EC^𝟬^\mathsfbf{0}^\mbfsanszero^N^mathord^mathsfbf^mathematical sans-serif bold digit 0
1D7ED^𝟭^\mathsfbf{1}^\mbfsansone^N^mathord^mathsfbf^mathematical sans-serif bold digit 1
1D7EE^𝟮^\mathsfbf{2}^\mbfsanstwo^N^mathord^mathsfbf^mathematical sans-serif bold digit 2
1D7EF^𝟯^\mathsfbf{3}^\mbfsansthree^N^mathord^mathsfbf^mathematical sans-serif bold digit 3
1D7F0^𝟰^\mathsfbf{4}^\mbfsansfour^N^mathord^mathsfbf^mathematical sans-serif bold digit 4
1D7F1^𝟱^\mathsfbf{5}^\mbfsansfive^N^mathord^mathsfbf^mathematical sans-serif bold digit 5
1D7F2^𝟲^\mathsfbf{6}^\mbfsanssix^N^mathord^mathsfbf^mathematical sans-serif bold digit 6
1D7F3^𝟳^\mathsfbf{7}^\mbfsansseven^N^mathord^mathsfbf^mathematical sans-serif bold digit 7
1D7F4^𝟴^\mathsfbf{8}^\mbfsanseight^N^mathord^mathsfbf^mathematical sans-serif bold digit 8
1D7F5^𝟵^\mathsfbf{9}^\mbfsansnine^N^mathord^mathsfbf^mathematical sans-serif bold digit 9
1D7F6^𝟶^\mathtt{0}^\mttzero^N^mathord^^mathematical monospace digit 0
1D7F7^𝟷^\mathtt{1}^\mttone^N^mathord^^mathematical monospace digit 1
1D7F8^𝟸^\mathtt{2}^\mtttwo^N^mathord^^mathematical monospace digit 2
1D7F9^𝟹^\mathtt{3}^\mttthree^N^mathord^^mathematical monospace digit 3
1D7FA^𝟺^\mathtt{4}^\mttfour^N^mathord^^mathematical monospace digit 4
1D7FB^𝟻^\mathtt{5}^\mttfive^N^mathord^^mathematical monospace digit 5
1D7FC^𝟼^\mathtt{6}^\mttsix^N^mathord^^mathematical monospace digit 6
1D7FD^𝟽^\mathtt{7}^\mttseven^N^mathord^^mathematical monospace digit 7
1D7FE^𝟾^\mathtt{8}^\mtteight^N^mathord^^mathematical monospace digit 8
1D7FF^𝟿^\mathtt{9}^\mttnine^N^mathord^^mathematical monospace digit 9
