/* syntax version 1 */
SELECT
    Unicode::Strip("ываыва"u),
    Unicode::Strip(" ячсячсяаачы"u),
    Unicode::Strip("аавыаываыва "u),
    Unicode::Strip("аав ыа ыва ыва "u),
    Unicode::Strip("\u2009ыва\n"u),
    Unicode::Strip("\u200aваоао\u2002"u),
    Unicode::Strip(""u)
