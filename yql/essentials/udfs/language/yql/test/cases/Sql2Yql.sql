$cfg = @@
Yt {
    ClusterMapping {
        Name: "plato"
    }
}
SqlCore {
    ExtendedTranslationFlags {
        Name: "ForceYqlSelect"
    }
}
@@;

SELECT
    YqlLang::Sql2Yql("SELECT 1;", "", ""),
    YqlLang::Sql2Yql("SELECT 1;", "2026.02", ""),
    YqlLang::Sql2Yql("SELECT 1;", "2026.02", $cfg),
    YqlLang::Sql2Yql("SELECT * FROM plato.x", "2026.02", $cfg),
    YqlLang::Sql2Yql("SELECT * FROM plato.x", "2025.02", $cfg),
    YqlLang::Sql2Yql("SELECT * FROM plato.x GROUP COMPACT BY a", "2026.02", $cfg),
;
