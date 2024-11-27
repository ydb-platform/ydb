/* ytfile can not */
PRAGMA package("project.package", "yt://plato/package");
PRAGMA override_library("project/package/detail/bar.sql");
IMPORT pkg.project.package.total SYMBOLS $do_total;

SELECT
    $do_total(1);
