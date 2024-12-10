/* ytfile can not */
DECLARE $cluster AS String;

PRAGMA package("project.package", "yt://{$cluster}/package");

IMPORT pkg.project.package.total SYMBOLS $do_total;

SELECT
    $do_total(1)
;
