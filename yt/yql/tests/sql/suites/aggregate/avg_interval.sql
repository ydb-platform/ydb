/* syntax version 1 */
discard select EnsureType(avg(cast(key As Interval)), Interval?) from plato.Input;
