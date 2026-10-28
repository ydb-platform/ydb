Program nodes provide some optimizations that cannot be achieved via plain kernel execution.

### Shortcuts for one-value arrays

Our Coalesce/And/Or wrappers recognize array inputs in which all positions have the same value (`CheckOneValueAccessor`) and may return result immediately without calling the arrow kernel if it can be deduced for such input (for example, `FALSE AND <xxx>`).
This shortcut also avoids unwrapping dictionary and sparsed arrays that is otherwise required because relevant kernels require plain arrays.
