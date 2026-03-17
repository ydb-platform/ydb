from ..asciimath import get_symbols_for

unary_functions = get_symbols_for("unary_functions", "latex")
binary_functions = get_symbols_for("binary_functions", "latex")
left_parenthesis = get_symbols_for("left_parenthesis", "latex")
right_parenthesis = get_symbols_for("right_parenthesis", "latex")

smb = get_symbols_for("misc_symbols", "latex")
smb.update(get_symbols_for("colors", "latex"))
smb.update(get_symbols_for("function_symbols", "latex"))
smb.update(get_symbols_for("relation_symbols", "latex"))
smb.update(get_symbols_for("logical_symbols", "latex"))
smb.update(get_symbols_for("operation_symbols", "latex"))
smb.update(get_symbols_for("greek_letters", "latex"))
smb.update(get_symbols_for("arrows", "latex"))
smb = dict(sorted(smb.items(), key=lambda x: (-len(x[0]), x[0])))
