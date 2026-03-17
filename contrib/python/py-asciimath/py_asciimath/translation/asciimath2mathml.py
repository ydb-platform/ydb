from ..asciimath import get_symbols_for

unary_functions = get_symbols_for("unary_functions", "mathml")
binary_functions = get_symbols_for("binary_functions", "mathml")
left_parenthesis = get_symbols_for("left_parenthesis", "mathml")
right_parenthesis = get_symbols_for("right_parenthesis", "mathml")
colors = get_symbols_for("colors", "mathml")

smb = get_symbols_for("misc_symbols", "mathml")
smb.update(colors)
smb.update(get_symbols_for("function_symbols", "mathml"))
smb.update(get_symbols_for("relation_symbols", "mathml"))
smb.update(get_symbols_for("logical_symbols", "mathml"))
smb.update(get_symbols_for("operation_symbols", "mathml"))
smb.update(get_symbols_for("greek_letters", "mathml"))
smb.update(get_symbols_for("arrows", "mathml"))
smb = dict(sorted(smb.items(), key=lambda x: (-len(x[0]), x[0])))
