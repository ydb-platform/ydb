from ..latex import get_symbols_for

unary_functions = get_symbols_for("unary_functions", "asciimath")
binary_functions = get_symbols_for("binary_functions", "asciimath")
left_parenthesis = get_symbols_for("left_parenthesis", "asciimath")
right_parenthesis = get_symbols_for("right_parenthesis", "asciimath")

smb = get_symbols_for("misc_symbols", "asciimath")
smb.update(get_symbols_for("function_symbols", "asciimath"))
smb.update(get_symbols_for("colors", "asciimath"))
smb.update(get_symbols_for("relation_symbols", "asciimath"))
smb.update(get_symbols_for("logical_symbols", "asciimath"))
smb.update(get_symbols_for("operation_symbols", "asciimath"))
smb.update(get_symbols_for("greek_letters", "asciimath"))
smb.update(get_symbols_for("arrows", "asciimath"))
smb = dict(sorted(smb.items(), key=lambda x: (-len(x[0]), x[0])))
