# HTML5 makes many end tags optional or forbidden (void elements)
void = set("area base br col embed hr img input keygen link menuitem meta param source track wbr".split())
optional = set("html head body p colgroup thead tbody tfoot tr th td li dt dd optgroup option".split())
omit_endtag = void | optional
assert not void & optional


# HTML elements that cannot have content (list incomplete, not used so far)
# - We could automatically close these when opened
empty = set("iframe".split())
assert not empty & omit_endtag
