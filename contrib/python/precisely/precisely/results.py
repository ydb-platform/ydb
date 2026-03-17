import collections


Result = collections.namedtuple("_Result", ["is_match", "explanation"])


def matched():
    return Result(True, None)


def unmatched(explanation):
    return Result(False, explanation)


def indented_list(items, bullet=None):
    if bullet is None:
        bullet = lambda index: "*"
            
    
    def format_item(index, item):
        prefix = " {0} ".format(bullet(index))
        return "\n{0}{1}".format(prefix, indent(item, width=len(prefix)))
    
    return "".join(
        format_item(index, item)
        for index, item in enumerate(items)
    )


def indexed_indented_list(items):
    return indented_list(items, bullet=lambda index: "{0}:".format(index))


def indent(text, width=None):
    if width is None:
        width = 2
        
    return text.replace("\n", "\n" + " " * width)
