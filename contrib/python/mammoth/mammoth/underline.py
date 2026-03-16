from . import html


def element(name):
    def convert_underline(nodes):
        return [html.collapsible_element(name, {}, nodes)]
        
    return convert_underline
