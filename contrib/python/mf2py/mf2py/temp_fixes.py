def rm_templates(doc):
    for el in doc.find_all("template"):
        el.extract()
