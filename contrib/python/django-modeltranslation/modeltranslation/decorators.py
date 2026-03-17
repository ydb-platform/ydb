def register(model_or_iterable, **options):
    """
    Registers the given model(s) with the given translation options.

    The model(s) should be Model classes, not instances.

    Fields declared for translation on a base class are inherited by
    subclasses. If the model or one of its subclasses is already
    registered for translation, this will raise an exception.

    @register(Author)
    class AuthorTranslation(TranslationOptions):
        pass
    """
    from modeltranslation.translator import translator, TranslationOptions

    def wrapper(opts_class):
        if not issubclass(opts_class, TranslationOptions):
            raise ValueError("Wrapped class must subclass TranslationOptions.")
        translator.register(model_or_iterable, opts_class, **options)
        return opts_class

    return wrapper
