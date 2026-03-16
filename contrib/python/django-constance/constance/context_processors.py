import constance


def config(request):
    """
    Simple context processor that puts the config into every
    RequestContext. Just make sure you have a setting like this:

        TEMPLATE_CONTEXT_PROCESSORS = (
            # ...
            'constance.context_processors.config',
        )

    """
    return {"config": constance.config}
