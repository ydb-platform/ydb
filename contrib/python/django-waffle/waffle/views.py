from django.http import HttpResponse, JsonResponse
from django.template import loader
from django.views.decorators.cache import never_cache

from waffle import get_waffle_flag_model, get_waffle_switch_model, get_waffle_sample_model
from waffle.utils import get_setting


@never_cache
def wafflejs(request):
    return HttpResponse(_generate_waffle_js(request),
                        content_type='application/x-javascript')


def _generate_waffle_js(request):
    flags = get_waffle_flag_model().get_all()
    flag_values = [(f.name, f.is_active(request)) for f in flags]

    switches = get_waffle_switch_model().get_all()
    switch_values = [(s.name, s.is_active()) for s in switches]

    samples = get_waffle_sample_model().get_all()
    sample_values = [(s.name, s.is_active()) for s in samples]

    return loader.render_to_string('waffle/waffle.js', {
        'flags': flag_values,
        'switches': switch_values,
        'samples': sample_values,
        'flag_default': get_setting('FLAG_DEFAULT'),
        'switch_default': get_setting('SWITCH_DEFAULT'),
        'sample_default': get_setting('SAMPLE_DEFAULT'),
    })


@never_cache
def waffle_json(request):
    return JsonResponse(_generate_waffle_json(request))


def _generate_waffle_json(request):
    flags = get_waffle_flag_model().get_all()
    flag_values = {
        f.name: {
            'is_active': f.is_active(request),
            'last_modified': f.modified,
        }
        for f in flags
    }

    switches = get_waffle_switch_model().get_all()
    switch_values = {
        s.name: {
            'is_active': s.is_active(),
            'last_modified': s.modified,
        }
        for s in switches
    }

    samples = get_waffle_sample_model().get_all()
    sample_values = {
        s.name: {
            'is_active': s.is_active(),
            'last_modified': s.modified,
        }
        for s in samples
    }

    return {
        'flags': flag_values,
        'switches': switch_values,
        'samples': sample_values,
    }
