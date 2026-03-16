from django.shortcuts import redirect
from django.views.decorators.csrf import csrf_exempt

from localshop.apps.packages import xmlrpc


@csrf_exempt
def index(request):
    if request.method == 'POST':
        return xmlrpc.handle_request(request)
    return redirect('dashboard:index')
