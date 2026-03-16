from django.core.management.base import BaseCommand, CommandError
import __future__
from ModelTracker.models import History
import datetime
from django.apps import apps
def getModel(table_name):
    return next((m for m in apps.get_models() if m._meta.db_table==table_name), None)

class Command(BaseCommand):
    help = 'Restore Object to old status'

    def add_arguments(self, parser):
        parser.add_argument('--id', nargs='?', type=str,default=None)
        parser.add_argument("--state",type=str,nargs='?',default="new")
        parser.add_argument("--user",type=str,nargs='?',default="CLI")
    def handle(self, *args, **options):
        if not options.get("id",None):
            print ("Change ID is needed")
            exit(1)
        print (options)
        h = History.objects.get(id=int(options["id"]))
        model = getModel(h.table)
        if model == None:
            print("Can't find the Model")
            exit(2)
        d=[f.name for f in model._meta.get_fields()]
        if options["state"]=="old": state=h.old_state
        else: state=h.new_state
        keys2del=[]
        for key in state:
            if (key.startswith("_") and "_cache" in key) or (key not in d and not ("_id" in key and key[:-3] in d)):
                keys2del.append(key)
            if type(state[key])==type({}):
                if state[key].get("_type",None) == "datetime":
                    state[key]=datetime.datetime.strptime(state[key]["value"],"%Y-%m-%d %H:%M:%S")
                elif state[key].get("_type",None) == "date":
                    state[key]=datetime.datetime.strptime(state[key]["value"],"%Y-%m-%d")
        for key in keys2del:
            del state[key]

        print(state)
        m=model(**state)
        m.save(options["user"],event_name="Restore Record to %s (%s)"%(options["id"],options["state"]))




