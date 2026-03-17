from django.shortcuts import render
from django.http import HttpResponse
from django.utils.safestring import mark_safe
import datetime
from .models import *
import simplejson
def main(request):
    if request.method=="GET":
        models=request.session.get("models",None)
        if not models:
            models = [s['table'] for s in History.objects.values("table").distinct()]
            request.session["models"]=models
        res={"models": models}
        return render(request,"main.html",res)
    if request.method=="POST":
        id = request.POST["id"]
        table = request.POST["table"]
        models = request.session.get("models", None)
        res=fetchChanges(id,table)
        res["models"]=models
        return render(request,"main.html",res)
def get(lst,index,default):
    if index<len(lst): return lst[index]
    return default

def handle_dict(old_state,new_state,recursion=False):
    res = ""
    for key in old_state:
        if key == "_type":
            if old_state[key] == "datetime":
                if datetime.datetime.strptime(old_state["value"], "%Y-%m-%d %H:%M:%S").strftime(
                        "%Y-%m-%dT%H:%M:%SZ") != new_state:
                    res += "<li> %s ----> %s</li>" % (old_state.get("value"), new_state)
            elif old_state[key] == "date":
                if datetime.datetime.strptime(old_state["value"], "%Y-%m-%d").strftime("%Y-%m-%d") != new_state:
                    res += "<li> %s ----> %s</li>" % (old_state.get("value"), new_state)
            break
        elif new_state is None:
            res += "<li> %s ----> NONE</li>" % (old_state.get(key))
        elif old_state[key] != new_state.get(key, None):
            if type(old_state[key]) in [type({}), type([])]:
                try:
                    c = findChanges(old_state.get(key, {}), new_state.get(key, {}))
                    if c != "<ul></ul>":
                        res += "<li>" + c + "</li>"
                except:
                    pass
            else:
                res += "<li>%s:: %s ----> %s</li>" % (key, old_state.get(key, None), new_state.get(key, None))
    if recursion or type(new_state) != type({}): return res
    for key in new_state:
        print("Key => ", key)
        print("new_state => ", new_state)
        if old_state.get("_type"):
            if old_state["_type"] == "datetime":
                if datetime.datetime.strptime(old_state["value"], "%Y-%m-%d %H:%M:%S").strftime(
                        "%Y-%m-%dT%H:%M:%SZ") != datetime.datetime.strptime(new_state,
                                                                            "%Y-%m-%d %H:%M:%S").strftime(
                        "%Y-%m-%dT%H:%M:%SZ"):
                    res += "<li> %s ----> %s</li>" % (old_state.get("value"), new_state)
            elif old_state["_type"] == "date":
                if datetime.datetime.strptime(old_state["value"], "%Y-%m-%d").strftime("%Y-%m-%d") != new_state:
                    res += "<li> %s ----> %s</li>" % (old_state.get("value"), new_state)
            break
        elif old_state is None:
            res += "<li> NONE ----> %s</li>" % (new_state.get(key))

        elif new_state[key] != old_state.get(key, None):
            if type(new_state[key]) in [type({}), type([])]:
                try:
                    c = findChanges(old_state.get(key, {}), new_state.get(key, {}), True)
                    if c != "<ul></ul>":
                        res += "<li>" + c + "</li>"

                except:
                    pass
            else:
                res += "<li>%s:: %s ----> %s</li>" % (key, old_state.get(key, None), new_state.get(key, None))
    return res

def findChanges(old_state,new_state):
    res="<ul>"
    if type(old_state)==type({}):
        res+=handle_dict(old_state,new_state)
    elif type(old_state)==type([]):
        for key in range(len(old_state)):
            if old_state[key] != get(new_state,key,None):
                if type(old_state[key]) in [type({}), type([])]:
                    try:
                        res += findChanges(old_state[key], new_state[key])
                    except:
                        pass
                else:
                    res += "<li>%s:: %s ----> %s</li>" % (key, old_state[key], get(new_state,key,None))
    return res+"</ul>"


def fetchChanges(id,table):
    changes = History.objects.filter(primary_key=id, table=table).order_by("-id")
    rows = []
    for change in changes:
        row = {}
        row["event_time"] = change.done_on
        row["by"] = change.done_by
        row["changes"] = []
        row["name"]=change.name
        row["id"]=change.id
        for key in change.new_state.keys():
            if type(change.new_state[key]) ==type({}) and change.new_state[key].get("_type",None)!=None:

                if change.new_state[key]["_type"]=="datetime":
                    try:
                        change.new_state[key]=datetime.datetime.strptime(change.new_state[key]["value"],"%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%dT%H:%M:%SZ")
                    except ValueError as exp:
                        change.new_state[key]=datetime.datetime.strptime(change.new_state[key]["value"],"%Y-%m-%d").strftime("%Y-%m-%d")
                elif change.new_state[key]["_type"]=="date":
                    change.new_state[key]=datetime.datetime.strptime(change.new_state[key]["value"],"%Y-%m-%d").date().strftime("%Y-%m-%d")
            if change.old_state.get(key, None) != change.new_state.get(key, None):
                if type(change.old_state.get(key, None)) in [type({}), type([])]:
                    keyChanges = findChanges(change.old_state[key], change.new_state[key])
                    if keyChanges!="<ul></ul>":
                        text = "%s: <br/>" % key
                        text += keyChanges
                        row["changes"].append(mark_safe(text))
                else:
                    row["changes"].append(
                        "%s: %s ----> %s" % (key, change.old_state.get(key, None), change.new_state[key]))
        rows.append(row)
    count = len(rows)
    res = {"count": count, "changes": rows, "id": id, "selected_model": table}
    return res

def getChanges(request):
    id = request.GET["id"]
    table = request.GET["table"]
    res=fetchChanges(id,table)
    return HttpResponse(simplejson.dumps(res))

def showChanges(request):
    table = request.POST["table"]
    primary_key = request.POST["id"]
    res = fetchChanges(primary_key, table)
    return render(request,"changes.html", res)



def revert(request,id=None):
    from django.core import management
    if id is None:
        id =request.GET["id"]
    management.call_command('restoreObject',id=id,user=request.user.username)
    return HttpResponse("Done, Please Go <a href='javascript:void(0)' onclick='history.back()'>Back</a> and refresh")