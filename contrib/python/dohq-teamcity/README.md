# dohq-teamcity
[![docs](https://img.shields.io/badge/docs-published-brightgreen.svg)](https://devopshq.github.io/teamcity/)
[![build](https://travis-ci.org/devopshq/teamcity.svg?branch=master)](https://travis-ci.org/devopshq/teamcity)
[![pypi](https://img.shields.io/pypi/v/dohq-teamcity.svg)](https://pypi.python.org/pypi/dohq-teamcity)
[![license](https://img.shields.io/pypi/l/dohq-teamcity.svg)](https://github.com/devopshq/teamcity/blob/master/LICENSE)

`dohq-teamcity` is a Python package providing access to the JetBrains TeamCity server API. This library support ALL [TeamCity API](https://confluence.jetbrains.com/display/TCD10/REST+API) methods, if you don't find some - create issue, please.

## Installation
```
# Latest release
pip install dohq-teamcity

# Develop branch
git clone https://github.com/devopshq/teamcity
cd teamcity
python setup.py install
```


## Usage

```python
from dohq_teamcity import TeamCity

# username/password authentication
tc = TeamCity("https://teamcity.example.com", auth=('username', 'password'))

# list all the projects
projects = tc.projects.get_projects()
# OR
# projects = tc.project_api.get_projects()
for project in projects:
   print(project)

# get the group with name = groupname
group = tc.group.get('name:groupname')
print(group)

# get the user with name = username
user = tc.user.get('username:devopshq')
print(user)

# create a new user and delete
from dohq_teamcity import User
new_user = User(name='New user', username='new_user')
new_user = tc.users.create_user(body=new_user)
new_user.delete()

# other way - create object, connect with exist instance and load it
import dohq_teamcity
bt = dohq_teamcity.BuildType(id='MyBuildTypeId', teamcity=tc)
bt = bt.read()
```

## What next?
See more examples and full documantation on page: https://devopshq.github.io/teamcity

## How to release?
1. Bump version in `dohq_teamcity/version.py`
2. Merge changes to **master** branch
3. Create Github Release
