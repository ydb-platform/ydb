# Facebook Business SDK for Python

[![PyPI](https://img.shields.io/pypi/v/facebook-business)](https://pypi.org/project/facebook-business/)
[![Build Status](https://img.shields.io/github/actions/workflow/status/facebook/facebook-python-business-sdk/ci.yml)](https://github.com/facebook/facebook-python-business-sdk/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Facebook%20Platform-blue.svg?style=flat-square)](https://github.com/facebook/facebook-python-business-sdk/blob/main/LICENSE.txt)

### Introduction

The Facebook <a href="https://developers.facebook.com/docs/business-sdk" target="_blank">Business SDK</a> is a one-stop-shop to help our partners better serve their businesses. Partners are using multiple Facebook API's to serve the needs of their clients. Adopting all these API's and keeping them up to date across the various platforms can be time consuming and ultimately prohibitive. For this reason Facebook has developed the Business SDK bundling many of its APIs into one SDK to ease implementation and upkeep. The Business SDK is an upgraded version of the Marketing API SDK that includes the Marketing API as well as many Facebook APIs from different platforms such as Pages, Business Manager, Instagram, etc.

## Quick Start

Business SDK <a href="https://developers.facebook.com/docs/business-sdk/getting-started" target="_blank">Getting Started Guide</a>

Python is currently the most popular language for our third-party developers. `facebook_business` is a Python package that provides an interface between your Python application and <a href="https://developers.facebook.com/docs/business-sdk/reference" target="_blank">Facebook's APIs within the Business SDK</a>. This tutorial covers the basic knowledge needed to use the SDK and provides some exercises for the reader.

**NOTE**: ``facebook_business`` package is compatible with Python 2 and 3!

## Pre-requisites

### Register An App

To get started with the SDK, you must have an app
registered on <a href="https://developers.facebook.com/" target="_blank">developers.facebook.com</a>.

To manage the Marketing API, please visit your
<a href="https://developers.facebook.com/apps/<YOUR APP ID>/dashboard"> App Dashboard </a>
and add the <b>Marketing API</b> product to your app.

**IMPORTANT**: For security, it is recommended that you turn on 'App Secret
Proof for Server API calls' in your app's Settings->Advanced page.

### Obtain An Access Token

When someone connects with an app using Facebook Login and approves the request
for permissions, the app obtains an access token that provides temporary, secure
access to Facebook APIs.

An access token is an opaque string that identifies a User, app, or Page.

For example, to access the Marketing API, you need to generate a user access token
for your app and ask for the ``ads_management`` permission; to access Pages API,
you need to generate a Page access token for your app and ask for the ``manage_page`` permission.

Refer to our
<a href="https://developers.facebook.com/docs/facebook-login/access-tokens" target="_blank">
Access Token Guide</a> to learn more.

For now, we can use the
<a href="https://developers.facebook.com/tools/explorer" target="_blank">Graph Explorer</a>
to get an access token.

## Install package

The easiest way to install the SDK is via ``pip`` in your shell.

**NOTE**: For Python 3, use ``pip3`` and ``python3`` instead.

**NOTE**: Use ``sudo`` if any of these complain about permissions. (This might
happen if you are using a system installed Python.)

If you don't have pip:

```
easy_install pip
```

Now execute when you have pip:

```
pip install facebook_business
```

If you care for the latest version instead of a possibly outdated version in the
<a href="https://pypi.python.org" target="_blank">pypi.python.org</a> repository,
<a href="https://github.com/facebook/facebook-python-business-sdk">check out the
repository from GitHub or download a release tarball</a>. Once you've got the
package downloaded and unzipped, install it:

```
python setup.py install
```

Great, now you are ready to use the SDK!

## Bootstrapping

### Create test.py
Create a test.py file with the contents below (assuming your system is using python 2.7 and installed under /opt/homebrew. Update to your proper python location.):

```python
import sys
sys.path.append('/opt/homebrew/lib/python2.7/site-packages') # Replace this with the place you installed facebookads using pip
sys.path.append('/opt/homebrew/lib/python2.7/site-packages/facebook_business-3.0.0-py2.7.egg-info') # same as above

from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount

my_app_id = 'your-app-id'
my_app_secret = 'your-appsecret'
my_access_token = 'your-page-access-token'
FacebookAdsApi.init(my_app_id, my_app_secret, my_access_token)
my_account = AdAccount('act_<your-adaccount-id>')
campaigns = my_account.get_campaigns()
print(campaigns)
```

### Test Your Install
Test your install with the following command:
```python
python test.py
```
You should see the result in your terminal window. If it complains about an expired token, repeat the process for requesting a Page Access Token described in the prerequisites section above.

**NOTE**: We shall use the objects module throughout the rest of the tutorial. You can
also use the individual class files under adobjects directly.


## Understanding CRUD

The SDK implements a CRUD (create, read, update, delete) design. Objects
relevant to exploring the graph are located in the objects module of the
facebook_business package.

All objects on the graph are instances of ``AbstractObject``. Some objects can
be directly queried and thus are instances of ``AbstractCrudObject`` (a subclass
of ``AbstractObject``). Both these abstract classes are located in
``facebook_business.adobjects``.

There is and additional folder ``adobjects`` under facebook_business. Under this you will see a file for every ad object
in our Marketing API. These files are autogenerated from our API and therefore
are close in parity with what API has to offer. Based on what CRUD operations can be
performed on each object, you will see the presence of the following methods in them:

* ``api_get``
* ``api_update``
* ``api_delete``
* ``create_xxx``
* ``get_xxx``

For example, Campaign has all these methods but AdAccount does not. Read the
Marketing API documentation for more information about
<a href="https://developers.facebook.com/docs/marketing-api/reference" target="_blank">how different ad
objects are used</a>.

There are some deprecated function in ``AbstractCrudObject``, like
* ``remote_create``
* ``remote_read``
* ``remote_update``
* ``remote_delete``

Please try to stop use them since we may plan to deprecated them soon.

## Exploring the Graph

The way the SDK abstracts the API is by defining classes that represent objects
on the graph. These class definitions and their helpers are located in
``facebook_business.adobjects``.

### Initializing Objects

Look at ``AbstractObject``'s and ``AbstractCrudObject``'s ``__init__`` method
for more information. Most objects on the graph subclass from one of the two.

When instantiating an ad object, you can specify its id if it already exists by
defining ``fbid`` argument. Also, if you want to interact with the
API using a specific api object instead of the default, you can specify the
``api`` argument.

### Edges

Look at the methods of an object to see what associations over which we can
iterate. For example an ``User`` object has a method ``get_ad_accounts`` which
returns an iterator of ``AdAccount`` objects.

### Ad Account

Most ad-related operations are in the context of an ad account. You can go to
<a href="https://www.facebook.com/ads/manage">Ads Manager</a> to see accounts
for which you have permission. Most of you probably have a personal account.

Let's get all the ad accounts for the user with the given access token. I only
have one account so the following is printed:

```python
>>> from facebook_business.adobjects.user import User
>>> me = adobjects.User(fbid='me')
>>> my_accounts = list(me.get_ad_accounts())
>>> print(my_accounts)
[{   'account_id': u'17842443', 'id': u'act_17842443'}]
>>> type(my_accounts[0])
<class 'facebook_business.adobjects.AdAccount'>
```

**WARNING**: We do not specify a keyword argument ``api=api`` when instantiating
the ``User`` object here because we've already set the default api when
bootstrapping.

**NOTE**: We wrap the return value of ``get_ad_accounts`` with ``list()``
because ``get_ad_accounts`` returns an ``EdgeIterator`` object (located in
``facebook_business.adobjects``) and we want to get the full list right away instead of
having the iterator lazily loading accounts.

For our purposes, we can just pick an account and do our experiments in its
context:

```python
>>> my_account = my_accounts[0]
```

Or if you already know your account id:

```python
>>> my_account = adobjects.AdAccount('act_17842443')
```

## Create

Let's create a campaign. It's in the context of the account, i.e. its parent
should be the account.

```python

fields = [
]
params = {
  adobjects.Campaign.Field.name : 'Conversions Campaign',
  adobjects.Campaign.Field.configured_status: adobjects.Campaign.Status.paused,
}
campaign = AdAccount(id).create_campaign(fields, params)
```

Then we specify some details about the campaign. To figure out what properties
to define, you should look at the available fields of the object (located in
``Campaign.Field``) and also look at the ad object's documentation (e.g.
<a href="https://developers.facebook.com/docs/marketing-api/reference/ad-campaign-group">
Campaign</a>).

**NOTE**: To find out the fields, look at the individual class file under adobjects
directory.

If there's an error, an exception will be raised. Possible exceptions and their
descriptions are listed in ``facebook_business.exceptions``.

## Read

We can also read properties of an object from the api assuming that the object
is already created and has a node path. Accessing properties of an object is
simple since ``AbstractObject`` implements the ``collections.MutableMapping``.
You can access them just like accessing a key of a dictionary:

```python
>>> print(my_account)
{'account_id': u'17842443', 'id': u'act_17842443'}
>>> my_account = my_account.api_get(fields=[adobjects.AdAccount.Field.amount_spent])
>>> print(my_account[adobjects.AdAccount.Field.amount_spent])
{'amount_spent': 21167, 'account_id': u'17842443', 'id': u'act_17842443'}
```

## Update

To update an object, we can modify its properties and then call the
``api_update`` method to sync the object with the server. Let's correct the
typo "Campain" to "Campaign":

```python
>>> campaign.api_update(fields=[], params={adobjects.Campaign.Field.name:"Potato Campaign"})
```

You can see the results in ads manager.

## Delete

If we decide we don't want the campaign we created anymore:

```python
campaign.api_delete()
```

## Useful Arguments

### MULTIPLE ACCESS TOKENS

Throughout the docs, the method FacebookAdsApi.init is called before making any API calls. This
method set up a default FacebookAdsApi object to be used everywhere. That simplifies the usage
but it's not feasible when a system using the SDK will make calls on behalf of multiple users.

The reason why this is not feasible is because each user should have its own FacebookSession, with its own
access token, rather than using the same session for every one. Each session should be used to create a
separate FacebookAdsApi object. See example below:


```python
my_app_id = '<APP_ID>'
my_app_secret = '<APP_SECRET>'
my_access_token_1 = '<ACCESS_TOKEN_1>'
my_access_token_2 = '<ACCESS_TOKEN_2>'
proxies = {'http': '<HTTP_PROXY>', 'https': '<HTTPS_PROXY>'} # add proxies if needed

session1 = FacebookSession(
    my_app_id,
    my_app_secret,
    my_access_token_1,
    proxies,
)

session2 = FacebookSession(
    my_app_id,
    my_app_secret,
    my_access_token_2,
    proxies,
)

api1 = FacebookAdsApi(session1)
api2 = FacebookAdsApi(session2)
```
In the SDK examples, we always set a single FacebookAdsApi object as the default one.
However, working with multiples access_tokens, require us to use multiples apis. We may set a default
api for a user, but, for the other users,  we shall use its the api object as a param. In the example below,
we create two AdUsers, the first one using the default api and the second one using its api object:

```python
FacebookAdsApi.set_default_api(api1)

me1 = AdUser(fbid='me')
me2 = AdUser(fbid='me', api=api2)
```
Another way to create the same objects from above would be:

```python
me1 = AdUser(fbid='me', api=api1)
me2 = AdUser(fbid='me', api=api2)
```
From here, all the following workflow for these objects remains the same. The only exceptions are
the classmethods calls, where we now should pass the api we want to use as the last parameter
on every call. For instance, a call to the Aduser.get_by_ids method should be like this:

```python
session = FacebookSession(
 my_app_id,
 my_app_secret,
 my_access_token_1,
 proxies,
)

api = FacebookAdsApi(session1)
Aduser.get_by_ids(ids=['<UID_1>', '<UID_2>'], api=api)
```
### CRUD

All CRUD calls support a ``params`` keyword argument which takes a dictionary
mapping parameter names to values in case advanced modification is required. You
can find the list of parameter names as attributes of
``{your object class}.Field``. Under the Field class there may be other classes
which contain, as attributes, valid fields of the value of one of the parent
properties.

``api_update`` and ``create_xxx`` support a ``files`` keyword argument
which takes a dictionary mapping file reference names to binary opened file
objects.

``api_get`` supports a ``fields`` keyword argument which is a convenient way
of specifying the 'fields' parameter. ``fields`` takes a list of fields which
should be read during the call. The valid fields can be found as attributes of
the class Field.

### Edges

When initializing an ``EdgeIterator`` or when calling a method such as
``AdAccount.get_ad_campaigns``:

* You can specify a ``fields`` argument which takes a list of fields to read for
the objects being read.
* You can specify a ``params`` argument that can help you specify or filter the
edge more precisely.

## Batch Calling

It is efficient to group together large numbers of calls into one http request.
The SDK makes this process simple. You can group together calls into an instance
of ``FacebookAdsApiBatch`` (available in facebook_business.api). To easily get one
for your api instance:

```python
my_api_batch = api.new_batch()
```

Calls can be added to the batch instead of being executed immediately:

```python
campaign.api_delete(batch=my_api_batch)
```

Once you're finished adding calls to the batch, you can send off the request:

```python
my_api_batch.execute()
```

Please follow <a href="https://developers.facebook.com/docs/graph-api/making-multiple-requests">
batch call guidelines in the Marketing API documentation</a>. There are optimal
numbers of calls per batch. In addition, you may need to watch out that for rate
limiting as a batch call simply improves network performance and each call does
count individually towards rate limiting.

## Exceptions

See ``facebook_business.exceptions`` for a list of exceptions which may be thrown by
the SDK.

## Tests

### Unit tests

The unit tests don't require an access token or network access. Run them
with your default installed Python as follows:

```
python -m facebook_business.test.unit
```

You can also use tox to run the unit tests with multiple Python versions:

```
sudo apt-get install python-tox  # Debian/Ubuntu
sudo yum install python-tox      # Fedora
tox --skip-missing-interpreters
```

You can increase interpreter coverage by installing additional versions of
Python. On Ubuntu you can use the
[deadsnakes PPA](https://launchpad.net/~fkrull/+archive/ubuntu/deadsnakes).
On other distributions you can
[build from source](https://www.python.org/downloads/) and then use
`sudo make altinstall` to avoid conflicts with your system-installed
version.

## Examples

Examples of usage are located in the ``examples/`` folder.


## Debug

If this SDK is not working as expected, it may be either a SDK issue or API issue.

This can be identified by constructing a raw cURL request and seeing if the response is as expected

for example:

```python
from facebook_business.adobjects.page import Page
from facebook_business.api import FacebookAdsApi

FacebookAdsApi.init(access_token=access_token, debug=True)
page = Page(page_id).api_get(fields=fields,params=params)
```

When running this code, this cURL request will be printed to the console as:
```
curl -X 'GET' -H 'Accept: */*' -H 'Accept-Encoding: gzip, deflate' -H 'Connection: keep-alive' -H 'User-Agent: fbbizsdk-python-v3.3.1' 'https://graph.facebook.com/v3.3/<pageid>/?access_token=<access_token>&fields=name%2Cbirthday%2Cphone'
```

## SDK Codegen
Our SDK is autogenerated from [SDK Codegen](https://github.com/facebook/facebook-business-sdk-codegen). If you want to learn more about how our SDK code is generated, please check this repository.

## Issue
Since we want to handle bugs more efficiently, we've decided to close issue reporting in Github and move to our dedicated bug reporting channel.
If you encounter a bug with Business SDK (Python), please report the issue at [our developer bug reporting channel](https://developers.facebook.com/support/bugs/).

## License
Facebook Business SDK for Python is licensed under the LICENSE file in the root directory of this source tree.
