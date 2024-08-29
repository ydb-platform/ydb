# Url

## Normalize {#normalize}

* ```Url::Normalize(String) -> String?```

Normalizes the URL in a robot-friendly way: converts the hostname into lowercase, strips out certain fragments, and so on.
The normalization result only depends on the URL itself. The normalization **DOES NOT** include operations depending on the external data: transformation based on duplicates, mirrors, etc.

Returned value:

* Normalized URL.
* `NULL`, if the passed string argument can't be parsed as a URL.

**Examples**

```sql
SELECT Url::Normalize("hTTp://wWw.yDb.TECH/"); -- "http://www.ydb.tech/"
SELECT Url::Normalize("http://ydb.tech#foo");      -- "http://ydb.tech/"
```

## NormalizeWithDefaultHttpScheme {#normalizewithdefaulthttpscheme}

* ```Url::NormalizeWithDefaultHttpScheme(String?) -> String?```

Normalizes similarly to `Url::Normalize`, but inserts the `http://` schema in case there is no schema.

Returned value:

* Normalized URL.
* Source URL, if the normalization has failed.

**Examples**

```sql
SELECT Url::NormalizeWithDefaultHttpScheme("wWw.yDb.TECH");    -- "http://www.ydb.tech/"
SELECT Url::NormalizeWithDefaultHttpScheme("http://ydb.tech#foo"); -- "http://ydb.tech/"
```

## Encode / Decode {#encode}

Encode a UTF-8 string to the urlencoded format (`Url::Encode`) and back (`Url::Decode`).

**List of functions**

* ```Url::Encode(String?) -> String?```
* ```Url::Decode(String?) -> String?```

**Examples**

```sql
SELECT Url::Decode("http://ydb.tech/%D1%81%D1%82%D1%80%D0%B0%D0%BD%D0%B8%D1%86%D0%B0");
  -- "http://ydb.tech/page"
SELECT Url::Encode("http://ydb.tech/page");
  -- "http://ydb.tech/%D1%81%D1%82%D1%80%D0%B0%D0%BD%D0%B8%D1%86%D0%B0"
```

## Parse {#parse}

Parses the URL into parts.

* ```Url::Parse(Parse{Flags:AutoMap}) -> Struct< Frag: String?, Host: String?, ParseError: String?, Pass: String?, Path: String?, Port: String?, Query: String?, Scheme: String?, User: String? >```

**Examples**

```sql
SELECT Url::Parse(
  "https://en.wikipedia.org/wiki/Isambard_Kingdom_Brunel?s=24&g=h-24#Great_Western_Railway");
/*
(
  "Frag": "Great_Western_Railway",
  "Host": "en.wikipedia.org",
  "ParseError": null,
  "Pass": null,
  "Path": "/wiki/Isambard_Kingdom_Brunel",
  "Port": null,
  "Query": "s=24&g=h-24",
  "Scheme": "https",
  "User": null
)
*/
```

## Get... {#get}

Get a component of the URL.

**List of functions**

* ```Url::GetScheme(String{Flags:AutoMap}) -> String```

* ```Url::GetHost(String?) -> String?```

* ```Url::GetHostPort(String?) -> String?```

* ```Url::GetSchemeHost(String?) -> String?```

* ```Url::GetSchemeHostPort(String?) -> String?```

* ```Url::GetPort(String?) -> String?```

* ```Url::GetTail(String?) -> String?``` -- everything following the host: path + query + fragment

* ```Url::GetPath(String?) -> String?```

* ```Url::GetFragment(String?) -> String?```

* ```Url::GetCGIParam(String?, String) -> String?``` -- The second parameter is the name of the intended CGI parameter.

* ```Url::GetDomain(String?, Uint8) -> String?``` -- The second parameter is the required domain level.

* ```Url::GetTLD(String{Flags:AutoMap}) -> String```

* ```Url::IsKnownTLD(String{Flags:AutoMap}) -> Bool``` -- Registered on http://www.iana.org/

* ```Url::IsWellKnownTLD(String{Flags:AutoMap}) -> Bool``` -- Belongs to a small whitelist of com, net, org, ru, and so on.

* ```Url::GetDomainLevel(String{Flags:AutoMap}) -> Uint64```

* ```Url::GetSignificantDomain(String{Flags:AutoMap}, [List<String>?]) -> String```
  Returns a second-level domain in most cases and a third-level domain for the hostnames like: ***.XXX.YY, where XXX is com, net, org, co, gov, or edu. You can redefine this list using an optional second argument

* ```Url::GetOwner(String{Flags:AutoMap}) -> String```
  Returns the domain that's most likely owned by an individual or organization. Unlike Url::GetSignificantDomain, it uses a special whitelist. Besides the ***.co.uk domains, it can return a third-level domain used by free hosting sites and blogs (for example: something.livejournal.com)

**Examples**

```sql
SELECT Url::GetScheme("https://ydb.tech");       -- "https://"
SELECT Url::GetDomain("http://www.ydb.tech", 2); -- "ydb.tech"
```

## Cut... {#cut}

* ```Url::CutScheme(String?) -> String?```
  Returns the passed URL without the schema (http://, https://, etc.).

* ```Url::CutWWW(String?) -> String?```
  Returns the passed domain without the "www." prefix (if any).

* ```Url::CutWWW2(String?) -> String?```
  Returns the passed domain without the prefixes like "www.", "www2.", "wwww777." (if any).

* ```Url::CutQueryStringA­ndFragment(String{Flags:AutoMap}) -> String```
  Returns a copy of the passed URL, stripping out all the CGI parameters and fragments ("?foo=bar" and/or "#baz").

**Examples**

```sql
SELECT Url::CutScheme("http://www.ydb.tech"); -- "www.ydb.tech"
SELECT Url::CutWWW("www.ydb.tech");           -- "ydb.tech"
```

## ...Punycode... {#punycode}

[Punycode](https://en.wikipedia.org/wiki/Punycode) transformations.

**List of functions**

* ```Url::HostNameToPunycode(String{Flag:AutoMap}) -> String?```
* ```Url::ForceHostNameToPunycode(String{Flag:AutoMap}) -> String```
* ```Url::PunycodeToHostName(String{Flag:AutoMap}) -> String?```
* ```Url::ForcePunycodeToHostName(String{Flag:AutoMap}) -> String```
* ```Url::CanBePunycodeHostName(String{Flag:AutoMap}) -> Bool```

**Examples**

```sql
SELECT Url::PunycodeToHostName("xn--80aniges7g.xn--j1aef"); -- "example.com"
```

## ...Query... {#query}

[Query](https://docs.python.org/3/library/urllib.parse.html) transformations.

**List of functions**

```sql
Url::QueryStringToList(String{Flag:AutoMap}, [
  KeepBlankValues:Bool?,  -- Empty values in percent-encoded queries are interpreted as empty strings, defaults to false.
  Strict:Bool?,           -- If false, parsing errors are ignored and incorrect fields are skipped, defaults to true.
  MaxFields:Uint32?,      -- The maximum number of fields. If exceeded, an exception is thrown. Defaults to Max<Uint32>.
  Separator:String?       -- A key-value pair separator, defaults to '&'.
]) -> List<Tuple<String, String>>
Url::QueryStringToDict(String{Flag:AutoMap}, [
  KeepBlankValues:Bool?,  -- Empty values in percent-encoded queries are interpreted as empty strings, defaults to false.
  Strict:Bool?,           -- If false, parsing errors are ignored and incorrect fields are skipped, defaults to true.
  MaxFields:Uint32?,      -- The maximum number of fields. If exceeded, an exception is thrown. Defaults to Max<Uint32>.
  Separator:String?       -- A key-value pair separator, defaults to '&'.
]) -> Dict<String, List<String>>
Url::BuildQueryString(Dict<String, List<String?>>{Flag:AutoMap}, [
  Separator:String?       -- A key-value pair separator, defaults to '&'.
]) -> String
Url::BuildQueryString(Dict<String, String?>{Flag:AutoMap}, [
  Separator:String?       -- A key-value pair separator, defaults to '&'.
]) -> String
Url::BuildQueryString(List<Tuple<String, String?>>{Flag:AutoMap}, [
  Separator:String?       -- A key-value pair separator, defaults to '&'.
]) -> String
```

**Examples**

```sql
SELECT Url::QueryStringToList("a=1&b=2&a=3");                       -- [("a", "1"), ("b", "2"), ("a", "3")]
SELECT Url::QueryStringToDict("a=1&b=2&a=3");                       -- {"b" : ["2"], "a" : ["1", "3"]}
SELECT Url::BuildQueryString([("a", "1"), ("a", "3"), ("b", "2")]); -- "a=1&a=3&b=2"
SELECT Url::BuildQueryString({"a" : "1", "b" : "2"});               -- "b=2&a=1"
SELECT Url::BuildQueryString({"a" : ["1", "3"], "b" : ["2", "4"]}); -- "b=2&b=4&a=1&a=3"
```

