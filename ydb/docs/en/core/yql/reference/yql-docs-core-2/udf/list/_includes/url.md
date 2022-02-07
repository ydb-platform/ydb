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
SELECT Url::Normalize("hTTp://wWw.yAnDeX.RU/"); -- "http://www.yandex.ru/"
SELECT Url::Normalize("http://ya.ru#foo");      -- "http://ya.ru/"
```

## NormalizeWithDefaultHttpScheme {#normalizewithdefaulthttpscheme}

* ```Url::NormalizeWithDefaultHttpScheme(String?) -> String?```

Normalizes similarly to `Url::Normalize`, but inserts the `http://` schema in case there is no schema.

Returned value:

* Normalized URL.
* Source URL, if the normalization has failed.

**Examples**

```sql
SELECT Url::NormalizeWithDefaultHttpScheme("wWw.yAnDeX.RU");    -- "http://www.yandex.ru/"
SELECT Url::NormalizeWithDefaultHttpScheme("http://ya.ru#foo"); -- "http://ya.ru/"
```

## Encode / Decode {#encode}

Encode a UTF-8 string to the urlencoded format (`Url::Encode`) and back (`Url::Decode`).

**List of functions **

* ```Url::Encode(String?) -> String?```
* ```Url::Decode(String?) -> String?```

**Examples**

```sql
SELECT Url::Decode("http://ya.ru/%D1%81%D1%82%D1%80%D0%B0%D0%BD%D0%B8%D1%86%D0%B0"); 
  -- "http://ya.ru/page"
SELECT Url::Encode("http://ya.ru/page");                                         
  -- "http://ya.ru/%D1%81%D1%82%D1%80%D0%B0%D0%BD%D0%B8%D1%86%D0%B0"
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

**List of functions **

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
Возвращает домен второго уровня в большинстве случаев и домен третьего уровня для хостеймов вида: ***.XXX.YY, где XXX — одно из com, net, org, co, gov, edu. Этот список можно переопределить через опциональный второй аргумент

* ```Url::GetOwner(String{Flags:AutoMap}) -> String```
Возвращает домен, которым с наибольшей вероятностью владеет отдельный человек или организация. В отличие от Url::GetSignificantDomain работает по специальному whitelist, и помимо доменов из серии ***.co.uk возвращает домен третьего уровня для, например, бесплатных хостингов и блогов, скажем something.livejournal.com

**Examples**

```sql
SELECT Url::GetScheme("https://ya.ru");           -- "https://"
SELECT Url::GetDomain("http://www.yandex.ru", 2); -- "yandex.ru"
```

## Cut... {#cut}

* ```Url::CutScheme(String?) -> String?```
Возвращает переданный URL уже без схемы (http://, https:// и т.п.).

* ```Url::CutWWW(String?) -> String?```
Возвращает переданный домен без префикса "www.", если он имелся.

* ```Url::CutWWW2(String?) -> String?```
Возвращает переданный домен без префикса "www.", "www2.", "wwww777." и тому подобных, если он имелся.

* ```Url::CutQueryStringA­ndFragment(String{Flags:AutoMap}) -> String```
Возращает копию переданного URL с удаленными всеми CGI параметрами и фрагментами ("?foo=bar" и/или "#baz").

**Examples**

```sql
SELECT Url::CutScheme("http://www.yandex.ru"); -- "www.yandex.ru"
SELECT Url::CutWWW("www.yandex.ru");           -- "yandex.ru"
```

## ...Punycode... {#punycode}

[Punycode](https://en.wikipedia.org/wiki/Punycode) transformations.

**List of functions **

* ```Url::HostNameToPunycode(String{Flag:AutoMap}) -> String?```
* ```Url::ForceHostNameToPunycode(String{Flag:AutoMap}) -> String```
* ```Url::PunycodeToHostName(String{Flag:AutoMap}) -> String?```
* ```Url::ForcePunycodeToHostName(String{Flag:AutoMap}) -> String```
* ```Url::CanBePunycodeHostName(String{Flag:AutoMap}) -> Bool```

**Examples**

```sql
SELECT Url::PunycodeToHostName("xn--d1acpjx3f.xn--p1ai"); -- "яндекс.рф"
```

