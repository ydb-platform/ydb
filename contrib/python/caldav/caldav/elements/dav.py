#!/usr/bin/env python
from typing import ClassVar

from .base import BaseElement
from .base import ValuedBaseElement
from caldav.lib.namespace import ns


# Operations
class Propfind(BaseElement):
    tag: ClassVar[str] = ns("D", "propfind")


class PropertyUpdate(BaseElement):
    tag: ClassVar[str] = ns("D", "propertyupdate")


class Mkcol(BaseElement):
    tag: ClassVar[str] = ns("D", "mkcol")


class SyncCollection(BaseElement):
    tag: ClassVar[str] = ns("D", "sync-collection")


class PrincipalPropertySearch(BaseElement):
    tag: ClassVar[str] = ns("D", "principal-property-search")


class PropertySearch(BaseElement):
    tag: ClassVar[str] = ns("D", "property-search")


# Filters


class Match(BaseElement):
    tag: ClassVar[str] = ns("D", "match")


# Conditions
class SyncToken(BaseElement):
    tag: ClassVar[str] = ns("D", "sync-token")


class SyncLevel(BaseElement):
    tag: ClassVar[str] = ns("D", "sync-level")


# Components / Data


class Prop(BaseElement):
    tag: ClassVar[str] = ns("D", "prop")


class Collection(BaseElement):
    tag: ClassVar[str] = ns("D", "collection")


class Set(BaseElement):
    tag: ClassVar[str] = ns("D", "set")


# Properties
class ResourceType(BaseElement):
    tag: ClassVar[str] = ns("D", "resourcetype")


class DisplayName(ValuedBaseElement):
    tag: ClassVar[str] = ns("D", "displayname")


class GetEtag(ValuedBaseElement):
    tag: ClassVar[str] = ns("D", "getetag")


class Href(BaseElement):
    tag: ClassVar[str] = ns("D", "href")


class SupportedReportSet(BaseElement):
    tag = ns("D", "supported-report-set")


class Response(BaseElement):
    tag: ClassVar[str] = ns("D", "response")


class Status(BaseElement):
    tag: ClassVar[str] = ns("D", "status")


class PropStat(BaseElement):
    tag: ClassVar[str] = ns("D", "propstat")


class MultiStatus(BaseElement):
    tag: ClassVar[str] = ns("D", "multistatus")


class CurrentUserPrincipal(BaseElement):
    tag: ClassVar[str] = ns("D", "current-user-principal")


class PrincipalCollectionSet(BaseElement):
    tag: ClassVar[str] = ns("D", "principal-collection-set")


class Allprop(BaseElement):
    tag: ClassVar[str] = ns("D", "allprop")


class Owner(BaseElement):
    tag: ClassVar[str] = ns("D", "owner")
