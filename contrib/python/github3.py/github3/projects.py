"""This module contains all the classes relating to projects."""
from json import dumps

from . import exceptions
from . import models
from . import pulls
from . import users
from .decorators import requires_auth
from .issues import issue


class Project(models.GitHubCore):
    """Object representing a single project from the API.

    See http://developer.github.com/v3/projects/ for more details.

    .. attribute:: body

        The Markdown formatted text describing the project.

    .. attribute:: created_at

        A :class:`~datetime.datetime` representing the date and time when
        this project was created.

    .. attribute:: creator

        A :class:`~github3.users.ShortUser` instance representing the user who
        created this project.

    .. attribute:: id

        The unique identifier for this project on GitHub.

    .. attribute:: name

        The name given to this project.

    .. attribute:: number

        The repository-local identifier of this project.

    .. attribute:: owner_url

        The URL of the resource in the API of the owning resource - either
        a repository or an organization.

    .. attribute:: updated_at

        A :class:`~datetime.datetime` representing the date and time when
        this project was last updated.
    """

    CUSTOM_HEADERS = {"Accept": "application/vnd.github.inertia-preview+json"}

    def _update_attributes(self, project):
        self._api = project["url"]
        self.body = project["body"]
        self.created_at = self._strptime(project["created_at"])
        self.creator = users.ShortUser(project["creator"], self)
        self.id = project["id"]
        self.name = project["name"]
        self.number = project["number"]
        self.owner_url = project["owner_url"]
        self.updated_at = self._strptime(project["updated_at"])

    def _repr(self):
        return f"<Project [#{self.id}]>"

    def column(self, id):
        """Get a project column with the given ID.

        :param int id:
            (required), the column ID
        :returns:
            the desired column in the project
        :rtype:
            :class:`~github3.projects.ProjectColumn`
        """
        url = self._build_url("projects", "columns", str(id))
        json = self._json(self._get(url, headers=Project.CUSTOM_HEADERS), 200)
        return self._instance_or_null(ProjectColumn, json)

    def columns(self, number=-1, etag=None):
        """Iterate over the columns in this project.

        :param int number:
            (optional), number of columns to return. Default: -1 returns all
            available columns.
        :param str etag:
            (optional), ETag from a previous request to the same endpoint
        :returns:
            generator of columns
        :rtype:
            :class:`~github3.project.ProjectColumn`
        """
        # TODO(sigmaviurs24): Determine if we need to construct from scratch
        # or if we can use `self._api` with 'columns' to build the URL
        url = self._build_url("projects", str(self.id), "columns")
        return self._iter(
            int(number),
            url,
            ProjectColumn,
            headers=Project.CUSTOM_HEADERS,
            etag=etag,
        )

    @requires_auth
    def create_column(self, name):
        """Create a column in this project.

        :param str name:
            (required), name of the column
        :returns:
            the created project column
        :rtype:
            :class:`~github3.projects.ProjectColumn`
        """
        url = self._build_url("columns", base_url=self._api)
        json = None
        if name:
            json = self._json(
                self._post(
                    url, data={"name": name}, headers=Project.CUSTOM_HEADERS
                ),
                201,
            )
        return self._instance_or_null(ProjectColumn, json)

    @requires_auth
    def delete(self):
        """Delete this project.

        :returns:
            True if successfully deleted, False otherwise
        :rtype:
            bool
        """
        return self._boolean(
            self._delete(self._api, headers=self.CUSTOM_HEADERS), 204, 404
        )

    @requires_auth
    def update(self, name=None, body=None):
        """Update this project.

        :param str name:
            (optional), new name of the project
        :param str body:
            (optional), new body of the project
        :returns:
            True if successfully updated, False otherwise
        :rtype:
            bool
        """
        data = {"name": name, "body": body}
        json = None
        self._remove_none(data)

        if data:
            json = self._json(
                self._patch(
                    self._api, data=dumps(data), headers=self.CUSTOM_HEADERS
                ),
                200,
            )

        if json:
            self._update_attributes(json)
            return True
        return False


class ProjectColumn(models.GitHubCore):
    """Object representing a column in a project.

    See http://developer.github.com/v3/projects/columns/

    .. attribute:: created_at

        A :class:`~datetime.datetime` object representing the date and time
        when the column was created.

    .. attribute:: id

        The unique identifier for this column across GitHub.

    .. attribute:: name

        The name given to this column.

    .. attribute:: project_url

        The URL used to retrieve the project that owns this column via the API.

    .. attribute:: updated_at

        A :class:`~datetime.datetime` object representing the date and time
        when the column was last updated.
    """

    def _update_attributes(self, project_column):
        self.created_at = self._strptime(project_column["created_at"])
        self.id = project_column["id"]
        self.name = project_column["name"]
        self.project_url = project_column["project_url"]
        self.updated_at = self._strptime(project_column["updated_at"])

    def _repr(self):
        return f"<ProjectColumn [#{self.id}]>"

    def card(self, id):
        """Get a project card with the given ID.

        :param int id:
            (required), the card ID
        :returns:
            the card identified by the provided id
        :rtype:
            :class:`~github3.projects.ProjectCard`
        """
        url = self._build_url("projects", "columns", "cards", str(id))
        json = self._json(self._get(url, headers=Project.CUSTOM_HEADERS), 200)
        return self._instance_or_null(ProjectCard, json)

    def cards(self, number=-1, etag=None):
        """Iterate over the cards in this column.

        :param int number:
            (optional), number of cards to return. Default: -1 returns all
            available cards.
        :param str etag:
            (optional), ETag from a previous request to the same endpoint
        :returns:
            generator of cards
        :rtype:
            :class:`~github3.project.ProjectCard`
        """
        url = self._build_url("projects", "columns", str(self.id), "cards")
        return self._iter(
            int(number),
            url,
            ProjectCard,
            headers=Project.CUSTOM_HEADERS,
            etag=etag,
        )

    @requires_auth
    def create_card_with_content_id(self, content_id, content_type):
        """Create a content card in this project column.

        :param int content_id:
            (required), the ID of the content
        :param str content_type:
            (required), the type of the content
        :returns:
            the created card
        :rtype:
            :class:`~github3.projects.ProjectCard`
        """
        if not content_id or not content_type:
            return None

        url = self._build_url("projects", "columns", str(self.id), "cards")
        json = None
        data = {"content_id": content_id, "content_type": content_type}
        json = self._json(
            self._post(url, data=data, headers=Project.CUSTOM_HEADERS), 201
        )
        return self._instance_or_null(ProjectCard, json)

    @requires_auth
    def create_card_with_issue(self, issue):
        """Create a card in this project column linked with an Issue.

        :param issue:
            (required), an issue with which to link the card. Can also be
            :class:`~github3.issues.ShortIssue`.
        :type issue:
            :class:`~github3.issues.Issue`
        :returns:
            the created card
        :rtype:
            :class:`~github3.projects.ProjectCard`
        """
        if not issue:
            return None
        return self.create_card_with_content_id(issue.id, "Issue")

    @requires_auth
    def create_card_with_note(self, note):
        """Create a note card in this project column.

        :param str note:
            (required), the note content
        :returns:
            the created card
        :rtype:
            :class:`~github3.projects.ProjectCard`
        """
        url = self._build_url("projects", "columns", str(self.id), "cards")
        json = None
        if note:
            json = self._json(
                self._post(
                    url, data={"note": note}, headers=Project.CUSTOM_HEADERS
                ),
                201,
            )
        return self._instance_or_null(ProjectCard, json)

    @requires_auth
    def delete(self):
        """Delete this column.

        :returns:
            True if successful, False otherwise
        :rtype:
            bool
        """
        url = self._build_url("projects", "columns", str(self.id))
        return self._boolean(
            self._delete(url, headers=Project.CUSTOM_HEADERS), 204, 404
        )

    @requires_auth
    def move(self, position):
        """Move this column.

        :param str position:
            (required), can be one of `first`, `last`, or `after:<column-id>`,
            where `<column-id>` is the id value of a column in the same
            project.
        :returns:
            True if successful, False otherwise
        :rtype:
            bool
        """
        if not position:
            return False

        url = self._build_url("projects", "columns", str(self.id), "moves")
        data = {"position": position}
        return self._boolean(
            self._post(url, data=data, headers=Project.CUSTOM_HEADERS),
            201,
            404,
        )

    @requires_auth
    def update(self, name=None):
        """Update this column.

        :param str name:
            (optional), name of the column
        :returns:
            True if successful, False otherwise
        :rtype:
            bool
        """
        data = {"name": name}
        json = None
        self._remove_none(data)

        if data:
            url = self._build_url("projects", "columns", str(self.id))
            json = self._json(
                self._patch(
                    url, data=dumps(data), headers=Project.CUSTOM_HEADERS
                ),
                200,
            )

        if json:
            self._update_attributes(json)
            return True
        return False


class ProjectCard(models.GitHubCore):
    """Object representing a "card" on a project.

    See http://developer.github.com/v3/projects/cards/

    .. attribute:: column_url

        The URL to retrieve this card's column via the API.

    .. attribute:: content_url

        The URl to retrieve this card's body content via the API.

    .. attribute:: created_at

        A :class:`~datetime.datetime` object representing the date and time
        when the column was created.

    .. attribute:: id

        The globally unique identifier for this card.

    .. attribute:: note

        The body of the note attached to this card.

    .. attribute:: updated_at

        A :class:`~datetime.datetime` object representing the date and time
        when the column was last updated.
    """

    def _update_attributes(self, project_card):
        #: The URL of this card's parent column
        self.column_url = project_card["column_url"]

        #: The URL of this card's associated content
        self.content_url = project_card.get("content_url")

        #: datetime object representing the last time the object was created
        self.created_at = project_card["created_at"]

        #: The ID of this card
        self.id = project_card["id"]

        #: The note attached to the card
        self.note = project_card["note"]

        #: datetime object representing the last time the object was changed
        self.updated_at = project_card["updated_at"]

    def _repr(self):
        return f"<ProjectCard [#{self.id}]>"

    @requires_auth
    def delete(self):
        """Delete this card.

        :returns:
            True if successfully deleted, False otherwise
        :rtype:
            bool
        """
        url = self._build_url("projects", "columns", "cards", str(self.id))
        return self._boolean(
            self._delete(url, headers=Project.CUSTOM_HEADERS), 204, 404
        )

    @requires_auth
    def move(self, position, column_id):
        """Move this card.

        :param str position:
            (required), can be one of `top`, `bottom`, or `after:<card-id>`,
            where `<card-id>` is the id value of a card in the same column, or
            in the new column specified by `column_id`.
        :param int column_id:
            (required), the id value of a column in the same project.
        :returns:
            True if successfully moved, False
        :rtype:
            bool
        """
        if not position or not column_id:
            return False

        url = self._build_url(
            "projects", "columns", "cards", str(self.id), "moves"
        )
        data = {"position": position, "column_id": column_id}
        return self._boolean(
            self._post(url, data=data, headers=Project.CUSTOM_HEADERS),
            201,
            404,
        )

    @requires_auth
    def update(self, note=None):
        """Update this card.

        :param str note:
            (optional), the card's note content. Only valid for cards without
            another type of content, so this cannot be specified if the card
            already has a content_id and content_type.
        :returns:
            True if successfully updated, False otherwise
        :rtype:
            bool
        """
        data = {"note": note}
        json = None
        self._remove_none(data)

        if data:
            url = self._build_url(
                "projects", "columns", "cards", str(self.id)
            )
            json = self._json(
                self._patch(
                    url, data=dumps(data), headers=Project.CUSTOM_HEADERS
                ),
                200,
            )

        if json:
            self._update_attributes(json)
            return True
        return False

    def retrieve_issue_from_content(self):
        """Attempt to retrieve an Issue from the content url.

        :returns:
            The issue that backs up this particular project card if the card
            has a content_url.

            .. note::

                Cards can be created from Issues and Pull Requests. Pull
                Requests are also technically Issues so this method is always
                safe to call.
        :rtype:
            :class:`~github3.issues.issue.Issue`
        :raises:
            :class:`~github3.exceptions.CardHasNoContentUrl`
        """
        if self.content_url is None:
            raise exceptions.CardHasNoContentUrl(
                f"Card {self.id} has no content_url"
            )
        parsed = self._uri_parse(self.content_url)
        _, owner, repository, _, number = parsed.path[1:].split("/", 5)
        resp = self._get(
            self._build_url("repos", owner, repository, "issues", number)
        )
        json = self._json(resp, 200)
        return self._instance_or_null(issue.Issue, json)

    def retrieve_pull_request_from_content(self):
        """Attempt to retrieve an PullRequest from the content url.

        :returns:
            The pull request that backs this particular project card if the
            card has a content_url.

            .. note::

                Cards can be created from Issues and Pull Requests.
        :rtype:
            :class:`~github3.issues.issue.Issue`
        :raises:
            :class:`~github3.exceptions.CardHasNoContentUrl`
        """
        if self.content_url is None:
            raise exceptions.CardHasNoContentUrl(
                f"Card {self.id} has no content_url"
            )
        parsed = self._uri_parse(self.content_url)
        _, owner, repository, _, number = parsed.path[1:].split("/", 5)
        resp = self._get(
            self._build_url("repos", owner, repository, "pulls", number)
        )
        json = self._json(resp, 200)
        return self._instance_or_null(pulls.PullRequest, json)
