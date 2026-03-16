# Copyright (c) 2020 Canonical Ltd
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.
from pylxd.models import _model as model


class Project(model.Model):
    """A LXD project.

    This corresponds to the LXD endpoint at /1.0/projects.

    api_extension: 'projects'
    """

    name = model.Attribute(readonly=True)
    config = model.Attribute()
    description = model.Attribute()
    used_by = model.Attribute(readonly=True)

    @classmethod
    def exists(cls, client, name):
        """Determine whether a project exists."""
        try:
            client.projects.get(name)
            return True
        except cls.NotFound:
            return False

    @classmethod
    def get(cls, client, name):
        """Get a project."""
        response = client.api.projects[name].get()
        return cls(client, **response.json()["metadata"])

    @classmethod
    def all(cls, client):
        """Get all projects."""
        response = client.api.projects.get()

        projects = []
        for url in response.json()["metadata"]:
            name = url.split("/")[-1]
            projects.append(cls(client, name=name))
        return projects

    @classmethod
    def create(
        cls,
        client,
        name,
        description=None,
        config=None,
    ):
        """Create a project."""
        project = {"name": name}
        if config is not None:
            project["config"] = config
        if description is not None:
            project["description"] = description
        client.api.projects.post(json=project)
        return cls.get(client, name)

    @property
    def api(self):
        return self.client.api.projects[self.name]

    def rename(self, new_name):
        """Rename the project."""
        self.api.post(json={"name": new_name})

        return Project.get(self.client, new_name)
