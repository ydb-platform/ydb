// Licensed to ClickHouse, Inc. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. ClickHouse, Inc. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package proto

import (
	"fmt"
	chproto "github.com/ClickHouse/ch-go/proto"
	"gopkg.in/yaml.v3"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/timezone"
)

const ClientName = "Golang SQLDriver"

const (
	ClientVersionMajor       = 2
	ClientVersionMinor       = 3
	ClientTCPProtocolVersion = DBMS_TCP_PROTOCOL_VERSION
)

type ClientHandshake struct{}

func (ClientHandshake) Encode(buffer *chproto.Buffer) {
	buffer.PutString(ClientName)
	buffer.PutUVarInt(ClientVersionMajor)
	buffer.PutUVarInt(ClientVersionMinor)
	buffer.PutUVarInt(ClientTCPProtocolVersion)
}

func (ClientHandshake) String() string {
	return fmt.Sprintf("%s %d.%d.%d", ClientName, ClientVersionMajor, ClientVersionMinor, ClientTCPProtocolVersion)
}

type ServerHandshake struct {
	Name        string
	DisplayName string
	Revision    uint64
	Version     Version
	Timezone    *time.Location
}

type Version struct {
	Major uint64
	Minor uint64
	Patch uint64
}

func ParseVersion(v string) (ver Version, err error) {
	parts := strings.Split(v, ".")
	if len(parts) < 3 {
		return Version{}, fmt.Errorf("%s is not a valid version", v)
	}
	if ver.Major, err = strconv.ParseUint(parts[0], 10, 8); err != nil {
		return Version{}, err
	}
	if ver.Minor, err = strconv.ParseUint(parts[1], 10, 8); err != nil {
		return Version{}, err
	}
	if ver.Patch, err = strconv.ParseUint(parts[2], 10, 8); err != nil {
		return Version{}, err
	}
	return ver, nil
}

func CheckMinVersion(constraint Version, version Version) bool {
	if version.Major < constraint.Major || (version.Major == constraint.Major && version.Minor < constraint.Minor) || (version.Major == constraint.Major && version.Minor == constraint.Minor && version.Patch < constraint.Patch) {
		return false
	}
	return true
}

func (srv *ServerHandshake) Decode(reader *chproto.Reader) (err error) {
	if srv.Name, err = reader.Str(); err != nil {
		return fmt.Errorf("could not read server name: %v", err)
	}
	if srv.Version.Major, err = reader.UVarInt(); err != nil {
		return fmt.Errorf("could not read server major version: %v", err)
	}
	if srv.Version.Minor, err = reader.UVarInt(); err != nil {
		return fmt.Errorf("could not read server minor version: %v", err)
	}
	if srv.Revision, err = reader.UVarInt(); err != nil {
		return fmt.Errorf("could not read server revision: %v", err)
	}
	if srv.Revision >= DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE {
		name, err := reader.Str()
		if err != nil {
			return fmt.Errorf("could not read server timezone: %v", err)
		}
		if srv.Timezone, err = timezone.Load(name); err != nil {
			return fmt.Errorf("could not load time location: %v", err)
		}
	}
	if srv.Revision >= DBMS_MIN_REVISION_WITH_SERVER_DISPLAY_NAME {
		if srv.DisplayName, err = reader.Str(); err != nil {
			return fmt.Errorf("could not read server display name: %v", err)
		}
	}
	if srv.Revision >= DBMS_MIN_REVISION_WITH_VERSION_PATCH {
		if srv.Version.Patch, err = reader.UVarInt(); err != nil {
			return fmt.Errorf("could not read server patch: %v", err)
		}
	} else {
		srv.Version.Patch = srv.Revision
	}
	return nil
}

func (srv ServerHandshake) String() string {
	return fmt.Sprintf("%s (%s) server version %d.%d.%d revision %d (timezone %s)", srv.Name, srv.DisplayName,
		srv.Version.Major,
		srv.Version.Minor,
		srv.Version.Patch,
		srv.Revision,
		srv.Timezone,
	)
}

func (v Version) String() string {
	return fmt.Sprintf("%d.%d.%d",
		v.Major,
		v.Minor,
		v.Patch,
	)
}

func (v *Version) UnmarshalYAML(value *yaml.Node) (err error) {
	versions := strings.Split(value.Value, ".")
	if len(versions) < 1 || len(versions) > 3 {
		return fmt.Errorf("%s is not a valid version", value.Value)
	}
	for i := range versions {
		switch i {
		case 0:
			if v.Major, err = strconv.ParseUint(versions[i], 10, 8); err != nil {
				return err
			}
		case 1:
			if v.Minor, err = strconv.ParseUint(versions[i], 10, 8); err != nil {
				return err
			}
		case 2:
			if v.Patch, err = strconv.ParseUint(versions[i], 10, 8); err != nil {
				return err
			}
		}
	}
	return nil
}
