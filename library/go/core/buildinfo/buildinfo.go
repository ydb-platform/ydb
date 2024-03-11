package buildinfo

import "strings"

type BuildInfo struct {
	// ProgramVersion is multiline string completely describing
	// version of current binary.
	//
	//     Svn info:
	//         URL: svn+ssh://arcadia.yandex.ru/arc/trunk/arcadia
	//         Last Changed Rev: 4479764
	//         Last Changed Author: robot-yappy
	//         Last Changed Date: 2019-02-19 16:33:55 +0300 (Tue, 19 Feb 2019)
	//
	//     Other info:
	//         Build by: prime
	//         Top src dir: /home/prime/Code/go/src/a.yandex-team.ru
	//         Top build dir: /home/prime/.ya/build/build_root/qbh0/000002
	//         Hostname: 77.88.18.146-red.dhcp.yndx.net
	//         Host information:
	//             Linux 77.88.18.146-red.dhcp.yndx.net 4.19.10-300.fc29.x86_64 #1 SMP Mon Dec 17 15:34:44 UTC 2018 x86_64 x86_64 x86_64 GNU/Linux
	ProgramVersion string
	CustomVersion  string

	User string
	Host string
	Date string

	// VCS is one of "svn", "arc", "hg", "git" or ""
	VCS string

	SVNRevision              string
	ArcadiaSourceRevision    string
	ArcadiaSourceLastChanged string
	ArcadiaPatchNumber       string
	ArcadiaSourcePath        string
	BuildTimestamp           string
	Hash                     string
	Tag                      string
	Dirty                    string
	Branch                   string
}

// Info holds information about current build.
//
// Info might not available in distributed build and when building from
// IDE. Users of api must gracefully handle such cases.
var Info BuildInfo

// InitBuildInfo is internal, but exported API.
//
// This function is called from the main package by the code generated in build/scripts/vcs_info.py
func InitBuildInfo(buildinfo map[string]string) {
	Info.ProgramVersion = strings.TrimRight(buildinfo["PROGRAM_VERSION"], " ")
	Info.CustomVersion = strings.TrimRight(buildinfo["CUSTOM_VERSION"], " ")
	Info.User = buildinfo["BUILD_USER"]
	Info.Host = buildinfo["BUILD_HOST"]
	Info.Date = buildinfo["BUILD_DATE"]
	Info.VCS = buildinfo["VCS"]
	Info.SVNRevision = buildinfo["ARCADIA_SOURCE_REVISION"]
	Info.ArcadiaSourceRevision = buildinfo["ARCADIA_SOURCE_REVISION"]
	Info.ArcadiaSourceLastChanged = buildinfo["ARCADIA_SOURCE_LAST_CHANGE"]
	Info.ArcadiaPatchNumber = buildinfo["ARCADIA_PATCH_NUMBER"]
	Info.ArcadiaSourcePath = buildinfo["ARCADIA_SOURCE_PATH"]
	Info.BuildTimestamp = buildinfo["BUILD_TIMESTAMP"]
	Info.Hash = buildinfo["ARCADIA_SOURCE_HG_HASH"]
	Info.Tag = buildinfo["ARCADIA_TAG"]
	Info.Dirty = buildinfo["DIRTY"]
	Info.Branch = buildinfo["BRANCH"]
}
