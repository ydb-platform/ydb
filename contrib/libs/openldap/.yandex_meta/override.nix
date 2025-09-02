pkgs: attrs: with pkgs; rec {
  version = "2.6.10";

  versionWithUnderscores = "${lib.replaceStrings ["."] ["_"] version}";

  src = fetchFromGitLab {
    owner = "openldap";
    repo = "openldap";
    rev = "OPENLDAP_REL_ENG_${versionWithUnderscores}";
    hash = "sha256-KHVcSFkIyPR6LxDM+33GAWp6mAEgZhpBoUN77WFsT4Q=";
  };

  patches = [];

  buildPhase = ''
    make -C include
    make -j$NIX_BUILD_CORES -C libraries/liblber liblber.la
    make -j$NIX_BUILD_CORES -C libraries/libldap libldap.la
    make -j$NIX_BUILD_CORES -C libraries/liblmdb liblmdb.so
  '';
}
