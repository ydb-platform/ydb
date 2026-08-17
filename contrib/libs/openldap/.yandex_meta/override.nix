pkgs: attrs: with pkgs; rec {
  version = "2.6.13";

  versionWithUnderscores = "${lib.replaceStrings ["."] ["_"] version}";

  src = fetchFromGitLab {
    owner = "openldap";
    repo = "openldap";
    rev = "OPENLDAP_REL_ENG_${versionWithUnderscores}";
    hash = "sha256-x+Ch0UvKSCerKKs7h2x/b4lRcq3P1b8cCcNiPQ54x6o=";
  };

  patches = [];

  buildPhase = ''
    make -C include
    make -j$NIX_BUILD_CORES -C libraries/liblber liblber.la
    make -j$NIX_BUILD_CORES -C libraries/libldap libldap.la
    make -j$NIX_BUILD_CORES -C libraries/liblmdb liblmdb.so
  '';
}
