pkgs: attrs: with pkgs; rec {
  version = "2.6.8";

  versionWithUnderscores = "${lib.replaceStrings ["."] ["_"] version}";

  src = fetchFromGitLab {
    owner = "openldap";
    repo = "openldap";
    rev = "OPENLDAP_REL_ENG_${versionWithUnderscores}";
    hash = "sha256-fJ6cpLR4DUlNlxGvKliGHu4aT93vR2mNB6AUcqDl+gk=";
  };

  patches = [];

  buildPhase = ''
    make -C include
    make -j$NIX_BUILD_CORES -C libraries/liblber liblber.la
    make -j$NIX_BUILD_CORES -C libraries/libldap libldap.la
    make -j$NIX_BUILD_CORES -C libraries/liblmdb liblmdb.so
  '';
}
