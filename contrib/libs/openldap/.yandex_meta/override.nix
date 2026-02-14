pkgs: attrs: with pkgs; rec {
  version = "2.6.12";

  versionWithUnderscores = "${lib.replaceStrings ["."] ["_"] version}";

  src = fetchFromGitLab {
    owner = "openldap";
    repo = "openldap";
    rev = "OPENLDAP_REL_ENG_${versionWithUnderscores}";
    hash = "sha256-nKmUHArLzNvATNL65Fuk/ztXAwOlyRPA59SY9EGT1S0=";
  };

  patches = [];

  buildPhase = ''
    make -C include
    make -j$NIX_BUILD_CORES -C libraries/liblber liblber.la
    make -j$NIX_BUILD_CORES -C libraries/libldap libldap.la
    make -j$NIX_BUILD_CORES -C libraries/liblmdb liblmdb.so
  '';
}
