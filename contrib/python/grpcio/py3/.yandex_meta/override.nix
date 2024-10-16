pkgs: attrs: with pkgs; with python310.pkgs; with attrs; rec {
  pname = "grpcio";
  version = "1.54.2";

  src = fetchPypi {
    inherit pname version;
    hash = "sha256-UKnwde7aUJeqmhgrs4d/4ScodeRTcDaKwO4Wq54i0Bk=";
  };

  prePatch = "";

  # pypi package specifies the hardcoded
  # dependencies for third-party libraries:
  # /usr/include. In result in nix build we use
  # system headers instead of nix headers.
  # This patch removes dependencies on /usr/include
  patches = [ ./setup.patch ];
}
