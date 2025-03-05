pkgs: attrs: with pkgs; with python310.pkgs; with attrs; rec {
  pname = "grpcio";
  version = "1.54.3";

  src = fetchPypi {
    inherit pname version;
    hash = "sha256-ipuc8BEDeVBy9IdLSwZMXjeFhx1it3Txql+brKu8rCA=";
  };

  prePatch = "";

  # pypi package specifies the hardcoded
  # dependencies for third-party libraries:
  # /usr/include. In result in nix build we use
  # system headers instead of nix headers.
  # This patch removes dependencies on /usr/include
  patches = [ ./setup.patch ];
}
