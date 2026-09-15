pkgs: attrs: with pkgs; with python310.pkgs; with attrs; rec {
  pname = "grpcio";
  version = "1.63.2";

  src = fetchPypi {
    inherit pname version;
    hash = "sha256-jcz5d3tPIcrQqMhJFq6bvd58zRDv3sfZXzGAWkGAEGQ=";
  };

  prePatch = "";

  # pypi package specifies the hardcoded
  # dependencies for third-party libraries:
  # /usr/include. In result in nix build we use
  # system headers instead of nix headers.
  # This patch removes dependencies on /usr/include
  patches = [ ./setup.patch ];
}
