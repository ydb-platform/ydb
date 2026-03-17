pkgs: attrs: with pkgs; rec {
  version = "8.0.17";

  src = fetchFromGitHub {
    owner = "mysql";
    repo = "mysql-server";
    rev = "mysql-cluster-${version}";
    sha256 = "sha256-JInQhS9i/uR/vNb81JDhQr4sEtqZskW/0ux0y8+PDF8=";
  };

  patches = [];
}
