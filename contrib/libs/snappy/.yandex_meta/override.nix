pkgs: attrs: with pkgs; with attrs; rec {
  version = "1.2.1";

  src = fetchFromGitHub {
    owner = "google";
    repo = "snappy";
    rev = version;
    hash = "sha256-IzKzrMDjh+Weor+OrKdX62cAKYTdDXgldxCgNE2/8vk=";
  };

  patches = [];
}
