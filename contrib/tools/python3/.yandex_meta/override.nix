pkgs: attrs: with pkgs; with attrs; rec {
  version = "3.12.9";

  src = fetchFromGitHub {
    owner = "python";
    repo = "cpython";
    rev = "v${version}";
    hash = "sha256-5Hl+kKhavxihPmxVOyzUpchxiYMfYRfcjTbjiIq1i1o=";
  };

  patches = [];
  postPatch = "";
}
