pkgs: attrs: with pkgs; with attrs; rec {
  version = "3.13.13";

  src = fetchFromGitHub {
    owner = "python";
    repo = "cpython";
    rev = "v${version}";
    hash = "sha256-JUA430BZJ4cMS/0XLuuYsU85jPkDJLXtl+Jccdlm+fk=";
  };

  patches = [];
  postPatch = "";
}
