pkgs: attrs: with pkgs; with attrs; rec {
  version = "9.3.1";

  src = fetchFromGitHub {
    owner = "nodejs";
    repo = "llhttp";
    rev = "release/v${version}";
    hash = "sha256-eHy8sjmfLA+q1WWuo4bkZ0wRI4q9fkNaW8c2OgKv/MM=";
  };

  patches = [];

  cmakeFlags = [
    "-DBUILD_STATIC_LIBS=OFF"
  ];
}
