self: super: with self; rec {
  pname = "tree-sitter-proto";
  version = "e9f6b43f6844bd2189b50a422d4e2094313f6aa3";

  src = fetchFromGitHub {
    owner = "treywood";
    repo = pname;
    rev = "e9f6b43f6844bd2189b50a422d4e2094313f6aa3";
    hash = "sha256-Ue6w6HWy+NTJt+AKTFfJIUf3HXHTwkUkDk4UdDMSD+U=";
  };
}
