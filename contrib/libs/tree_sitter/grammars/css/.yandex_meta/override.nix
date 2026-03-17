self: super: with self; rec {
  pname = "tree-sitter-css";
  version = "0.20.0";

  src = fetchFromGitHub {
    owner = "tree-sitter";
    repo = "${pname}";
    rev = "v${version}";
    hash = "sha256-+30AJq3L30QmLXvTnePGW39crd7mLBUJ+sGsF7Wd9qI=";
  };
}
