pkgs: attrs: with attrs; with pkgs.python.pkgs; rec {
  version = "1.0.8";

  src = fetchPypi {
    inherit pname version;
    hash = "sha256-cJiLqyY2lkVvddPYkQx1rqR3qdCAVK1++FvlRw3T9ls=";
  };
}
